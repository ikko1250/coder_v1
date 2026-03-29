//! 条件 JSON エディタ（ビューポート／埋め込み）。親モジュール `app` の子として `App` の非公開状態にアクセスする。
//!
//! **境界（P1-06）**
//! - 本モジュール: `ConditionEditorState`、読み書き・確認フロー、egui ウィンドウ／パネル。
//! - [`crate::condition_editor_view`]: リスト・詳細・フッター等の純粋 UI 部品。

const VIEWPORT_ID: &str = "condition_editor_viewport";

use super::app_analysis_job;
use super::App;
use crate::condition_editor::{
    build_default_condition_item, load_condition_document, save_condition_document_atomic,
    ConditionDocumentLoadInfo, ConditionEditorItem, FilterConfigDocument,
};
use crate::condition_editor_filter::{
    build_condition_list_filter_options, condition_matches_list_filters,
    normalize_condition_list_filter_search_text, ConditionListFilterColumn,
};
use crate::condition_editor_view::{
    draw_condition_editor_confirm_overlay as render_condition_editor_confirm_overlay,
    draw_condition_editor_footer_panel as render_condition_editor_footer_panel,
    draw_condition_editor_global_settings as render_condition_editor_global_settings,
    draw_condition_editor_header_panel as render_condition_editor_header_panel,
    draw_condition_editor_list_panel as render_condition_editor_list_panel,
    draw_condition_editor_skip_warning_overlay as render_condition_editor_skip_warning_overlay,
    draw_condition_editor_selected_condition as render_condition_editor_selected_condition,
    ConditionEditorDetailResponse, ConditionEditorFooterResponse, ConditionEditorHeaderResponse,
    ConditionEditorListResponse, ConfirmOverlayResponse, SkipWarningOverlayResponse,
};
use crate::model::FilterOption;
use eframe::egui;
use egui::{Color32, RichText, ScrollArea, Ui};
use egui_extras::{Size, StripBuilder};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;

const CONDITION_EDITOR_LIST_PANEL_WIDTH: f32 = 560.0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ConditionEditorConfirmAction {
    CloseWindow,
    ReloadPath(PathBuf),
    OpenPickedPath(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConditionEditorModalResponse {
    Continue,
    Cancel,
}

#[derive(Clone, Debug, Default)]
struct ConditionEditorSkipWarning {
    target_indices: Vec<usize>,
    target_condition_ids: Vec<String>,
    immediate_referrer_ids: Vec<String>,
    cascade_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
struct ConditionEditorSelectionDraft {
    requested_selection: Option<usize>,
    requested_group_selection: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ConditionEditorCommandDraft {
    should_save: bool,
    should_reload: bool,
    should_add_condition: bool,
    should_delete_condition: Option<usize>,
    close_requested: bool,
    confirm_modal_response: Option<ConditionEditorModalResponse>,
    skip_warning_response: Option<SkipWarningOverlayResponse>,
    select_clicked: bool,
}

#[derive(Clone, Debug)]
struct ConditionEditorWindowInputs {
    can_modify: bool,
    resolved_path_result: Result<PathBuf, String>,
    resolved_path_label: String,
    loaded_path_label: String,
    current_confirm_action: Option<ConditionEditorConfirmAction>,
    current_skip_warning: Option<ConditionEditorSkipWarning>,
    panel_fill: Color32,
}

#[derive(Clone, Debug, Default)]
struct ConditionEditorListPanelFilterInputs {
    active_filter_column: ConditionListFilterColumn,
    active_filter_count: usize,
    filtered_indices: Vec<usize>,
    total_count: usize,
    matching_options: Vec<FilterOption>,
    selected_non_matching_options: Vec<FilterOption>,
    active_values: Vec<(ConditionListFilterColumn, String)>,
    candidate_query: String,
    has_any_options: bool,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ConditionEditorState {
    pub(super) window_open: bool,
    /// P2-09: 条件 JSON を最後に読み込んだときの `core.data_source_generation`。
    data_source_generation_at_load: Option<u64>,
    loaded_path: Option<PathBuf>,
    pending_path_sync: Option<PathBuf>,
    document: Option<FilterConfigDocument>,
    selected_index: Option<usize>,
    selected_group_index: Option<usize>,
    projected_legacy_condition_count: usize,
    list_active_filter_column: ConditionListFilterColumn,
    list_selected_filter_values: HashMap<ConditionListFilterColumn, BTreeSet<String>>,
    list_filter_candidate_queries: HashMap<ConditionListFilterColumn, String>,
    pub(super) status_message: Option<String>,
    pub(super) status_is_error: bool,
    pub(super) is_dirty: bool,
    confirm_action: Option<ConditionEditorConfirmAction>,
    pending_skip_warning: Option<ConditionEditorSkipWarning>,
}

pub(super) fn focus_condition_editor_viewport(_app: &App, ctx: &egui::Context) {
    let viewport_id = egui::ViewportId::from_hash_of(VIEWPORT_ID);
    ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(false));
    ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Focus);
}

pub(super) fn open_condition_editor(app: &mut App, ctx: &egui::Context) -> Result<(), String> {
    if app.condition_editor_state.window_open {
        app.condition_editor_state.status_message =
            Some("condition editor は既に開いています。".to_string());
        app.condition_editor_state.status_is_error = false;
        focus_condition_editor_viewport(app, ctx);
        return Ok(());
    }
    let path = app_analysis_job::resolved_filter_config_path(app)?;
    load_condition_editor_from_path(app, path, "条件 JSON を読み込みました。")
}

/// `load_condition_document` 済みの結果をエディター状態へ一括反映する（二重 I/O 回避用）。
fn apply_condition_editor_loaded_bundle(
    app: &mut App,
    path: PathBuf,
    document: FilterConfigDocument,
    load_info: ConditionDocumentLoadInfo,
    status_message: &str,
) {
    let projected_count = load_info.projected_legacy_condition_count;
    let mut final_status_message = status_message.to_string();
    if projected_count > 0 {
        final_status_message.push_str(&format!(
            " legacy 条件 {} 件を group editor 用に投影しました。",
            projected_count
        ));
    }
    app.condition_editor_state.window_open = true;
    app.condition_editor_state.data_source_generation_at_load = Some(app.core.data_source_generation);
    app.condition_editor_state.loaded_path = Some(path);
    app.condition_editor_state.pending_path_sync = None;
    app.condition_editor_state.document = Some(document);
    app.condition_editor_state.selected_index = clamp_condition_editor_selection(app, Some(0));
    app.condition_editor_state.selected_group_index = clamp_condition_editor_group_selection(
        app,
        Some(0),
        app.condition_editor_state.selected_index,
    );
    app.condition_editor_state.projected_legacy_condition_count = projected_count;
    app.condition_editor_state.list_active_filter_column = ConditionListFilterColumn::default();
    app.condition_editor_state.list_selected_filter_values.clear();
    app.condition_editor_state.list_filter_candidate_queries.clear();
    app.condition_editor_state.status_message = Some(final_status_message);
    app.condition_editor_state.status_is_error = false;
    app.condition_editor_state.is_dirty = false;
    app.condition_editor_state.confirm_action = None;
    app.condition_editor_state.pending_skip_warning = None;
}

fn load_condition_editor_from_path(
    app: &mut App,
    path: PathBuf,
    status_message: &str,
) -> Result<(), String> {
    let (document, load_info) = load_condition_document(&path)?;
    apply_condition_editor_loaded_bundle(app, path, document, load_info, status_message);
    Ok(())
}

/// ファイルダイアログで選んだパスをコミットする（読込検証 → editor → override → refresh。失敗時は T0-1／§4.7）。
fn commit_picked_condition_json_path(
    app: &mut App,
    ctx: &egui::Context,
    path: PathBuf,
) -> Result<(), String> {
    if app.condition_editor_state.loaded_path.as_ref() == Some(&path) {
        app.condition_editor_state.status_message =
            Some("既に開いているファイルです。".to_string());
        app.condition_editor_state.status_is_error = false;
        return Ok(());
    }

    let Some(runtime_snap) = app.capture_idle_runtime_snapshot() else {
        return Err("分析ジョブ実行中は条件 JSON を切り替えられません。".to_string());
    };

    let prev_override = app.analysis_request_state.filter_config_path_override.clone();
    let prev_editor_path = app.condition_editor_state.loaded_path.clone();

    let (document, load_info) = load_condition_document(&path)?;

    apply_condition_editor_loaded_bundle(
        app,
        path.clone(),
        document,
        load_info,
        "条件 JSON を読み込みました（ファイルから選択）。",
    );

    app.analysis_request_state.filter_config_path_override = Some(path);
    app.refresh_analysis_runtime();

    if app.analysis_runtime_state.runtime.is_none() {
        let fail_summary = app.analysis_runtime_state.status_text();
        app.analysis_request_state.filter_config_path_override = prev_override;
        app.apply_idle_runtime_snapshot(runtime_snap);
        if let Some(rp) = prev_editor_path {
            if let Err(restore_err) = load_condition_editor_from_path(
                app,
                rp,
                "分析ランタイムの更新に失敗したため、表示を元に戻しました。",
            ) {
                return Err(format!(
                    "{fail_summary} さらにエディターの復元に失敗: {restore_err}"
                ));
            }
        }
        return Err(fail_summary);
    }

    ctx.request_repaint();
    Ok(())
}

fn handle_condition_editor_json_select_click(app: &mut App, ctx: &egui::Context) {
    if app.analysis_runtime_state.current_job.is_some() {
        return;
    }
    let Some(picked) = app.file_dialog_host.pick_open_json() else {
        return;
    };
    if app.condition_editor_state.is_dirty {
        app.condition_editor_state.confirm_action =
            Some(ConditionEditorConfirmAction::OpenPickedPath(picked));
        return;
    }
    if let Err(error) = commit_picked_condition_json_path(app, ctx, picked) {
        app.condition_editor_state.status_message = Some(error);
        app.condition_editor_state.status_is_error = true;
    }
}

fn clamp_condition_editor_selection(
    app: &App,
    selected_index: Option<usize>,
) -> Option<usize> {
    let Some(document) = app.condition_editor_state.document.as_ref() else {
        return None;
    };
    match (selected_index, document.cooccurrence_conditions.len()) {
        (_, 0) => None,
        (Some(index), len) => Some(index.min(len - 1)),
        (None, len) => Some(len - 1),
    }
}

fn clamp_condition_editor_group_selection(
    app: &App,
    selected_group_index: Option<usize>,
    condition_index: Option<usize>,
) -> Option<usize> {
    let Some(document) = app.condition_editor_state.document.as_ref() else {
        return None;
    };
    let Some(condition_index) = condition_index else {
        return None;
    };
    let Some(condition) = document.cooccurrence_conditions.get(condition_index) else {
        return None;
    };
    match (selected_group_index, condition.form_groups.len()) {
        (_, 0) => None,
        (Some(index), len) => Some(index.min(len - 1)),
        (None, len) => Some(len - 1),
    }
}

fn mark_condition_editor_dirty(app: &mut App) {
    app.condition_editor_state.is_dirty = true;
    app.condition_editor_state.status_message = Some("未保存の変更があります。".to_string());
    app.condition_editor_state.status_is_error = false;
}

fn condition_reference_ids(condition: &ConditionEditorItem) -> impl Iterator<Item = &str> + '_ {
    condition
        .required_condition_ids_all
        .iter()
        .chain(condition.required_condition_ids_any.iter())
        .chain(condition.excluded_condition_ids_any.iter())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
}

fn build_condition_reverse_reference_map(
    document: &FilterConfigDocument,
) -> HashMap<String, Vec<usize>> {
    let mut reverse_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (condition_index, condition) in document.cooccurrence_conditions.iter().enumerate() {
        for referenced_condition_id in condition_reference_ids(condition) {
            reverse_map
                .entry(referenced_condition_id.to_string())
                .or_default()
                .push(condition_index);
        }
    }
    reverse_map
}

fn build_condition_skip_warning(
    document: &FilterConfigDocument,
    target_indices: &[usize],
) -> Option<ConditionEditorSkipWarning> {
    let reverse_reference_map = build_condition_reverse_reference_map(document);
    let mut target_indices_set = BTreeSet::new();
    let mut target_condition_ids = Vec::new();
    let mut immediate_referrer_ids = BTreeSet::new();
    let mut cascade_indices = BTreeSet::new();
    let mut queued_condition_ids = BTreeSet::new();
    let mut queue = VecDeque::new();

    for &target_index in target_indices {
        let Some(condition) = document.cooccurrence_conditions.get(target_index) else {
            continue;
        };
        target_indices_set.insert(target_index);
        cascade_indices.insert(target_index);
        let condition_id = condition.condition_id.trim();
        if condition_id.is_empty() {
            continue;
        }
        target_condition_ids.push(condition_id.to_string());
        if queued_condition_ids.insert(condition_id.to_string()) {
            queue.push_back(condition_id.to_string());
        }
        if let Some(referrer_indices) = reverse_reference_map.get(condition_id) {
            for &referrer_index in referrer_indices {
                if target_indices_set.contains(&referrer_index) {
                    continue;
                }
                if let Some(referrer) = document.cooccurrence_conditions.get(referrer_index) {
                    let referrer_id = referrer.condition_id.trim();
                    if !referrer_id.is_empty() {
                        immediate_referrer_ids.insert(referrer_id.to_string());
                    }
                }
            }
        }
    }

    if immediate_referrer_ids.is_empty() {
        return None;
    }

    while let Some(condition_id) = queue.pop_front() {
        let Some(referrer_indices) = reverse_reference_map.get(&condition_id) else {
            continue;
        };
        for &referrer_index in referrer_indices {
            if cascade_indices.insert(referrer_index) {
                if let Some(referrer) = document.cooccurrence_conditions.get(referrer_index) {
                    let referrer_id = referrer.condition_id.trim();
                    if !referrer_id.is_empty()
                        && queued_condition_ids.insert(referrer_id.to_string())
                    {
                        queue.push_back(referrer_id.to_string());
                    }
                }
            }
        }
    }

    Some(ConditionEditorSkipWarning {
        target_indices: target_indices_set.into_iter().collect(),
        target_condition_ids,
        immediate_referrer_ids: immediate_referrer_ids.into_iter().collect(),
        cascade_indices: cascade_indices.into_iter().collect(),
    })
}

fn apply_condition_skip_change(
    app: &mut App,
    condition_indices: &[usize],
    skip: bool,
    status_message: String,
) {
    let mut changed = false;
    if let Some(document) = app.condition_editor_state.document.as_mut() {
        for &condition_index in condition_indices {
            let Some(condition) = document.cooccurrence_conditions.get_mut(condition_index) else {
                continue;
            };
            if condition.skip != skip {
                condition.skip = skip;
                changed = true;
            }
        }
    }
    if changed {
        mark_condition_editor_dirty(app);
        app.condition_editor_state.status_message = Some(status_message);
        app.condition_editor_state.status_is_error = false;
    }
}

fn request_condition_skip_toggle(app: &mut App, condition_index: usize, skip: bool) {
    if !skip {
        apply_condition_skip_change(
            app,
            &[condition_index],
            false,
            "condition のスキップを解除しました。".to_string(),
        );
        app.condition_editor_state.pending_skip_warning = None;
        return;
    }

    let warning = app
        .condition_editor_state
        .document
        .as_ref()
        .and_then(|document| build_condition_skip_warning(document, &[condition_index]));
    if let Some(warning) = warning {
        app.condition_editor_state.pending_skip_warning = Some(warning);
        return;
    }

    apply_condition_skip_change(
        app,
        &[condition_index],
        true,
        "condition をスキップ対象にしました。".to_string(),
    );
}

fn condition_editor_selection_draft(app: &App) -> ConditionEditorSelectionDraft {
    ConditionEditorSelectionDraft {
        requested_selection: app.condition_editor_state.selected_index,
        requested_group_selection: app.condition_editor_state.selected_group_index,
    }
}

fn condition_editor_window_inputs(app: &App, ctx: &egui::Context) -> ConditionEditorWindowInputs {
    let resolved_path_result = app_analysis_job::resolved_filter_config_path(app);
    let resolved_path_label = resolved_path_result
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|error| format!("解決失敗: {error}"));
    let loaded_path_label = app
        .condition_editor_state
        .loaded_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "-".to_string());

    ConditionEditorWindowInputs {
        can_modify: app.analysis_runtime_state.current_job.is_none(),
        resolved_path_result,
        resolved_path_label,
        loaded_path_label,
        current_confirm_action: app.condition_editor_state.confirm_action.clone(),
        current_skip_warning: app.condition_editor_state.pending_skip_warning.clone(),
        panel_fill: ctx.style().visuals.panel_fill,
    }
}

fn apply_condition_editor_selection_draft(
    app: &mut App,
    selection_draft: ConditionEditorSelectionDraft,
) {
    app.condition_editor_state.selected_index =
        clamp_condition_editor_selection(app, selection_draft.requested_selection);
    app.condition_editor_state.selected_group_index = clamp_condition_editor_group_selection(
        app,
        selection_draft.requested_group_selection,
        app.condition_editor_state.selected_index,
    );
}

fn reload_condition_editor(app: &mut App, path: PathBuf) -> Result<(), String> {
    load_condition_editor_from_path(app, path, "条件 JSON を再読込しました。")
}

fn request_condition_editor_reload(app: &mut App, path: PathBuf) -> Result<(), String> {
    if app.condition_editor_state.is_dirty {
        app.condition_editor_state.confirm_action =
            Some(ConditionEditorConfirmAction::ReloadPath(path));
        return Ok(());
    }
    reload_condition_editor(app, path)
}

fn save_condition_editor_document(app: &mut App) -> Result<(), String> {
    let path = app
        .condition_editor_state
        .loaded_path
        .clone()
        .ok_or_else(|| "保存先の条件 JSON パスが未設定です".to_string())?;
    let document = app
        .condition_editor_state
        .document
        .as_ref()
        .ok_or_else(|| "保存対象の条件 JSON が読み込まれていません".to_string())?;
    save_condition_document_atomic(&path, document)?;
    load_condition_editor_from_path(app, path.clone(), "条件 JSON を保存しました。")?;
    app.condition_editor_state.status_message =
        Some(format!("条件 JSON を保存しました: {}", path.display()));
    app.condition_editor_state.status_is_error = false;
    Ok(())
}

pub(super) fn sync_condition_editor_with_runtime_path(app: &mut App) {
    if !app.condition_editor_state.window_open {
        return;
    }

    let Ok(resolved_path) = app_analysis_job::resolved_filter_config_path(app) else {
        return;
    };
    let Some(loaded_path) = app.condition_editor_state.loaded_path.clone() else {
        return;
    };
    if resolved_path == loaded_path {
        app.condition_editor_state.pending_path_sync = None;
        return;
    }

    if app.condition_editor_state.is_dirty {
        app.condition_editor_state.pending_path_sync = Some(resolved_path.clone());
        app.condition_editor_state.status_message = Some(format!(
            "分析設定で条件 JSON の解決先が変更されました。再読込が必要です: {}",
            resolved_path.display()
        ));
        app.condition_editor_state.status_is_error = true;
        return;
    }

    match reload_condition_editor(app, resolved_path.clone()) {
        Ok(()) => {
            app.condition_editor_state.pending_path_sync = None;
            app.condition_editor_state.status_message = Some(format!(
                "分析設定の変更に合わせて条件 JSON を再読込しました: {}",
                resolved_path.display()
            ));
            app.condition_editor_state.status_is_error = false;
        }
        Err(error) => {
            app.condition_editor_state.pending_path_sync = Some(resolved_path);
            app.condition_editor_state.status_message = Some(error);
            app.condition_editor_state.status_is_error = true;
        }
    }
}

fn draw_condition_editor_body_panel(
    app: &mut App,
    ui: &mut Ui,
    can_modify: bool,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
) {
    let panel_fill = ui.style().visuals.panel_fill;
    sync_condition_editor_selection_with_filters(app);
    selection_draft.requested_selection = app.condition_editor_state.selected_index;
    selection_draft.requested_group_selection = app.condition_editor_state.selected_group_index;
    let list_filter_inputs = build_condition_editor_list_panel_filter_inputs(app);
    StripBuilder::new(ui)
        .size(Size::exact(CONDITION_EDITOR_LIST_PANEL_WIDTH))
        .size(Size::remainder())
        .horizontal(|mut strip| {
            strip.cell(|ui| {
                let list_response = render_condition_editor_list_panel(
                    ui,
                    can_modify,
                    app.condition_editor_state.document.as_ref(),
                    selection_draft.requested_selection,
                    &list_filter_inputs.filtered_indices,
                    list_filter_inputs.total_count,
                    list_filter_inputs.active_filter_column,
                    list_filter_inputs.active_filter_count,
                    &list_filter_inputs.matching_options,
                    &list_filter_inputs.selected_non_matching_options,
                    app.condition_editor_state
                        .list_selected_filter_values
                        .get(&list_filter_inputs.active_filter_column),
                    &list_filter_inputs.active_values,
                    &list_filter_inputs.candidate_query,
                    list_filter_inputs.has_any_options,
                );
                apply_condition_editor_list_response(
                    app,
                    selection_draft,
                    command_draft,
                    list_response,
                );
            });

            strip.cell(|ui| {
                draw_condition_editor_detail_panel(
                    app,
                    ui,
                    panel_fill,
                    can_modify,
                    selection_draft,
                    command_draft,
                );
            });
        });
}

fn build_condition_editor_list_panel_filter_inputs(
    app: &App,
) -> ConditionEditorListPanelFilterInputs {
    let Some(document) = app.condition_editor_state.document.as_ref() else {
        return ConditionEditorListPanelFilterInputs::default();
    };

    let filtered_indices = condition_editor_filtered_indices(app);
    let active_filter_column = app.condition_editor_state.list_active_filter_column;
    let filter_options = build_condition_list_filter_options(&document.cooccurrence_conditions);
    let options = filter_options
        .get(&active_filter_column)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let selected_values = app
        .condition_editor_state
        .list_selected_filter_values
        .get(&active_filter_column);
    let candidate_query = app
        .condition_editor_state
        .list_filter_candidate_queries
        .get(&active_filter_column)
        .cloned()
        .unwrap_or_default();
    let normalized_query = normalize_condition_list_filter_search_text(&candidate_query);

    let mut matching_options = Vec::new();
    let mut selected_non_matching_options = Vec::new();
    for option in options {
        let is_selected = selected_values.is_some_and(|values| values.contains(&option.value));
        let matches_query = normalized_query.is_empty()
            || normalize_condition_list_filter_search_text(&option.value)
                .contains(&normalized_query);

        if matches_query {
            matching_options.push(option.clone());
        } else if is_selected {
            selected_non_matching_options.push(option.clone());
        }
    }

    let active_values = app
        .condition_editor_state
        .list_selected_filter_values
        .iter()
        .flat_map(|(column, values)| {
            values
                .iter()
                .cloned()
                .map(|value| (*column, value))
                .collect::<Vec<_>>()
        })
        .collect();

    ConditionEditorListPanelFilterInputs {
        active_filter_column,
        active_filter_count: app
            .condition_editor_state
            .list_selected_filter_values
            .values()
            .map(BTreeSet::len)
            .sum(),
        filtered_indices,
        total_count: document.cooccurrence_conditions.len(),
        matching_options,
        selected_non_matching_options,
        active_values,
        candidate_query,
        has_any_options: !options.is_empty(),
    }
}

fn condition_editor_filtered_indices(app: &App) -> Vec<usize> {
    let Some(document) = app.condition_editor_state.document.as_ref() else {
        return Vec::new();
    };

    document
        .cooccurrence_conditions
        .iter()
        .enumerate()
        .filter_map(|(index, condition)| {
            condition_matches_list_filters(
                condition,
                &app.condition_editor_state.list_selected_filter_values,
            )
            .then_some(index)
        })
        .collect()
}

fn sync_condition_editor_selection_with_filters(app: &mut App) {
    let filtered_indices = condition_editor_filtered_indices(app);
    let current_selection = app.condition_editor_state.selected_index;
    let current_group_selection = app.condition_editor_state.selected_group_index;
    let next_selection = match (current_selection, filtered_indices.first()) {
        (_, None) => None,
        (Some(selected_index), Some(_))
            if filtered_indices.iter().any(|index| *index == selected_index) =>
        {
            Some(selected_index)
        }
        (_, Some(first_visible)) => Some(*first_visible),
    };

    let next_group_selection =
        clamp_condition_editor_group_selection(app, current_group_selection, next_selection);
    app.condition_editor_state.selected_index = next_selection;
    app.condition_editor_state.selected_group_index = next_group_selection;
}

fn draw_condition_editor_detail_panel(
    app: &mut App,
    ui: &mut Ui,
    panel_fill: Color32,
    can_modify: bool,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
) {
    egui::Frame::default()
        .fill(panel_fill)
        .inner_margin(egui::Margin {
            left: 24,
            right: 24,
            top: 10,
            bottom: 10,
        })
        .show(ui, |ui| {
            ScrollArea::vertical()
                .id_salt("condition_editor_detail_scroll")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if draw_condition_editor_detail_contents(
                        app,
                        ui,
                        can_modify,
                        selection_draft,
                        command_draft,
                    ) {
                        mark_condition_editor_dirty(app);
                    }
                });
        });
}

fn draw_condition_editor_detail_contents(
    app: &mut App,
    ui: &mut Ui,
    can_modify: bool,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
) -> bool {
    let Some(document) = app.condition_editor_state.document.as_mut() else {
        ui.label(RichText::new("条件 JSON 未読込").italics());
        return false;
    };

    let mut changed = false;
    selection_draft.requested_selection = clamp_condition_index(
        selection_draft.requested_selection,
        document.cooccurrence_conditions.len(),
    );
    selection_draft.requested_group_selection = clamp_condition_group_selection_for_document(
        document,
        selection_draft.requested_selection,
        selection_draft.requested_group_selection,
    );

    changed |= render_condition_editor_global_settings(ui, document);

    let Some(selected_index) = selection_draft.requested_selection else {
        ui.label(RichText::new("condition を選択してください").italics());
        return changed;
    };
    let Some(condition) = document.cooccurrence_conditions.get_mut(selected_index) else {
        ui.label(RichText::new("condition を選択してください").italics());
        return changed;
    };

    let detail_response = render_condition_editor_selected_condition(
        ui,
        can_modify,
        selection_draft.requested_group_selection,
        condition,
    );
    apply_condition_editor_detail_response(
        selected_index,
        selection_draft,
        command_draft,
        detail_response,
    );
    changed |= detail_response.changed;

    changed
}

fn condition_editor_status_message(app: &App) -> Option<(&str, bool)> {
    app.condition_editor_state
        .status_message
        .as_deref()
        .map(|message| (message, app.condition_editor_state.status_is_error))
}

fn condition_editor_data_source_stale(app: &App) -> bool {
    match app.condition_editor_state.data_source_generation_at_load {
        Some(snapshot) => snapshot != app.core.data_source_generation,
        None => false,
    }
}

fn condition_editor_save_enabled(app: &App, can_modify: bool, resolved_path_ok: bool) -> bool {
    can_modify
        && app.condition_editor_state.document.is_some()
        && app.condition_editor_state.pending_path_sync.is_none()
        && resolved_path_ok
}

fn condition_editor_confirm_message(confirm_action: &ConditionEditorConfirmAction) -> String {
    match confirm_action {
        ConditionEditorConfirmAction::CloseWindow => {
            "未保存の変更があります。condition editor を閉じますか。".to_string()
        }
        ConditionEditorConfirmAction::ReloadPath(path) => format!(
            "未保存の変更があります。次の条件 JSON を再読込すると変更は破棄されます。\n{}",
            path.display()
        ),
        ConditionEditorConfirmAction::OpenPickedPath(path) => format!(
            "未保存の変更があります。次の条件 JSON を開くと変更は破棄されます。\n{}",
            path.display()
        ),
    }
}

fn condition_editor_skip_warning_message(skip_warning: &ConditionEditorSkipWarning) -> String {
    let target_ids = skip_warning.target_condition_ids.join(", ");
    let referrer_ids = skip_warning.immediate_referrer_ids.join(", ");
    let cascade_only_count = skip_warning
        .cascade_indices
        .len()
        .saturating_sub(skip_warning.target_indices.len());

    if cascade_only_count > 0 {
        format!(
            "condition_id [{target_ids}] は他の condition_id から参照されています。\n参照元: {referrer_ids}\n無視して選択中の condition だけをスキップするか、参照元連鎖を含む {} 件を一括スキップするか選択してください。",
            skip_warning.cascade_indices.len()
        )
    } else {
        format!(
            "condition_id [{target_ids}] は他の condition_id から参照されています。\n参照元: {referrer_ids}\n無視して選択中の condition だけをスキップするか、参照元も一括スキップするか選択してください。"
        )
    }
}

fn draw_condition_editor_embedded_window(
    app: &mut App,
    viewport_ctx: &egui::Context,
    can_modify: bool,
    loaded_path_label: &str,
    resolved_path_label: &str,
    resolved_path_ok: bool,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
) {
    let mut fallback_open = true;
    egui::Window::new("条件編集")
        .open(&mut fallback_open)
        .default_width(1120.0)
        .default_height(760.0)
        .resizable(true)
        .show(viewport_ctx, |ui| {
            let header_response = render_condition_editor_header_panel(
                ui,
                can_modify,
                loaded_path_label,
                resolved_path_label,
                app.condition_editor_state.pending_path_sync.as_deref(),
                condition_editor_status_message(app),
                app.condition_editor_state.projected_legacy_condition_count,
                condition_editor_data_source_stale(app),
            );
            apply_condition_editor_header_response(command_draft, header_response);
            ui.separator();
            draw_condition_editor_body_panel(
                app,
                ui,
                can_modify,
                selection_draft,
                command_draft,
            );
            ui.separator();
            let footer_response = render_condition_editor_footer_panel(
                ui,
                condition_editor_save_enabled(app, can_modify, resolved_path_ok),
                can_modify && resolved_path_ok,
                app.condition_editor_state.is_dirty,
            );
            apply_condition_editor_footer_response(command_draft, footer_response);
        });
    if !fallback_open {
        command_draft.close_requested = true;
    }
}

fn draw_condition_editor_viewport_panels(
    app: &mut App,
    viewport_ctx: &egui::Context,
    can_modify: bool,
    loaded_path_label: &str,
    resolved_path_label: &str,
    resolved_path_ok: bool,
    panel_fill: Color32,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
) {
    egui::TopBottomPanel::top("condition_editor_viewport_header")
        .frame(
            egui::Frame::default()
                .fill(panel_fill)
                .inner_margin(egui::Margin::same(10)),
        )
        .show(viewport_ctx, |ui| {
            let header_response = render_condition_editor_header_panel(
                ui,
                can_modify,
                loaded_path_label,
                resolved_path_label,
                app.condition_editor_state.pending_path_sync.as_deref(),
                condition_editor_status_message(app),
                app.condition_editor_state.projected_legacy_condition_count,
                condition_editor_data_source_stale(app),
            );
            apply_condition_editor_header_response(command_draft, header_response);
        });

    egui::TopBottomPanel::bottom("condition_editor_viewport_footer")
        .frame(
            egui::Frame::default()
                .fill(panel_fill)
                .inner_margin(egui::Margin::same(10)),
        )
        .show(viewport_ctx, |ui| {
            let footer_response = render_condition_editor_footer_panel(
                ui,
                condition_editor_save_enabled(app, can_modify, resolved_path_ok),
                can_modify && resolved_path_ok,
                app.condition_editor_state.is_dirty,
            );
            apply_condition_editor_footer_response(command_draft, footer_response);
        });

    egui::CentralPanel::default()
        .frame(
            egui::Frame::default()
                .fill(panel_fill)
                .inner_margin(egui::Margin::same(10)),
        )
        .show(viewport_ctx, |ui| {
            draw_condition_editor_body_panel(
                app,
                ui,
                can_modify,
                selection_draft,
                command_draft,
            );
        });
}

fn apply_condition_editor_close_request(app: &mut App, close_requested: bool) {
    if !close_requested {
        return;
    }

    if app.condition_editor_state.is_dirty {
        app.condition_editor_state.confirm_action = Some(ConditionEditorConfirmAction::CloseWindow);
    } else {
        app.condition_editor_state.window_open = false;
    }
}

fn apply_condition_editor_footer_response(
    command_draft: &mut ConditionEditorCommandDraft,
    footer_response: ConditionEditorFooterResponse,
) {
    if footer_response.save_clicked {
        command_draft.should_save = true;
    }
    if footer_response.reload_clicked {
        command_draft.should_reload = true;
    }
}

fn apply_condition_editor_header_response(
    command_draft: &mut ConditionEditorCommandDraft,
    header_response: ConditionEditorHeaderResponse,
) {
    if header_response.select_clicked {
        command_draft.select_clicked = true;
    }
}

fn apply_condition_editor_confirm_overlay_response(
    command_draft: &mut ConditionEditorCommandDraft,
    response: Option<ConfirmOverlayResponse>,
) {
    command_draft.confirm_modal_response = match response {
        Some(ConfirmOverlayResponse::Continue) => Some(ConditionEditorModalResponse::Continue),
        Some(ConfirmOverlayResponse::Cancel) => Some(ConditionEditorModalResponse::Cancel),
        None => command_draft.confirm_modal_response,
    };
}

fn apply_condition_editor_skip_warning_overlay_response(
    command_draft: &mut ConditionEditorCommandDraft,
    response: Option<SkipWarningOverlayResponse>,
) {
    command_draft.skip_warning_response = response.or(command_draft.skip_warning_response);
}

fn apply_condition_editor_list_response(
    app: &mut App,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
    list_response: ConditionEditorListResponse,
) {
    if list_response.add_clicked {
        command_draft.should_add_condition = true;
    }
    let response_column = app.condition_editor_state.list_active_filter_column;
    if let Some(updated_query) = list_response.updated_query {
        if updated_query.is_empty() {
            app.condition_editor_state
                .list_filter_candidate_queries
                .remove(&response_column);
        } else {
            app.condition_editor_state
                .list_filter_candidate_queries
                .insert(response_column, updated_query);
        }
    }
    if let Some(selected_column) = list_response.selected_filter_column {
        app.condition_editor_state.list_active_filter_column = selected_column;
    }
    if list_response.clear_column_clicked {
        app.condition_editor_state
            .list_selected_filter_values
            .remove(&app.condition_editor_state.list_active_filter_column);
    }
    if list_response.clear_all_clicked {
        app.condition_editor_state.list_selected_filter_values.clear();
    }
    for (value, selected) in list_response.toggled_filter_options {
        let active_column = app.condition_editor_state.list_active_filter_column;
        let mut should_remove_entry = false;
        {
            let selected_values = app
                .condition_editor_state
                .list_selected_filter_values
                .entry(active_column)
                .or_default();
            if selected {
                selected_values.insert(value);
            } else {
                selected_values.remove(&value);
                should_remove_entry = selected_values.is_empty();
            }
        }
        if should_remove_entry {
            app.condition_editor_state
                .list_selected_filter_values
                .remove(&active_column);
        }
    }
    for (column, value) in list_response.removed_active_filter_values {
        let mut should_remove_entry = false;
        if let Some(selected_values) = app
            .condition_editor_state
            .list_selected_filter_values
            .get_mut(&column)
        {
            selected_values.remove(&value);
            should_remove_entry = selected_values.is_empty();
        }
        if should_remove_entry {
            app.condition_editor_state
                .list_selected_filter_values
                .remove(&column);
        }
    }
    for (condition_index, skip) in list_response.toggled_skip {
        request_condition_skip_toggle(app, condition_index, skip);
    }
    if let Some(selected_index) = list_response.selected_index {
        selection_draft.requested_selection = Some(selected_index);
        selection_draft.requested_group_selection = list_response.selected_group_index;
    } else {
        sync_condition_editor_selection_with_filters(app);
        selection_draft.requested_selection = app.condition_editor_state.selected_index;
        selection_draft.requested_group_selection = app.condition_editor_state.selected_group_index;
    }
}

fn apply_condition_editor_detail_response(
    selected_index: usize,
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
    detail_response: ConditionEditorDetailResponse,
) {
    if detail_response.delete_clicked {
        command_draft.should_delete_condition = Some(selected_index);
    }
    selection_draft.requested_group_selection = detail_response.requested_group_selection;
}

fn apply_condition_editor_add_request(app: &mut App, should_add_condition: bool) {
    if !should_add_condition {
        return;
    }

    let mut new_index = None;
    if let Some(document) = app.condition_editor_state.document.as_mut() {
        document
            .cooccurrence_conditions
            .push(build_default_condition_item());
        new_index = Some(document.cooccurrence_conditions.len().saturating_sub(1));
    }
    if let Some(index) = new_index {
        app.condition_editor_state.selected_index = Some(index);
        app.condition_editor_state.selected_group_index = Some(0);
        mark_condition_editor_dirty(app);
    }
}

fn apply_condition_editor_delete_request(app: &mut App, delete_index: Option<usize>) {
    let Some(delete_index) = delete_index else {
        return;
    };

    if let Some(document) = app.condition_editor_state.document.as_mut() {
        if delete_index < document.cooccurrence_conditions.len() {
            document.cooccurrence_conditions.remove(delete_index);
            app.condition_editor_state.selected_index = clamp_condition_index(
                Some(delete_index),
                document.cooccurrence_conditions.len(),
            );
            app.condition_editor_state.selected_group_index =
                clamp_condition_group_selection_for_document(
                    document,
                    app.condition_editor_state.selected_index,
                    Some(0),
                );
            mark_condition_editor_dirty(app);
            app.condition_editor_state.status_message = Some("condition を削除しました。".to_string());
            app.condition_editor_state.status_is_error = false;
        }
    }
}

fn apply_condition_editor_reload_request(
    app: &mut App,
    should_reload: bool,
    resolved_path_result: &Result<PathBuf, String>,
) -> Option<String> {
    if !should_reload {
        return None;
    }

    match resolved_path_result {
        Ok(path) => request_condition_editor_reload(app, path.clone()).err(),
        Err(error) => Some(error.clone()),
    }
}

fn apply_condition_editor_modal_response(
    app: &mut App,
    ctx: &egui::Context,
    viewport_id: egui::ViewportId,
    confirm_modal_response: Option<ConditionEditorModalResponse>,
) {
    let Some(response) = confirm_modal_response else {
        return;
    };

    match response {
        ConditionEditorModalResponse::Continue => {
            if let Some(confirm_action) = app.condition_editor_state.confirm_action.clone() {
                match confirm_action {
                    ConditionEditorConfirmAction::CloseWindow => {
                        app.condition_editor_state.window_open = false;
                        app.condition_editor_state.confirm_action = None;
                        ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Close);
                    }
                    ConditionEditorConfirmAction::ReloadPath(path) => {
                        if let Err(error) = reload_condition_editor(app, path) {
                            app.condition_editor_state.status_message = Some(error);
                            app.condition_editor_state.status_is_error = true;
                        }
                        app.condition_editor_state.confirm_action = None;
                    }
                    ConditionEditorConfirmAction::OpenPickedPath(path) => {
                        if let Err(error) = commit_picked_condition_json_path(app, ctx, path) {
                            app.condition_editor_state.status_message = Some(error);
                            app.condition_editor_state.status_is_error = true;
                        }
                        app.condition_editor_state.confirm_action = None;
                    }
                }
            }
        }
        ConditionEditorModalResponse::Cancel => {
            app.condition_editor_state.confirm_action = None;
        }
    }
}

fn apply_condition_editor_skip_warning_response(
    app: &mut App,
    skip_warning_response: Option<SkipWarningOverlayResponse>,
) {
    let Some(response) = skip_warning_response else {
        return;
    };

    let Some(skip_warning) = app.condition_editor_state.pending_skip_warning.clone() else {
        return;
    };

    match response {
        SkipWarningOverlayResponse::Ignore => {
            apply_condition_skip_change(
                app,
                &skip_warning.target_indices,
                true,
                "condition をスキップ対象にしました。参照元はそのまま残しています。".to_string(),
            );
        }
        SkipWarningOverlayResponse::Cascade => {
            apply_condition_skip_change(
                app,
                &skip_warning.cascade_indices,
                true,
                format!(
                    "参照元を含む {} 件の condition をスキップ対象にしました。",
                    skip_warning.cascade_indices.len()
                ),
            );
        }
        SkipWarningOverlayResponse::Cancel => {}
    }

    app.condition_editor_state.pending_skip_warning = None;
}

fn apply_condition_editor_command_draft(
    app: &mut App,
    ctx: &egui::Context,
    viewport_id: egui::ViewportId,
    selection_draft: ConditionEditorSelectionDraft,
    command_draft: ConditionEditorCommandDraft,
    resolved_path_result: &Result<PathBuf, String>,
) -> Option<String> {
    apply_condition_editor_selection_draft(app, selection_draft);
    apply_condition_editor_close_request(app, command_draft.close_requested);
    apply_condition_editor_add_request(app, command_draft.should_add_condition);
    apply_condition_editor_delete_request(app, command_draft.should_delete_condition);

    // §12.0: モーダル応答があるフレームでは save / reload / pick と競合させない。
    if command_draft.confirm_modal_response.is_some() {
        apply_condition_editor_modal_response(
            app,
            ctx,
            viewport_id,
            command_draft.confirm_modal_response,
        );
        return None;
    }
    if command_draft.skip_warning_response.is_some() {
        apply_condition_editor_skip_warning_response(app, command_draft.skip_warning_response);
        return None;
    }

    let mut save_error = None;
    if command_draft.should_save {
        if let Err(error) = save_condition_editor_document(app) {
            save_error = Some(error);
        }
    }
    let reload_error = apply_condition_editor_reload_request(
        app,
        command_draft.should_reload,
        resolved_path_result,
    );

    let pick_exclusive =
        command_draft.select_clicked && !command_draft.should_save && !command_draft.should_reload;
    if pick_exclusive {
        handle_condition_editor_json_select_click(app, ctx);
    }

    save_error.or(reload_error)
}

pub(super) fn draw_condition_editor_window(app: &mut App, ctx: &egui::Context) {
    if !app.condition_editor_state.window_open {
        return;
    }

    let viewport_id = egui::ViewportId::from_hash_of(VIEWPORT_ID);
    let builder = egui::ViewportBuilder::default()
        .with_title("条件編集")
        .with_inner_size([1120.0, 760.0])
        .with_resizable(true);
    let window_inputs = condition_editor_window_inputs(app, ctx);
    let mut selection_draft = condition_editor_selection_draft(app);
    let mut command_draft = ConditionEditorCommandDraft::default();

    ctx.show_viewport_immediate(viewport_id, builder, |viewport_ctx, class| {
        command_draft.close_requested =
            viewport_ctx.input(|input| input.viewport().close_requested());
        if command_draft.close_requested && app.condition_editor_state.is_dirty {
            viewport_ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
        }

        match class {
            egui::ViewportClass::Embedded => {
                draw_condition_editor_embedded_window(
                    app,
                    viewport_ctx,
                    window_inputs.can_modify,
                    &window_inputs.loaded_path_label,
                    &window_inputs.resolved_path_label,
                    window_inputs.resolved_path_result.is_ok(),
                    &mut selection_draft,
                    &mut command_draft,
                );
            }
            _ => {
                draw_condition_editor_viewport_panels(
                    app,
                    viewport_ctx,
                    window_inputs.can_modify,
                    &window_inputs.loaded_path_label,
                    &window_inputs.resolved_path_label,
                    window_inputs.resolved_path_result.is_ok(),
                    window_inputs.panel_fill,
                    &mut selection_draft,
                    &mut command_draft,
                );
            }
        }

        if let Some(confirm_action) = window_inputs.current_confirm_action.as_ref() {
            let message = condition_editor_confirm_message(confirm_action);
            let response = render_condition_editor_confirm_overlay(viewport_ctx, &message);
            apply_condition_editor_confirm_overlay_response(&mut command_draft, response);
        } else if let Some(skip_warning) = window_inputs.current_skip_warning.as_ref() {
            let message = condition_editor_skip_warning_message(skip_warning);
            let response = render_condition_editor_skip_warning_overlay(viewport_ctx, &message);
            apply_condition_editor_skip_warning_overlay_response(&mut command_draft, response);
        }
    });

    if let Some(error) = apply_condition_editor_command_draft(
        app,
        ctx,
        viewport_id,
        selection_draft,
        command_draft,
        &window_inputs.resolved_path_result,
    ) {
        app.condition_editor_state.status_message = Some(error);
        app.condition_editor_state.status_is_error = true;
    }
}

fn clamp_condition_index(selected_index: Option<usize>, len: usize) -> Option<usize> {
    match (selected_index, len) {
        (_, 0) => None,
        (Some(index), _) => Some(index.min(len - 1)),
        (None, _) => Some(0),
    }
}

fn clamp_condition_group_selection_for_document(
    document: &FilterConfigDocument,
    condition_index: Option<usize>,
    selected_group_index: Option<usize>,
) -> Option<usize> {
    let Some(condition_index) = condition_index else {
        return None;
    };
    let Some(condition) = document.cooccurrence_conditions.get(condition_index) else {
        return None;
    };
    clamp_condition_index(selected_group_index, condition.form_groups.len())
}

#[cfg(test)]
mod commit_pick_tests {
    use super::*;
    use crate::app::App;
    use std::io::Write;

    #[test]
    fn invalid_json_leaves_override_loaded_path_and_dirty_unchanged() {
        let mut app = App::new(None);
        let stable_loaded = PathBuf::from("asset/cooccurrence-conditions.json");
        let preset_override = PathBuf::from("preset-override-for-test.json");
        app.condition_editor_state.loaded_path = Some(stable_loaded.clone());
        app.condition_editor_state.is_dirty = true;
        app.analysis_request_state.filter_config_path_override = Some(preset_override.clone());

        let tmp = std::env::temp_dir().join("coder_v1_condition_pick_invalid_xyz.json");
        let mut file = std::fs::File::create(&tmp).expect("temp file");
        file.write_all(b"{").expect("write");

        let ctx = egui::Context::default();
        let result = commit_picked_condition_json_path(&mut app, &ctx, tmp.clone());
        assert!(result.is_err(), "expected load error, got {result:?}");

        assert_eq!(
            app.analysis_request_state.filter_config_path_override,
            Some(preset_override)
        );
        assert_eq!(app.condition_editor_state.loaded_path, Some(stable_loaded));
        assert!(app.condition_editor_state.is_dirty);

        let _ = std::fs::remove_file(&tmp);
    }
}
