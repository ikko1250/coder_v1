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
    ConditionDocumentLoadInfo, FilterConfigDocument,
};
use crate::condition_editor_view::{
    draw_condition_editor_confirm_overlay as render_condition_editor_confirm_overlay,
    draw_condition_editor_footer_panel as render_condition_editor_footer_panel,
    draw_condition_editor_global_settings as render_condition_editor_global_settings,
    draw_condition_editor_header_panel as render_condition_editor_header_panel,
    draw_condition_editor_list_panel as render_condition_editor_list_panel,
    draw_condition_editor_selected_condition as render_condition_editor_selected_condition,
    ConditionEditorDetailResponse, ConditionEditorFooterResponse, ConditionEditorHeaderResponse,
    ConditionEditorListResponse, ConfirmOverlayResponse,
};
use eframe::egui;
use egui::{Color32, RichText, ScrollArea, Ui};
use egui_extras::{Size, StripBuilder};
use std::path::PathBuf;

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
    modal_response: Option<ConditionEditorModalResponse>,
    select_clicked: bool,
}

#[derive(Clone, Debug)]
struct ConditionEditorWindowInputs {
    can_modify: bool,
    resolved_path_result: Result<PathBuf, String>,
    resolved_path_label: String,
    loaded_path_label: String,
    current_confirm_action: Option<ConditionEditorConfirmAction>,
    panel_fill: Color32,
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
    pub(super) status_message: Option<String>,
    pub(super) status_is_error: bool,
    pub(super) is_dirty: bool,
    confirm_action: Option<ConditionEditorConfirmAction>,
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
    app.condition_editor_state.status_message = Some(final_status_message);
    app.condition_editor_state.status_is_error = false;
    app.condition_editor_state.is_dirty = false;
    app.condition_editor_state.confirm_action = None;
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
    StripBuilder::new(ui)
        .size(Size::exact(340.0))
        .size(Size::remainder())
        .horizontal(|mut strip| {
            strip.cell(|ui| {
                let list_response = render_condition_editor_list_panel(
                    ui,
                    can_modify,
                    app.condition_editor_state.document.as_ref(),
                    selection_draft.requested_selection,
                );
                apply_condition_editor_list_response(selection_draft, command_draft, list_response);
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
    command_draft.modal_response = match response {
        Some(ConfirmOverlayResponse::Continue) => Some(ConditionEditorModalResponse::Continue),
        Some(ConfirmOverlayResponse::Cancel) => Some(ConditionEditorModalResponse::Cancel),
        None => command_draft.modal_response,
    };
}

fn apply_condition_editor_list_response(
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
    list_response: ConditionEditorListResponse,
) {
    if list_response.add_clicked {
        command_draft.should_add_condition = true;
    }
    if let Some(selected_index) = list_response.selected_index {
        selection_draft.requested_selection = Some(selected_index);
        selection_draft.requested_group_selection = list_response.selected_group_index;
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
    modal_response: Option<ConditionEditorModalResponse>,
) {
    let Some(response) = modal_response else {
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
    if command_draft.modal_response.is_some() {
        apply_condition_editor_modal_response(app, ctx, viewport_id, command_draft.modal_response);
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
