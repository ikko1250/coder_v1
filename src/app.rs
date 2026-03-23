//! 条例分析ビューアのメイン UI（egui）。
//!
//! **`impl App` の機能別メソッド一覧と切り出し候補モジュール**は P1-01 として
//! [`docs/p1-01-app-impl-inventory.md`](../docs/p1-01-app-impl-inventory.md) に記載する。
//!
//! トップツールバーは [`app_toolbar`](app_toolbar) サブモジュール（`src/app_toolbar.rs`）。
//! DB 参照ウィンドウは [`app_db_viewer`](app_db_viewer) サブモジュール（`src/app_db_viewer.rs`）。
//! 分析設定ウィンドウは [`app_analysis_settings`](app_analysis_settings)（`src/app_analysis_settings.rs`）。
//! 分析ジョブ・警告一覧は [`app_analysis_job`](app_analysis_job)（`src/app_analysis_job.rs`）。
//! 中央ペイン（フィルタ・一覧・詳細）は [`app_main_layout`](app_main_layout)（`src/app_main_layout.rs`）。

#[path = "app_toolbar.rs"]
mod app_toolbar;

#[path = "app_db_viewer.rs"]
mod app_db_viewer;

#[path = "app_analysis_settings.rs"]
mod app_analysis_settings;

#[path = "app_analysis_job.rs"]
mod app_analysis_job;

#[path = "app_main_layout.rs"]
mod app_main_layout;

use crate::analysis_runner::{
    build_runtime_config, resolve_annotation_csv_path, AnalysisJobEvent, AnalysisRuntimeConfig,
    AnalysisRuntimeOverrides, AnalysisWarningMessage,
};
use crate::condition_editor::{
    build_default_condition_item, load_condition_document, save_condition_document_atomic,
    FilterConfigDocument,
};
use crate::condition_editor_view::{
    draw_condition_editor_confirm_overlay as render_condition_editor_confirm_overlay,
    draw_condition_editor_footer_panel as render_condition_editor_footer_panel,
    draw_condition_editor_global_settings as render_condition_editor_global_settings,
    draw_condition_editor_header_panel as render_condition_editor_header_panel,
    draw_condition_editor_list_panel as render_condition_editor_list_panel,
    draw_condition_editor_selected_condition as render_condition_editor_selected_condition,
    ConditionEditorDetailResponse, ConditionEditorFooterResponse, ConditionEditorListResponse,
    ConfirmOverlayResponse,
};
use crate::csv_loader::load_records;
use crate::db::resolve_default_db_path;
use crate::filter::build_filter_options;
use crate::manual_annotation_store::{
    append_manual_annotation_namespaces_text, append_manual_annotation_pairs_text,
    append_manual_annotation_row, build_manual_annotation_pair, first_manual_annotation_line,
    increment_manual_annotation_count, ManualAnnotationAppendRow,
};
use crate::model::{AnalysisRecord, DbViewerState, FilterColumn, FilterOption, TextSegment};
use crate::tagged_text::parse_tagged_text;
use eframe::egui;
use egui::{Color32, RichText, ScrollArea, Ui};
use egui_extras::{Column, Size, StripBuilder};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TreeScrollRequest {
    row_index: usize,
    align: Option<egui::Align>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScrollBehavior {
    None,
    KeepVisible,
    AlignMin,
    AlignMax,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectionChange {
    selected_row: Option<usize>,
    scroll_behavior: ScrollBehavior,
}

#[derive(Clone, Copy)]
struct TreeColumnSpec {
    header: &'static str,
    build_column: fn() -> Column,
    value: fn(&AnalysisRecord) -> String,
}

const TREE_COLUMN_SPECS: &[TreeColumnSpec] = &[
    TreeColumnSpec {
        header: "No",
        build_column: build_tree_row_no_column,
        value: tree_row_no_value,
    },
    TreeColumnSpec {
        header: "unit_id",
        build_column: build_tree_paragraph_id_column,
        value: tree_paragraph_id_value,
    },
    TreeColumnSpec {
        header: "自治体",
        build_column: build_tree_municipality_column,
        value: tree_municipality_value,
    },
    TreeColumnSpec {
        header: "条例/規則",
        build_column: build_tree_ordinance_column,
        value: tree_ordinance_value,
    },
    TreeColumnSpec {
        header: "カテゴリ",
        build_column: build_tree_category_column,
        value: tree_category_value,
    },
    TreeColumnSpec {
        header: "annotation",
        build_column: build_tree_annotation_column,
        value: tree_annotation_value,
    },
    TreeColumnSpec {
        header: "強調token数",
        build_column: build_tree_annotated_token_count_column,
        value: tree_annotated_token_count_value,
    },
];

const DB_VIEWER_VIEWPORT_ID: &str = "db_viewer_viewport";
const CONDITION_EDITOR_VIEWPORT_ID: &str = "condition_editor_viewport";
const RECORD_LIST_PANEL_MIN_WIDTH: f32 = 360.0;
const RECORD_LIST_PANEL_DEFAULT_RATIO: f32 = 0.33;
const RECORD_LIST_PANEL_MAX_RATIO: f32 = 0.85;

struct RunningAnalysisJob {
    receiver: Receiver<AnalysisJobEvent>,
}

#[derive(Clone)]
struct AnalysisExportContext {
    db_path: PathBuf,
    filter_config_path: PathBuf,
    annotation_csv_path: PathBuf,
}

enum AnalysisJobStatus {
    Idle,
    RunningAnalysis { job_id: String },
    RunningExport { job_id: String },
    Succeeded { summary: String },
    Failed { summary: String },
}

struct AnalysisRuntimeState {
    runtime: Option<AnalysisRuntimeConfig>,
    current_job: Option<RunningAnalysisJob>,
    status: AnalysisJobStatus,
    last_warnings: Vec<AnalysisWarningMessage>,
    warning_window_open: bool,
    last_export_context: Option<AnalysisExportContext>,
}

impl AnalysisRuntimeState {
    fn from_runtime(runtime: Result<AnalysisRuntimeConfig, String>) -> Self {
        match runtime {
            Ok(runtime) => Self {
                runtime: Some(runtime),
                current_job: None,
                status: AnalysisJobStatus::Idle,
                last_warnings: Vec::new(),
                warning_window_open: false,
                last_export_context: None,
            },
            Err(error) => Self {
                runtime: None,
                current_job: None,
                status: AnalysisJobStatus::Failed { summary: error },
                last_warnings: Vec::new(),
                warning_window_open: false,
                last_export_context: None,
            },
        }
    }

    fn can_start(&self) -> bool {
        self.runtime.is_some() && self.current_job.is_none()
    }

    fn status_text(&self) -> String {
        match &self.status {
            AnalysisJobStatus::Idle => "分析待機中".to_string(),
            AnalysisJobStatus::RunningAnalysis { job_id } => format!("分析実行中: {job_id}"),
            AnalysisJobStatus::RunningExport { job_id } => format!("CSV 保存中: {job_id}"),
            AnalysisJobStatus::Succeeded { summary } => format!("分析成功: {summary}"),
            AnalysisJobStatus::Failed { summary } => format!("分析失敗: {summary}"),
        }
    }

    fn has_warning_details(&self) -> bool {
        !self.last_warnings.is_empty()
    }

    fn can_export(&self) -> bool {
        self.runtime.is_some() && self.current_job.is_none() && self.last_export_context.is_some()
    }
}

#[derive(Default)]
struct AnalysisRequestState {
    python_path_override: Option<PathBuf>,
    filter_config_path_override: Option<PathBuf>,
    annotation_csv_path_override: Option<PathBuf>,
    settings_window_open: bool,
}

impl AnalysisRequestState {
    fn runtime_overrides(&self) -> AnalysisRuntimeOverrides {
        AnalysisRuntimeOverrides {
            python_path: self.python_path_override.clone(),
            filter_config_path: self.filter_config_path_override.clone(),
            annotation_csv_path: self.annotation_csv_path_override.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AnnotationEditorState {
    namespace_input: String,
    key_input: String,
    value_input: String,
    tagged_by_input: String,
    confidence_input: String,
    note_input: String,
    status_message: Option<String>,
    status_is_error: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConditionEditorConfirmAction {
    CloseWindow,
    ReloadPath(PathBuf),
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
struct ConditionEditorState {
    window_open: bool,
    loaded_path: Option<PathBuf>,
    pending_path_sync: Option<PathBuf>,
    document: Option<FilterConfigDocument>,
    selected_index: Option<usize>,
    selected_group_index: Option<usize>,
    projected_legacy_condition_count: usize,
    status_message: Option<String>,
    status_is_error: bool,
    is_dirty: bool,
    confirm_action: Option<ConditionEditorConfirmAction>,
}

impl SelectionChange {
    fn new(selected_row: Option<usize>, scroll_behavior: ScrollBehavior) -> Self {
        Self {
            selected_row,
            scroll_behavior,
        }
    }

    fn first_filtered_row(filtered_len: usize, scroll_behavior: ScrollBehavior) -> Self {
        Self::new((filtered_len > 0).then_some(0), scroll_behavior)
    }
}

fn clamp_selected_row(selected_row: Option<usize>, filtered_len: usize) -> Option<usize> {
    match (selected_row, filtered_len) {
        (_, 0) => None,
        (Some(idx), len) => Some(idx.min(len - 1)),
        (None, _) => None,
    }
}

fn build_tree_scroll_request(
    selected_row: Option<usize>,
    scroll_behavior: ScrollBehavior,
) -> Option<TreeScrollRequest> {
    match scroll_behavior {
        ScrollBehavior::None => None,
        ScrollBehavior::KeepVisible => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: None,
        }),
        ScrollBehavior::AlignMin => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: Some(egui::Align::Min),
        }),
        ScrollBehavior::AlignMax => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: Some(egui::Align::Max),
        }),
    }
}

pub(crate) struct App {
    records_source_label: String,
    db_viewer_state: DbViewerState,
    analysis_request_state: AnalysisRequestState,
    analysis_runtime_state: AnalysisRuntimeState,
    all_records: Vec<AnalysisRecord>,
    filtered_indices: Vec<usize>,
    filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    filter_candidate_queries: HashMap<FilterColumn, String>,
    active_filter_column: FilterColumn,
    selected_row: Option<usize>,
    pending_tree_scroll: Option<TreeScrollRequest>,
    pub(crate) error_message: Option<String>,
    cached_segments: Option<(usize, Vec<TextSegment>)>,
    annotation_editor_state: AnnotationEditorState,
    condition_editor_state: ConditionEditorState,
    record_list_panel_ratio: f32,
    annotation_panel_expanded: bool,
}

impl App {
    pub(crate) fn new(initial_csv_path: Option<PathBuf>) -> Self {
        let analysis_request_state = AnalysisRequestState::default();
        let runtime = build_runtime_config(&analysis_request_state.runtime_overrides());
        let mut app = Self {
            records_source_label: "分析結果なし".to_string(),
            db_viewer_state: DbViewerState::new(resolve_default_db_path()),
            analysis_request_state,
            analysis_runtime_state: AnalysisRuntimeState::from_runtime(runtime),
            all_records: Vec::new(),
            filtered_indices: Vec::new(),
            filter_options: HashMap::new(),
            selected_filter_values: HashMap::new(),
            filter_candidate_queries: HashMap::new(),
            active_filter_column: FilterColumn::MatchedCategories,
            selected_row: None,
            pending_tree_scroll: None,
            error_message: None,
            cached_segments: None,
            annotation_editor_state: AnnotationEditorState::default(),
            condition_editor_state: ConditionEditorState::default(),
            record_list_panel_ratio: RECORD_LIST_PANEL_DEFAULT_RATIO,
            annotation_panel_expanded: false,
        };
        app.try_cleanup_analysis_jobs();
        if let Some(csv_path) = initial_csv_path {
            app.load_csv(csv_path);
        }
        app
    }

    fn load_csv(&mut self, path: PathBuf) {
        match load_records(&path) {
            Ok(records) => {
                self.replace_records(records, path.display().to_string());
                self.analysis_runtime_state.last_export_context = None;
            }
            Err(e) => {
                self.error_message = Some(e);
            }
        }
    }

    fn replace_records(&mut self, records: Vec<AnalysisRecord>, source_label: String) {
        self.all_records = records;
        self.records_source_label = source_label;
        self.db_viewer_state.reset_loaded_state();
        self.filter_options = build_filter_options(&self.all_records);
        self.selected_filter_values.clear();
        self.filter_candidate_queries.clear();
        self.filtered_indices = (0..self.all_records.len()).collect();
        self.cached_segments = None;
        self.apply_selection_change(SelectionChange::first_filtered_row(
            self.filtered_indices.len(),
            ScrollBehavior::AlignMin,
        ));
        self.error_message = None;
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    fn apply_selection_change(&mut self, change: SelectionChange) -> bool {
        let next = clamp_selected_row(change.selected_row, self.filtered_indices.len());
        let selection_changed = self.selected_row != next;
        if selection_changed {
            self.selected_row = next;
            self.cached_segments = None;
            self.clear_annotation_editor_status();
        }

        let next_scroll_request = build_tree_scroll_request(next, change.scroll_behavior);
        let scroll_changed = self.pending_tree_scroll != next_scroll_request;
        self.pending_tree_scroll = next_scroll_request;

        selection_changed || scroll_changed
    }

    fn select_first_filtered_row(&mut self, scroll_behavior: ScrollBehavior) -> bool {
        self.apply_selection_change(SelectionChange::first_filtered_row(
            self.filtered_indices.len(),
            scroll_behavior,
        ))
    }

    fn move_selection_up(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        match self.selected_row {
            Some(idx) if idx > 0 => {
                self.apply_selection_change(SelectionChange::new(
                    Some(idx - 1),
                    ScrollBehavior::KeepVisible,
                ));
            }
            None => {
                self.select_first_filtered_row(ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    fn move_selection_down(&mut self) {
        let current_len = self.filtered_indices.len();
        if current_len == 0 {
            return;
        }

        match self.selected_row {
            Some(idx) if idx + 1 < current_len => {
                self.apply_selection_change(SelectionChange::new(
                    Some(idx + 1),
                    ScrollBehavior::KeepVisible,
                ));
            }
            None => {
                self.select_first_filtered_row(ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    fn handle_keyboard_navigation(&mut self, ctx: &egui::Context) {
        if self.error_message.is_some()
            || self.filtered_indices.is_empty()
            || ctx.wants_keyboard_input()
        {
            return;
        }

        let (up_pressed, down_pressed) = ctx.input(|i| {
            (
                i.key_pressed(egui::Key::ArrowUp),
                i.key_pressed(egui::Key::ArrowDown),
            )
        });

        if down_pressed {
            self.move_selection_down();
        } else if up_pressed {
            self.move_selection_up();
        }
    }

    fn selected_record(&self) -> Option<&AnalysisRecord> {
        let filtered_idx = self.selected_row?;
        let record_idx = *self.filtered_indices.get(filtered_idx)?;
        self.all_records.get(record_idx)
    }

    fn selected_record_index(&self) -> Option<usize> {
        let filtered_idx = self.selected_row?;
        self.filtered_indices.get(filtered_idx).copied()
    }

    fn selected_record_mut(&mut self) -> Option<&mut AnalysisRecord> {
        let record_idx = self.selected_record_index()?;
        self.all_records.get_mut(record_idx)
    }

    fn resolved_annotation_csv_path(&self) -> Result<PathBuf, String> {
        resolve_annotation_csv_path(&self.analysis_request_state.runtime_overrides())
    }

    fn annotation_save_enabled(&self) -> bool {
        self.selected_record()
            .is_some_and(AnalysisRecord::supports_manual_annotation)
            && self.analysis_runtime_state.current_job.is_none()
    }

    fn clear_annotation_editor_status(&mut self) {
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    fn clear_annotation_editor_inputs(&mut self) {
        self.annotation_editor_state.value_input.clear();
        self.annotation_editor_state.confidence_input.clear();
        self.annotation_editor_state.note_input.clear();
    }

    fn build_annotation_append_row(&self) -> Result<ManualAnnotationAppendRow, String> {
        let record = self
            .selected_record()
            .ok_or_else(|| "レコードが選択されていません".to_string())?;
        if !record.supports_manual_annotation() {
            return Err("manual annotation は paragraph 行のみ対応です".to_string());
        }

        let paragraph_id = record.paragraph_id.trim();
        if paragraph_id.is_empty() {
            return Err("paragraph_id が空のため annotation を保存できません".to_string());
        }

        let namespace = self.annotation_editor_state.namespace_input.trim();
        if namespace.is_empty() {
            return Err("namespace を入力してください".to_string());
        }

        let key = self.annotation_editor_state.key_input.trim();
        if key.is_empty() {
            return Err("key を入力してください".to_string());
        }

        let value = self.annotation_editor_state.value_input.trim();
        if value.is_empty() {
            return Err("value を入力してください".to_string());
        }

        Ok(ManualAnnotationAppendRow {
            target_type: "paragraph".to_string(),
            target_id: paragraph_id.to_string(),
            label_namespace: namespace.to_string(),
            label_key: key.to_string(),
            label_value: value.to_string(),
            tagged_by: self
                .annotation_editor_state
                .tagged_by_input
                .trim()
                .to_string(),
            tagged_at: String::new(),
            confidence: self
                .annotation_editor_state
                .confidence_input
                .trim()
                .to_string(),
            note: self.annotation_editor_state.note_input.trim().to_string(),
        })
    }

    fn apply_saved_annotation_to_selected_record(
        &mut self,
        annotation_row: &ManualAnnotationAppendRow,
    ) -> Result<(), String> {
        let pair = build_manual_annotation_pair(
            &annotation_row.label_namespace,
            &annotation_row.label_key,
            &annotation_row.label_value,
        );
        {
            let updated_record = self
                .selected_record_mut()
                .ok_or_else(|| "レコードが選択されていません".to_string())?;
            updated_record.manual_annotation_count =
                increment_manual_annotation_count(&updated_record.manual_annotation_count);
            updated_record.manual_annotation_pairs_text = append_manual_annotation_pairs_text(
                &updated_record.manual_annotation_pairs_text,
                &pair,
            );
            updated_record.manual_annotation_namespaces_text =
                append_manual_annotation_namespaces_text(
                    &updated_record.manual_annotation_namespaces_text,
                    &annotation_row.label_namespace,
                );
        }
        self.filter_options = build_filter_options(&self.all_records);
        Ok(())
    }

    fn save_annotation_for_selected_record(&mut self) {
        self.clear_annotation_editor_status();
        let annotation_row = match self.build_annotation_append_row() {
            Ok(annotation_row) => annotation_row,
            Err(error) => {
                self.annotation_editor_state.status_message = Some(error);
                self.annotation_editor_state.status_is_error = true;
                return;
            }
        };

        let annotation_csv_path = match self.resolved_annotation_csv_path() {
            Ok(annotation_csv_path) => annotation_csv_path,
            Err(error) => {
                self.annotation_editor_state.status_message = Some(error);
                self.annotation_editor_state.status_is_error = true;
                return;
            }
        };

        if let Err(error) = append_manual_annotation_row(&annotation_csv_path, &annotation_row) {
            self.annotation_editor_state.status_message = Some(error);
            self.annotation_editor_state.status_is_error = true;
            return;
        }

        if let Err(error) = self.apply_saved_annotation_to_selected_record(&annotation_row) {
            self.annotation_editor_state.status_message = Some(error);
            self.annotation_editor_state.status_is_error = true;
            return;
        }

        self.clear_annotation_editor_inputs();
        self.annotation_editor_state.status_message = Some(format!(
            "annotation を追記しました: {}",
            annotation_csv_path.display()
        ));
        self.annotation_editor_state.status_is_error = false;
    }

    fn apply_filters(&mut self) {
        self.filtered_indices = self
            .all_records
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| self.record_matches_filters(record).then_some(idx))
            .collect();
        self.cached_segments = None;
        self.select_first_filtered_row(ScrollBehavior::AlignMin);
    }

    fn record_matches_filters(&self, record: &AnalysisRecord) -> bool {
        self.selected_filter_values
            .iter()
            .all(|(column, selected)| column.matches(record, selected))
    }

    fn clear_filters_for_column(&mut self, column: FilterColumn) {
        if self.selected_filter_values.remove(&column).is_some() {
            self.apply_filters();
        }
    }

    fn clear_all_filters(&mut self) {
        if !self.selected_filter_values.is_empty() {
            self.selected_filter_values.clear();
            self.apply_filters();
        }
    }

    fn toggle_filter_value(&mut self, column: FilterColumn, value: &str, selected: bool) {
        let changed = {
            let entry = self.selected_filter_values.entry(column).or_default();
            if selected {
                entry.insert(value.to_string())
            } else {
                entry.remove(value)
            }
        };

        if self
            .selected_filter_values
            .get(&column)
            .is_some_and(BTreeSet::is_empty)
        {
            self.selected_filter_values.remove(&column);
        }

        if changed {
            self.apply_filters();
        }
    }

    fn get_segments(&mut self) -> Vec<TextSegment> {
        if let Some(record) = self.selected_record() {
            let row_no = record.row_no;
            if let Some((cached_row, ref segs)) = self.cached_segments {
                if cached_row == row_no {
                    return segs.clone();
                }
            }
            let tagged = if record.primary_text_tagged().trim().is_empty() {
                record.primary_text().to_string()
            } else {
                record.primary_text_tagged().to_string()
            };
            let segs = parse_tagged_text(&tagged);
            self.cached_segments = Some((row_no, segs.clone()));
            segs
        } else {
            Vec::new()
        }
    }

    fn try_cleanup_analysis_jobs(&mut self) {
        app_analysis_job::try_cleanup_analysis_jobs(self);
    }

    fn refresh_analysis_runtime(&mut self) {
        app_analysis_job::refresh_analysis_runtime(self);
    }

    fn focus_condition_editor_viewport(&self, ctx: &egui::Context) {
        let viewport_id = egui::ViewportId::from_hash_of(CONDITION_EDITOR_VIEWPORT_ID);
        ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(false));
        ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Focus);
    }

    fn open_condition_editor(&mut self, ctx: &egui::Context) -> Result<(), String> {
        if self.condition_editor_state.window_open {
            self.condition_editor_state.status_message =
                Some("condition editor は既に開いています。".to_string());
            self.condition_editor_state.status_is_error = false;
            self.focus_condition_editor_viewport(ctx);
            return Ok(());
        }
        let path = app_analysis_job::resolved_filter_config_path(self)?;
        self.load_condition_editor_from_path(path, "条件 JSON を読み込みました。")
    }

    fn load_condition_editor_from_path(
        &mut self,
        path: PathBuf,
        status_message: &str,
    ) -> Result<(), String> {
        let (document, load_info) = load_condition_document(&path)?;
        let projected_count = load_info.projected_legacy_condition_count;
        let mut final_status_message = status_message.to_string();
        if projected_count > 0 {
            final_status_message.push_str(&format!(
                " legacy 条件 {} 件を group editor 用に投影しました。",
                projected_count
            ));
        }
        self.condition_editor_state.window_open = true;
        self.condition_editor_state.loaded_path = Some(path);
        self.condition_editor_state.pending_path_sync = None;
        self.condition_editor_state.document = Some(document);
        self.condition_editor_state.selected_index = self.clamp_condition_editor_selection(Some(0));
        self.condition_editor_state.selected_group_index = self
            .clamp_condition_editor_group_selection(
                Some(0),
                self.condition_editor_state.selected_index,
            );
        self.condition_editor_state.projected_legacy_condition_count = projected_count;
        self.condition_editor_state.status_message = Some(final_status_message);
        self.condition_editor_state.status_is_error = false;
        self.condition_editor_state.is_dirty = false;
        self.condition_editor_state.confirm_action = None;
        Ok(())
    }

    fn clamp_condition_editor_selection(&self, selected_index: Option<usize>) -> Option<usize> {
        let Some(document) = self.condition_editor_state.document.as_ref() else {
            return None;
        };
        match (selected_index, document.cooccurrence_conditions.len()) {
            (_, 0) => None,
            (Some(index), len) => Some(index.min(len - 1)),
            (None, len) => Some(len - 1),
        }
    }

    fn clamp_condition_editor_group_selection(
        &self,
        selected_group_index: Option<usize>,
        condition_index: Option<usize>,
    ) -> Option<usize> {
        let Some(document) = self.condition_editor_state.document.as_ref() else {
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

    fn mark_condition_editor_dirty(&mut self) {
        self.condition_editor_state.is_dirty = true;
        self.condition_editor_state.status_message = Some("未保存の変更があります。".to_string());
        self.condition_editor_state.status_is_error = false;
    }

    fn condition_editor_selection_draft(&self) -> ConditionEditorSelectionDraft {
        ConditionEditorSelectionDraft {
            requested_selection: self.condition_editor_state.selected_index,
            requested_group_selection: self.condition_editor_state.selected_group_index,
        }
    }

    fn condition_editor_window_inputs(&self, ctx: &egui::Context) -> ConditionEditorWindowInputs {
        let resolved_path_result = app_analysis_job::resolved_filter_config_path(self);
        let resolved_path_label = resolved_path_result
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let loaded_path_label = self
            .condition_editor_state
            .loaded_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string());

        ConditionEditorWindowInputs {
            can_modify: self.analysis_runtime_state.current_job.is_none(),
            resolved_path_result,
            resolved_path_label,
            loaded_path_label,
            current_confirm_action: self.condition_editor_state.confirm_action.clone(),
            panel_fill: ctx.style().visuals.panel_fill,
        }
    }

    fn apply_condition_editor_selection_draft(
        &mut self,
        selection_draft: ConditionEditorSelectionDraft,
    ) {
        self.condition_editor_state.selected_index =
            self.clamp_condition_editor_selection(selection_draft.requested_selection);
        self.condition_editor_state.selected_group_index = self
            .clamp_condition_editor_group_selection(
                selection_draft.requested_group_selection,
                self.condition_editor_state.selected_index,
            );
    }

    fn reload_condition_editor(&mut self, path: PathBuf) -> Result<(), String> {
        self.load_condition_editor_from_path(path, "条件 JSON を再読込しました。")
    }

    fn request_condition_editor_reload(&mut self, path: PathBuf) -> Result<(), String> {
        if self.condition_editor_state.is_dirty {
            self.condition_editor_state.confirm_action =
                Some(ConditionEditorConfirmAction::ReloadPath(path));
            return Ok(());
        }
        self.reload_condition_editor(path)
    }

    fn save_condition_editor_document(&mut self) -> Result<(), String> {
        let path = self
            .condition_editor_state
            .loaded_path
            .clone()
            .ok_or_else(|| "保存先の条件 JSON パスが未設定です".to_string())?;
        let document = self
            .condition_editor_state
            .document
            .as_ref()
            .ok_or_else(|| "保存対象の条件 JSON が読み込まれていません".to_string())?;
        save_condition_document_atomic(&path, document)?;
        self.load_condition_editor_from_path(path.clone(), "条件 JSON を保存しました。")?;
        self.condition_editor_state.status_message =
            Some(format!("条件 JSON を保存しました: {}", path.display()));
        self.condition_editor_state.status_is_error = false;
        Ok(())
    }

    fn sync_condition_editor_with_runtime_path(&mut self) {
        if !self.condition_editor_state.window_open {
            return;
        }

        let Ok(resolved_path) = app_analysis_job::resolved_filter_config_path(self) else {
            return;
        };
        let Some(loaded_path) = self.condition_editor_state.loaded_path.clone() else {
            return;
        };
        if resolved_path == loaded_path {
            self.condition_editor_state.pending_path_sync = None;
            return;
        }

        if self.condition_editor_state.is_dirty {
            self.condition_editor_state.pending_path_sync = Some(resolved_path.clone());
            self.condition_editor_state.status_message = Some(format!(
                "分析設定で条件 JSON の解決先が変更されました。再読込が必要です: {}",
                resolved_path.display()
            ));
            self.condition_editor_state.status_is_error = true;
            return;
        }

        match self.reload_condition_editor(resolved_path.clone()) {
            Ok(()) => {
                self.condition_editor_state.pending_path_sync = None;
                self.condition_editor_state.status_message = Some(format!(
                    "分析設定の変更に合わせて条件 JSON を再読込しました: {}",
                    resolved_path.display()
                ));
                self.condition_editor_state.status_is_error = false;
            }
            Err(error) => {
                self.condition_editor_state.pending_path_sync = Some(resolved_path);
                self.condition_editor_state.status_message = Some(error);
                self.condition_editor_state.status_is_error = true;
            }
        }
    }

    fn start_analysis_job(&mut self) -> Result<(), String> {
        app_analysis_job::start_analysis_job(self)
    }

    fn start_export_job(&mut self, output_csv_path: PathBuf) -> Result<(), String> {
        app_analysis_job::start_export_job(self, output_csv_path)
    }

    fn poll_analysis_job(&mut self, ctx: &egui::Context) {
        app_analysis_job::poll_analysis_job(self, ctx);
    }

    fn draw_warning_details_window(&mut self, ctx: &egui::Context) {
        app_analysis_job::draw_warning_details_window(self, ctx);
    }

    fn guard_root_close_with_dirty_editor(&mut self, ctx: &egui::Context) {
        app_analysis_job::guard_root_close_with_dirty_editor(self, ctx);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_analysis_job(ctx);
        self.handle_keyboard_navigation(ctx);
        self.guard_root_close_with_dirty_editor(ctx);

        if let Some(err) = self.error_message.clone() {
            egui::Window::new("エラー")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label(&err);
                    if ui.button("閉じる").clicked() {
                        self.error_message = None;
                    }
                });
        }

        self.draw_warning_details_window(ctx);

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            self.draw_toolbar(ui);
        });

        let consumed_tree_scroll = self.pending_tree_scroll;
        let mut clicked_row = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            clicked_row = self.draw_body(ui, consumed_tree_scroll);
        });

        self.draw_db_viewer_window(ctx);
        self.draw_analysis_settings_window(ctx);
        self.draw_condition_editor_window(ctx);

        if let Some(row_index) = clicked_row {
            if self.apply_selection_change(SelectionChange::new(
                Some(row_index),
                ScrollBehavior::KeepVisible,
            )) {
                ctx.request_repaint();
            }
        }

        if self.pending_tree_scroll == consumed_tree_scroll {
            self.pending_tree_scroll = None;
        }
    }
}

impl App {
    fn draw_db_viewer_button(&mut self, ui: &mut Ui, enabled: bool) {
        app_db_viewer::draw_db_viewer_button(self, ui, enabled);
    }

    fn draw_db_viewer_window(&mut self, ctx: &egui::Context) {
        app_db_viewer::draw_db_viewer_window(self, ctx);
    }

    fn draw_toolbar(&mut self, ui: &mut Ui) {
        app_toolbar::draw_toolbar(self, ui);
    }

    fn draw_analysis_settings_window(&mut self, ctx: &egui::Context) {
        app_analysis_settings::draw_analysis_settings_window(self, ctx);
    }

    fn draw_condition_editor_body_panel(
        &mut self,
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
                        self.condition_editor_state.document.as_ref(),
                        selection_draft.requested_selection,
                    );
                    self.apply_condition_editor_list_response(
                        selection_draft,
                        command_draft,
                        list_response,
                    );
                });

                strip.cell(|ui| {
                    self.draw_condition_editor_detail_panel(
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
        &mut self,
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
                        if self.draw_condition_editor_detail_contents(
                            ui,
                            can_modify,
                            selection_draft,
                            command_draft,
                        ) {
                            self.mark_condition_editor_dirty();
                        }
                    });
            });
    }

    fn draw_condition_editor_detail_contents(
        &mut self,
        ui: &mut Ui,
        can_modify: bool,
        selection_draft: &mut ConditionEditorSelectionDraft,
        command_draft: &mut ConditionEditorCommandDraft,
    ) -> bool {
        let Some(document) = self.condition_editor_state.document.as_mut() else {
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
        self.apply_condition_editor_detail_response(
            selected_index,
            selection_draft,
            command_draft,
            detail_response,
        );
        changed |= detail_response.changed;

        changed
    }

    fn condition_editor_status_message(&self) -> Option<(&str, bool)> {
        self.condition_editor_state
            .status_message
            .as_deref()
            .map(|message| (message, self.condition_editor_state.status_is_error))
    }

    fn condition_editor_save_enabled(&self, can_modify: bool, resolved_path_ok: bool) -> bool {
        can_modify
            && self.condition_editor_state.document.is_some()
            && self.condition_editor_state.pending_path_sync.is_none()
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
        }
    }

    fn draw_condition_editor_embedded_window(
        &mut self,
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
                render_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                    self.condition_editor_state.pending_path_sync.as_deref(),
                    self.condition_editor_status_message(),
                    self.condition_editor_state.projected_legacy_condition_count,
                );
                ui.separator();
                self.draw_condition_editor_body_panel(
                    ui,
                    can_modify,
                    selection_draft,
                    command_draft,
                );
                ui.separator();
                let footer_response = render_condition_editor_footer_panel(
                    ui,
                    self.condition_editor_save_enabled(can_modify, resolved_path_ok),
                    can_modify && resolved_path_ok,
                    self.condition_editor_state.is_dirty,
                );
                self.apply_condition_editor_footer_response(command_draft, footer_response);
            });
        if !fallback_open {
            command_draft.close_requested = true;
        }
    }

    fn draw_condition_editor_viewport_panels(
        &mut self,
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
                render_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                    self.condition_editor_state.pending_path_sync.as_deref(),
                    self.condition_editor_status_message(),
                    self.condition_editor_state.projected_legacy_condition_count,
                );
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
                    self.condition_editor_save_enabled(can_modify, resolved_path_ok),
                    can_modify && resolved_path_ok,
                    self.condition_editor_state.is_dirty,
                );
                self.apply_condition_editor_footer_response(command_draft, footer_response);
            });

        egui::CentralPanel::default()
            .frame(
                egui::Frame::default()
                    .fill(panel_fill)
                    .inner_margin(egui::Margin::same(10)),
            )
            .show(viewport_ctx, |ui| {
                self.draw_condition_editor_body_panel(
                    ui,
                    can_modify,
                    selection_draft,
                    command_draft,
                );
            });
    }

    fn apply_condition_editor_close_request(&mut self, close_requested: bool) {
        if !close_requested {
            return;
        }

        if self.condition_editor_state.is_dirty {
            self.condition_editor_state.confirm_action =
                Some(ConditionEditorConfirmAction::CloseWindow);
        } else {
            self.condition_editor_state.window_open = false;
        }
    }

    fn apply_condition_editor_footer_response(
        &self,
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

    fn apply_condition_editor_confirm_overlay_response(
        &self,
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
        &self,
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
        &self,
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

    fn apply_condition_editor_add_request(&mut self, should_add_condition: bool) {
        if !should_add_condition {
            return;
        }

        let mut new_index = None;
        if let Some(document) = self.condition_editor_state.document.as_mut() {
            document
                .cooccurrence_conditions
                .push(build_default_condition_item());
            new_index = Some(document.cooccurrence_conditions.len().saturating_sub(1));
        }
        if let Some(index) = new_index {
            self.condition_editor_state.selected_index = Some(index);
            self.condition_editor_state.selected_group_index = Some(0);
            self.mark_condition_editor_dirty();
        }
    }

    fn apply_condition_editor_delete_request(&mut self, delete_index: Option<usize>) {
        let Some(delete_index) = delete_index else {
            return;
        };

        if let Some(document) = self.condition_editor_state.document.as_mut() {
            if delete_index < document.cooccurrence_conditions.len() {
                document.cooccurrence_conditions.remove(delete_index);
                self.condition_editor_state.selected_index = clamp_condition_index(
                    Some(delete_index),
                    document.cooccurrence_conditions.len(),
                );
                self.condition_editor_state.selected_group_index =
                    clamp_condition_group_selection_for_document(
                        document,
                        self.condition_editor_state.selected_index,
                        Some(0),
                    );
                self.mark_condition_editor_dirty();
                self.condition_editor_state.status_message =
                    Some("condition を削除しました。".to_string());
                self.condition_editor_state.status_is_error = false;
            }
        }
    }

    fn apply_condition_editor_reload_request(
        &mut self,
        should_reload: bool,
        resolved_path_result: &Result<PathBuf, String>,
    ) -> Option<String> {
        if !should_reload {
            return None;
        }

        match resolved_path_result {
            Ok(path) => self.request_condition_editor_reload(path.clone()).err(),
            Err(error) => Some(error.clone()),
        }
    }

    fn apply_condition_editor_modal_response(
        &mut self,
        ctx: &egui::Context,
        viewport_id: egui::ViewportId,
        modal_response: Option<ConditionEditorModalResponse>,
    ) {
        let Some(response) = modal_response else {
            return;
        };

        match response {
            ConditionEditorModalResponse::Continue => {
                if let Some(confirm_action) = self.condition_editor_state.confirm_action.clone() {
                    match confirm_action {
                        ConditionEditorConfirmAction::CloseWindow => {
                            self.condition_editor_state.window_open = false;
                            self.condition_editor_state.confirm_action = None;
                            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Close);
                        }
                        ConditionEditorConfirmAction::ReloadPath(path) => {
                            if let Err(error) = self.reload_condition_editor(path) {
                                self.condition_editor_state.status_message = Some(error);
                                self.condition_editor_state.status_is_error = true;
                            }
                            self.condition_editor_state.confirm_action = None;
                        }
                    }
                }
            }
            ConditionEditorModalResponse::Cancel => {
                self.condition_editor_state.confirm_action = None;
            }
        }
    }

    fn apply_condition_editor_command_draft(
        &mut self,
        ctx: &egui::Context,
        viewport_id: egui::ViewportId,
        selection_draft: ConditionEditorSelectionDraft,
        command_draft: ConditionEditorCommandDraft,
        resolved_path_result: &Result<PathBuf, String>,
    ) -> Option<String> {
        self.apply_condition_editor_selection_draft(selection_draft);
        self.apply_condition_editor_close_request(command_draft.close_requested);
        self.apply_condition_editor_add_request(command_draft.should_add_condition);
        self.apply_condition_editor_delete_request(command_draft.should_delete_condition);

        let mut save_error = None;
        if command_draft.should_save {
            if let Err(error) = self.save_condition_editor_document() {
                save_error = Some(error);
            }
        }
        let reload_error = self.apply_condition_editor_reload_request(
            command_draft.should_reload,
            resolved_path_result,
        );
        self.apply_condition_editor_modal_response(ctx, viewport_id, command_draft.modal_response);

        save_error.or(reload_error)
    }

    fn draw_condition_editor_window(&mut self, ctx: &egui::Context) {
        if !self.condition_editor_state.window_open {
            return;
        }

        let viewport_id = egui::ViewportId::from_hash_of(CONDITION_EDITOR_VIEWPORT_ID);
        let builder = egui::ViewportBuilder::default()
            .with_title("条件編集")
            .with_inner_size([1120.0, 760.0])
            .with_resizable(true);
        let window_inputs = self.condition_editor_window_inputs(ctx);
        let mut selection_draft = self.condition_editor_selection_draft();
        let mut command_draft = ConditionEditorCommandDraft::default();

        ctx.show_viewport_immediate(viewport_id, builder, |viewport_ctx, class| {
            command_draft.close_requested =
                viewport_ctx.input(|input| input.viewport().close_requested());
            if command_draft.close_requested && self.condition_editor_state.is_dirty {
                viewport_ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            }

            match class {
                egui::ViewportClass::Embedded => {
                    self.draw_condition_editor_embedded_window(
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
                    self.draw_condition_editor_viewport_panels(
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
                let message = Self::condition_editor_confirm_message(confirm_action);
                let response = render_condition_editor_confirm_overlay(viewport_ctx, &message);
                self.apply_condition_editor_confirm_overlay_response(&mut command_draft, response);
            }
        });

        if let Some(error) = self.apply_condition_editor_command_draft(
            ctx,
            viewport_id,
            selection_draft,
            command_draft,
            &window_inputs.resolved_path_result,
        ) {
            self.condition_editor_state.status_message = Some(error);
            self.condition_editor_state.status_is_error = true;
        }
    }

    fn draw_body(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        app_main_layout::draw_body(self, ui, tree_scroll_request)
    }
}

fn build_tree_row_no_column() -> Column {
    Column::initial(56.0).at_least(48.0).clip(true)
}

fn build_tree_paragraph_id_column() -> Column {
    Column::initial(140.0).at_least(96.0).clip(true)
}

fn build_tree_municipality_column() -> Column {
    Column::initial(128.0).at_least(96.0).clip(true)
}

fn build_tree_ordinance_column() -> Column {
    Column::initial(120.0).at_least(88.0).clip(true)
}

fn build_tree_category_column() -> Column {
    Column::remainder().at_least(140.0).clip(true)
}

fn build_tree_annotation_column() -> Column {
    Column::initial(220.0).at_least(140.0).clip(true)
}

fn build_tree_annotated_token_count_column() -> Column {
    Column::initial(92.0).at_least(72.0).clip(true)
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

fn tree_row_no_value(record: &AnalysisRecord) -> String {
    record.row_no.to_string()
}

fn tree_paragraph_id_value(record: &AnalysisRecord) -> String {
    record.unit_id().to_string()
}

fn tree_municipality_value(record: &AnalysisRecord) -> String {
    record.municipality_name.clone()
}

fn tree_ordinance_value(record: &AnalysisRecord) -> String {
    record.ordinance_or_rule.clone()
}

fn tree_category_value(record: &AnalysisRecord) -> String {
    record.matched_categories_text.clone()
}

fn tree_annotation_value(record: &AnalysisRecord) -> String {
    first_manual_annotation_line(&record.manual_annotation_pairs_text)
}

fn tree_annotated_token_count_value(record: &AnalysisRecord) -> String {
    record.annotated_token_count.clone()
}
