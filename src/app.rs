//! 条例分析ビューアのメイン UI（egui）。
//!
//! **`impl App` の機能別メソッド一覧と切り出し候補モジュール**は P1-01 として
//! [`docs/p1-01-app-impl-inventory.md`](../docs/p1-01-app-impl-inventory.md) に記載する。
//! **`all_records` / `filtered_indices` / `selected_row` の変更経路**は P1-08 として
//! [`docs/p1-08-record-selection-mutation-paths.md`](../docs/p1-08-record-selection-mutation-paths.md) に記載する。
//! **`cached_segments` 等のキャッシュ無効化・更新**は P1-09 として
//! [`docs/p1-09-cache-invalidation-paths.md`](../docs/p1-09-cache-invalidation-paths.md) に記載する。
//! **副作用の境界（コア候補 / ホスト必須）**は P1-10 として
//! [`docs/p1-10-side-effect-boundaries.md`](../docs/p1-10-side-effect-boundaries.md) に記載する。
//! **公開 API・可視性の整理**は P1-11 として
//! [`docs/p1-11-public-api-review.md`](../docs/p1-11-public-api-review.md) に記載する。
//!
//! トップツールバーは [`app_toolbar`](app_toolbar) サブモジュール（`src/app_toolbar.rs`）。
//! DB 参照ウィンドウは [`app_db_viewer`](app_db_viewer) サブモジュール（`src/app_db_viewer.rs`）。
//! 分析設定ウィンドウは [`app_analysis_settings`](app_analysis_settings)（`src/app_analysis_settings.rs`）。
//! 分析ジョブ・警告一覧は [`app_analysis_job`](app_analysis_job)（`src/app_analysis_job.rs`）。
//! 中央ペイン（フィルタ・一覧・詳細）は [`app_main_layout`](app_main_layout)（`src/app_main_layout.rs`）。
//! エラーダイアログは [`app_error_dialog`](app_error_dialog)（`src/app_error_dialog.rs`）。
//! 条件 JSON エディタは [`app_condition_editor`](app_condition_editor)（`src/app_condition_editor.rs`）。
//! フレーム先頭（ジョブポーリング・キーボード・終了ガード）は [`app_lifecycle`](app_lifecycle)（`src/app_lifecycle.rs`）。

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

#[path = "app_error_dialog.rs"]
mod app_error_dialog;

#[path = "app_condition_editor.rs"]
mod app_condition_editor;

#[path = "app_lifecycle.rs"]
mod app_lifecycle;

use crate::analysis_runner::{
    build_runtime_config, resolve_annotation_csv_path, AnalysisJobEvent, AnalysisRuntimeConfig,
    AnalysisRuntimeOverrides, AnalysisWarningMessage,
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
use egui::Ui;
use egui_extras::Column;
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
    condition_editor_state: app_condition_editor::ConditionEditorState,
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
            condition_editor_state: app_condition_editor::ConditionEditorState::default(),
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
        self.cached_segments = None;
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
        app_condition_editor::focus_condition_editor_viewport(self, ctx);
    }

    fn open_condition_editor(&mut self, ctx: &egui::Context) -> Result<(), String> {
        app_condition_editor::open_condition_editor(self, ctx)
    }

    fn sync_condition_editor_with_runtime_path(&mut self) {
        app_condition_editor::sync_condition_editor_with_runtime_path(self);
    }

    fn start_analysis_job(&mut self) -> Result<(), String> {
        app_analysis_job::start_analysis_job(self)
    }

    fn start_export_job(&mut self, output_csv_path: PathBuf) -> Result<(), String> {
        app_analysis_job::start_export_job(self, output_csv_path)
    }

    fn draw_warning_details_window(&mut self, ctx: &egui::Context) {
        app_analysis_job::draw_warning_details_window(self, ctx);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        app_lifecycle::run_update_prelude(self, ctx);

        app_error_dialog::draw_error_dialog_if_any(self, ctx);

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

    fn draw_condition_editor_window(&mut self, ctx: &egui::Context) {
        app_condition_editor::draw_condition_editor_window(self, ctx);
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
