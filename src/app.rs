use crate::analysis_runner::{
    build_runtime_config, cleanup_job_directories, spawn_analysis_job, spawn_export_job,
    resolve_annotation_csv_path, AnalysisExportRequest, AnalysisExportSuccess, AnalysisJobEvent,
    AnalysisJobFailure, AnalysisJobRequest, AnalysisJobSuccess, AnalysisRuntimeConfig,
    AnalysisRuntimeOverrides, AnalysisWarningMessage, resolve_filter_config_path,
};
use crate::condition_editor::{
    build_default_condition_item, load_condition_document, save_condition_document_atomic,
    FilterConfigDocument, FormGroupEditorItem,
};
use crate::condition_editor_view::{
    condition_group_count, draw_annotation_filter_editor, draw_form_group_editor,
    draw_string_list_editor, edit_optional_choice, summarize_condition_list,
    summarize_form_group_label, total_forms_count, CONDITION_EDITOR_CHOICE_WIDTH,
    CONDITION_EDITOR_FIELD_LABEL_WIDTH, CONDITION_EDITOR_TEXT_INPUT_WIDTH,
};
use crate::csv_loader::load_records;
use crate::db::{
    fetch_paragraph_context, fetch_paragraph_context_by_location, resolve_default_db_path,
};
use crate::filter::{build_filter_options, display_filter_value};
use crate::manual_annotation_store::{
    append_manual_annotation_namespaces_text, append_manual_annotation_pairs_text,
    append_manual_annotation_row, build_manual_annotation_pair, first_manual_annotation_line,
    increment_manual_annotation_count, ManualAnnotationAppendRow,
};
use crate::model::{AnalysisRecord, DbViewerState, FilterColumn, FilterOption, TextSegment};
use crate::tagged_text::parse_tagged_text;
use crate::ui_helpers::{ime_safe_multiline, ime_safe_singleline};
use eframe::egui;
use egui::text::{LayoutJob, TextFormat};
use egui::{Color32, RichText, ScrollArea, TextStyle, TextWrapMode, Ui};
use egui_extras::{Column, Size, StripBuilder, TableBuilder};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreeScrollRequest {
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
        header: "paragraph_id",
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
    active_filter_column: FilterColumn,
    selected_row: Option<usize>,
    pending_tree_scroll: Option<TreeScrollRequest>,
    pub(crate) error_message: Option<String>,
    cached_segments: Option<(usize, Vec<TextSegment>)>,
    annotation_editor_state: AnnotationEditorState,
    condition_editor_state: ConditionEditorState,
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
            active_filter_column: FilterColumn::MatchedCategories,
            selected_row: None,
            pending_tree_scroll: None,
            error_message: None,
            cached_segments: None,
            annotation_editor_state: AnnotationEditorState::default(),
            condition_editor_state: ConditionEditorState::default(),
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

    #[allow(dead_code)]
    fn db_viewer_state(&self) -> &DbViewerState {
        &self.db_viewer_state
    }

    #[allow(dead_code)]
    fn db_viewer_state_mut(&mut self) -> &mut DbViewerState {
        &mut self.db_viewer_state
    }

    #[allow(dead_code)]
    fn selected_paragraph_id_for_db(&self) -> Result<i64, String> {
        let record = self
            .selected_record()
            .ok_or_else(|| "レコードが選択されていません".to_string())?;

        record.paragraph_id.parse::<i64>().map_err(|error| {
            format!(
                "paragraph_id を数値として解釈できません: {} ({error})",
                record.paragraph_id
            )
        })
    }

    #[allow(dead_code)]
    fn prepare_db_viewer_state(&mut self) -> Result<(), String> {
        let paragraph_id = self.selected_paragraph_id_for_db()?;
        let source_paragraph_text = self
            .selected_record()
            .map(|record| record.paragraph_text.clone())
            .ok_or_else(|| "レコードが選択されていません".to_string())?;

        self.db_viewer_state.is_open = true;
        self.db_viewer_state.source_paragraph_id = Some(paragraph_id);
        self.db_viewer_state.source_paragraph_text = Some(source_paragraph_text);
        self.db_viewer_state.context = None;
        self.db_viewer_state.error_message = None;
        Ok(())
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
        self.selected_record().is_some() && self.analysis_runtime_state.current_job.is_none()
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
            tagged_by: self.annotation_editor_state.tagged_by_input.trim().to_string(),
            tagged_at: String::new(),
            confidence: self.annotation_editor_state.confidence_input.trim().to_string(),
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
            let tagged = record.paragraph_text_tagged.clone();
            let segs = parse_tagged_text(&tagged);
            self.cached_segments = Some((row_no, segs.clone()));
            segs
        } else {
            Vec::new()
        }
    }

    fn try_cleanup_analysis_jobs(&mut self) {
        let Some(runtime) = self.analysis_runtime_state.runtime.as_ref() else {
            return;
        };

        if let Err(error) = cleanup_job_directories(&runtime.jobs_root) {
            self.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary: error };
        }
    }

    fn refresh_analysis_runtime(&mut self) {
        if self.analysis_runtime_state.current_job.is_some() {
            return;
        }

        let runtime = build_runtime_config(&self.analysis_request_state.runtime_overrides());
        self.analysis_runtime_state = AnalysisRuntimeState::from_runtime(runtime);
        self.try_cleanup_analysis_jobs();
        self.sync_condition_editor_with_runtime_path();
    }

    fn resolved_filter_config_path(&self) -> Result<PathBuf, String> {
        if let Some(runtime) = self.analysis_runtime_state.runtime.as_ref() {
            return Ok(runtime.filter_config_path.clone());
        }
        resolve_filter_config_path(&self.analysis_request_state.runtime_overrides())
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
        let path = self.resolved_filter_config_path()?;
        self.load_condition_editor_from_path(
            path,
            "条件 JSON を読み込みました。",
        )
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
        self.condition_editor_state.selected_index =
            self.clamp_condition_editor_selection(Some(0));
        self.condition_editor_state.selected_group_index =
            self.clamp_condition_editor_group_selection(
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

    fn apply_condition_editor_selection_draft(
        &mut self,
        selection_draft: ConditionEditorSelectionDraft,
    ) {
        self.condition_editor_state.selected_index =
            self.clamp_condition_editor_selection(selection_draft.requested_selection);
        self.condition_editor_state.selected_group_index =
            self.clamp_condition_editor_group_selection(
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

        let Ok(resolved_path) = self.resolved_filter_config_path() else {
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
        if self.analysis_runtime_state.current_job.is_some() {
            return Err("分析ジョブは既に実行中です".to_string());
        }

        let runtime = self
            .analysis_runtime_state
            .runtime
            .clone()
            .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;

        cleanup_job_directories(&runtime.jobs_root)?;

        let (job_id, receiver) = spawn_analysis_job(AnalysisJobRequest {
            db_path: self.db_viewer_state.db_path.clone(),
            runtime,
        });

        self.analysis_runtime_state.last_warnings.clear();
        self.analysis_runtime_state.warning_window_open = false;
        self.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
        self.analysis_runtime_state.status = AnalysisJobStatus::RunningAnalysis { job_id };
        Ok(())
    }

    fn start_export_job(&mut self, output_csv_path: PathBuf) -> Result<(), String> {
        if self.analysis_runtime_state.current_job.is_some() {
            return Err("分析ジョブは既に実行中です".to_string());
        }

        let runtime = self
            .analysis_runtime_state
            .runtime
            .clone()
            .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;
        let export_context = self
            .analysis_runtime_state
            .last_export_context
            .clone()
            .ok_or_else(|| "保存対象の分析結果がありません".to_string())?;

        let (job_id, receiver) = spawn_export_job(AnalysisExportRequest {
            db_path: export_context.db_path,
            filter_config_path: export_context.filter_config_path,
            annotation_csv_path: export_context.annotation_csv_path,
            output_csv_path,
            runtime,
        });

        self.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
        self.analysis_runtime_state.status = AnalysisJobStatus::RunningExport { job_id };
        Ok(())
    }

    fn poll_analysis_job(&mut self, ctx: &egui::Context) {
        let Some(running_job) = self.analysis_runtime_state.current_job.as_ref() else {
            return;
        };

        match running_job.receiver.try_recv() {
            Ok(AnalysisJobEvent::AnalysisCompleted(result)) => {
                self.analysis_runtime_state.current_job = None;
                match result {
                    Ok(success) => self.handle_analysis_success(success),
                    Err(failure) => self.handle_analysis_failure(failure),
                }
                ctx.request_repaint();
            }
            Ok(AnalysisJobEvent::ExportCompleted(result)) => {
                self.analysis_runtime_state.current_job = None;
                match result {
                    Ok(success) => self.handle_export_success(success),
                    Err(failure) => self.handle_analysis_failure(failure),
                }
                ctx.request_repaint();
            }
            Err(TryRecvError::Empty) => {
                ctx.request_repaint_after(Duration::from_millis(100));
            }
            Err(TryRecvError::Disconnected) => {
                self.analysis_runtime_state.current_job = None;
                self.analysis_runtime_state.status = AnalysisJobStatus::Failed {
                    summary: "分析ジョブの完了通知を受け取れませんでした".to_string(),
                };
                ctx.request_repaint();
            }
        }
    }

    fn handle_analysis_success(&mut self, success: AnalysisJobSuccess) {
        let warnings = success.meta.warning_messages.clone();
        let warning_count = warnings.len();
        let source_label = format!("分析結果: {}", success.meta.job_id);
        self.replace_records(success.records, source_label);
        let mut summary = format!(
            "{} 件抽出 / {:.2} 秒",
            success.meta.selected_paragraph_count, success.meta.duration_seconds
        );
        if warning_count > 0 {
            summary.push_str(&format!(" / 警告 {} 件", warning_count));
        }
        self.analysis_runtime_state.last_warnings = warnings;
        self.analysis_runtime_state.warning_window_open = false;
        let annotation_csv_path = self
            .analysis_runtime_state
            .runtime
            .as_ref()
            .map(|runtime| runtime.annotation_csv_path.clone())
            .or_else(|| self.resolved_annotation_csv_path().ok())
            .unwrap_or_default();
        self.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
            db_path: PathBuf::from(&success.meta.db_path),
            filter_config_path: PathBuf::from(&success.meta.filter_config_path),
            annotation_csv_path,
        });
        self.analysis_runtime_state.status = AnalysisJobStatus::Succeeded { summary };
    }

    fn handle_export_success(&mut self, success: AnalysisExportSuccess) {
        self.analysis_runtime_state.status = AnalysisJobStatus::Succeeded {
            summary: format!(
                "CSV 保存完了: {}",
                success.output_csv_path.display()
            ),
        };
        self.error_message = Some(format!(
            "CSV を保存しました。\n\n保存先:\n{}",
            success.output_csv_path.display()
        ));
    }

    fn handle_analysis_failure(&mut self, failure: AnalysisJobFailure) {
        let warnings = failure
            .meta
            .as_ref()
            .map(|meta| meta.warning_messages.clone())
            .unwrap_or_default();
        let summary = failure.message.clone();
        self.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary };
        self.analysis_runtime_state.last_warnings = warnings;
        self.analysis_runtime_state.warning_window_open = false;

        let mut error_message = failure.message;
        if !failure.stderr.is_empty() {
            error_message.push_str("\n\nstderr:\n");
            error_message.push_str(&failure.stderr);
        }
        if let Some(meta) = failure.meta {
            if !meta.error_summary.trim().is_empty() {
                error_message.push_str("\n\nmeta.errorSummary:\n");
                error_message.push_str(&meta.error_summary);
            }
        }
        self.error_message = Some(error_message);
    }

    fn warning_headline(&self, warning: &AnalysisWarningMessage) -> String {
        match warning.code.as_str() {
            "distance_match_fallback" => {
                let requested_mode = warning.requested_mode.as_deref().unwrap_or("unknown");
                let used_mode = warning.used_mode.as_deref().unwrap_or("unknown");
                match (warning.combination_count, warning.combination_cap) {
                    (Some(count), Some(cap)) if count > cap => format!(
                        "distance matching: {requested_mode} -> {used_mode} ({count} / cap {cap}, +{})",
                        count - cap
                    ),
                    (Some(count), Some(cap)) => {
                        format!("distance matching: {requested_mode} -> {used_mode} ({count} / cap {cap})")
                    }
                    _ => format!("distance matching: {requested_mode} -> {used_mode}"),
                }
            }
            code if code.ends_with("_defaulted") => warning
                .field_name
                .as_ref()
                .map(|field_name| format!("設定を既定値へ補正: {field_name}"))
                .filter(|headline| !headline.trim().is_empty())
                .or_else(|| (!warning.message.trim().is_empty()).then(|| warning.message.clone()))
                .unwrap_or_else(|| format!("警告コード: {}", warning.code)),
            code if code.starts_with("sqlite_") => warning
                .query_name
                .as_ref()
                .map(|query_name| format!("DB 読込失敗: {query_name}"))
                .filter(|headline| !headline.trim().is_empty())
                .or_else(|| (!warning.message.trim().is_empty()).then(|| warning.message.clone()))
                .unwrap_or_else(|| format!("警告コード: {}", warning.code)),
            _ if !warning.message.trim().is_empty() => warning.message.clone(),
            _ if !warning.code.trim().is_empty() => format!("警告コード: {}", warning.code),
            _ => "詳細不明の警告".to_string(),
        }
    }

    fn warning_detail_lines(&self, warning: &AnalysisWarningMessage) -> Vec<String> {
        let mut lines = Vec::new();
        if !warning.message.trim().is_empty() && self.warning_headline(warning) != warning.message {
            lines.push(format!("message: {}", warning.message));
        }
        if !warning.code.trim().is_empty() {
            lines.push(format!("code: {}", warning.code));
        }
        if let Some(severity) = &warning.severity {
            lines.push(format!("severity: {severity}"));
        }
        if let Some(scope) = &warning.scope {
            lines.push(format!("scope: {scope}"));
        }
        if let Some(condition_id) = &warning.condition_id {
            lines.push(format!("conditionId: {condition_id}"));
        }
        if let Some(field_name) = &warning.field_name {
            lines.push(format!("fieldName: {field_name}"));
        }
        if let Some(unit_id) = warning.unit_id {
            lines.push(format!("unitId: {unit_id}"));
        }
        if let Some(query_name) = &warning.query_name {
            lines.push(format!("queryName: {query_name}"));
        }
        match (&warning.requested_mode, &warning.used_mode) {
            (Some(requested_mode), Some(used_mode)) => {
                lines.push(format!("mode: {requested_mode} -> {used_mode}"));
            }
            (Some(requested_mode), None) => lines.push(format!("requestedMode: {requested_mode}")),
            (None, Some(used_mode)) => lines.push(format!("usedMode: {used_mode}")),
            (None, None) => {}
        }
        match (warning.combination_count, warning.combination_cap) {
            (Some(count), Some(cap)) if count > cap => {
                lines.push(format!("combinationCount: {count} / cap {cap} (+{})", count - cap));
            }
            (Some(count), Some(cap)) => lines.push(format!("combinationCount: {count} / cap {cap}")),
            (Some(count), None) => lines.push(format!("combinationCount: {count}")),
            (None, Some(cap)) => lines.push(format!("combinationCap: {cap}")),
            (None, None) => {}
        }
        if let Some(safety_limit) = warning.safety_limit {
            lines.push(format!("safetyLimit: {safety_limit}"));
        }
        if let Some(db_path) = &warning.db_path {
            lines.push(format!("dbPath: {db_path}"));
        }
        lines
    }

    fn draw_warning_details_window(&mut self, ctx: &egui::Context) {
        if !self.analysis_runtime_state.warning_window_open {
            return;
        }

        let mut window_open = self.analysis_runtime_state.warning_window_open;
        egui::Window::new(format!(
            "警告詳細 ({})",
            self.analysis_runtime_state.last_warnings.len()
        ))
        .open(&mut window_open)
        .resizable(true)
        .default_width(620.0)
        .show(ctx, |ui| {
            ScrollArea::vertical()
                .max_height(480.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for (idx, warning) in self.analysis_runtime_state.last_warnings.iter().enumerate() {
                        ui.group(|ui| {
                            ui.label(
                                RichText::new(format!("{}. {}", idx + 1, self.warning_headline(warning))).strong(),
                            );
                            for line in self.warning_detail_lines(warning) {
                                ui.add(
                                    egui::Label::new(line)
                                        .wrap_mode(TextWrapMode::Wrap),
                                );
                            }
                        });
                        if idx + 1 < self.analysis_runtime_state.last_warnings.len() {
                            ui.add_space(6.0);
                        }
                    }
                });
        });
        self.analysis_runtime_state.warning_window_open = window_open;
    }

    fn guard_root_close_with_dirty_editor(&mut self, ctx: &egui::Context) {
        if !self.condition_editor_state.is_dirty {
            return;
        }
        let close_requested = ctx.input(|input| input.viewport().close_requested());
        if !close_requested {
            return;
        }

        ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
        self.error_message = Some(
            "condition editor に未保存の変更があるため、アプリ終了を中止しました。保存または破棄してから閉じてください。"
                .to_string(),
        );
        if self.condition_editor_state.window_open {
            self.focus_condition_editor_viewport(ctx);
        }
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
        let response = ui.add_enabled(enabled, egui::Button::new("DB参照"));
        if response.clicked() {
            if let Err(error) = self.open_db_viewer_for_selected_record() {
                self.error_message = Some(error);
            }
        }
    }

    fn open_db_viewer_for_selected_record(&mut self) -> Result<(), String> {
        self.prepare_db_viewer_state()?;
        self.load_db_viewer_context();
        Ok(())
    }

    fn load_db_viewer_context(&mut self) {
        let Some(paragraph_id) = self.db_viewer_state.source_paragraph_id else {
            self.db_viewer_state.context = None;
            self.db_viewer_state.error_message = Some("参照元 paragraph_id が未設定です".to_string());
            self.db_viewer_state.is_open = true;
            return;
        };

        match fetch_paragraph_context(&self.db_viewer_state.db_path, paragraph_id) {
            Ok(context) => {
                self.db_viewer_state.context = Some(context);
                self.db_viewer_state.error_message = None;
                self.db_viewer_state.is_open = true;
            }
            Err(error) => {
                self.db_viewer_state.context = None;
                self.db_viewer_state.error_message = Some(error);
                self.db_viewer_state.is_open = true;
            }
        }
    }

    fn load_db_viewer_context_for_location(&mut self, document_id: i64, paragraph_no: i64) {
        match fetch_paragraph_context_by_location(
            &self.db_viewer_state.db_path,
            document_id,
            paragraph_no,
        ) {
            Ok(context) => {
                self.db_viewer_state.context = Some(context);
                self.db_viewer_state.error_message = None;
            }
            Err(error) => {
                self.db_viewer_state.context = None;
                self.db_viewer_state.error_message = Some(error);
            }
        }
    }

    fn previous_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.db_viewer_state.context.as_ref()?;
        let previous_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no < context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .max()?;

        Some((context.center.document_id, previous_paragraph_no))
    }

    fn next_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.db_viewer_state.context.as_ref()?;
        let next_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no > context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .min()?;

        Some((context.center.document_id, next_paragraph_no))
    }

    fn draw_db_viewer_window(&mut self, ctx: &egui::Context) {
        if !self.db_viewer_state.is_open {
            return;
        }

        let snapshot = self.db_viewer_state.clone();
        let previous_location = self.previous_db_viewer_location();
        let next_location = self.next_db_viewer_location();
        let mut requested_location = None;
        let mut close_requested = false;
        let viewport_id = egui::ViewportId::from_hash_of(DB_VIEWER_VIEWPORT_ID);
        let builder = egui::ViewportBuilder::default()
            .with_title("DB コンテキスト参照")
            .with_inner_size([760.0, 820.0])
            .with_resizable(true);

        ctx.show_viewport_immediate(viewport_id, builder, |viewport_ctx, class| {
            close_requested = viewport_ctx.input(|input| input.viewport().close_requested());

            match class {
                egui::ViewportClass::Embedded => {
                    let mut fallback_open = true;
                    egui::Window::new("DB コンテキスト参照")
                        .open(&mut fallback_open)
                        .default_width(760.0)
                        .default_height(820.0)
                        .resizable(true)
                        .show(viewport_ctx, |ui| {
                            render_db_viewer_contents(
                                ui,
                                &snapshot,
                                previous_location,
                                next_location,
                                &mut requested_location,
                            );
                        });

                    if !fallback_open {
                        close_requested = true;
                    }
                }
                _ => {
                    egui::CentralPanel::default().show(viewport_ctx, |ui| {
                        render_db_viewer_contents(
                            ui,
                            &snapshot,
                            previous_location,
                            next_location,
                            &mut requested_location,
                        );
                    });
                }
            }
        });

        if close_requested {
            self.db_viewer_state.is_open = false;
            return;
        }

        if let Some((document_id, paragraph_no)) = requested_location {
            self.load_db_viewer_context_for_location(document_id, paragraph_no);
            ctx.request_repaint();
        }
    }

    fn draw_toolbar(&mut self, ui: &mut Ui) {
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                ui.label("表示元:");
                let path_str = self.records_source_label.clone();
                ui.add(
                    ime_safe_singleline(&mut path_str.as_str())
                        .desired_width(600.0)
                        .interactive(false),
                );

                if ui.button("CSVを開く").clicked() {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("CSV files", &["csv"])
                        .add_filter("All files", &["*"])
                        .pick_file()
                    {
                        self.load_csv(path);
                    }
                }

                ui.separator();
                let selected_position = self
                    .selected_row
                    .map(|idx| idx + 1)
                    .map(|position| position.to_string())
                    .unwrap_or_else(|| "-".to_string());
                ui.label(format!(
                    "総件数: {} 件  抽出後: {} 件  選択: {} / {}",
                    self.all_records.len(),
                    self.filtered_indices.len(),
                    selected_position,
                    self.filtered_indices.len()
                ));
            });

            ui.horizontal_wrapped(|ui| {
                let can_start = self.analysis_runtime_state.can_start();
                let can_export = self.analysis_runtime_state.can_export();
                let settings_enabled = self.analysis_runtime_state.current_job.is_none();
                let python_label = self
                    .analysis_runtime_state
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.python_label.clone())
                    .unwrap_or_else(|| "-".to_string());
                let filter_config_label = self
                    .analysis_runtime_state
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.filter_config_path.display().to_string())
                    .unwrap_or_else(|| "-".to_string());
                let annotation_label = self
                    .resolved_annotation_csv_path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| "-".to_string());
                let db_label = self.db_viewer_state.db_path.display().to_string();

                if matches!(
                    self.analysis_runtime_state.status,
                    AnalysisJobStatus::RunningAnalysis { .. } | AnalysisJobStatus::RunningExport { .. }
                ) {
                    ui.add(egui::Spinner::new());
                }

                if ui
                    .add_enabled(can_start, egui::Button::new("分析実行"))
                    .clicked()
                {
                    if let Err(error) = self.start_analysis_job() {
                        self.error_message = Some(error);
                    }
                }

                if ui
                    .add_enabled(can_export, egui::Button::new("CSV保存(全件)"))
                    .clicked()
                {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("CSV files", &["csv"])
                        .set_file_name("analysis-result.csv")
                        .save_file()
                    {
                        if let Err(error) = self.start_export_job(path) {
                            self.error_message = Some(error);
                        }
                    }
                }

                if ui
                    .add_enabled(settings_enabled, egui::Button::new("分析設定"))
                    .clicked()
                {
                    self.analysis_request_state.settings_window_open = true;
                }

                if ui
                    .add_enabled(settings_enabled, egui::Button::new("条件編集"))
                    .clicked()
                {
                    if let Err(error) = self.open_condition_editor(ui.ctx()) {
                        self.error_message = Some(error);
                    }
                }

                ui.label(format!("DB: {db_label}"));
                ui.label(format!("条件: {filter_config_label}"));
                ui.label(format!("Annotation: {annotation_label}"));
                ui.label(format!("Python: {python_label}"));
                if can_export {
                    ui.label("保存対象は直近分析結果の全件です");
                }
                if self.analysis_runtime_state.has_warning_details()
                    && ui.button("警告詳細").clicked()
                {
                    self.analysis_runtime_state.warning_window_open = true;
                }

                let status_text = self.analysis_runtime_state.status_text();
                let status_color =
                    analysis_status_color(ui, &self.analysis_runtime_state.status);
                ui.label(RichText::new(status_text).color(status_color));
            });
        });
    }

    fn draw_analysis_settings_window(&mut self, ctx: &egui::Context) {
        if !self.analysis_request_state.settings_window_open {
            return;
        }

        let mut window_open = self.analysis_request_state.settings_window_open;
        let mut selected_python_path = None;
        let mut selected_filter_config_path = None;
        let mut selected_annotation_csv_path = None;
        let mut clear_python_override = false;
        let mut clear_filter_config_override = false;
        let mut clear_annotation_csv_override = false;
        let settings_enabled = self.analysis_runtime_state.current_job.is_none();
        let python_override_label = self
            .analysis_request_state
            .python_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "自動解決".to_string());
        let filter_override_label = self
            .analysis_request_state
            .filter_config_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "既定値 (asset/cooccurrence-conditions.json)".to_string());
        let annotation_override_label = self
            .analysis_request_state
            .annotation_csv_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "既定値 (asset/manual-annotations.csv)".to_string());
        let resolved_python_label = self
            .analysis_runtime_state
            .runtime
            .as_ref()
            .map(|runtime| runtime.python_label.clone())
            .unwrap_or_else(|| "-".to_string());
        let resolved_filter_label = self
            .analysis_runtime_state
            .runtime
            .as_ref()
            .map(|runtime| runtime.filter_config_path.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        let resolved_annotation_label = self
            .resolved_annotation_csv_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let status_text = self.analysis_runtime_state.status_text();

        egui::Window::new("分析設定")
            .open(&mut window_open)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label("分析実行に使う Python と条件 JSON を切り替えます。");
                ui.label("この設定は現在のセッション内だけで有効です。");
                ui.separator();

                draw_analysis_path_override_row(
                    ui,
                    "Python 実行ファイル",
                    &python_override_label,
                    "自動解決",
                    settings_enabled,
                    || {
                        rfd::FileDialog::new()
                            .add_filter("Python", &["exe"])
                            .add_filter("All files", &["*"])
                            .pick_file()
                    },
                    &mut selected_python_path,
                    &mut clear_python_override,
                );
                ui.label(format!("現在の解決結果: {resolved_python_label}"));
                ui.separator();

                draw_analysis_path_override_row(
                    ui,
                    "条件 JSON",
                    &filter_override_label,
                    "既定値",
                    settings_enabled,
                    || {
                        rfd::FileDialog::new()
                            .add_filter("JSON files", &["json"])
                            .add_filter("All files", &["*"])
                            .pick_file()
                    },
                    &mut selected_filter_config_path,
                    &mut clear_filter_config_override,
                );
                ui.label(format!("現在の解決結果: {resolved_filter_label}"));
                ui.separator();

                draw_analysis_path_override_row(
                    ui,
                    "annotation CSV",
                    &annotation_override_label,
                    "既定値",
                    settings_enabled,
                    || {
                        rfd::FileDialog::new()
                            .add_filter("CSV files", &["csv"])
                            .set_file_name("manual-annotations.csv")
                            .save_file()
                    },
                    &mut selected_annotation_csv_path,
                    &mut clear_annotation_csv_override,
                );
                ui.label(format!("現在の解決結果: {resolved_annotation_label}"));
                ui.separator();

                ui.label(format!("状態: {status_text}"));
                if !settings_enabled {
                    ui.label("分析ジョブ実行中は設定を変更できません。");
                }
            });

        self.analysis_request_state.settings_window_open = window_open;

        let mut runtime_changed = false;
        if let Some(path) = selected_python_path {
            self.analysis_request_state.python_path_override = Some(path);
            runtime_changed = true;
        }
        if clear_python_override {
            self.analysis_request_state.python_path_override = None;
            runtime_changed = true;
        }
        if let Some(path) = selected_filter_config_path {
            self.analysis_request_state.filter_config_path_override = Some(path);
            runtime_changed = true;
        }
        if clear_filter_config_override {
            self.analysis_request_state.filter_config_path_override = None;
            runtime_changed = true;
        }
        if let Some(path) = selected_annotation_csv_path {
            self.analysis_request_state.annotation_csv_path_override = Some(path);
            runtime_changed = true;
        }
        if clear_annotation_csv_override {
            self.analysis_request_state.annotation_csv_path_override = None;
            runtime_changed = true;
        }

        if runtime_changed {
            self.refresh_analysis_runtime();
            ctx.request_repaint();
        }
    }

    fn draw_condition_editor_header_panel(
        &self,
        ui: &mut Ui,
        can_modify: bool,
        loaded_path_label: &str,
        resolved_path_label: &str,
    ) {
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("読込中: {loaded_path_label}"));
            ui.label(format!("現在の解決先: {resolved_path_label}"));
        });

        if let Some(pending_path) = self.condition_editor_state.pending_path_sync.as_ref() {
            ui.colored_label(
                Color32::from_rgb(200, 64, 64),
                format!(
                    "分析設定で条件 JSON の解決先が変更されています。保存前に再読込してください: {}",
                    pending_path.display()
                ),
            );
        }

        if let Some(status_message) = &self.condition_editor_state.status_message {
            ui.colored_label(
                editor_status_color(self.condition_editor_state.status_is_error),
                status_message,
            );
        }

        if self.condition_editor_state.projected_legacy_condition_count > 0 {
            ui.colored_label(
                Color32::from_rgb(180, 120, 40),
                format!(
                    "legacy 投影: {} 件の condition を group editor 表示へ変換中",
                    self.condition_editor_state.projected_legacy_condition_count
                ),
            );
        }

        if !can_modify {
            ui.label("分析ジョブ実行中は条件 JSON を保存・再読込できません。");
        }
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
                    ui.vertical(|ui| {
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("condition 一覧").strong());
                            if ui
                                .add_enabled(can_modify, egui::Button::new("追加"))
                                .clicked()
                            {
                                command_draft.should_add_condition = true;
                            }
                        });

                        ScrollArea::vertical()
                            .id_salt("condition_editor_list_scroll")
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                let Some(document) = self.condition_editor_state.document.as_ref() else {
                                    ui.label(RichText::new("条件 JSON 未読込").italics());
                                    return;
                                };
                                if document.cooccurrence_conditions.is_empty() {
                                    ui.label(RichText::new("condition がありません").italics());
                                } else {
                                    for (index, condition) in document.cooccurrence_conditions.iter().enumerate() {
                                        let selected =
                                            selection_draft.requested_selection == Some(index);
                                        let categories_preview =
                                            summarize_condition_list(&condition.categories, 2);
                                        let label = format!(
                                            "{}. {} [{}] groups:{} forms:{} filters:{} refs:{}",
                                            index + 1,
                                            condition.condition_id,
                                            categories_preview,
                                            condition_group_count(condition),
                                            total_forms_count(condition),
                                            condition.annotation_filters.len(),
                                            condition.required_categories_all.len()
                                                + condition.required_categories_any.len()
                                        );
                                        if ui.selectable_label(selected, label).clicked() {
                                            selection_draft.requested_selection = Some(index);
                                            selection_draft.requested_group_selection =
                                                clamp_condition_index(Some(0), condition.form_groups.len());
                                        }
                                    }
                                }
                            });
                    });
                });

                strip.cell(|ui| {
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
                                    let should_mark_dirty = if let Some(document) =
                                        self.condition_editor_state.document.as_mut()
                                    {
                                        let mut changed = false;
                                        selection_draft.requested_selection = clamp_condition_index(
                                            selection_draft.requested_selection,
                                            document.cooccurrence_conditions.len(),
                                        );
                                        selection_draft.requested_group_selection =
                                            clamp_condition_group_selection_for_document(
                                                document,
                                                selection_draft.requested_selection,
                                                selection_draft.requested_group_selection,
                                            );
                                        if let Some(selected_index) =
                                            selection_draft.requested_selection
                                        {
                                            if let Some(condition) =
                                                document.cooccurrence_conditions.get_mut(selected_index)
                                            {
                                                ui.horizontal(|ui| {
                                                    ui.label(RichText::new("condition 詳細").strong());
                                                    if ui
                                                        .add_enabled(
                                                            can_modify,
                                                            egui::Button::new("condition削除"),
                                                        )
                                                        .clicked()
                                                    {
                                                        command_draft.should_delete_condition =
                                                            Some(selected_index);
                                                    }
                                                });
                                                ui.horizontal(|ui| {
                                                    ui.add_sized(
                                                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                                                        egui::Label::new("condition_id"),
                                                    );
                                                    let response = ui.add_sized(
                                                        [CONDITION_EDITOR_TEXT_INPUT_WIDTH, 0.0],
                                                        ime_safe_singleline(
                                                            &mut condition.condition_id,
                                                        ),
                                                    );
                                                    changed |= response.changed();
                                                });

                                                ui.horizontal(|ui| {
                                                    ui.add_sized(
                                                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                                                        egui::Label::new("overall_search_scope"),
                                                    );
                                                    changed |= edit_optional_choice(
                                                        ui,
                                                        &mut condition.overall_search_scope,
                                                        &["paragraph", "sentence"],
                                                        CONDITION_EDITOR_CHOICE_WIDTH,
                                                    );
                                                });

                                                ui.add_space(8.0);
                                                changed |= draw_string_list_editor(
                                                    ui,
                                                    "categories",
                                                    &mut condition.categories,
                                                );

                                                ui.group(|ui| {
                                                    ui.horizontal(|ui| {
                                                        ui.label(RichText::new("form_groups").strong());
                                                        if ui.button("group追加").clicked() {
                                                            condition.form_groups.push(
                                                                build_default_form_group_item(
                                                                    condition.form_groups.len(),
                                                                ),
                                                            );
                                                            selection_draft.requested_group_selection =
                                                                clamp_condition_index(
                                                                    Some(condition.form_groups.len() - 1),
                                                                    condition.form_groups.len(),
                                                                );
                                                            changed = true;
                                                        }
                                                    });

                                                    if condition.form_groups.is_empty() {
                                                        ui.label(RichText::new("group がありません").italics());
                                                    } else {
                                                        let mut remove_group_index = None;
                                                        for (group_index, group) in
                                                            condition.form_groups.iter().enumerate()
                                                        {
                                                            ui.horizontal(|ui| {
                                                                let label =
                                                                    summarize_form_group_label(
                                                                        group,
                                                                        group_index,
                                                                    );
                                                                if ui
                                                                    .selectable_label(
                                                                        selection_draft
                                                                            .requested_group_selection
                                                                            == Some(group_index),
                                                                        label,
                                                                    )
                                                                    .clicked()
                                                                {
                                                                    selection_draft
                                                                        .requested_group_selection =
                                                                        Some(group_index);
                                                                }
                                                                if ui.button("削除").clicked() {
                                                                    remove_group_index =
                                                                        Some(group_index);
                                                                }
                                                            });
                                                        }

                                                        if let Some(group_index) = remove_group_index {
                                                            condition.form_groups.remove(group_index);
                                                            selection_draft.requested_group_selection =
                                                                clamp_condition_index(
                                                                    selection_draft
                                                                        .requested_group_selection
                                                                        .map(|current| {
                                                                            if current > group_index {
                                                                                current - 1
                                                                            } else {
                                                                                current
                                                                            }
                                                                        }),
                                                                    condition.form_groups.len(),
                                                                );
                                                            changed = true;
                                                        }

                                                        selection_draft.requested_group_selection =
                                                            clamp_condition_index(
                                                                selection_draft.requested_group_selection,
                                                                condition.form_groups.len(),
                                                            );
                                                        let overall_search_scope =
                                                            condition.overall_search_scope.clone();
                                                        let search_scope_locked =
                                                            !condition.annotation_filters.is_empty();
                                                        if let Some(group_index) =
                                                            selection_draft
                                                                .requested_group_selection
                                                        {
                                                            if let Some(group) =
                                                                condition.form_groups.get_mut(group_index)
                                                            {
                                                                changed |= draw_form_group_editor(
                                                                    ui,
                                                                    group_index,
                                                                    overall_search_scope.as_deref(),
                                                                    search_scope_locked,
                                                                    group,
                                                                );
                                                            }
                                                        }
                                                    }
                                                });

                                                if condition.projected_from_legacy {
                                                    ui.colored_label(
                                                        Color32::from_rgb(180, 120, 40),
                                                        "legacy 条件を group editor 表示へ投影中です。保存時に互換形式または新形式へ正規化されます。",
                                                    );
                                                }

                                                changed |= draw_annotation_filter_editor(
                                                    ui,
                                                    &mut condition.annotation_filters,
                                                );
                                                changed |= draw_string_list_editor(
                                                    ui,
                                                    "required_categories_all",
                                                    &mut condition.required_categories_all,
                                                );
                                                changed |= draw_string_list_editor(
                                                    ui,
                                                    "required_categories_any",
                                                    &mut condition.required_categories_any,
                                                );
                                            } else {
                                                ui.label(
                                                    RichText::new("condition を選択してください")
                                                        .italics(),
                                                );
                                            }
                                        } else {
                                            ui.label(
                                                RichText::new("condition を選択してください")
                                                    .italics(),
                                            );
                                        }
                                        changed
                                    } else {
                                        ui.label(RichText::new("条件 JSON 未読込").italics());
                                        false
                                    };

                                    if should_mark_dirty {
                                        self.mark_condition_editor_dirty();
                                    }
                                });
                        });
                });
            });
    }

    fn draw_condition_editor_footer_panel(
        &self,
        ui: &mut Ui,
        can_modify: bool,
        resolved_path_ok: bool,
        command_draft: &mut ConditionEditorCommandDraft,
    ) {
        ui.horizontal(|ui| {
            let save_enabled = can_modify
                && self.condition_editor_state.document.is_some()
                && self.condition_editor_state.pending_path_sync.is_none()
                && resolved_path_ok;
            if ui
                .add_enabled(save_enabled, egui::Button::new("保存"))
                .clicked()
            {
                command_draft.should_save = true;
            }
            if ui
                .add_enabled(can_modify && resolved_path_ok, egui::Button::new("再読込"))
                .clicked()
            {
                command_draft.should_reload = true;
            }
            if self.condition_editor_state.is_dirty {
                ui.label("未保存");
            } else {
                ui.label("保存済み");
            }
        });
    }

    fn draw_condition_editor_confirm_overlay(
        &self,
        viewport_ctx: &egui::Context,
        confirm_action: &ConditionEditorConfirmAction,
        command_draft: &mut ConditionEditorCommandDraft,
    ) {
        let screen_rect = viewport_ctx.screen_rect();

        egui::Area::new(egui::Id::new("condition_editor_confirm_overlay"))
            .order(egui::Order::Foreground)
            .fixed_pos(screen_rect.min)
            .show(viewport_ctx, |ui| {
                ui.set_min_size(screen_rect.size());
                ui.painter().rect_filled(
                    ui.max_rect(),
                    0.0,
                    Color32::from_black_alpha(160),
                );
                ui.with_layout(
                    egui::Layout::top_down(egui::Align::Center),
                    |ui| {
                        ui.add_space((screen_rect.height() * 0.22).max(80.0));
                        egui::Frame::window(ui.style()).show(ui, |ui| {
                            ui.set_min_width(420.0);
                            match confirm_action {
                                ConditionEditorConfirmAction::CloseWindow => {
                                    ui.label("未保存の変更があります。condition editor を閉じますか。");
                                }
                                ConditionEditorConfirmAction::ReloadPath(path) => {
                                    ui.label(format!(
                                        "未保存の変更があります。次の条件 JSON を再読込すると変更は破棄されます。\n{}",
                                        path.display()
                                    ));
                                }
                            }
                            ui.add_space(8.0);
                            ui.horizontal(|ui| {
                                if ui.button("続行").clicked() {
                                    command_draft.modal_response =
                                        Some(ConditionEditorModalResponse::Continue);
                                }
                                if ui.button("キャンセル").clicked() {
                                    command_draft.modal_response =
                                        Some(ConditionEditorModalResponse::Cancel);
                                }
                            });
                        });
                    },
                );
            });
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
                self.draw_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                );
                ui.separator();
                self.draw_condition_editor_body_panel(
                    ui,
                    can_modify,
                    selection_draft,
                    command_draft,
                );
                ui.separator();
                self.draw_condition_editor_footer_panel(
                    ui,
                    can_modify,
                    resolved_path_ok,
                    command_draft,
                );
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
                self.draw_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                );
            });

        egui::TopBottomPanel::bottom("condition_editor_viewport_footer")
            .frame(
                egui::Frame::default()
                    .fill(panel_fill)
                    .inner_margin(egui::Margin::same(10)),
            )
            .show(viewport_ctx, |ui| {
                self.draw_condition_editor_footer_panel(
                    ui,
                    can_modify,
                    resolved_path_ok,
                    command_draft,
                );
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
                self.condition_editor_state.selected_index =
                    clamp_condition_index(Some(delete_index), document.cooccurrence_conditions.len());
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
            Ok(path) => self
                .request_condition_editor_reload(path.clone())
                .err(),
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

    fn draw_condition_editor_window(&mut self, ctx: &egui::Context) {
        if !self.condition_editor_state.window_open {
            return;
        }

        let viewport_id = egui::ViewportId::from_hash_of(CONDITION_EDITOR_VIEWPORT_ID);
        let builder = egui::ViewportBuilder::default()
            .with_title("条件編集")
            .with_inner_size([1120.0, 760.0])
            .with_resizable(true);
        let can_modify = self.analysis_runtime_state.current_job.is_none();
        let resolved_path_result = self.resolved_filter_config_path();
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
        let mut selection_draft = self.condition_editor_selection_draft();
        let mut command_draft = ConditionEditorCommandDraft::default();
        let mut save_error = None;
        let current_confirm_action = self.condition_editor_state.confirm_action.clone();
        let panel_fill = ctx.style().visuals.panel_fill;

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
                        can_modify,
                        &loaded_path_label,
                        &resolved_path_label,
                        resolved_path_result.is_ok(),
                        &mut selection_draft,
                        &mut command_draft,
                    );
                }
                _ => {
                    self.draw_condition_editor_viewport_panels(
                        viewport_ctx,
                        can_modify,
                        &loaded_path_label,
                        &resolved_path_label,
                        resolved_path_result.is_ok(),
                        panel_fill,
                        &mut selection_draft,
                        &mut command_draft,
                    );
                }
            }

            if let Some(confirm_action) = current_confirm_action.as_ref() {
                self.draw_condition_editor_confirm_overlay(
                    viewport_ctx,
                    confirm_action,
                    &mut command_draft,
                );
            }
        });

        self.apply_condition_editor_selection_draft(selection_draft);
        self.apply_condition_editor_close_request(command_draft.close_requested);
        self.apply_condition_editor_add_request(command_draft.should_add_condition);
        self.apply_condition_editor_delete_request(command_draft.should_delete_condition);

        if command_draft.should_save {
            if let Err(error) = self.save_condition_editor_document() {
                save_error = Some(error);
            }
        }
        let reload_error = self.apply_condition_editor_reload_request(
            command_draft.should_reload,
            &resolved_path_result,
        );
        self.apply_condition_editor_modal_response(
            ctx,
            viewport_id,
            command_draft.modal_response,
        );

        if let Some(error) = save_error.or(reload_error) {
            self.condition_editor_state.status_message = Some(error);
            self.condition_editor_state.status_is_error = true;
        }
    }

    fn draw_body(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let mut clicked_row = None;
        egui::SidePanel::left("record_list_panel")
            .resizable(true)
            .default_width(620.0)
            .min_width(360.0)
            .show_inside(ui, |ui| {
                self.draw_filters(ui);
                ui.separator();
                clicked_row = self.draw_tree(ui, tree_scroll_request);
            });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.draw_detail(ui);
        });

        clicked_row
    }

    fn draw_filters(&mut self, ui: &mut Ui) {
        let active_count: usize = self
            .selected_filter_values
            .values()
            .map(BTreeSet::len)
            .sum();

        egui::CollapsingHeader::new(format!("Filters ({})", active_count))
            .id_salt("filters_panel")
            .default_open(true)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label("フィルター対象:");
                    egui::ComboBox::from_id_salt("filter_column_selector")
                        .selected_text(self.active_filter_column.label())
                        .show_ui(ui, |ui| {
                            for &column in FilterColumn::all() {
                                ui.selectable_value(
                                    &mut self.active_filter_column,
                                    column,
                                    column.label(),
                                );
                            }
                        });
                    ui.label(format!("適用中: {} 件", active_count));
                    if ui.button("現在の列をクリア").clicked() {
                        self.clear_filters_for_column(self.active_filter_column);
                    }
                    if ui.button("全解除").clicked() {
                        self.clear_all_filters();
                    }
                });

                let options = self
                    .filter_options
                    .get(&self.active_filter_column)
                    .cloned()
                    .unwrap_or_default();

                ScrollArea::vertical()
                    .id_salt("filter_options_scroll")
                    .max_height(180.0)
                    .show(ui, |ui| {
                        if options.is_empty() {
                            ui.label(RichText::new("候補なし").italics());
                        } else {
                            for option in options {
                                let is_selected = self
                                    .selected_filter_values
                                    .get(&self.active_filter_column)
                                    .is_some_and(|values| values.contains(&option.value));
                                let mut checked = is_selected;
                                let label = format!(
                                    "{} ({})",
                                    display_filter_value(&option.value),
                                    option.count
                                );
                                if ui.checkbox(&mut checked, label).changed() {
                                    self.toggle_filter_value(
                                        self.active_filter_column,
                                        &option.value,
                                        checked,
                                    );
                                }
                            }
                        }
                    });

                if !self.selected_filter_values.is_empty() {
                    ui.add_space(4.0);
                    ui.horizontal_wrapped(|ui| {
                        ui.label("適用中:");
                        let active_values: Vec<(FilterColumn, String)> = self
                            .selected_filter_values
                            .iter()
                            .flat_map(|(column, values)| {
                                values
                                    .iter()
                                    .cloned()
                                    .map(|value| (*column, value))
                                    .collect::<Vec<_>>()
                            })
                            .collect();
                        for (column, value) in active_values {
                            let button_label =
                                format!("{}: {} ×", column.label(), display_filter_value(&value));
                            if ui.small_button(button_label).clicked() {
                                self.toggle_filter_value(column, &value, false);
                            }
                        }
                    });
                }
            });
    }

    fn draw_tree(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let filtered_indices = &self.filtered_indices;
        let selected_row = self.selected_row;
        let mut clicked_row = None;
        let selected_fill = Color32::from_rgb(70, 130, 180);
        let mut table = TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

        for spec in TREE_COLUMN_SPECS {
            table = table.column((spec.build_column)());
        }

        if let Some(scroll_request) = tree_scroll_request {
            if scroll_request.row_index < filtered_indices.len() {
                table = table.scroll_to_row(scroll_request.row_index, scroll_request.align);
            }
        }

        table
            .header(24.0, |mut header| {
                for spec in TREE_COLUMN_SPECS {
                    header.col(|ui| {
                        ui.strong(spec.header);
                    });
                }
            })
            .body(|body| {
                body.rows(22.0, filtered_indices.len(), |mut row| {
                    let i = row.index();
                    let record = &self.all_records[filtered_indices[i]];
                    let is_selected = selected_row == Some(i);

                    let mut row_clicked = false;
                    for spec in TREE_COLUMN_SPECS {
                        let value = (spec.value)(record);
                        row.col(|ui| {
                            let cell_rect = ui.max_rect();
                            if is_selected {
                                ui.painter().rect_filled(cell_rect, 0.0, selected_fill);
                            }

                            let cell_response = ui.interact(
                                cell_rect,
                                ui.id().with("cell_click"),
                                egui::Sense::click(),
                            );

                            let rich_text = if is_selected {
                                RichText::new(value).color(Color32::WHITE)
                            } else {
                                RichText::new(value)
                            };

                            let label_response = ui.add(
                                egui::Label::new(rich_text)
                                    .truncate()
                                    .sense(egui::Sense::click()),
                            );
                            if (cell_response | label_response).clicked() {
                                row_clicked = true;
                            }
                        });
                    }

                    if row_clicked {
                        clicked_row = Some(i);
                    }
                });
            });

        clicked_row
    }

    fn draw_detail(&mut self, ui: &mut Ui) {
        if let Some(record) = self.selected_record().cloned() {
            self.draw_db_viewer_button(ui, true);
            ui.add_space(6.0);
            self.draw_record_summary(ui, &record);
            ui.separator();

            let detail_job = build_record_text_layout_job(ui, &self.get_segments());

            egui::TopBottomPanel::bottom("annotation_editor_panel")
                .resizable(false)
                .default_height(230.0)
                .min_height(200.0)
                .show_inside(ui, |ui| {
                    self.draw_annotation_editor_panel(ui, &record);
                });

            egui::CentralPanel::default().show_inside(ui, |ui| {
                self.draw_record_text_panel(ui, &record, detail_job);
            });
        } else {
            self.draw_db_viewer_button(ui, false);
            ui.add_space(6.0);
            ui.label(RichText::new("レコード未選択").italics());
        }
    }

    fn draw_record_summary(&self, ui: &mut Ui, record: &AnalysisRecord) {
        ui.label(
            RichText::new(format!(
                "{} / {} / paragraph_id={}",
                record.municipality_name, record.ordinance_or_rule, record.paragraph_id
            ))
            .size(14.0)
            .strong(),
        );

        ui.add_space(6.0);
        ui.label(format!("document_id: {}", record.document_id));
        ui.label(format!("doc_type: {}", record.doc_type));
        ui.label(format!("sentence_count: {}", record.sentence_count));

        ui.add_space(6.0);
        ui.label(format!("categories: {}", record.matched_categories_text));
        ui.label(format!("conditions: {}", record.matched_condition_ids_text));
        ui.label(format!("match_groups: {}", record.match_group_ids_text));
        ui.label(format!("annotated_tokens: {}", record.annotated_token_count));
        ui.label(format!(
            "manual_annotations: {}",
            record.manual_annotation_count
        ));
        if !record.mixed_scope_warning_text.trim().is_empty() {
            ui.colored_label(
                Color32::from_rgb(196, 110, 0),
                format!("promotion warning: {}", record.mixed_scope_warning_text),
            );
        }
    }

    fn draw_record_text_panel(
        &self,
        ui: &mut Ui,
        record: &AnalysisRecord,
        detail_job: LayoutJob,
    ) {
        ScrollArea::vertical()
            .id_salt("detail_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.add(egui::Label::new(detail_job).wrap());
                self.draw_form_group_explanations_panel(ui, record);
            });
    }

    fn draw_form_group_explanations_panel(&self, ui: &mut Ui, record: &AnalysisRecord) {
        if record.form_group_explanations_text.trim().is_empty() {
            return;
        }

        ui.add_space(10.0);
        egui::CollapsingHeader::new("高度条件の説明")
            .default_open(false)
            .show(ui, |ui| {
                ui.label(
                    RichText::new("高度条件の説明を表示中。本文強調は一部未対応です。")
                        .italics(),
                );
                if !record.matched_form_group_ids_text.trim().is_empty() {
                    ui.label(format!("group_ids: {}", record.matched_form_group_ids_text));
                }
                if !record.matched_form_group_logics_text.trim().is_empty() {
                    ui.label(format!(
                        "group_logics: {}",
                        record.matched_form_group_logics_text
                    ));
                }
                if !record.mixed_scope_warning_text.trim().is_empty() {
                    ui.colored_label(
                        Color32::from_rgb(196, 110, 0),
                        record.mixed_scope_warning_text.as_str(),
                    );
                }
                ScrollArea::vertical()
                    .id_salt("form_group_explanations_scroll")
                    .max_height(160.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(record.form_group_explanations_text.as_str())
                                .wrap_mode(TextWrapMode::Wrap),
                        );
                    });
            });
    }

    fn draw_annotation_editor_panel(&mut self, ui: &mut Ui, record: &AnalysisRecord) {
        let annotation_summary = if record.manual_annotation_pairs_text.trim().is_empty() {
            "annotation なし".to_string()
        } else {
            record.manual_annotation_pairs_text.clone()
        };
        let annotation_path_label = self
            .resolved_annotation_csv_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let annotation_save_enabled = self.annotation_save_enabled();

        ui.group(|ui| {
            ui.label(RichText::new("annotation 追記").strong());
            ui.label(format!("保存先: {annotation_path_label}"));
            ui.label(format!(
                "現在件数: {} / namespaces: {}",
                record.manual_annotation_count,
                if record.manual_annotation_namespaces_text.trim().is_empty() {
                    "(なし)"
                } else {
                    &record.manual_annotation_namespaces_text
                }
            ));

            ScrollArea::vertical()
                .id_salt("annotation_summary_scroll")
                .max_height(56.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(egui::Label::new(annotation_summary.as_str()).wrap());
                });

            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("namespace");
                ui.add(ime_safe_singleline(
                    &mut self.annotation_editor_state.namespace_input,
                ));
                ui.label("key");
                ui.add(ime_safe_singleline(&mut self.annotation_editor_state.key_input));
            });
            ui.horizontal(|ui| {
                ui.label("tagged_by");
                ui.add(ime_safe_singleline(
                    &mut self.annotation_editor_state.tagged_by_input,
                ));
                ui.label("confidence");
                ui.add(ime_safe_singleline(
                    &mut self.annotation_editor_state.confidence_input,
                ));
            });
            ui.label(RichText::new("改行は Shift+Enter").italics());
            ui.label("value");
            ui.add(
                ime_safe_multiline(&mut self.annotation_editor_state.value_input)
                    .desired_rows(2),
            );
            ui.label("note");
            ui.add(
                ime_safe_multiline(&mut self.annotation_editor_state.note_input)
                    .desired_rows(2),
            );

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(annotation_save_enabled, egui::Button::new("追記"))
                    .clicked()
                {
                    self.save_annotation_for_selected_record();
                }
                if ui.button("入力クリア").clicked() {
                    self.clear_annotation_editor_inputs();
                    self.clear_annotation_editor_status();
                }
                if !annotation_save_enabled {
                    ui.label("分析ジョブ実行中は保存できません。");
                }
            });

            if let Some(status_message) = &self.annotation_editor_state.status_message {
                ui.colored_label(
                    editor_status_color(self.annotation_editor_state.status_is_error),
                    status_message,
                );
            }
        });
    }
}

fn build_record_text_layout_job(ui: &Ui, segments: &[TextSegment]) -> LayoutJob {
    let mut job = LayoutJob::default();
    let normal_format = TextFormat {
        font_id: TextStyle::Body.resolve(ui.style()),
        color: ui.visuals().text_color(),
        ..Default::default()
    };
    let hit_format = TextFormat {
        background: Color32::from_rgb(255, 224, 138),
        ..normal_format.clone()
    };

    for seg in segments {
        if seg.text.is_empty() {
            continue;
        }
        let format = if seg.is_hit {
            hit_format.clone()
        } else {
            normal_format.clone()
        };
        job.append(&seg.text, 0.0, format);
    }

    job
}

fn analysis_status_color(ui: &Ui, status: &AnalysisJobStatus) -> Color32 {
    match status {
        AnalysisJobStatus::Idle => ui.visuals().text_color(),
        AnalysisJobStatus::RunningAnalysis { .. } | AnalysisJobStatus::RunningExport { .. } => {
            Color32::from_rgb(70, 130, 180)
        }
        AnalysisJobStatus::Succeeded { .. } => Color32::from_rgb(70, 130, 70),
        AnalysisJobStatus::Failed { .. } => Color32::from_rgb(200, 64, 64),
    }
}

fn editor_status_color(is_error: bool) -> Color32 {
    if is_error {
        Color32::from_rgb(200, 64, 64)
    } else {
        Color32::from_rgb(70, 130, 70)
    }
}

fn draw_analysis_path_override_row<F>(
    ui: &mut Ui,
    label: &str,
    current_label: &str,
    reset_label: &str,
    settings_enabled: bool,
    mut choose_path: F,
    selected_path: &mut Option<PathBuf>,
    clear_override: &mut bool,
) where
    F: FnMut() -> Option<PathBuf>,
{
    ui.label(label);
    ui.horizontal(|ui| {
        let mut displayed_label = current_label.to_string();
        ui.add(
            ime_safe_singleline(&mut displayed_label)
                .desired_width(460.0)
                .interactive(false),
        );
        if ui
            .add_enabled(settings_enabled, egui::Button::new("選択"))
            .clicked()
        {
            *selected_path = choose_path();
        }
        if ui
            .add_enabled(settings_enabled, egui::Button::new(reset_label))
            .clicked()
        {
            *clear_override = true;
        }
    });
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

fn build_default_form_group_item(group_index: usize) -> FormGroupEditorItem {
    FormGroupEditorItem {
        match_logic: Some(if group_index == 0 { "and" } else { "or" }.to_string()),
        combine_logic: (group_index > 0).then_some("and".to_string()),
        forms: vec![String::new()],
        ..Default::default()
    }
}

fn tree_row_no_value(record: &AnalysisRecord) -> String {
    record.row_no.to_string()
}

fn tree_paragraph_id_value(record: &AnalysisRecord) -> String {
    record.paragraph_id.clone()
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

fn render_db_viewer_contents(
    ui: &mut Ui,
    state: &DbViewerState,
    previous_location: Option<(i64, i64)>,
    next_location: Option<(i64, i64)>,
    requested_location: &mut Option<(i64, i64)>,
) {
    let db_file_label = state
        .db_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ordinance_analysis3.db");
    let source_text = state.source_paragraph_text.clone().unwrap_or_default();
    let comparison_label = match state.context.as_ref() {
        Some(context) if context.center.paragraph_text != source_text => {
            RichText::new("表示中テキスト と DB 中心段落は不一致")
                .color(Color32::from_rgb(200, 64, 64))
        }
        Some(_) => {
            RichText::new("表示中テキスト と DB 中心段落は一致")
                .color(Color32::from_rgb(70, 130, 70))
        }
        None => RichText::new("DB 中心段落未取得").italics(),
    };
    let body_scroll_id = state
        .context
        .as_ref()
        .map(|context| format!("db_viewer_body_{}", context.center.paragraph_id))
        .or_else(|| {
            state
                .source_paragraph_id
                .map(|paragraph_id| format!("db_viewer_body_source_{}", paragraph_id))
        })
        .unwrap_or_else(|| "db_viewer_body_unknown".to_string());

    ui.label(format!("DB: {}", db_file_label));
    ui.label(format!("パス: {}", state.db_path.display()));

    if let Some(context) = &state.context {
        ui.label(format!(
            "paragraph_id: {} / document_id: {} / paragraph_no: {}",
            context.center.paragraph_id,
            context.center.document_id,
            context.center.paragraph_no
        ));
    } else if let Some(paragraph_id) = state.source_paragraph_id {
        ui.label(format!("paragraph_id: {}", paragraph_id));
    }

    ui.separator();

    ui.horizontal(|ui| {
        if ui
            .add_enabled(
                previous_location.is_some(),
                egui::Button::new("◀ 前へ"),
            )
            .clicked()
        {
            *requested_location = previous_location;
        }

        let center_label = state
            .context
            .as_ref()
            .map(|context| format!("中心段落番号: {}", context.center.paragraph_no))
            .unwrap_or_else(|| "中心段落番号: -".to_string());
        ui.label(center_label);

        if ui
            .add_enabled(next_location.is_some(), egui::Button::new("次へ ▶"))
            .clicked()
        {
            *requested_location = next_location;
        }
    });

    ui.separator();

    if let Some(error) = &state.error_message {
        ui.colored_label(Color32::from_rgb(200, 64, 64), error);
        return;
    }

    ScrollArea::vertical()
        .id_salt(body_scroll_id)
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.group(|ui| {
                ui.label(comparison_label);
            });

            ui.add_space(6.0);
            egui::CollapsingHeader::new("表示中テキスト/DB 比較")
                .id_salt("db_viewer_comparison_header")
                .default_open(false)
                .show(ui, |ui| {
                    ui.label(RichText::new("表示中テキスト").strong());
                    ui.label(&source_text);

                    ui.add_space(6.0);
                    ui.label(RichText::new("DB 中心段落").strong());
                    if let Some(context) = &state.context {
                        ui.group(|ui| {
                            ui.label(format!("paragraph_no: {}", context.center.paragraph_no));
                            ui.label(&context.center.paragraph_text);
                        });
                    } else {
                        ui.label(RichText::new("DB 中心段落未取得").italics());
                    }
                });

            ui.separator();
            ui.label(RichText::new("前後コンテキスト").strong());

            if let Some(context) = &state.context {
                for paragraph in &context.paragraphs {
                    let is_center = paragraph.paragraph_id == context.center.paragraph_id;
                    if is_center {
                        ui.group(|ui| {
                            ui.label(
                                RichText::new(format!("段落 {} (中心)", paragraph.paragraph_no))
                                    .strong()
                                    .color(Color32::from_rgb(200, 120, 40)),
                            );
                            ui.label(&paragraph.paragraph_text);
                        });
                    } else {
                        ui.label(
                            RichText::new(format!("段落 {}", paragraph.paragraph_no)).strong(),
                        );
                        ui.label(&paragraph.paragraph_text);
                    }
                    ui.add_space(8.0);
                }
            } else {
                ui.label(RichText::new("DB コンテキスト未取得").italics());
            }
        });
}
