use crate::analysis_runner::{
    resolve_annotation_csv_path, AnalysisRuntimeConfig, AnalysisRuntimeOverrides,
    AnalysisWarningMessage,
};
use crate::app_ui_state::{AppUiState, TreeScrollRequest};
use crate::condition_editor::FilterConfigDocument;
use crate::filter::build_filter_options;
use crate::manual_annotation_store::{
    append_manual_annotation_namespaces_text, append_manual_annotation_pairs_text,
    build_manual_annotation_pair, increment_manual_annotation_count, ManualAnnotationAppendRow,
};
use crate::model::{AnalysisRecord, DbViewerState, FilterColumn, FilterOption, TextSegment};
use crate::tagged_text::parse_tagged_text;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScrollBehavior {
    None,
    KeepVisible,
    AlignMin,
    AlignMax,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SelectionChange {
    pub(crate) selected_row: Option<usize>,
    pub(crate) scroll_behavior: ScrollBehavior,
}

#[derive(Clone)]
pub(crate) struct AnalysisExportContext {
    pub(crate) db_path: PathBuf,
    pub(crate) filter_config_path: PathBuf,
    pub(crate) annotation_csv_path: PathBuf,
}

pub(crate) enum AnalysisJobStatus {
    Idle,
    RunningAnalysis { job_id: String },
    RunningExport { job_id: String },
    Succeeded { summary: String },
    Failed { summary: String },
}

pub(crate) struct AnalysisRuntimeState {
    pub(crate) runtime: Option<AnalysisRuntimeConfig>,
    pub(crate) status: AnalysisJobStatus,
    pub(crate) last_warnings: Vec<AnalysisWarningMessage>,
    pub(crate) last_export_context: Option<AnalysisExportContext>,
}

impl AnalysisRuntimeState {
    pub(crate) fn from_runtime(runtime: Result<AnalysisRuntimeConfig, String>) -> Self {
        match runtime {
            Ok(runtime) => Self {
                runtime: Some(runtime),
                status: AnalysisJobStatus::Idle,
                last_warnings: Vec::new(),
                last_export_context: None,
            },
            Err(error) => Self {
                runtime: None,
                status: AnalysisJobStatus::Failed { summary: error },
                last_warnings: Vec::new(),
                last_export_context: None,
            },
        }
    }

    pub(crate) fn can_start(&self) -> bool {
        self.runtime.is_some() && !self.is_job_running()
    }

    pub(crate) fn status_text(&self) -> String {
        match &self.status {
            AnalysisJobStatus::Idle => "分析待機中".to_string(),
            AnalysisJobStatus::RunningAnalysis { job_id } => format!("分析実行中: {job_id}"),
            AnalysisJobStatus::RunningExport { job_id } => format!("CSV 保存中: {job_id}"),
            AnalysisJobStatus::Succeeded { summary } => format!("分析成功: {summary}"),
            AnalysisJobStatus::Failed { summary } => format!("分析失敗: {summary}"),
        }
    }

    pub(crate) fn has_warning_details(&self) -> bool {
        !self.last_warnings.is_empty()
    }

    pub(crate) fn can_export(&self) -> bool {
        self.runtime.is_some() && !self.is_job_running() && self.last_export_context.is_some()
    }

    pub(crate) fn is_job_running(&self) -> bool {
        matches!(
            self.status,
            AnalysisJobStatus::RunningAnalysis { .. } | AnalysisJobStatus::RunningExport { .. }
        )
    }
}

#[derive(Default)]
pub(crate) struct AnalysisRequestState {
    pub(crate) python_path_override: Option<PathBuf>,
    pub(crate) filter_config_path_override: Option<PathBuf>,
    pub(crate) annotation_csv_path_override: Option<PathBuf>,
}

impl AnalysisRequestState {
    pub(crate) fn runtime_overrides(&self) -> AnalysisRuntimeOverrides {
        AnalysisRuntimeOverrides {
            python_path: self.python_path_override.clone(),
            filter_config_path: self.filter_config_path_override.clone(),
            annotation_csv_path: self.annotation_csv_path_override.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AnnotationEditorState {
    pub(crate) namespace_input: String,
    pub(crate) key_input: String,
    pub(crate) value_input: String,
    pub(crate) tagged_by_input: String,
    pub(crate) confidence_input: String,
    pub(crate) note_input: String,
    pub(crate) status_message: Option<String>,
    pub(crate) status_is_error: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConditionEditorConfirmAction {
    CloseWindow,
    ReloadPath(PathBuf),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ConditionEditorState {
    pub(crate) loaded_path: Option<PathBuf>,
    pub(crate) pending_path_sync: Option<PathBuf>,
    pub(crate) document: Option<FilterConfigDocument>,
    pub(crate) selected_index: Option<usize>,
    pub(crate) selected_group_index: Option<usize>,
    pub(crate) projected_legacy_condition_count: usize,
    pub(crate) status_message: Option<String>,
    pub(crate) status_is_error: bool,
    pub(crate) is_dirty: bool,
}

pub(crate) struct AppState {
    pub(crate) records_source_label: String,
    pub(crate) db_viewer_state: DbViewerState,
    pub(crate) analysis_request_state: AnalysisRequestState,
    pub(crate) analysis_runtime_state: AnalysisRuntimeState,
    pub(crate) all_records: Vec<AnalysisRecord>,
    pub(crate) filtered_indices: Vec<usize>,
    pub(crate) filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    pub(crate) selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    pub(crate) filter_candidate_queries: HashMap<FilterColumn, String>,
    pub(crate) active_filter_column: FilterColumn,
    pub(crate) selected_row: Option<usize>,
    pub(crate) cached_segments: Option<(usize, Vec<TextSegment>)>,
    pub(crate) annotation_editor_state: AnnotationEditorState,
    pub(crate) condition_editor_state: ConditionEditorState,
}

impl SelectionChange {
    pub(crate) fn new(selected_row: Option<usize>, scroll_behavior: ScrollBehavior) -> Self {
        Self {
            selected_row,
            scroll_behavior,
        }
    }

    pub(crate) fn first_filtered_row(filtered_len: usize, scroll_behavior: ScrollBehavior) -> Self {
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

impl AppState {
    pub(crate) fn new(runtime: Result<AnalysisRuntimeConfig, String>, db_path: PathBuf) -> Self {
        Self {
            records_source_label: "分析結果なし".to_string(),
            db_viewer_state: DbViewerState::new(db_path),
            analysis_request_state: AnalysisRequestState::default(),
            analysis_runtime_state: AnalysisRuntimeState::from_runtime(runtime),
            all_records: Vec::new(),
            filtered_indices: Vec::new(),
            filter_options: HashMap::new(),
            selected_filter_values: HashMap::new(),
            filter_candidate_queries: HashMap::new(),
            active_filter_column: FilterColumn::MatchedCategories,
            selected_row: None,
            cached_segments: None,
            annotation_editor_state: AnnotationEditorState::default(),
            condition_editor_state: ConditionEditorState::default(),
        }
    }

    pub(crate) fn replace_records(
        &mut self,
        ui_state: &mut AppUiState,
        records: Vec<AnalysisRecord>,
        source_label: String,
    ) {
        self.all_records = records;
        self.records_source_label = source_label;
        self.db_viewer_state.reset_loaded_state();
        self.filter_options = build_filter_options(&self.all_records);
        self.selected_filter_values.clear();
        self.filter_candidate_queries.clear();
        self.filtered_indices = (0..self.all_records.len()).collect();
        self.cached_segments = None;
        self.apply_selection_change(
            ui_state,
            SelectionChange::first_filtered_row(
                self.filtered_indices.len(),
                ScrollBehavior::AlignMin,
            ),
        );
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    pub(crate) fn apply_selection_change(
        &mut self,
        ui_state: &mut AppUiState,
        change: SelectionChange,
    ) -> bool {
        let next = clamp_selected_row(change.selected_row, self.filtered_indices.len());
        let selection_changed = self.selected_row != next;
        if selection_changed {
            self.selected_row = next;
            self.cached_segments = None;
            self.clear_annotation_editor_status();
        }

        let next_scroll_request = build_tree_scroll_request(next, change.scroll_behavior);
        let scroll_changed = ui_state.pending_tree_scroll != next_scroll_request;
        ui_state.pending_tree_scroll = next_scroll_request;

        selection_changed || scroll_changed
    }

    pub(crate) fn select_first_filtered_row(
        &mut self,
        ui_state: &mut AppUiState,
        scroll_behavior: ScrollBehavior,
    ) -> bool {
        self.apply_selection_change(
            ui_state,
            SelectionChange::first_filtered_row(self.filtered_indices.len(), scroll_behavior),
        )
    }

    pub(crate) fn move_selection_up(&mut self, ui_state: &mut AppUiState) {
        if self.filtered_indices.is_empty() {
            return;
        }

        match self.selected_row {
            Some(idx) if idx > 0 => {
                self.apply_selection_change(
                    ui_state,
                    SelectionChange::new(Some(idx - 1), ScrollBehavior::KeepVisible),
                );
            }
            None => {
                self.select_first_filtered_row(ui_state, ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    pub(crate) fn move_selection_down(&mut self, ui_state: &mut AppUiState) {
        let current_len = self.filtered_indices.len();
        if current_len == 0 {
            return;
        }

        match self.selected_row {
            Some(idx) if idx + 1 < current_len => {
                self.apply_selection_change(
                    ui_state,
                    SelectionChange::new(Some(idx + 1), ScrollBehavior::KeepVisible),
                );
            }
            None => {
                self.select_first_filtered_row(ui_state, ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    pub(crate) fn selected_record(&self) -> Option<&AnalysisRecord> {
        let filtered_idx = self.selected_row?;
        let record_idx = *self.filtered_indices.get(filtered_idx)?;
        self.all_records.get(record_idx)
    }

    pub(crate) fn selected_record_index(&self) -> Option<usize> {
        let filtered_idx = self.selected_row?;
        self.filtered_indices.get(filtered_idx).copied()
    }

    pub(crate) fn selected_record_mut(&mut self) -> Option<&mut AnalysisRecord> {
        let record_idx = self.selected_record_index()?;
        self.all_records.get_mut(record_idx)
    }

    pub(crate) fn resolved_annotation_csv_path(&self) -> Result<PathBuf, String> {
        resolve_annotation_csv_path(&self.analysis_request_state.runtime_overrides())
    }

    pub(crate) fn annotation_save_enabled(&self) -> bool {
        self.selected_record()
            .is_some_and(AnalysisRecord::supports_manual_annotation)
            && !self.analysis_runtime_state.is_job_running()
    }

    pub(crate) fn clear_annotation_editor_status(&mut self) {
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    pub(crate) fn clear_annotation_editor_inputs(&mut self) {
        self.annotation_editor_state.value_input.clear();
        self.annotation_editor_state.confidence_input.clear();
        self.annotation_editor_state.note_input.clear();
    }

    pub(crate) fn build_annotation_append_row(&self) -> Result<ManualAnnotationAppendRow, String> {
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

    pub(crate) fn apply_saved_annotation_to_selected_record(
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

    pub(crate) fn apply_filters(&mut self, ui_state: &mut AppUiState) {
        self.filtered_indices = self
            .all_records
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| self.record_matches_filters(record).then_some(idx))
            .collect();
        self.cached_segments = None;
        self.select_first_filtered_row(ui_state, ScrollBehavior::AlignMin);
    }

    fn record_matches_filters(&self, record: &AnalysisRecord) -> bool {
        self.selected_filter_values
            .iter()
            .all(|(column, selected)| column.matches(record, selected))
    }

    pub(crate) fn clear_filters_for_column(
        &mut self,
        ui_state: &mut AppUiState,
        column: FilterColumn,
    ) {
        if self.selected_filter_values.remove(&column).is_some() {
            self.apply_filters(ui_state);
        }
    }

    pub(crate) fn clear_all_filters(&mut self, ui_state: &mut AppUiState) {
        if !self.selected_filter_values.is_empty() {
            self.selected_filter_values.clear();
            self.apply_filters(ui_state);
        }
    }

    pub(crate) fn toggle_filter_value(
        &mut self,
        ui_state: &mut AppUiState,
        column: FilterColumn,
        value: &str,
        selected: bool,
    ) {
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
            self.apply_filters(ui_state);
        }
    }

    pub(crate) fn get_segments(&mut self) -> Vec<TextSegment> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_ui_state::AppUiState;
    use crate::model::{AnalysisRecord, AnalysisUnit, FilterColumn};
    use std::path::PathBuf;

    fn build_record(row_no: usize, paragraph_id: &str, matched_categories: &str) -> AnalysisRecord {
        AnalysisRecord {
            row_no,
            analysis_unit: AnalysisUnit::Paragraph,
            paragraph_id: paragraph_id.to_string(),
            sentence_id: String::new(),
            document_id: format!("doc-{row_no}"),
            municipality_name: "Test City".to_string(),
            ordinance_or_rule: "Test Rule".to_string(),
            doc_type: "ordinance".to_string(),
            sentence_count: "1".to_string(),
            sentence_no_in_paragraph: String::new(),
            sentence_no_in_document: String::new(),
            sentence_text: String::new(),
            sentence_text_tagged: String::new(),
            paragraph_text: format!("paragraph {row_no}"),
            paragraph_text_tagged: String::new(),
            matched_condition_ids_text: String::new(),
            matched_categories_text: matched_categories.to_string(),
            matched_form_group_ids_text: String::new(),
            matched_form_group_logics_text: String::new(),
            form_group_explanations_text: String::new(),
            mixed_scope_warning_text: String::new(),
            match_group_ids_text: String::new(),
            match_group_count: "0".to_string(),
            annotated_token_count: "0".to_string(),
            manual_annotation_count: "0".to_string(),
            manual_annotation_pairs_text: String::new(),
            manual_annotation_namespaces_text: String::new(),
        }
    }

    #[test]
    fn replace_records_sets_initial_selection_and_scroll_request_on_ui_state() {
        let mut state = AppState::new(
            Err("runtime unavailable".to_string()),
            PathBuf::from("test.db"),
        );
        let mut ui_state = AppUiState::new(0.33);

        state.replace_records(
            &mut ui_state,
            vec![
                build_record(1, "p1", "alpha"),
                build_record(2, "p2", "beta"),
            ],
            "source.csv".to_string(),
        );

        assert_eq!(state.selected_row, Some(0));
        assert_eq!(
            ui_state
                .pending_tree_scroll
                .map(|request| request.row_index),
            Some(0)
        );
        assert_eq!(ui_state.record_list_panel_ratio, 0.33);
    }

    #[test]
    fn toggle_filter_value_updates_filtered_rows_and_resets_scroll_request() {
        let mut state = AppState::new(
            Err("runtime unavailable".to_string()),
            PathBuf::from("test.db"),
        );
        let mut ui_state = AppUiState::new(0.4);
        state.replace_records(
            &mut ui_state,
            vec![
                build_record(1, "p1", "alpha"),
                build_record(2, "p2", "beta"),
            ],
            "source.csv".to_string(),
        );
        ui_state.pending_tree_scroll = None;

        state.toggle_filter_value(
            &mut ui_state,
            FilterColumn::MatchedCategories,
            "alpha",
            true,
        );

        assert_eq!(state.filtered_indices, vec![0]);
        assert_eq!(state.selected_row, Some(0));
        assert_eq!(
            ui_state
                .pending_tree_scroll
                .map(|request| request.row_index),
            Some(0)
        );
    }
}
