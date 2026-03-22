use crate::analysis_job_manager::{AnalysisJobManager, JobPollResult};
use crate::analysis_runner::{
    build_runtime_config, cleanup_job_directories, resolve_filter_config_path,
    AnalysisExportRequest, AnalysisExportSuccess, AnalysisJobEvent, AnalysisJobFailure,
    AnalysisJobRequest, AnalysisJobSuccess,
};
use crate::app_state::{AnalysisExportContext, AnalysisJobStatus, AnalysisRuntimeState, AppState};
use crate::app_ui_state::AppUiState;
use crate::condition_editor::{load_condition_document, save_condition_document_atomic};
use crate::csv_loader::load_records;
use crate::db::{fetch_paragraph_context, fetch_paragraph_context_by_location};
use crate::manual_annotation_store::append_manual_annotation_row;
use std::path::PathBuf;

pub(crate) enum PollAnalysisJobResult {
    Idle,
    Pending,
    Updated { error_message: Option<String> },
}

#[derive(Debug, Default)]
pub(crate) enum OptionalPathUpdate {
    #[default]
    Unchanged,
    Set(PathBuf),
    Clear,
}

#[derive(Debug, Default)]
pub(crate) struct AnalysisSettingsUpdate {
    pub(crate) python_path: OptionalPathUpdate,
    pub(crate) filter_config_path: OptionalPathUpdate,
    pub(crate) annotation_csv_path: OptionalPathUpdate,
}

impl AnalysisSettingsUpdate {
    pub(crate) fn has_changes(&self) -> bool {
        !matches!(self.python_path, OptionalPathUpdate::Unchanged)
            || !matches!(self.filter_config_path, OptionalPathUpdate::Unchanged)
            || !matches!(self.annotation_csv_path, OptionalPathUpdate::Unchanged)
    }
}

fn apply_optional_path_update(target: &mut Option<PathBuf>, update: OptionalPathUpdate) -> bool {
    match update {
        OptionalPathUpdate::Unchanged => false,
        OptionalPathUpdate::Set(path) => {
            let changed = target.as_ref() != Some(&path);
            *target = Some(path);
            changed
        }
        OptionalPathUpdate::Clear => {
            let changed = target.is_some();
            *target = None;
            changed
        }
    }
}

pub(crate) fn apply_analysis_settings_update(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    update: AnalysisSettingsUpdate,
) -> bool {
    if !update.has_changes() {
        return false;
    }

    let mut changed = false;
    changed |= apply_optional_path_update(
        &mut state.analysis_request_state.python_path_override,
        update.python_path,
    );
    changed |= apply_optional_path_update(
        &mut state.analysis_request_state.filter_config_path_override,
        update.filter_config_path,
    );
    changed |= apply_optional_path_update(
        &mut state.analysis_request_state.annotation_csv_path_override,
        update.annotation_csv_path,
    );

    if changed {
        refresh_analysis_runtime(state, ui_state);
    }

    changed
}

pub(crate) fn load_csv(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    path: PathBuf,
) -> Result<(), String> {
    let records = load_records(&path)?;
    state.replace_records(ui_state, records, path.display().to_string());
    state.analysis_runtime_state.last_export_context = None;
    Ok(())
}

pub(crate) fn save_annotation_for_selected_record(state: &mut AppState) -> Result<PathBuf, String> {
    state.clear_annotation_editor_status();
    let annotation_row = state.build_annotation_append_row()?;
    let annotation_csv_path = state.resolved_annotation_csv_path()?;
    append_manual_annotation_row(&annotation_csv_path, &annotation_row)?;
    state.apply_saved_annotation_to_selected_record(&annotation_row)?;
    state.clear_annotation_editor_inputs();
    state.annotation_editor_state.status_message = Some(format!(
        "annotation を追記しました: {}",
        annotation_csv_path.display()
    ));
    state.annotation_editor_state.status_is_error = false;
    Ok(annotation_csv_path)
}

pub(crate) fn open_db_viewer_for_selected_record(state: &mut AppState) -> Result<(), String> {
    let selected_record = state
        .selected_record()
        .ok_or_else(|| "レコードが選択されていません".to_string())?;
    if !selected_record.supports_db_viewer() {
        return Err("sentence 行では DB viewer は未対応です".to_string());
    }

    let paragraph_id = selected_record
        .paragraph_id
        .parse::<i64>()
        .map_err(|error| {
            format!(
                "paragraph_id を数値として解釈できません: {} ({error})",
                selected_record.paragraph_id
            )
        })?;
    let source_paragraph_text = selected_record.paragraph_text.clone();

    state.db_viewer_state.is_open = true;
    state.db_viewer_state.source_paragraph_id = Some(paragraph_id);
    state.db_viewer_state.source_paragraph_text = Some(source_paragraph_text);
    state.db_viewer_state.context = None;
    state.db_viewer_state.error_message = None;

    load_db_viewer_context(state);
    Ok(())
}

pub(crate) fn load_db_viewer_context(state: &mut AppState) {
    let Some(paragraph_id) = state.db_viewer_state.source_paragraph_id else {
        state.db_viewer_state.context = None;
        state.db_viewer_state.error_message = Some("参照元 paragraph_id が未設定です".to_string());
        state.db_viewer_state.is_open = true;
        return;
    };

    match fetch_paragraph_context(&state.db_viewer_state.db_path, paragraph_id) {
        Ok(context) => {
            state.db_viewer_state.context = Some(context);
            state.db_viewer_state.error_message = None;
            state.db_viewer_state.is_open = true;
        }
        Err(error) => {
            state.db_viewer_state.context = None;
            state.db_viewer_state.error_message = Some(error);
            state.db_viewer_state.is_open = true;
        }
    }
}

pub(crate) fn load_db_viewer_context_for_location(
    state: &mut AppState,
    document_id: i64,
    paragraph_no: i64,
) {
    match fetch_paragraph_context_by_location(
        &state.db_viewer_state.db_path,
        document_id,
        paragraph_no,
    ) {
        Ok(context) => {
            state.db_viewer_state.context = Some(context);
            state.db_viewer_state.error_message = None;
        }
        Err(error) => {
            state.db_viewer_state.context = None;
            state.db_viewer_state.error_message = Some(error);
        }
    }
}

pub(crate) fn cleanup_analysis_jobs(state: &mut AppState) {
    let Some(runtime) = state.analysis_runtime_state.runtime.as_ref() else {
        return;
    };

    if let Err(error) = cleanup_job_directories(&runtime.jobs_root) {
        state.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary: error };
    }
}

pub(crate) fn refresh_analysis_runtime(state: &mut AppState, ui_state: &mut AppUiState) {
    if state.analysis_runtime_state.is_job_running() {
        return;
    }

    let runtime = build_runtime_config(&state.analysis_request_state.runtime_overrides());
    state.analysis_runtime_state = AnalysisRuntimeState::from_runtime(runtime);
    cleanup_analysis_jobs(state);
    sync_condition_editor_with_runtime_path(state, ui_state);
}

pub(crate) fn resolved_filter_config_path(state: &AppState) -> Result<PathBuf, String> {
    if let Some(runtime) = state.analysis_runtime_state.runtime.as_ref() {
        return Ok(runtime.filter_config_path.clone());
    }
    resolve_filter_config_path(&state.analysis_request_state.runtime_overrides())
}

pub(crate) fn load_condition_editor_from_path(
    state: &mut AppState,
    ui_state: &mut AppUiState,
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
    ui_state.condition_editor.window_open = true;
    state.condition_editor_state.loaded_path = Some(path);
    state.condition_editor_state.pending_path_sync = None;
    state.condition_editor_state.document = Some(document);
    state.condition_editor_state.selected_index = clamp_condition_index(
        Some(0),
        state
            .condition_editor_state
            .document
            .as_ref()
            .map_or(0, |doc| doc.cooccurrence_conditions.len()),
    );
    state.condition_editor_state.selected_group_index = clamp_condition_group_selection(
        state,
        Some(0),
        state.condition_editor_state.selected_index,
    );
    state
        .condition_editor_state
        .projected_legacy_condition_count = projected_count;
    state.condition_editor_state.status_message = Some(final_status_message);
    state.condition_editor_state.status_is_error = false;
    state.condition_editor_state.is_dirty = false;
    ui_state.condition_editor.confirm_action = None;
    Ok(())
}

pub(crate) fn save_condition_editor_document(
    state: &mut AppState,
    ui_state: &mut AppUiState,
) -> Result<(), String> {
    let path = state
        .condition_editor_state
        .loaded_path
        .clone()
        .ok_or_else(|| "保存先の条件 JSON パスが未設定です".to_string())?;
    let document = state
        .condition_editor_state
        .document
        .as_ref()
        .ok_or_else(|| "保存対象の条件 JSON が読み込まれていません".to_string())?;
    save_condition_document_atomic(&path, document)?;
    load_condition_editor_from_path(state, ui_state, path.clone(), "条件 JSON を保存しました。")?;
    state.condition_editor_state.status_message =
        Some(format!("条件 JSON を保存しました: {}", path.display()));
    state.condition_editor_state.status_is_error = false;
    Ok(())
}

pub(crate) fn sync_condition_editor_with_runtime_path(
    state: &mut AppState,
    ui_state: &mut AppUiState,
) {
    if !ui_state.condition_editor.window_open {
        return;
    }

    let Ok(resolved_path) = resolved_filter_config_path(state) else {
        return;
    };
    let Some(loaded_path) = state.condition_editor_state.loaded_path.clone() else {
        return;
    };
    if resolved_path == loaded_path {
        state.condition_editor_state.pending_path_sync = None;
        return;
    }

    if state.condition_editor_state.is_dirty {
        state.condition_editor_state.pending_path_sync = Some(resolved_path.clone());
        state.condition_editor_state.status_message = Some(format!(
            "分析設定で条件 JSON の解決先が変更されました。再読込が必要です: {}",
            resolved_path.display()
        ));
        state.condition_editor_state.status_is_error = true;
        return;
    }

    match load_condition_editor_from_path(
        state,
        ui_state,
        resolved_path.clone(),
        "条件 JSON を再読込しました。",
    ) {
        Ok(()) => {
            state.condition_editor_state.pending_path_sync = None;
            state.condition_editor_state.status_message = Some(format!(
                "分析設定の変更に合わせて条件 JSON を再読込しました: {}",
                resolved_path.display()
            ));
            state.condition_editor_state.status_is_error = false;
        }
        Err(error) => {
            state.condition_editor_state.pending_path_sync = Some(resolved_path);
            state.condition_editor_state.status_message = Some(error);
            state.condition_editor_state.status_is_error = true;
        }
    }
}

pub(crate) fn start_analysis_job(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    job_manager: &mut AnalysisJobManager,
) -> Result<(), String> {
    if job_manager.has_running_job() {
        return Err("分析ジョブは既に実行中です".to_string());
    }

    let runtime = state
        .analysis_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;

    cleanup_job_directories(&runtime.jobs_root)?;

    let job_id = job_manager.start_analysis_job(AnalysisJobRequest {
        db_path: state.db_viewer_state.db_path.clone(),
        runtime,
    })?;

    state.analysis_runtime_state.last_warnings.clear();
    ui_state.warning_details_window_open = false;
    state.analysis_runtime_state.status = AnalysisJobStatus::RunningAnalysis { job_id };
    Ok(())
}

pub(crate) fn start_export_job(
    state: &mut AppState,
    _ui_state: &mut AppUiState,
    job_manager: &mut AnalysisJobManager,
    output_csv_path: PathBuf,
) -> Result<(), String> {
    if job_manager.has_running_job() {
        return Err("分析ジョブは既に実行中です".to_string());
    }

    let runtime = state
        .analysis_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;
    let export_context = state
        .analysis_runtime_state
        .last_export_context
        .clone()
        .ok_or_else(|| "保存対象の分析結果がありません".to_string())?;

    let job_id = job_manager.start_export_job(AnalysisExportRequest {
        db_path: export_context.db_path,
        filter_config_path: export_context.filter_config_path,
        annotation_csv_path: export_context.annotation_csv_path,
        output_csv_path,
        runtime,
    })?;

    state.analysis_runtime_state.status = AnalysisJobStatus::RunningExport { job_id };
    Ok(())
}

pub(crate) fn poll_analysis_job(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    job_manager: &mut AnalysisJobManager,
) -> PollAnalysisJobResult {
    match job_manager.poll() {
        JobPollResult::Idle => PollAnalysisJobResult::Idle,
        JobPollResult::Pending => PollAnalysisJobResult::Pending,
        JobPollResult::Completed(AnalysisJobEvent::AnalysisCompleted(result)) => {
            let error_message = match result {
                Ok(success) => handle_analysis_success(state, ui_state, success),
                Err(failure) => Some(handle_analysis_failure(state, ui_state, failure)),
            };
            PollAnalysisJobResult::Updated { error_message }
        }
        JobPollResult::Completed(AnalysisJobEvent::ExportCompleted(result)) => {
            let error_message = match result {
                Ok(success) => Some(handle_export_success(state, success)),
                Err(failure) => Some(handle_analysis_failure(state, ui_state, failure)),
            };
            PollAnalysisJobResult::Updated { error_message }
        }
        JobPollResult::Disconnected => {
            state.analysis_runtime_state.status = AnalysisJobStatus::Failed {
                summary: "分析ジョブの完了通知を受け取れませんでした".to_string(),
            };
            PollAnalysisJobResult::Updated {
                error_message: None,
            }
        }
    }
}

fn handle_analysis_success(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    success: AnalysisJobSuccess,
) -> Option<String> {
    let warnings = success.meta.warning_messages.clone();
    let warning_count = warnings.len();
    let source_label = format!("分析結果: {}", success.meta.job_id);
    state.replace_records(ui_state, success.records, source_label);
    let mut summary = format!(
        "{}{}抽出 / {:.2} 秒",
        success.meta.selected_unit_count(),
        success.meta.analysis_unit.count_label(),
        success.meta.duration_seconds
    );
    if warning_count > 0 {
        summary.push_str(&format!(" / 警告 {} 件", warning_count));
    }
    state.analysis_runtime_state.last_warnings = warnings;
    ui_state.warning_details_window_open = false;
    let annotation_csv_path = state
        .analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.annotation_csv_path.clone())
        .or_else(|| state.resolved_annotation_csv_path().ok())
        .unwrap_or_default();
    state.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path: PathBuf::from(&success.meta.filter_config_path),
        annotation_csv_path,
    });
    state.analysis_runtime_state.status = AnalysisJobStatus::Succeeded { summary };
    None
}

fn handle_export_success(state: &mut AppState, success: AnalysisExportSuccess) -> String {
    state.analysis_runtime_state.status = AnalysisJobStatus::Succeeded {
        summary: format!("CSV 保存完了: {}", success.output_csv_path.display()),
    };
    format!(
        "CSV を保存しました。\n\n保存先:\n{}",
        success.output_csv_path.display()
    )
}

fn handle_analysis_failure(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    failure: AnalysisJobFailure,
) -> String {
    let warnings = failure
        .meta
        .as_ref()
        .map(|meta| meta.warning_messages.clone())
        .unwrap_or_default();
    let summary = failure.message.clone();
    state.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary };
    state.analysis_runtime_state.last_warnings = warnings;
    ui_state.warning_details_window_open = false;

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
    error_message
}

fn clamp_condition_index(selected_index: Option<usize>, len: usize) -> Option<usize> {
    match (selected_index, len) {
        (_, 0) => None,
        (Some(index), _) => Some(index.min(len - 1)),
        (None, _) => Some(0),
    }
}

fn clamp_condition_group_selection(
    state: &AppState,
    selected_group_index: Option<usize>,
    condition_index: Option<usize>,
) -> Option<usize> {
    let Some(document) = state.condition_editor_state.document.as_ref() else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use std::path::PathBuf;

    #[test]
    fn apply_analysis_settings_update_sets_requested_overrides() {
        let mut state = AppState::new(
            Err("runtime unavailable".to_string()),
            PathBuf::from("test.db"),
        );
        let mut ui_state = AppUiState::new(0.33);

        let changed = apply_analysis_settings_update(
            &mut state,
            &mut ui_state,
            AnalysisSettingsUpdate {
                python_path: OptionalPathUpdate::Set(PathBuf::from("python.exe")),
                filter_config_path: OptionalPathUpdate::Set(PathBuf::from("conditions.json")),
                annotation_csv_path: OptionalPathUpdate::Set(PathBuf::from("annotations.csv")),
            },
        );

        assert!(changed);
        assert_eq!(
            state.analysis_request_state.python_path_override,
            Some(PathBuf::from("python.exe"))
        );
        assert_eq!(
            state.analysis_request_state.filter_config_path_override,
            Some(PathBuf::from("conditions.json"))
        );
        assert_eq!(
            state.analysis_request_state.annotation_csv_path_override,
            Some(PathBuf::from("annotations.csv"))
        );
    }

    #[test]
    fn apply_analysis_settings_update_clears_existing_overrides() {
        let mut state = AppState::new(
            Err("runtime unavailable".to_string()),
            PathBuf::from("test.db"),
        );
        let mut ui_state = AppUiState::new(0.33);
        state.analysis_request_state.python_path_override = Some(PathBuf::from("python.exe"));
        state.analysis_request_state.filter_config_path_override =
            Some(PathBuf::from("conditions.json"));
        state.analysis_request_state.annotation_csv_path_override =
            Some(PathBuf::from("annotations.csv"));

        let changed = apply_analysis_settings_update(
            &mut state,
            &mut ui_state,
            AnalysisSettingsUpdate {
                python_path: OptionalPathUpdate::Clear,
                filter_config_path: OptionalPathUpdate::Clear,
                annotation_csv_path: OptionalPathUpdate::Clear,
            },
        );

        assert!(changed);
        assert_eq!(state.analysis_request_state.python_path_override, None);
        assert_eq!(
            state.analysis_request_state.filter_config_path_override,
            None
        );
        assert_eq!(
            state.analysis_request_state.annotation_csv_path_override,
            None
        );
    }
}
