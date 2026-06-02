//! Python 分析・エクスポート・警告ウィンドウ・終了ガード（未保存）。親モジュール `app` の子。

use super::{
    AnalysisExportContext, AnalysisJobStatus, AnalysisRunContext, AnalysisRuntimeState, App,
    BuilderJobStatus, RunningAnalysisJob, RunningBuildJob,
};
use crate::analysis_runner::{
    build_runtime_config, cleanup_job_directories, resolve_filter_config_path,
    AnalysisDbBuildRequest, AnalysisDbBuildSuccess, AnalysisExportSuccess, AnalysisJobEvent,
    AnalysisJobFailure, AnalysisJobRequest, AnalysisJobSuccess, AnalysisMeta,
    AnalysisRuntimeConfig, AnalysisWarningMessage,
};
use crate::analysis_session_cache::{
    build_session_cache_key, AnalysisResultSnapshot, AnalysisSessionCacheKey,
};
use crate::condition_authoring_bridge::compile_authoring_to_runtime;
use crate::condition_config_format::{detect_condition_config_format, ConditionConfigFormat};
use crate::model::{AnalysisRecord, AnalysisUnit};
use crate::viewer_core::{ViewerCoreCloseInput, ViewerCoreMessage};
use crate::viewer_export::write_visible_records_csv;
use eframe::egui::{self, RichText, ScrollArea};
use egui::TextWrapMode;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Default)]
pub(super) struct AnalysisJobPollOutput {
    pub(super) core_event: Option<ViewerCoreMessage>,
    pub(super) needs_repaint: bool,
    pub(super) repaint_after: Option<Duration>,
}

pub(super) fn try_cleanup_analysis_jobs(app: &mut App) {
    let Some(runtime) = app.analysis_runtime_state.runtime.as_ref() else {
        return;
    };

    if let Err(error) = cleanup_job_directories(&runtime.jobs_root) {
        app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisFailed { summary: error };
    }
}

pub(super) fn refresh_analysis_runtime(app: &mut App) {
    if app.analysis_runtime_state.current_job.is_some() {
        return;
    }

    let runtime = build_runtime_config(&app.analysis_request_state.runtime_overrides());
    app.analysis_runtime_state = AnalysisRuntimeState::from_runtime(runtime);
    try_cleanup_analysis_jobs(app);
    app.sync_condition_editor_with_runtime_path();
}

pub(super) fn invalidate_session_analysis_cache(app: &mut App) {
    app.analysis_runtime_state.session_analysis_cache = None;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum AnalysisStartMode {
    /// 入力が直近成功と一致すれば worker を呼ばない。
    #[default]
    Normal,
    /// レベル A: セッションキャッシュを無視して worker を呼ぶ（`force_reload` は false）。
    ForceWorkerRun,
    /// レベル B: セッション無視 + worker の DB フレーム再読込。
    ForceWorkerRunAndReloadDb,
}

pub(super) fn resolved_filter_config_path(app: &App) -> Result<PathBuf, String> {
    if let Some(runtime) = app.analysis_runtime_state.runtime.as_ref() {
        return Ok(runtime.filter_config_path.clone());
    }
    resolve_filter_config_path(&app.analysis_request_state.runtime_overrides())
}

pub(super) fn start_analysis_job(app: &mut App) -> Result<(), String> {
    start_analysis_job_with_mode(app, AnalysisStartMode::default())
}

pub(super) fn start_analysis_job_with_mode(
    app: &mut App,
    mode: AnalysisStartMode,
) -> Result<(), String> {
    if app.is_any_job_running() {
        return Err("別のジョブが既に実行中です".to_string());
    }

    let runtime = app
        .analysis_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;
    let prepared_runtime = prepare_runtime_for_analysis(runtime)?;
    let runtime = prepared_runtime.runtime;
    let compile_warnings = prepared_runtime.authoring_warnings;
    let analysis_context = AnalysisRunContext {
        runtime: runtime.clone(),
        compile_warnings: compile_warnings.clone(),
    };

    if mode == AnalysisStartMode::Normal {
        let snapshot_hit = app
            .analysis_runtime_state
            .session_analysis_cache
            .as_ref()
            .and_then(|(cached_key, snapshot)| {
                current_session_cache_key(app, &runtime)
                    .filter(|current_key| current_key == cached_key)
                    .map(|_| snapshot.clone())
            });
        if let Some(snapshot) = snapshot_hit {
            return apply_session_cache_hit(app, &snapshot);
        }
    }

    cleanup_job_directories(&runtime.jobs_root)?;

    let force_reload_db_frames = mode == AnalysisStartMode::ForceWorkerRunAndReloadDb;

    let (job_id, receiver) = app
        .analysis_process_host
        .spawn_analysis_job(AnalysisJobRequest {
            db_path: app.db_viewer_state.db_path.clone(),
            runtime,
            force_reload_db_frames,
        });

    app.analysis_runtime_state.last_warnings = compile_warnings;
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob {
        receiver,
        analysis_context: Some(analysis_context),
    });
    app.analysis_runtime_state.status = AnalysisJobStatus::RunningAnalysis {
        job_id: job_id.clone(),
    };
    app.core.set_expected_job_id(job_id.clone());
    app.logger.info(&format!("analysis job started: {job_id}"));
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedAnalysisRuntime {
    runtime: AnalysisRuntimeConfig,
    authoring_warnings: Vec<AnalysisWarningMessage>,
}

fn compiled_conditions_output_dir(runtime: &AnalysisRuntimeConfig) -> PathBuf {
    runtime
        .project_root
        .join("runtime")
        .join("compiled-conditions")
}

fn prepare_runtime_for_analysis(
    mut runtime: AnalysisRuntimeConfig,
) -> Result<PreparedAnalysisRuntime, String> {
    match detect_condition_config_format(&runtime.filter_config_path)? {
        ConditionConfigFormat::Runtime => Ok(PreparedAnalysisRuntime {
            runtime,
            authoring_warnings: Vec::new(),
        }),
        ConditionConfigFormat::AuthoringV1 => {
            let source_path = runtime.filter_config_path.clone();
            let output_dir = compiled_conditions_output_dir(&runtime);
            let compile_result = compile_authoring_to_runtime(&runtime, &source_path, &output_dir)?;
            if compile_result.has_errors() {
                let summary = compile_result
                    .issues
                    .iter()
                    .filter(|issue| issue.is_error())
                    .map(|issue| issue.to_analysis_warning_message().message)
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(if summary.trim().is_empty() {
                    "authoring JSON のコンパイル中にエラーが発生しました。".to_string()
                } else {
                    format!("authoring JSON のコンパイル中にエラーが発生しました: {summary}")
                });
            }
            let authoring_warnings = compile_result
                .issues
                .iter()
                .map(|issue| issue.to_analysis_warning_message())
                .collect();
            runtime.filter_config_path = compile_result.runtime_json_path;
            runtime.filter_config_source_path = Some(source_path);
            Ok(PreparedAnalysisRuntime {
                runtime,
                authoring_warnings,
            })
        }
        ConditionConfigFormat::UnsupportedYaml => Err(format!(
            "authoring condition YAML is not supported in MVP: {}",
            runtime.filter_config_path.display()
        )),
        ConditionConfigFormat::Invalid(message) => Err(format!(
            "condition config format is invalid ({}): {message}",
            runtime.filter_config_path.display()
        )),
    }
}

fn resolved_annotation_csv_path_for_runtime(app: &App) -> PathBuf {
    app.analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.annotation_csv_path.clone())
        .or_else(|| app.resolved_annotation_csv_path().ok())
        .unwrap_or_default()
}

fn current_session_cache_key(
    app: &App,
    runtime: &AnalysisRuntimeConfig,
) -> Option<AnalysisSessionCacheKey> {
    build_session_cache_key(
        &app.db_viewer_state.db_path,
        &runtime.filter_config_path,
        &runtime.annotation_csv_path,
        runtime,
    )
}

fn build_status_summary_with_warnings(meta: &AnalysisMeta, warning_count: usize) -> String {
    let mut summary = format!(
        "{}{}抽出 / {:.2} 秒",
        meta.selected_unit_count(),
        meta.analysis_unit.count_label(),
        meta.duration_seconds
    );
    if warning_count > 0 {
        summary.push_str(&format!(" / 警告 {} 件", warning_count));
    }
    summary
}

/// compile warnings と worker warnings を併合する。
/// compile warnings が上書きで消えないよう、worker warnings を末尾に追加する。
fn merge_warnings(
    compile_warnings: Vec<AnalysisWarningMessage>,
    worker_warnings: Vec<AnalysisWarningMessage>,
) -> Vec<AnalysisWarningMessage> {
    let mut merged = compile_warnings;
    merged.extend(worker_warnings);
    merged
}

fn runtime_for_completed_analysis<'a>(
    app: &'a App,
    context: &'a Option<AnalysisRunContext>,
) -> Option<&'a AnalysisRuntimeConfig> {
    context
        .as_ref()
        .map(|ctx| &ctx.runtime)
        .or_else(|| app.analysis_runtime_state.runtime.as_ref())
}

fn analysis_result_snapshot_from_success(
    success: &AnalysisJobSuccess,
    runtime: &AnalysisRuntimeConfig,
    merged_warnings: &[AnalysisWarningMessage],
    status_summary: &str,
) -> AnalysisResultSnapshot {
    AnalysisResultSnapshot {
        records: success.records.clone(),
        source_label: format!("分析結果: {}", success.meta.job_id),
        last_warnings: merged_warnings.to_vec(),
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path: runtime.filter_config_path.clone(),
        filter_config_source_path: runtime.filter_config_source_path.clone(),
        annotation_csv_path: runtime.annotation_csv_path.clone(),
        status_summary: status_summary.to_string(),
    }
}

fn try_store_session_analysis_cache(
    app: &mut App,
    success: &AnalysisJobSuccess,
    runtime: &AnalysisRuntimeConfig,
    merged_warnings: &[AnalysisWarningMessage],
    status_summary: &str,
) {
    let Some(key) = current_session_cache_key(app, runtime) else {
        return;
    };
    let snapshot =
        analysis_result_snapshot_from_success(success, runtime, merged_warnings, status_summary);
    app.analysis_runtime_state.session_analysis_cache = Some((key, snapshot));
}

fn apply_session_cache_hit(app: &mut App, snapshot: &AnalysisResultSnapshot) -> Result<(), String> {
    app.analysis_runtime_state.last_warnings = snapshot.last_warnings.clone();
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: snapshot.db_path.clone(),
        filter_config_path: snapshot.filter_config_path.clone(),
        filter_config_source_path: snapshot.filter_config_source_path.clone(),
        annotation_csv_path: snapshot.annotation_csv_path.clone(),
    });
    let summary = format!("前回結果を再表示（{}）", snapshot.status_summary);
    app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisSucceeded { summary };
    app.logger
        .info("session analysis cache hit; skipped Python worker analyze request");
    let _ = app.apply_event(ViewerCoreMessage::ReplaceRecords {
        records: snapshot.records.clone(),
        source_label: snapshot.source_label.clone(),
    });
    Ok(())
}

pub(super) fn start_export_job(app: &mut App, output_csv_path: PathBuf) -> Result<(), String> {
    if app.is_any_job_running() {
        return Err("別のジョブが既に実行中です".to_string());
    }

    let export_snapshot = build_visible_export_snapshot(app)?;
    let job_id = build_local_job_id();
    let request_job_id = job_id.clone();
    let db_path = app.db_viewer_state.db_path.clone();
    let filter_config_path = app
        .analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.filter_config_path.clone())
        .or_else(|| resolved_filter_config_path(app).ok())
        .unwrap_or_default();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let started_at = current_timestamp_marker();
        let started = Instant::now();
        let result = write_visible_records_csv(&output_csv_path, &export_snapshot.records);
        let finished_at = current_timestamp_marker();
        let duration_seconds = started.elapsed().as_secs_f64();
        let meta = build_synthetic_export_meta(
            &request_job_id,
            &export_snapshot,
            &db_path,
            &filter_config_path,
            &output_csv_path,
            &started_at,
            &finished_at,
            duration_seconds,
            result.as_ref().err().map(String::as_str).unwrap_or(""),
        );
        let event = match result {
            Ok(()) => AnalysisJobEvent::ExportCompleted(Ok(AnalysisExportSuccess {
                meta,
                output_csv_path,
            })),
            Err(message) => AnalysisJobEvent::ExportCompleted(Err(AnalysisJobFailure {
                meta: Some(meta),
                stderr: String::new(),
                message,
            })),
        };
        let _ = sender.send(event);
    });

    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob {
        receiver,
        analysis_context: None,
    });
    app.analysis_runtime_state.status = AnalysisJobStatus::RunningExport {
        job_id: job_id.clone(),
    };
    app.core.set_expected_job_id(job_id.clone());
    app.logger.info(&format!("export job started: {job_id}"));
    Ok(())
}

#[derive(Clone, Debug)]
struct VisibleExportSnapshot {
    records: Vec<AnalysisRecord>,
    analysis_unit: AnalysisUnit,
    selected_paragraph_count: usize,
    selected_sentence_count: usize,
}

fn build_visible_export_snapshot(app: &App) -> Result<VisibleExportSnapshot, String> {
    let filtered_indices = app.core.filtered_indices.clone();
    if filtered_indices.is_empty() {
        return Err("保存対象の表示レコードがありません".to_string());
    }

    let records: Vec<AnalysisRecord> = filtered_indices
        .iter()
        .map(|&index| {
            app.core
                .all_records
                .get(index)
                .cloned()
                .ok_or_else(|| format!("表示レコードの参照が不正です: index={index}"))
        })
        .collect::<Result<_, _>>()?;

    let Some(first_record) = records.first() else {
        return Err("保存対象の表示レコードがありません".to_string());
    };
    let analysis_unit = first_record.analysis_unit;
    if records
        .iter()
        .any(|record| record.analysis_unit != analysis_unit)
    {
        return Err("保存対象の表示レコードに paragraph / sentence が混在しています".to_string());
    }

    let selected_sentence_count = if analysis_unit == AnalysisUnit::Sentence {
        records.len()
    } else {
        0
    };
    let selected_paragraph_count = if analysis_unit == AnalysisUnit::Paragraph {
        records.len()
    } else {
        records
            .iter()
            .map(|record| record.paragraph_id.clone())
            .collect::<BTreeSet<_>>()
            .len()
    };

    Ok(VisibleExportSnapshot {
        records,
        analysis_unit,
        selected_paragraph_count,
        selected_sentence_count,
    })
}

fn build_synthetic_export_meta(
    job_id: &str,
    snapshot: &VisibleExportSnapshot,
    db_path: &std::path::Path,
    filter_config_path: &std::path::Path,
    output_csv_path: &std::path::Path,
    started_at: &str,
    finished_at: &str,
    duration_seconds: f64,
    error_summary: &str,
) -> AnalysisMeta {
    AnalysisMeta {
        job_id: job_id.to_string(),
        status: if error_summary.is_empty() {
            "succeeded".to_string()
        } else {
            "failed".to_string()
        },
        started_at: started_at.to_string(),
        finished_at: finished_at.to_string(),
        duration_seconds,
        db_path: db_path.display().to_string(),
        filter_config_path: filter_config_path.display().to_string(),
        output_csv_path: output_csv_path.display().to_string(),
        analysis_unit: snapshot.analysis_unit,
        target_paragraph_count: snapshot.selected_paragraph_count,
        selected_paragraph_count: snapshot.selected_paragraph_count,
        selected_sentence_count: snapshot.selected_sentence_count,
        warning_messages: Vec::new(),
        error_summary: error_summary.to_string(),
    }
}

fn build_local_job_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("job-{millis}")
}

fn current_timestamp_marker() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

fn parse_builder_limit(limit_input: &str) -> Result<Option<usize>, String> {
    let trimmed = limit_input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<usize>()
        .map(Some)
        .map_err(|_| format!("limit は正の整数で指定してください: {trimmed}"))
}

pub(super) fn start_build_job(app: &mut App) -> Result<(), String> {
    if app.is_any_job_running() {
        return Err("別のジョブが既に実行中です".to_string());
    }
    app.refresh_builder_runtime();

    let input_dir = app
        .builder_request_state
        .input_dir_path
        .clone()
        .ok_or_else(|| "入力フォルダーを指定してください".to_string())?;
    if !input_dir.is_dir() {
        return Err(format!(
            "入力フォルダーが見つからないか、ディレクトリではありません: {}",
            input_dir.display()
        ));
    }
    let runtime = app
        .builder_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "builder 用 Python 実行環境を解決できません".to_string())?;
    let forbidden_dirs = crate::analysis_runner::resolve_forbidden_dirs(&runtime.project_root);
    if let Some(msg) =
        crate::analysis_runner::check_forbidden_input_dir(&input_dir, &forbidden_dirs)
    {
        return Err(msg);
    }
    if app
        .builder_request_state
        .analysis_db_path
        .as_os_str()
        .is_empty()
    {
        return Err("出力 DB パスを指定してください".to_string());
    }
    if app.builder_request_state.purge && app.builder_request_state.fresh_db {
        return Err("--purge と --fresh-db は同時指定できません".to_string());
    }
    let limit = parse_builder_limit(&app.builder_request_state.limit_input)?;

    let runtime = app
        .builder_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "builder 用 Python 実行環境を解決できません".to_string())?;
    let report_path = app.builder_request_state.resolved_report_path();

    let (job_id, receiver, control) =
        app.analysis_process_host
            .spawn_build_job(AnalysisDbBuildRequest {
                runtime,
                input_dir,
                analysis_db_path: app.builder_request_state.analysis_db_path.clone(),
                report_path: report_path.clone(),
                skip_tokenize: app.builder_request_state.skip_tokenize,
                sudachi_dict: app.builder_request_state.sudachi_dict.as_str().to_string(),
                split_mode: app.builder_request_state.split_mode.as_str().to_string(),
                split_inside_parentheses: app.builder_request_state.split_inside_parentheses,
                merge_table_lines: app.builder_request_state.merge_table_lines,
                purge: app.builder_request_state.purge,
                fresh_db: app.builder_request_state.fresh_db,
                limit,
                note: app.builder_request_state.note_input.trim().to_string(),
            })?;

    app.builder_runtime_state.pending_switch_db_path = None;
    app.builder_runtime_state.current_job = Some(RunningBuildJob {
        receiver,
        control,
        started_at: std::time::Instant::now(),
    });
    app.builder_runtime_state.status = BuilderJobStatus::Running {
        job_id: job_id.clone(),
    };
    app.logger.info(&format!("builder job started: {job_id}"));
    Ok(())
}

pub(super) fn poll_analysis_job(app: &mut App) -> AnalysisJobPollOutput {
    let Some(mut running_job) = app.analysis_runtime_state.current_job.take() else {
        return poll_builder_job(app);
    };

    match running_job.receiver.try_recv() {
        Ok(AnalysisJobEvent::AnalysisCompleted(result)) => {
            let analysis_context = running_job.analysis_context.take();
            match result {
                Ok(success) => {
                    if !app.core.job_id_matches_expected(&success.meta.job_id) {
                        app.core.clear_expected_job_id();
                        app.analysis_runtime_state.status = AnalysisJobStatus::Idle;
                        app.logger.warn(&format!(
                            "ignored stale analysis completion event: {}",
                            success.meta.job_id
                        ));
                        return AnalysisJobPollOutput {
                            core_event: None,
                            needs_repaint: true,
                            repaint_after: None,
                        };
                    }
                    app.core.clear_expected_job_id();
                    return AnalysisJobPollOutput {
                        core_event: Some(handle_analysis_success(app, success, analysis_context)),
                        needs_repaint: true,
                        repaint_after: None,
                    };
                }
                Err(failure) => {
                    let accept = match failure.meta.as_ref() {
                        Some(meta) => app.core.job_id_matches_expected(&meta.job_id),
                        None => app.core.accept_failure_without_meta_job_id(),
                    };
                    if !accept {
                        app.core.clear_expected_job_id();
                        app.analysis_runtime_state.status = AnalysisJobStatus::Idle;
                        app.logger.warn("ignored stale analysis failure event");
                        return AnalysisJobPollOutput {
                            core_event: None,
                            needs_repaint: true,
                            repaint_after: None,
                        };
                    }
                    app.core.clear_expected_job_id();
                    handle_analysis_failure(app, failure, analysis_context);
                    return AnalysisJobPollOutput {
                        core_event: None,
                        needs_repaint: true,
                        repaint_after: None,
                    };
                }
            }
        }
        Ok(AnalysisJobEvent::ExportCompleted(result)) => match result {
            Ok(success) => {
                if !app.core.job_id_matches_expected(&success.meta.job_id) {
                    app.core.clear_expected_job_id();
                    app.analysis_runtime_state.status = AnalysisJobStatus::Idle;
                    app.logger.warn(&format!(
                        "ignored stale export completion event: {}",
                        success.meta.job_id
                    ));
                    return AnalysisJobPollOutput {
                        core_event: None,
                        needs_repaint: true,
                        repaint_after: None,
                    };
                }
                app.core.clear_expected_job_id();
                handle_export_success(app, success);
                return AnalysisJobPollOutput {
                    core_event: None,
                    needs_repaint: true,
                    repaint_after: None,
                };
            }
            Err(failure) => {
                let accept = match failure.meta.as_ref() {
                    Some(meta) => app.core.job_id_matches_expected(&meta.job_id),
                    None => app.core.accept_failure_without_meta_job_id(),
                };
                if !accept {
                    app.core.clear_expected_job_id();
                    app.analysis_runtime_state.status = AnalysisJobStatus::Idle;
                    app.logger.warn("ignored stale export failure event");
                    return AnalysisJobPollOutput {
                        core_event: None,
                        needs_repaint: true,
                        repaint_after: None,
                    };
                }
                app.core.clear_expected_job_id();
                handle_export_failure(app, failure);
                return AnalysisJobPollOutput {
                    core_event: None,
                    needs_repaint: true,
                    repaint_after: None,
                };
            }
        },
        Err(TryRecvError::Empty) => {
            app.analysis_runtime_state.current_job = Some(running_job);
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: false,
                repaint_after: Some(Duration::from_millis(100)),
            }
        }
        Ok(AnalysisJobEvent::BuildCompleted(_)) => {
            app.core.clear_expected_job_id();
            set_failed_status_for_current_job(
                app,
                "想定外の builder 完了イベントを受信しました".to_string(),
            );
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
        Err(TryRecvError::Disconnected) => {
            app.core.clear_expected_job_id();
            set_failed_status_for_current_job(
                app,
                "分析ジョブの完了通知を受け取れませんでした".to_string(),
            );
            app.logger
                .error("analysis job channel disconnected while waiting for completion");
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
    }
}

fn poll_builder_job(app: &mut App) -> AnalysisJobPollOutput {
    let Some(running_job) = app.builder_runtime_state.current_job.as_ref() else {
        return AnalysisJobPollOutput::default();
    };

    match running_job.receiver.try_recv() {
        Ok(AnalysisJobEvent::BuildCompleted(result)) => {
            app.builder_runtime_state.current_job = None;
            match result {
                Ok(success) => {
                    handle_build_success(app, success);
                }
                Err(failure) => {
                    handle_build_failure(app, failure);
                }
            }
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
        Ok(_) => {
            app.builder_runtime_state.current_job = None;
            app.builder_runtime_state.status = BuilderJobStatus::Failed {
                summary: "想定外のイベントを受信しました".to_string(),
            };
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
        Err(TryRecvError::Empty) => AnalysisJobPollOutput {
            core_event: None,
            needs_repaint: false,
            repaint_after: Some(Duration::from_millis(100)),
        },
        Err(TryRecvError::Disconnected) => {
            app.builder_runtime_state.current_job = None;
            app.builder_runtime_state.status = BuilderJobStatus::Failed {
                summary: "DB 生成ジョブの完了通知を受け取れませんでした".to_string(),
            };
            app.logger
                .error("builder job channel disconnected while waiting for completion");
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
    }
}

fn handle_analysis_success(
    app: &mut App,
    success: AnalysisJobSuccess,
    context: Option<AnalysisRunContext>,
) -> ViewerCoreMessage {
    let effective_runtime = runtime_for_completed_analysis(app, &context).cloned();
    let filter_config_path = effective_runtime
        .as_ref()
        .map(|runtime| runtime.filter_config_path.clone())
        .or_else(|| resolved_filter_config_path(app).ok())
        .unwrap_or_default();
    let filter_config_source_path = effective_runtime
        .as_ref()
        .and_then(|runtime| runtime.filter_config_source_path.clone());
    let annotation_csv_path = effective_runtime
        .as_ref()
        .map(|runtime| runtime.annotation_csv_path.clone())
        .unwrap_or_else(|| resolved_annotation_csv_path_for_runtime(app));
    let worker_warnings = success.meta.warning_messages.clone();
    let compile_warnings = context
        .as_ref()
        .map(|ctx| ctx.compile_warnings.clone())
        .unwrap_or_default();
    let merged_warnings = merge_warnings(compile_warnings, worker_warnings);
    let summary = build_status_summary_with_warnings(&success.meta, merged_warnings.len());
    app.analysis_runtime_state.last_warnings = merged_warnings.clone();
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path,
        filter_config_source_path,
        annotation_csv_path,
    });
    app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisSucceeded {
        summary: summary.clone(),
    };
    if let Some(runtime) = effective_runtime.as_ref() {
        try_store_session_analysis_cache(app, &success, runtime, &merged_warnings, &summary);
    }
    app.logger
        .info(&format!("analysis job succeeded: {}", success.meta.job_id));
    let source_label = format!("分析結果: {}", success.meta.job_id);
    ViewerCoreMessage::ReplaceRecords {
        records: success.records,
        source_label,
    }
}

fn handle_export_success(app: &mut App, success: AnalysisExportSuccess) {
    app.logger.info("export job succeeded");
    app.analysis_runtime_state.status = AnalysisJobStatus::ExportSucceeded {
        summary: format!("CSV 保存完了: {}", success.output_csv_path.display()),
    };
    app.error_message = Some(format!(
        "CSV を保存しました。\n\n保存先:\n{}",
        success.output_csv_path.display()
    ));
}

fn handle_build_success(app: &mut App, success: AnalysisDbBuildSuccess) {
    let summary = format!("DB 生成完了: {}", success.analysis_db_path.display());
    app.builder_runtime_state.pending_switch_db_path = Some(success.analysis_db_path.clone());
    app.builder_runtime_state.status = BuilderJobStatus::Succeeded { summary };
    app.logger.info("builder job succeeded");
    app.error_message = Some(format!(
        "DB 生成が完了しました。\n\nDB:\n{}\n\nreport:\n{}",
        success.analysis_db_path.display(),
        success.report_path.display()
    ));
}

fn handle_build_failure(app: &mut App, failure: AnalysisJobFailure) {
    let summary = failure.message.clone();
    app.builder_runtime_state.status = BuilderJobStatus::Failed { summary };
    app.builder_runtime_state.pending_switch_db_path = None;
    app.logger.error("builder job failed");

    let mut error_message = failure.message;
    if !failure.stderr.is_empty() {
        error_message.push_str("\n\nstderr:\n");
        error_message.push_str(&failure.stderr);
    }
    app.error_message = Some(error_message);
}

fn handle_analysis_failure(
    app: &mut App,
    failure: AnalysisJobFailure,
    context: Option<AnalysisRunContext>,
) {
    let worker_warnings = failure
        .meta
        .as_ref()
        .map(|meta| meta.warning_messages.clone())
        .unwrap_or_default();
    let compile_warnings = context
        .as_ref()
        .map(|ctx| ctx.compile_warnings.clone())
        .unwrap_or_default();
    let merged_warnings = merge_warnings(compile_warnings, worker_warnings);
    let summary = failure.message.clone();
    app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisFailed { summary };
    app.logger.error("analysis job failed");
    app.analysis_runtime_state.last_warnings = merged_warnings;
    app.analysis_runtime_state.warning_window_open = false;

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
    app.error_message = Some(error_message);
}

fn handle_export_failure(app: &mut App, failure: AnalysisJobFailure) {
    let summary = failure.message.clone();
    app.analysis_runtime_state.status = AnalysisJobStatus::ExportFailed { summary };
    app.logger.error("export job failed");
    app.analysis_runtime_state.warning_window_open = false;

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
    app.error_message = Some(error_message);
}

fn set_failed_status_for_current_job(app: &mut App, summary: String) {
    let is_export_status = matches!(
        &app.analysis_runtime_state.status,
        AnalysisJobStatus::RunningExport { .. }
            | AnalysisJobStatus::ExportSucceeded { .. }
            | AnalysisJobStatus::ExportFailed { .. }
    );
    app.analysis_runtime_state.status = if is_export_status {
        AnalysisJobStatus::ExportFailed { summary }
    } else {
        AnalysisJobStatus::AnalysisFailed { summary }
    };
}

fn warning_headline(warning: &AnalysisWarningMessage) -> String {
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
                    format!(
                        "distance matching: {requested_mode} -> {used_mode} ({count} / cap {cap})"
                    )
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

fn warning_detail_lines(warning: &AnalysisWarningMessage) -> Vec<String> {
    let mut lines = Vec::new();
    if !warning.message.trim().is_empty() && warning_headline(warning) != warning.message {
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
            lines.push(format!(
                "combinationCount: {count} / cap {cap} (+{})",
                count - cap
            ));
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

pub(super) fn draw_warning_details_window(app: &mut App, ctx: &egui::Context) {
    if !app.analysis_runtime_state.warning_window_open {
        return;
    }

    let mut window_open = app.analysis_runtime_state.warning_window_open;
    egui::Window::new(format!(
        "警告詳細 ({})",
        app.analysis_runtime_state.last_warnings.len()
    ))
    .open(&mut window_open)
    .resizable(true)
    .default_width(620.0)
    .show(ctx, |ui| {
        ScrollArea::vertical()
            .max_height(480.0)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                for (idx, warning) in app.analysis_runtime_state.last_warnings.iter().enumerate() {
                    ui.group(|ui| {
                        ui.label(
                            RichText::new(format!("{}. {}", idx + 1, warning_headline(warning)))
                                .strong(),
                        );
                        for line in warning_detail_lines(warning) {
                            ui.add(egui::Label::new(line).wrap_mode(TextWrapMode::Wrap));
                        }
                    });
                    if idx + 1 < app.analysis_runtime_state.last_warnings.len() {
                        ui.add_space(6.0);
                    }
                }
            });
    });
    app.analysis_runtime_state.warning_window_open = window_open;
}

pub(super) fn guard_root_close_with_dirty_editor(app: &mut App, ctx: &egui::Context) {
    let close_requested = ctx.input(|input| input.viewport().close_requested());
    let input = ViewerCoreCloseInput {
        condition_editor_dirty: app.condition_editor_state.is_dirty,
    };
    if app.core.can_close(&input).is_ok() {
        if close_requested && app.builder_runtime_state.current_job.is_some() {
            app.cancel_running_builder_job();
        }
        return;
    }
    if !close_requested {
        return;
    }

    ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
    app.error_message = Some(
        "condition editor に未保存の変更があるため、アプリ終了を中止しました。保存または破棄してから閉じてください。"
            .to_string(),
    );
    if app.condition_editor_state.window_open {
        app.focus_condition_editor_viewport(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_runner::BuilderRuntimeConfig;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "csv_viewer_app_analysis_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn python_command_for_test() -> OsString {
        std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python"))
    }

    fn write_fake_authoring_cli(project_root: &Path, body: &str) {
        let backend_dir = project_root.join("analysis_backend");
        fs::create_dir_all(&backend_dir).unwrap();
        fs::write(backend_dir.join("__init__.py"), "").unwrap();
        fs::write(backend_dir.join("condition_authoring_cli.py"), body).unwrap();
    }

    fn runtime_for_test(project_root: &Path, filter_config_path: PathBuf) -> AnalysisRuntimeConfig {
        AnalysisRuntimeConfig {
            python_command: python_command_for_test(),
            python_args: vec![],
            python_label: "python-test".to_string(),
            project_root: project_root.to_path_buf(),
            script_path: project_root.join("run-analysis.py"),
            filter_config_path,
            filter_config_source_path: None,
            annotation_csv_path: project_root.join("asset/manual-annotations.csv"),
            jobs_root: project_root.join("runtime/jobs"),
        }
    }

    fn minimal_paragraph_record(row_no: usize) -> AnalysisRecord {
        AnalysisRecord {
            row_no,
            analysis_unit: AnalysisUnit::Paragraph,
            paragraph_id: format!("p-{row_no}"),
            sentence_id: String::new(),
            document_id: "doc-1".to_string(),
            category1: "cat-1".to_string(),
            category2: "cat-2".to_string(),
            sentence_count: "1".to_string(),
            sentence_no_in_paragraph: String::new(),
            sentence_no_in_document: String::new(),
            sentence_text: String::new(),
            sentence_text_tagged: String::new(),
            paragraph_text: format!("paragraph text {row_no}"),
            paragraph_text_tagged: String::new(),
            matched_condition_ids_text: String::new(),
            matched_categories_text: String::new(),
            matched_form_group_ids_text: String::new(),
            matched_form_group_logics_text: String::new(),
            form_group_explanations_text: String::new(),
            text_groups_explanations_text: String::new(),
            mixed_scope_warning_text: String::new(),
            match_group_ids_text: String::new(),
            match_group_count: String::new(),
            annotated_token_count: String::new(),
            manual_annotation_count: String::new(),
            manual_annotation_pairs_text: String::new(),
            manual_annotation_namespaces_text: String::new(),
        }
    }

    fn builder_runtime_for_test(
        project_root: &Path,
        builder_script_path: PathBuf,
    ) -> BuilderRuntimeConfig {
        BuilderRuntimeConfig {
            python_command: python_command_for_test(),
            python_args: vec![],
            python_label: "python-test".to_string(),
            project_root: project_root.to_path_buf(),
            builder_script_path,
        }
    }

    #[test]
    fn poll_analysis_empty_preserves_running_job_context() {
        let mut app = App::new(None);
        let root = temp_root("poll_empty_context");
        let runtime = runtime_for_test(&root, root.join("conditions.json"));
        let warning = compile_warning();
        let (_sender, receiver) = mpsc::channel();
        app.analysis_runtime_state.current_job = Some(RunningAnalysisJob {
            receiver,
            analysis_context: Some(AnalysisRunContext {
                runtime: runtime.clone(),
                compile_warnings: vec![warning.clone()],
            }),
        });

        let output = poll_analysis_job(&mut app);

        assert!(!output.needs_repaint);
        assert_eq!(output.repaint_after, Some(Duration::from_millis(100)));
        let running_job = app
            .analysis_runtime_state
            .current_job
            .as_ref()
            .expect("empty poll must restore running analysis job");
        let context = running_job
            .analysis_context
            .as_ref()
            .expect("empty poll must preserve analysis context");
        assert_eq!(context.runtime, runtime);
        assert_eq!(context.compile_warnings, vec![warning]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn export_job_uses_no_analysis_context() {
        let mut app = App::new(None);
        let root = temp_root("export_no_context");
        let filter_config_path = root.join("conditions.json");
        fs::write(&filter_config_path, r#"{"cooccurrence_conditions": []}"#).unwrap();
        app.analysis_runtime_state.runtime = Some(runtime_for_test(&root, filter_config_path));
        app.core.all_records = vec![minimal_paragraph_record(1)];
        app.core.filtered_indices = vec![0];
        let output_csv_path = root.join("export.csv");

        start_export_job(&mut app, output_csv_path.clone()).unwrap();

        assert!(matches!(
            app.analysis_runtime_state.status,
            AnalysisJobStatus::RunningExport { .. }
        ));
        assert!(app
            .analysis_runtime_state
            .current_job
            .as_ref()
            .expect("export job should be running")
            .analysis_context
            .is_none());

        let deadline = Instant::now() + Duration::from_secs(5);
        while app.analysis_runtime_state.current_job.is_some() {
            let output = poll_analysis_job(&mut app);
            if output.needs_repaint {
                break;
            }
            assert_eq!(output.repaint_after, Some(Duration::from_millis(100)));
            assert!(
                Instant::now() < deadline,
                "export job was not polled to completion"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        assert!(app.analysis_runtime_state.current_job.is_none());
        assert!(matches!(
            app.analysis_runtime_state.status,
            AnalysisJobStatus::ExportSucceeded { .. }
        ));
        assert!(!matches!(
            app.analysis_runtime_state.status,
            AnalysisJobStatus::AnalysisFailed { .. }
        ));
        assert!(output_csv_path.is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn poll_analysis_without_analysis_job_still_polls_builder_job() {
        let mut app = App::new(None);
        let root = temp_root("poll_delegates_builder");
        let input_dir = root.join("input");
        fs::create_dir_all(&input_dir).unwrap();
        let analysis_db_path = root.join("built-analysis.db");
        let report_path = root.join("build-report.json");
        let builder_script_path = root.join("fake_builder.py");
        fs::write(
            &builder_script_path,
            r#"
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--input-dir', required=True)
parser.add_argument('--analysis-db', required=True)
parser.add_argument('--report-path', required=True)
parser.add_argument('--skip-tokenize', action='store_true')
parser.add_argument('--sudachi-dict')
parser.add_argument('--split-mode')
parser.add_argument('--split-inside-parentheses', action='store_true')
parser.add_argument('--merge-table-lines', action='store_true')
parser.add_argument('--purge', action='store_true')
parser.add_argument('--fresh-db', action='store_true')
parser.add_argument('--limit')
parser.add_argument('--note')
args = parser.parse_args()
with open(args.analysis_db, 'w', encoding='utf-8') as f:
    f.write('db')
with open(args.report_path, 'w', encoding='utf-8') as f:
    f.write('{}')
"#,
        )
        .unwrap();
        let (job_id, receiver, control) =
            crate::analysis_runner::spawn_build_job(AnalysisDbBuildRequest {
                runtime: builder_runtime_for_test(&root, builder_script_path),
                input_dir,
                analysis_db_path: analysis_db_path.clone(),
                report_path: report_path.clone(),
                skip_tokenize: true,
                sudachi_dict: "core".to_string(),
                split_mode: "C".to_string(),
                split_inside_parentheses: false,
                merge_table_lines: false,
                purge: false,
                fresh_db: false,
                limit: None,
                note: String::new(),
            })
            .unwrap();
        app.builder_runtime_state.current_job = Some(RunningBuildJob {
            receiver,
            control,
            started_at: Instant::now(),
        });
        app.builder_runtime_state.status = BuilderJobStatus::Running { job_id };
        assert!(app.analysis_runtime_state.current_job.is_none());
        assert!(app.builder_runtime_state.current_job.is_some());

        let deadline = Instant::now() + Duration::from_secs(5);
        while app.builder_runtime_state.current_job.is_some() {
            let output = poll_analysis_job(&mut app);
            if output.needs_repaint {
                break;
            }
            assert_eq!(output.repaint_after, Some(Duration::from_millis(100)));
            assert!(
                Instant::now() < deadline,
                "builder job was not polled to completion"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        assert!(app.analysis_runtime_state.current_job.is_none());
        assert!(app.builder_runtime_state.current_job.is_none());
        assert!(matches!(
            app.builder_runtime_state.status,
            BuilderJobStatus::Succeeded { .. }
        ));
        assert_eq!(
            app.builder_runtime_state.pending_switch_db_path,
            Some(analysis_db_path)
        );
        assert!(report_path.is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_keeps_runtime_json_effective_path() {
        let root = temp_root("runtime_json");
        let config_path = root.join("conditions.json");
        fs::write(&config_path, r#"{"cooccurrence_conditions": []}"#).unwrap();
        let runtime = runtime_for_test(&root, config_path.clone());

        let prepared = prepare_runtime_for_analysis(runtime).unwrap();

        assert_eq!(prepared.runtime.filter_config_path, config_path);
        assert_eq!(prepared.runtime.filter_config_source_path, None);
        assert!(prepared.authoring_warnings.is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_compiles_authoring_json_to_effective_runtime_path() {
        let root = temp_root("authoring_success");
        write_fake_authoring_cli(
            &root,
            r#"
import argparse, json
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--issues-json', required=True)
args = parser.parse_args()
with open(args.output, 'w', encoding='utf-8') as f:
    json.dump({'cooccurrence_conditions': [{'condition_id': 'compiled'}]}, f)
with open(args.issues_json, 'w', encoding='utf-8') as f:
    json.dump([{'code': 'label_ignored', 'severity': 'warning', 'scope': 'rule', 'message': 'warning only'}], f)
"#,
        );
        let source_path = root.join("authoring.json");
        fs::write(
            &source_path,
            r#"{"format":"condition-authoring/v1","rules":[]}"#,
        )
        .unwrap();
        let runtime = runtime_for_test(&root, source_path.clone());

        let prepared = prepare_runtime_for_analysis(runtime).unwrap();

        assert_eq!(
            prepared.runtime.filter_config_source_path,
            Some(source_path)
        );
        assert!(prepared
            .runtime
            .filter_config_path
            .starts_with(root.join("runtime/compiled-conditions")));
        assert!(prepared.runtime.filter_config_path.is_file());
        assert_eq!(prepared.authoring_warnings.len(), 1);
        assert_eq!(prepared.authoring_warnings[0].code, "label_ignored");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_blocks_authoring_error_issue() {
        let root = temp_root("authoring_error_issue");
        write_fake_authoring_cli(
            &root,
            r#"
import argparse, json
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--issues-json', required=True)
args = parser.parse_args()
with open(args.output, 'w', encoding='utf-8') as f:
    json.dump({'cooccurrence_conditions': []}, f)
with open(args.issues_json, 'w', encoding='utf-8') as f:
    json.dump([{'code': 'invalid_rule', 'severity': 'error', 'scope': 'rule', 'message': 'bad rule'}], f)
"#,
        );
        let source_path = root.join("authoring.json");
        fs::write(
            &source_path,
            r#"{"format":"condition-authoring/v1","rules":[]}"#,
        )
        .unwrap();
        let runtime = runtime_for_test(&root, source_path);

        let error = prepare_runtime_for_analysis(runtime).unwrap_err();

        assert!(error.contains("コンパイル中にエラー"), "{error}");
        assert!(error.contains("bad rule"), "{error}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_blocks_authoring_cli_nonzero() {
        let root = temp_root("authoring_nonzero");
        write_fake_authoring_cli(
            &root,
            r#"
import sys
print('compiler failed loudly', file=sys.stderr)
sys.exit(7)
"#,
        );
        let source_path = root.join("authoring.json");
        fs::write(
            &source_path,
            r#"{"format":"condition-authoring/v1","rules":[]}"#,
        )
        .unwrap();
        let runtime = runtime_for_test(&root, source_path);

        let error = prepare_runtime_for_analysis(runtime).unwrap_err();

        assert!(error.contains("exited with code 7"), "{error}");
        assert!(error.contains("compiler failed loudly"), "{error}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_authoring_runtime_cache_key_uses_generated_runtime_content() {
        let root = temp_root("authoring_cache_key");
        write_fake_authoring_cli(
            &root,
            r#"
import argparse, json
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--issues-json', required=True)
args = parser.parse_args()
with open(args.input, 'r', encoding='utf-8') as f:
    source = json.load(f)
compiled_id = source.get('compiled_id', 'none')
with open(args.output, 'w', encoding='utf-8') as f:
    json.dump({'cooccurrence_conditions': [{'condition_id': compiled_id}]}, f)
with open(args.issues_json, 'w', encoding='utf-8') as f:
    json.dump([], f)
"#,
        );
        let db_path = root.join("analysis.db");
        let annotation_path = root.join("annotations.csv");
        fs::write(&db_path, "db").unwrap();
        fs::write(&annotation_path, "annotation").unwrap();
        let source_a = root.join("authoring-a.json");
        let source_b = root.join("authoring-b.json");
        fs::write(
            &source_a,
            r#"{"format":"condition-authoring/v1","compiled_id":"a"}"#,
        )
        .unwrap();
        fs::write(
            &source_b,
            r#"{"format":"condition-authoring/v1","compiled_id":"b"}"#,
        )
        .unwrap();

        let mut runtime_a = runtime_for_test(&root, source_a);
        runtime_a.annotation_csv_path = annotation_path.clone();
        let mut runtime_b = runtime_for_test(&root, source_b);
        runtime_b.annotation_csv_path = annotation_path.clone();
        let prepared_a = prepare_runtime_for_analysis(runtime_a).unwrap();
        let prepared_b = prepare_runtime_for_analysis(runtime_b).unwrap();

        let key_a = build_session_cache_key(
            &db_path,
            &prepared_a.runtime.filter_config_path,
            &annotation_path,
            &prepared_a.runtime,
        )
        .unwrap();
        let key_b = build_session_cache_key(
            &db_path,
            &prepared_b.runtime.filter_config_path,
            &annotation_path,
            &prepared_b.runtime,
        )
        .unwrap();

        assert_ne!(key_a.filter_config_sha256, key_b.filter_config_sha256);
        let _ = fs::remove_dir_all(root);
    }

    // ---- Task 7: warning merge tests ----

    fn compile_warning() -> AnalysisWarningMessage {
        AnalysisWarningMessage {
            code: "authoring_unknown_field".to_string(),
            message: "unknown field ignored".to_string(),
            severity: Some("warning".to_string()),
            scope: Some("authoring_compiler".to_string()),
            condition_id: None,
            field_name: Some("extra".to_string()),
            unit_id: None,
            query_name: None,
            db_path: None,
            requested_mode: None,
            used_mode: None,
            combination_count: None,
            combination_cap: None,
            safety_limit: None,
        }
    }

    fn worker_warning() -> AnalysisWarningMessage {
        AnalysisWarningMessage {
            code: "distance_match_fallback".to_string(),
            message: "mode fallback".to_string(),
            severity: Some("warning".to_string()),
            scope: Some("worker".to_string()),
            condition_id: None,
            field_name: None,
            unit_id: None,
            query_name: None,
            db_path: None,
            requested_mode: Some("exact".to_string()),
            used_mode: Some("approx".to_string()),
            combination_count: None,
            combination_cap: None,
            safety_limit: None,
        }
    }

    fn analysis_meta_for_test(
        job_id: &str,
        warning_messages: Vec<AnalysisWarningMessage>,
    ) -> AnalysisMeta {
        AnalysisMeta {
            job_id: job_id.to_string(),
            status: "succeeded".to_string(),
            started_at: "0".to_string(),
            finished_at: "1".to_string(),
            duration_seconds: 1.5,
            db_path: "/tmp/db.sqlite3".to_string(),
            filter_config_path: "/tmp/worker-filter.json".to_string(),
            output_csv_path: "".to_string(),
            analysis_unit: crate::model::AnalysisUnit::Paragraph,
            target_paragraph_count: 10,
            selected_paragraph_count: 5,
            selected_sentence_count: 0,
            warning_messages,
            error_summary: "".to_string(),
        }
    }

    struct AuthoringSuccessFixture {
        app: App,
        root: PathBuf,
        stale_path: PathBuf,
        effective_path: PathBuf,
        source_path: PathBuf,
        effective_annotation_path: PathBuf,
        effective_runtime: AnalysisRuntimeConfig,
    }

    fn run_authoring_success_fixture(name: &str) -> AuthoringSuccessFixture {
        let mut app = App::new(None);
        let root = temp_root(name);
        let stale_path = root.join("source.authoring.json");
        let effective_path = root.join("effective.runtime.json");
        let source_path = root.join("original.authoring.json");
        let stale_annotation_path = root.join("stale-annotations.csv");
        let effective_annotation_path = root.join("effective-annotations.csv");
        let db_path = root.join("analysis.db");
        fs::write(
            &stale_path,
            r#"{"format":"condition-authoring/v1","rules":[]}"#,
        )
        .unwrap();
        fs::write(
            &source_path,
            r#"{"format":"condition-authoring/v1","rules":[]}"#,
        )
        .unwrap();
        fs::write(
            &effective_path,
            r#"{"cooccurrence_conditions": [{"condition_id":"effective"}]}"#,
        )
        .unwrap();
        fs::write(&stale_annotation_path, "stale\n").unwrap();
        fs::write(&effective_annotation_path, "effective\n").unwrap();
        fs::write(&db_path, "db").unwrap();

        app.db_viewer_state.db_path = db_path.clone();
        let mut stale_runtime = runtime_for_test(&root, stale_path.clone());
        stale_runtime.annotation_csv_path = stale_annotation_path;
        app.analysis_runtime_state.runtime = Some(stale_runtime);
        app.analysis_runtime_state.last_warnings = vec![AnalysisWarningMessage {
            code: "stale_warning".to_string(),
            ..compile_warning()
        }];

        let mut effective_runtime = runtime_for_test(&root, effective_path.clone());
        effective_runtime.filter_config_source_path = Some(source_path.clone());
        effective_runtime.annotation_csv_path = effective_annotation_path.clone();

        let mut success = AnalysisJobSuccess {
            meta: analysis_meta_for_test("success-context", vec![worker_warning()]),
            records: Vec::new(),
        };
        success.meta.db_path = db_path.display().to_string();
        success.meta.filter_config_path = root.join("worker-filter.json").display().to_string();
        let _message = handle_analysis_success(
            &mut app,
            success,
            Some(AnalysisRunContext {
                runtime: effective_runtime.clone(),
                compile_warnings: vec![compile_warning()],
            }),
        );

        AuthoringSuccessFixture {
            app,
            root,
            stale_path,
            effective_path,
            source_path,
            effective_annotation_path,
            effective_runtime,
        }
    }

    #[test]
    fn authoring_analysis_success_uses_effective_runtime_for_export_context() {
        let fixture = run_authoring_success_fixture("authoring_success_export_context");

        let export_context = fixture
            .app
            .analysis_runtime_state
            .last_export_context
            .as_ref()
            .expect("success should populate export context");
        assert_eq!(export_context.filter_config_path, fixture.effective_path);
        assert_eq!(
            export_context.filter_config_source_path,
            Some(fixture.source_path.clone())
        );
        assert_eq!(
            export_context.display_filter_config_path().as_ref(),
            fixture.source_path.as_path()
        );
        assert_eq!(
            export_context.annotation_csv_path,
            fixture.effective_annotation_path
        );
        assert_ne!(export_context.filter_config_path, fixture.stale_path);
        assert_ne!(
            export_context.filter_config_path,
            fixture
                .app
                .analysis_runtime_state
                .runtime
                .as_ref()
                .expect("stale app runtime is intentionally present")
                .filter_config_path
        );

        let _ = fs::remove_dir_all(fixture.root);
    }

    #[test]
    fn authoring_analysis_success_cache_snapshot_stores_merged_warnings() {
        let fixture = run_authoring_success_fixture("authoring_success_cache_snapshot");

        let (_cache_key, snapshot) = fixture
            .app
            .analysis_runtime_state
            .session_analysis_cache
            .as_ref()
            .expect("successful analysis with valid runtime paths should populate session cache");
        assert_eq!(snapshot.last_warnings.len(), 2);
        assert_eq!(snapshot.last_warnings[0].code, "authoring_unknown_field");
        assert_eq!(snapshot.last_warnings[1].code, "distance_match_fallback");
        assert_eq!(snapshot.filter_config_path, fixture.effective_path);
        assert_eq!(
            snapshot.filter_config_source_path,
            Some(fixture.source_path.clone())
        );
        assert_eq!(
            snapshot.annotation_csv_path,
            fixture.effective_annotation_path
        );
        assert_ne!(snapshot.filter_config_path, fixture.stale_path);
        assert!(matches!(
            fixture.app.analysis_runtime_state.status,
            AnalysisJobStatus::AnalysisSucceeded { ref summary } if summary.contains("警告 2 件")
        ));
        assert!(
            snapshot.status_summary.contains("警告 2 件"),
            "snapshot summary should preserve merged warning count: {}",
            snapshot.status_summary
        );

        let _ = fs::remove_dir_all(fixture.root);
    }

    #[test]
    fn authoring_analysis_cache_key_uses_effective_runtime_for_store() {
        let fixture = run_authoring_success_fixture("authoring_cache_key_store");

        let (stored_key, _snapshot) = fixture
            .app
            .analysis_runtime_state
            .session_analysis_cache
            .as_ref()
            .expect("successful analysis with valid runtime paths should populate session cache");
        let expected_key = build_session_cache_key(
            &fixture.app.db_viewer_state.db_path,
            &fixture.effective_runtime.filter_config_path,
            &fixture.effective_runtime.annotation_csv_path,
            &fixture.effective_runtime,
        )
        .expect("fixture paths should produce a cache key");
        assert_eq!(stored_key, &expected_key);
        assert_ne!(
            stored_key.filter_config_sha256,
            build_session_cache_key(
                &fixture.app.db_viewer_state.db_path,
                &fixture.stale_path,
                &fixture.effective_runtime.annotation_csv_path,
                &fixture.effective_runtime,
            )
            .expect("stale fixture path should still be hashable")
            .filter_config_sha256
        );

        let _ = fs::remove_dir_all(fixture.root);
    }

    #[test]
    fn authoring_analysis_failure_merges_compile_and_worker_warnings() {
        let mut app = App::new(None);
        let root = temp_root("authoring_failure_merged_warnings");
        let runtime = runtime_for_test(&root, root.join("effective.runtime.json"));
        app.analysis_runtime_state.warning_window_open = true;
        app.analysis_runtime_state.last_warnings = vec![AnalysisWarningMessage {
            code: "stale_warning".to_string(),
            ..compile_warning()
        }];
        let failure = AnalysisJobFailure {
            meta: Some(analysis_meta_for_test(
                "failure-context",
                vec![worker_warning()],
            )),
            stderr: String::new(),
            message: "failed".to_string(),
        };

        handle_analysis_failure(
            &mut app,
            failure,
            Some(AnalysisRunContext {
                runtime,
                compile_warnings: vec![compile_warning()],
            }),
        );

        assert_eq!(app.analysis_runtime_state.last_warnings.len(), 2);
        assert_eq!(
            app.analysis_runtime_state.last_warnings[0].code,
            "authoring_unknown_field"
        );
        assert_eq!(
            app.analysis_runtime_state.last_warnings[1].code,
            "distance_match_fallback"
        );
        assert!(!app
            .analysis_runtime_state
            .last_warnings
            .iter()
            .any(|warning| warning.code == "stale_warning"));
        assert!(!app.analysis_runtime_state.warning_window_open);
        assert!(matches!(
            app.analysis_runtime_state.status,
            AnalysisJobStatus::AnalysisFailed { ref summary } if summary == "failed"
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn analysis_failure_without_context_uses_worker_warnings_only() {
        let mut app = App::new(None);
        app.analysis_runtime_state.warning_window_open = true;
        app.analysis_runtime_state.last_warnings = vec![AnalysisWarningMessage {
            code: "stale_warning".to_string(),
            ..compile_warning()
        }];
        let failure = AnalysisJobFailure {
            meta: Some(analysis_meta_for_test(
                "failure-without-context",
                vec![worker_warning()],
            )),
            stderr: String::new(),
            message: "failed without context".to_string(),
        };

        handle_analysis_failure(&mut app, failure, None);

        assert_eq!(app.analysis_runtime_state.last_warnings.len(), 1);
        assert_eq!(
            app.analysis_runtime_state.last_warnings[0].code,
            "distance_match_fallback"
        );
        assert!(!app
            .analysis_runtime_state
            .last_warnings
            .iter()
            .any(|warning| warning.code == "authoring_unknown_field"
                || warning.code == "stale_warning"));
        assert!(!app.analysis_runtime_state.warning_window_open);
        assert!(matches!(
            app.analysis_runtime_state.status,
            AnalysisJobStatus::AnalysisFailed { ref summary } if summary == "failed without context"
        ));
    }

    #[test]
    fn analysis_result_snapshot_preserves_filter_config_source_path() {
        let source_path = PathBuf::from("/tmp/source.authoring.json");
        let effective_path = PathBuf::from("/tmp/effective.runtime.json");
        let mut runtime = runtime_for_test(&PathBuf::from("/tmp/project"), effective_path.clone());
        runtime.filter_config_source_path = Some(source_path.clone());
        let success = AnalysisJobSuccess {
            meta: AnalysisMeta {
                job_id: "test".to_string(),
                status: "succeeded".to_string(),
                started_at: "0".to_string(),
                finished_at: "1".to_string(),
                duration_seconds: 1.0,
                db_path: "/tmp/db.sqlite3".to_string(),
                filter_config_path: "/tmp/worker-filter.json".to_string(),
                output_csv_path: "".to_string(),
                analysis_unit: crate::model::AnalysisUnit::Paragraph,
                target_paragraph_count: 0,
                selected_paragraph_count: 0,
                selected_sentence_count: 0,
                warning_messages: Vec::new(),
                error_summary: "".to_string(),
            },
            records: Vec::new(),
        };

        let merged_warnings = vec![worker_warning()];
        let status_summary =
            build_status_summary_with_warnings(&success.meta, merged_warnings.len());
        let snapshot = analysis_result_snapshot_from_success(
            &success,
            &runtime,
            &merged_warnings,
            &status_summary,
        );

        assert_eq!(snapshot.filter_config_path, effective_path);
        assert_eq!(snapshot.filter_config_source_path, Some(source_path));
        assert_eq!(snapshot.last_warnings, merged_warnings);
        assert_eq!(snapshot.status_summary, status_summary);
    }

    #[test]
    fn apply_session_cache_hit_restores_filter_config_source_path() {
        let mut app = App::new(None);
        let source_path = PathBuf::from("/tmp/source.authoring.json");
        let effective_path = PathBuf::from("/tmp/effective.runtime.json");
        let snapshot = AnalysisResultSnapshot {
            records: Vec::new(),
            source_label: "cached".to_string(),
            last_warnings: Vec::new(),
            db_path: PathBuf::from("/tmp/db.sqlite3"),
            filter_config_path: effective_path.clone(),
            filter_config_source_path: Some(source_path.clone()),
            annotation_csv_path: PathBuf::from("/tmp/annotations.csv"),
            status_summary: "分析完了".to_string(),
        };

        apply_session_cache_hit(&mut app, &snapshot).unwrap();

        let export_context = app.analysis_runtime_state.last_export_context.unwrap();
        assert_eq!(export_context.filter_config_path, effective_path);
        assert_eq!(
            export_context.filter_config_source_path,
            Some(source_path.clone())
        );
        assert_eq!(
            export_context.display_filter_config_path(),
            source_path.as_path()
        );
    }

    #[test]
    fn merge_compile_and_worker_warnings_both_present() {
        let compile = vec![compile_warning()];
        let worker = vec![worker_warning()];
        let merged = merge_warnings(compile, worker);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].code, "authoring_unknown_field");
        assert_eq!(merged[1].code, "distance_match_fallback");
    }

    #[test]
    fn merge_compile_warnings_only_preserved() {
        let compile = vec![compile_warning()];
        let merged = merge_warnings(compile, vec![]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].code, "authoring_unknown_field");
    }

    #[test]
    fn merge_worker_warnings_only_when_no_compile_warnings() {
        let worker = vec![worker_warning()];
        let merged = merge_warnings(vec![], worker);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].code, "distance_match_fallback");
    }

    #[test]
    fn merge_warnings_no_duplication_in_single_run() {
        let compile = vec![compile_warning()];
        let worker = vec![worker_warning()];
        let merged = merge_warnings(compile.clone(), worker);
        let compile_count = merged
            .iter()
            .filter(|w| w.code == "authoring_unknown_field")
            .count();
        assert_eq!(
            compile_count, 1,
            "compile warning should appear exactly once"
        );
    }

    #[test]
    fn build_status_summary_reflects_total_warning_count() {
        let meta = AnalysisMeta {
            job_id: "test".to_string(),
            status: "succeeded".to_string(),
            started_at: "0".to_string(),
            finished_at: "1".to_string(),
            duration_seconds: 1.5,
            db_path: "db".to_string(),
            filter_config_path: "cfg".to_string(),
            output_csv_path: "".to_string(),
            analysis_unit: crate::model::AnalysisUnit::Paragraph,
            target_paragraph_count: 10,
            selected_paragraph_count: 5,
            selected_sentence_count: 0,
            warning_messages: vec![worker_warning()],
            error_summary: "".to_string(),
        };
        let summary = build_status_summary_with_warnings(&meta, 3);
        assert!(
            summary.contains("警告 3 件"),
            "summary should show total warnings: {}",
            summary
        );
    }

    #[test]
    fn build_status_summary_zero_warnings_omits_warning_text() {
        let meta = AnalysisMeta {
            job_id: "test".to_string(),
            status: "succeeded".to_string(),
            started_at: "0".to_string(),
            finished_at: "1".to_string(),
            duration_seconds: 1.5,
            db_path: "db".to_string(),
            filter_config_path: "cfg".to_string(),
            output_csv_path: "".to_string(),
            analysis_unit: crate::model::AnalysisUnit::Paragraph,
            target_paragraph_count: 10,
            selected_paragraph_count: 5,
            selected_sentence_count: 0,
            warning_messages: vec![],
            error_summary: "".to_string(),
        };
        let summary = build_status_summary_with_warnings(&meta, 0);
        assert!(
            !summary.contains("警告"),
            "summary should not mention warnings: {}",
            summary
        );
    }
}
