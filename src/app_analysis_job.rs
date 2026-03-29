//! Python 分析・エクスポート・警告ウィンドウ・終了ガード（未保存）。親モジュール `app` の子。

use super::{
    AnalysisExportContext, AnalysisJobStatus, AnalysisRuntimeState, App, BuilderJobStatus,
    RunningAnalysisJob, RunningBuildJob,
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

    let (job_id, receiver) = app.analysis_process_host.spawn_analysis_job(AnalysisJobRequest {
        db_path: app.db_viewer_state.db_path.clone(),
        runtime,
        force_reload_db_frames,
    });

    app.analysis_runtime_state.last_warnings.clear();
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
    app.analysis_runtime_state.status = AnalysisJobStatus::RunningAnalysis {
        job_id: job_id.clone(),
    };
    app.core.set_expected_job_id(job_id.clone());
    app.logger.info(&format!("analysis job started: {job_id}"));
    Ok(())
}

fn resolved_annotation_csv_path_for_runtime(app: &App) -> PathBuf {
    app.analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.annotation_csv_path.clone())
        .or_else(|| app.resolved_annotation_csv_path().ok())
        .unwrap_or_default()
}

fn current_session_cache_key(app: &App, runtime: &AnalysisRuntimeConfig) -> Option<AnalysisSessionCacheKey> {
    let annotation_csv_path = resolved_annotation_csv_path_for_runtime(app);
    build_session_cache_key(
        &app.db_viewer_state.db_path,
        &runtime.filter_config_path,
        &annotation_csv_path,
        runtime,
    )
}

fn build_status_summary_from_meta(meta: &AnalysisMeta) -> String {
    let warning_count = meta.warning_messages.len();
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

fn analysis_result_snapshot_from_success(app: &App, success: &AnalysisJobSuccess) -> AnalysisResultSnapshot {
    let annotation_csv_path = resolved_annotation_csv_path_for_runtime(app);
    AnalysisResultSnapshot {
        records: success.records.clone(),
        source_label: format!("分析結果: {}", success.meta.job_id),
        last_warnings: success.meta.warning_messages.clone(),
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path: PathBuf::from(&success.meta.filter_config_path),
        annotation_csv_path,
        status_summary: build_status_summary_from_meta(&success.meta),
    }
}

fn try_store_session_analysis_cache(app: &mut App, success: &AnalysisJobSuccess) {
    let Some(runtime) = app.analysis_runtime_state.runtime.as_ref() else {
        return;
    };
    let Some(key) = current_session_cache_key(app, runtime) else {
        return;
    };
    let snapshot = analysis_result_snapshot_from_success(app, success);
    app.analysis_runtime_state.session_analysis_cache = Some((key, snapshot));
}

fn apply_session_cache_hit(app: &mut App, snapshot: &AnalysisResultSnapshot) -> Result<(), String> {
    app.analysis_runtime_state.last_warnings = snapshot.last_warnings.clone();
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: snapshot.db_path.clone(),
        filter_config_path: snapshot.filter_config_path.clone(),
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

    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
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
    if app.builder_request_state.analysis_db_path.as_os_str().is_empty() {
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
        app.analysis_process_host.spawn_build_job(AnalysisDbBuildRequest {
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
    let Some(running_job) = app.analysis_runtime_state.current_job.as_ref() else {
        return poll_builder_job(app);
    };

    match running_job.receiver.try_recv() {
        Ok(AnalysisJobEvent::AnalysisCompleted(result)) => {
            app.analysis_runtime_state.current_job = None;
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
                        core_event: Some(handle_analysis_success(app, success)),
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
                    handle_analysis_failure(app, failure);
                    return AnalysisJobPollOutput {
                        core_event: None,
                        needs_repaint: true,
                        repaint_after: None,
                    };
                }
            }
        }
        Ok(AnalysisJobEvent::ExportCompleted(result)) => {
            app.analysis_runtime_state.current_job = None;
            match result {
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
            }
        }
        Err(TryRecvError::Empty) => AnalysisJobPollOutput {
            core_event: None,
            needs_repaint: false,
            repaint_after: Some(Duration::from_millis(100)),
        },
        Ok(AnalysisJobEvent::BuildCompleted(_)) => {
            app.analysis_runtime_state.current_job = None;
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
            app.analysis_runtime_state.current_job = None;
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

fn handle_analysis_success(app: &mut App, success: AnalysisJobSuccess) -> ViewerCoreMessage {
    try_store_session_analysis_cache(app, &success);

    let warnings = success.meta.warning_messages.clone();
    let source_label = format!("分析結果: {}", success.meta.job_id);
    let summary = build_status_summary_from_meta(&success.meta);
    app.analysis_runtime_state.last_warnings = warnings;
    app.analysis_runtime_state.warning_window_open = false;
    let annotation_csv_path = resolved_annotation_csv_path_for_runtime(app);
    app.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path: PathBuf::from(&success.meta.filter_config_path),
        annotation_csv_path,
    });
    app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisSucceeded { summary };
    app.logger
        .info(&format!("analysis job succeeded: {}", success.meta.job_id));
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

fn handle_analysis_failure(app: &mut App, failure: AnalysisJobFailure) {
    let warnings = failure
        .meta
        .as_ref()
        .map(|meta| meta.warning_messages.clone())
        .unwrap_or_default();
    let summary = failure.message.clone();
    app.analysis_runtime_state.status = AnalysisJobStatus::AnalysisFailed { summary };
    app.logger.error("analysis job failed");
    app.analysis_runtime_state.last_warnings = warnings;
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
