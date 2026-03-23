//! Python 分析・エクスポート・警告ウィンドウ・終了ガード（未保存）。親モジュール `app` の子。

use super::{
    AnalysisExportContext, AnalysisJobStatus, AnalysisRuntimeState, App, RunningAnalysisJob,
};
use crate::analysis_runner::{
    build_runtime_config, cleanup_job_directories, resolve_filter_config_path, AnalysisExportRequest,
    AnalysisExportSuccess, AnalysisJobEvent, AnalysisJobFailure, AnalysisJobRequest, AnalysisJobSuccess,
    AnalysisWarningMessage,
};
use crate::viewer_core::{ViewerCoreCloseInput, ViewerCoreMessage};
use eframe::egui::{self, RichText, ScrollArea};
use egui::TextWrapMode;
use std::path::PathBuf;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

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
        app.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary: error };
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

pub(super) fn resolved_filter_config_path(app: &App) -> Result<PathBuf, String> {
    if let Some(runtime) = app.analysis_runtime_state.runtime.as_ref() {
        return Ok(runtime.filter_config_path.clone());
    }
    resolve_filter_config_path(&app.analysis_request_state.runtime_overrides())
}

pub(super) fn start_analysis_job(app: &mut App) -> Result<(), String> {
    if app.analysis_runtime_state.current_job.is_some() {
        return Err("分析ジョブは既に実行中です".to_string());
    }

    let runtime = app
        .analysis_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;

    cleanup_job_directories(&runtime.jobs_root)?;

    let (job_id, receiver) = app.analysis_process_host.spawn_analysis_job(AnalysisJobRequest {
        db_path: app.db_viewer_state.db_path.clone(),
        runtime,
    });

    app.analysis_runtime_state.last_warnings.clear();
    app.analysis_runtime_state.warning_window_open = false;
    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
    app.analysis_runtime_state.status = AnalysisJobStatus::RunningAnalysis {
        job_id: job_id.clone(),
    };
    app.core.set_expected_job_id(job_id);
    Ok(())
}

pub(super) fn start_export_job(app: &mut App, output_csv_path: PathBuf) -> Result<(), String> {
    if app.analysis_runtime_state.current_job.is_some() {
        return Err("分析ジョブは既に実行中です".to_string());
    }

    let runtime = app
        .analysis_runtime_state
        .runtime
        .clone()
        .ok_or_else(|| "Python 実行環境を解決できません".to_string())?;
    let export_context = app
        .analysis_runtime_state
        .last_export_context
        .clone()
        .ok_or_else(|| "保存対象の分析結果がありません".to_string())?;

    let (job_id, receiver) = app.analysis_process_host.spawn_export_job(AnalysisExportRequest {
        db_path: export_context.db_path,
        filter_config_path: export_context.filter_config_path,
        annotation_csv_path: export_context.annotation_csv_path,
        output_csv_path,
        runtime,
    });

    app.analysis_runtime_state.current_job = Some(RunningAnalysisJob { receiver });
    app.analysis_runtime_state.status = AnalysisJobStatus::RunningExport {
        job_id: job_id.clone(),
    };
    app.core.set_expected_job_id(job_id);
    Ok(())
}

pub(super) fn poll_analysis_job(app: &mut App) -> AnalysisJobPollOutput {
    let Some(running_job) = app.analysis_runtime_state.current_job.as_ref() else {
        return AnalysisJobPollOutput::default();
    };

    match running_job.receiver.try_recv() {
        Ok(AnalysisJobEvent::AnalysisCompleted(result)) => {
            app.analysis_runtime_state.current_job = None;
            match result {
                Ok(success) => {
                    if !app.core.job_id_matches_expected(&success.meta.job_id) {
                        app.core.clear_expected_job_id();
                        app.analysis_runtime_state.status = AnalysisJobStatus::Idle;
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
        Err(TryRecvError::Empty) => AnalysisJobPollOutput {
            core_event: None,
            needs_repaint: false,
            repaint_after: Some(Duration::from_millis(100)),
        },
        Err(TryRecvError::Disconnected) => {
            app.analysis_runtime_state.current_job = None;
            app.core.clear_expected_job_id();
            app.analysis_runtime_state.status = AnalysisJobStatus::Failed {
                summary: "分析ジョブの完了通知を受け取れませんでした".to_string(),
            };
            AnalysisJobPollOutput {
                core_event: None,
                needs_repaint: true,
                repaint_after: None,
            }
        }
    }
}

fn handle_analysis_success(app: &mut App, success: AnalysisJobSuccess) -> ViewerCoreMessage {
    let warnings = success.meta.warning_messages.clone();
    let warning_count = warnings.len();
    let source_label = format!("分析結果: {}", success.meta.job_id);
    let mut summary = format!(
        "{}{}抽出 / {:.2} 秒",
        success.meta.selected_unit_count(),
        success.meta.analysis_unit.count_label(),
        success.meta.duration_seconds
    );
    if warning_count > 0 {
        summary.push_str(&format!(" / 警告 {} 件", warning_count));
    }
    app.analysis_runtime_state.last_warnings = warnings;
    app.analysis_runtime_state.warning_window_open = false;
    let annotation_csv_path = app
        .analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.annotation_csv_path.clone())
        .or_else(|| app.resolved_annotation_csv_path().ok())
        .unwrap_or_default();
    app.analysis_runtime_state.last_export_context = Some(AnalysisExportContext {
        db_path: PathBuf::from(&success.meta.db_path),
        filter_config_path: PathBuf::from(&success.meta.filter_config_path),
        annotation_csv_path,
    });
    app.analysis_runtime_state.status = AnalysisJobStatus::Succeeded { summary };
    ViewerCoreMessage::ReplaceRecords {
        records: success.records,
        source_label,
    }
}

fn handle_export_success(app: &mut App, success: AnalysisExportSuccess) {
    app.analysis_runtime_state.status = AnalysisJobStatus::Succeeded {
        summary: format!("CSV 保存完了: {}", success.output_csv_path.display()),
    };
    app.error_message = Some(format!(
        "CSV を保存しました。\n\n保存先:\n{}",
        success.output_csv_path.display()
    ));
}

fn handle_analysis_failure(app: &mut App, failure: AnalysisJobFailure) {
    let warnings = failure
        .meta
        .as_ref()
        .map(|meta| meta.warning_messages.clone())
        .unwrap_or_default();
    let summary = failure.message.clone();
    app.analysis_runtime_state.status = AnalysisJobStatus::Failed { summary };
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
    let input = ViewerCoreCloseInput {
        condition_editor_dirty: app.condition_editor_state.is_dirty,
    };
    if app.core.can_close(&input).is_ok() {
        return;
    }
    let close_requested = ctx.input(|input| input.viewport().close_requested());
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
