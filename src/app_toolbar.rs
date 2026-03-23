//! トップツールバー（CSV オープン・分析・設定・状態表示）。親モジュール `app` の子としており、
//! `App` の非公開フィールド・メソッドにアクセスできる。

use super::AnalysisJobStatus;
use super::App;
use crate::ui_helpers::ime_safe_singleline;
use eframe::egui::{self, RichText, Ui};
use egui::Color32;

pub(super) fn draw_toolbar(app: &mut App, ui: &mut Ui) {
    ui.vertical(|ui| {
        ui.horizontal(|ui| {
            ui.label("表示元:");
            let path_str = app.records_source_label.clone();
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
                    app.load_csv(path);
                }
            }

            ui.separator();
            let selected_position = app
                .core.selected_row
                .map(|idx| idx + 1)
                .map(|position| position.to_string())
                .unwrap_or_else(|| "-".to_string());
            ui.label(format!(
                "総件数: {} 件  抽出後: {} 件  選択: {} / {}",
                app.core.all_records.len(),
                app.core.filtered_indices.len(),
                selected_position,
                app.core.filtered_indices.len()
            ));
        });

        ui.horizontal_wrapped(|ui| {
            let can_start = app.analysis_runtime_state.can_start();
            let can_export = app.analysis_runtime_state.can_export();
            let settings_enabled = app.analysis_runtime_state.current_job.is_none();
            let python_label = app
                .analysis_runtime_state
                .runtime
                .as_ref()
                .map(|runtime| runtime.python_label.clone())
                .unwrap_or_else(|| "-".to_string());
            let filter_config_label = app
                .analysis_runtime_state
                .runtime
                .as_ref()
                .map(|runtime| runtime.filter_config_path.display().to_string())
                .unwrap_or_else(|| "-".to_string());
            let annotation_label = app
                .resolved_annotation_csv_path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|_| "-".to_string());
            let db_label = app.db_viewer_state.db_path.display().to_string();

            if matches!(
                app.analysis_runtime_state.status,
                AnalysisJobStatus::RunningAnalysis { .. } | AnalysisJobStatus::RunningExport { .. }
            ) {
                ui.add(egui::Spinner::new());
            }

            if ui
                .add_enabled(can_start, egui::Button::new("分析実行"))
                .clicked()
            {
                if let Err(error) = app.start_analysis_job() {
                    app.error_message = Some(error);
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
                    if let Err(error) = app.start_export_job(path) {
                        app.error_message = Some(error);
                    }
                }
            }

            if ui
                .add_enabled(settings_enabled, egui::Button::new("分析設定"))
                .clicked()
            {
                app.analysis_request_state.settings_window_open = true;
            }

            if ui
                .add_enabled(settings_enabled, egui::Button::new("条件編集"))
                .clicked()
            {
                if let Err(error) = app.open_condition_editor(ui.ctx()) {
                    app.error_message = Some(error);
                }
            }

            ui.label(format!("DB: {db_label}"));
            ui.label(format!("条件: {filter_config_label}"));
            ui.label(format!("Annotation: {annotation_label}"));
            ui.label(format!("Python: {python_label}"));
            if can_export {
                ui.label("保存対象は直近分析結果の全件です");
            }
            if app.analysis_runtime_state.has_warning_details() && ui.button("警告詳細").clicked()
            {
                app.analysis_runtime_state.warning_window_open = true;
            }

            let status_text = app.analysis_runtime_state.status_text();
            let status_color = analysis_status_color(ui, &app.analysis_runtime_state.status);
            ui.label(RichText::new(status_text).color(status_color));
        });
    });
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
