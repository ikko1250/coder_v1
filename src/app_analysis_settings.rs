//! 分析設定オーバーレイ（Python / 条件 JSON / annotation CSV の上書き）。親モジュール `app` の子。

use super::App;
use crate::ui_helpers::ime_safe_singleline;
use eframe::egui::{self, Ui};
use std::path::PathBuf;

pub(super) fn draw_analysis_settings_window(app: &mut App, ctx: &egui::Context) {
    if !app.analysis_request_state.settings_window_open {
        return;
    }

    let mut window_open = app.analysis_request_state.settings_window_open;
    let mut selected_python_path = None;
    let mut selected_filter_config_path = None;
    let mut selected_annotation_csv_path = None;
    let mut clear_python_override = false;
    let mut clear_filter_config_override = false;
    let mut clear_annotation_csv_override = false;
    let settings_enabled = !app.is_any_job_running();
    let python_override_label = app
        .analysis_request_state
        .python_path_override
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "自動解決".to_string());
    let filter_override_label = app
        .analysis_request_state
        .filter_config_path_override
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "既定値 (asset/cooccurrence-conditions.json)".to_string());
    let annotation_override_label = app
        .analysis_request_state
        .annotation_csv_path_override
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "既定値 (asset/manual-annotations.csv)".to_string());
    let resolved_python_label = app
        .analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.python_label.clone())
        .unwrap_or_else(|| "-".to_string());
    let resolved_filter_label = app
        .analysis_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.filter_config_path.display().to_string())
        .unwrap_or_else(|| "-".to_string());
    let resolved_annotation_label = app
        .resolved_annotation_csv_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|error| format!("解決失敗: {error}"));
    let status_text = app.analysis_runtime_state.status_text();

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
                || app.file_dialog_host.pick_python_executable(),
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
                || app.file_dialog_host.pick_open_json(),
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
                || app.file_dialog_host.pick_save_annotation_csv(),
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

    app.analysis_request_state.settings_window_open = window_open;

    let mut runtime_changed = false;
    if let Some(path) = selected_python_path {
        app.analysis_request_state.python_path_override = Some(path);
        runtime_changed = true;
    }
    if clear_python_override {
        app.analysis_request_state.python_path_override = None;
        runtime_changed = true;
    }
    if let Some(path) = selected_filter_config_path {
        app.analysis_request_state.filter_config_path_override = Some(path);
        runtime_changed = true;
    }
    if clear_filter_config_override {
        app.analysis_request_state.filter_config_path_override = None;
        runtime_changed = true;
    }
    if let Some(path) = selected_annotation_csv_path {
        app.analysis_request_state.annotation_csv_path_override = Some(path);
        runtime_changed = true;
    }
    if clear_annotation_csv_override {
        app.analysis_request_state.annotation_csv_path_override = None;
        runtime_changed = true;
    }

    if runtime_changed {
        app.refresh_analysis_runtime();
        ctx.request_repaint();
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
