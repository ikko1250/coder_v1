//! DB 生成 builder 設定ウィンドウ。親モジュール `app` の子。

use super::{App, BuilderSplitMode, BuilderSudachiDict};
use crate::ui_helpers::ime_safe_singleline;
use eframe::egui::{self, Ui};
use std::path::PathBuf;

pub(super) fn draw_builder_settings_window(app: &mut App, ctx: &egui::Context) {
    if !app.builder_request_state.settings_window_open {
        return;
    }

    let mut window_open = app.builder_request_state.settings_window_open;
    let settings_enabled = !app.is_any_job_running();
    let mut selected_python_path = None;
    let mut selected_input_dir = None;
    let mut selected_analysis_db_path = None;
    let mut selected_report_path = None;
    let mut clear_python_override = false;
    let mut clear_report_override = false;
    let python_override_label = app
        .builder_request_state
        .python_path_override
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "自動解決".to_string());
    let input_dir_label = app
        .builder_request_state
        .input_dir_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "未選択".to_string());
    let analysis_db_label = app
        .builder_request_state
        .analysis_db_path
        .display()
        .to_string();
    let report_path_label = app.builder_request_state.resolved_report_path().display().to_string();
    let resolved_python_label = app
        .builder_runtime_state
        .runtime
        .as_ref()
        .map(|runtime| runtime.python_label.clone())
        .unwrap_or_else(|| "-".to_string());
    let status_text = app.builder_runtime_state.status_text();

    egui::Window::new("[Builder] DB生成設定")
        .open(&mut window_open)
        .resizable(true)
        .default_width(720.0)
        .show(ctx, |ui| {
            ui.label("フォルダー入力から Analysis DB を生成します。");
            ui.label("この設定は現在のセッション内だけで有効です。");
            ui.label(
                "入力 .txt / .md のファイル名（拡張子除く stem）は \
                 <category1>_<category2> または <数字ID>_<category1>_<category2>。\
                 category 値に _ は含めません（Python ビルダーと同じ規則）。",
            );
            ui.separator();

            draw_path_override_row(
                ui,
                "Python 実行ファイル",
                &python_override_label,
                settings_enabled,
                || app.file_dialog_host.pick_python_executable(),
                &mut selected_python_path,
                Some((&mut clear_python_override, "自動解決")),
            );
            ui.label(format!("現在の解決結果: {resolved_python_label}"));
            ui.separator();

            draw_path_override_row(
                ui,
                "入力フォルダー",
                &input_dir_label,
                settings_enabled,
                || app.file_dialog_host.pick_open_folder(),
                &mut selected_input_dir,
                None,
            );
            ui.separator();

            draw_path_override_row(
                ui,
                "出力 DB",
                &analysis_db_label,
                settings_enabled,
                || app.file_dialog_host.pick_save_analysis_db(),
                &mut selected_analysis_db_path,
                None,
            );
            ui.separator();

            draw_path_override_row(
                ui,
                "report JSON",
                &report_path_label,
                settings_enabled,
                || app.file_dialog_host.pick_save_report_json(),
                &mut selected_report_path,
                Some((&mut clear_report_override, "既定値")),
            );
            ui.separator();

            ui.horizontal(|ui| {
                ui.checkbox(
                    &mut app.builder_request_state.skip_tokenize,
                    "tokenize をスキップ",
                );
                ui.checkbox(
                    &mut app.builder_request_state.split_inside_parentheses,
                    "括弧内でも文分割",
                );
                ui.checkbox(
                    &mut app.builder_request_state.merge_table_lines,
                    "表らしい行を段落結合",
                );
            });
            ui.horizontal(|ui| {
                ui.checkbox(&mut app.builder_request_state.purge, "既存 run を purge");
                ui.checkbox(&mut app.builder_request_state.fresh_db, "fresh_db");
            });
            ui.separator();

            ui.horizontal(|ui| {
                ui.label("Sudachi 辞書");
                egui::ComboBox::from_id_salt("builder-sudachi-dict")
                    .selected_text(app.builder_request_state.sudachi_dict.as_str())
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut app.builder_request_state.sudachi_dict,
                            BuilderSudachiDict::Core,
                            "core",
                        );
                        ui.selectable_value(
                            &mut app.builder_request_state.sudachi_dict,
                            BuilderSudachiDict::Full,
                            "full",
                        );
                        ui.selectable_value(
                            &mut app.builder_request_state.sudachi_dict,
                            BuilderSudachiDict::Small,
                            "small",
                        );
                    });
                ui.label("split_mode");
                egui::ComboBox::from_id_salt("builder-split-mode")
                    .selected_text(app.builder_request_state.split_mode.as_str())
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut app.builder_request_state.split_mode,
                            BuilderSplitMode::A,
                            "A",
                        );
                        ui.selectable_value(
                            &mut app.builder_request_state.split_mode,
                            BuilderSplitMode::B,
                            "B",
                        );
                        ui.selectable_value(
                            &mut app.builder_request_state.split_mode,
                            BuilderSplitMode::C,
                            "C",
                        );
                    });
            });
            ui.separator();

            ui.label("limit");
            ui.add(
                ime_safe_singleline(&mut app.builder_request_state.limit_input)
                    .desired_width(240.0)
                    .interactive(settings_enabled),
            );
            ui.label("note");
            ui.add(
                ime_safe_singleline(&mut app.builder_request_state.note_input)
                    .desired_width(520.0)
                    .interactive(settings_enabled),
            );
            ui.separator();

            if app.builder_request_state.purge && app.builder_request_state.fresh_db {
                ui.colored_label(
                    ui.visuals().error_fg_color,
                    "`purge` と `fresh_db` は同時に使えません。",
                );
            }
            if let Some(path) = app.builder_request_state.input_dir_path.as_ref() {
                if !path.is_dir() {
                    ui.colored_label(
                        ui.visuals().error_fg_color,
                        format!("入力フォルダーが存在しません: {}", path.display()),
                    );
                } else {
                    let project_root = app
                        .builder_runtime_state
                        .runtime
                        .as_ref()
                        .map(|r| r.project_root.clone())
                        .or_else(|| std::env::current_dir().ok())
                        .unwrap_or_else(|| PathBuf::from("."));
                    let forbidden_dirs = crate::analysis_runner::resolve_forbidden_dirs(&project_root);
                    if let Some(msg) = crate::analysis_runner::check_forbidden_input_dir(path, &forbidden_dirs) {
                        ui.colored_label(
                            ui.visuals().error_fg_color,
                            msg,
                        );
                    }
                }
            }

            ui.separator();
            ui.label(format!("状態: {status_text}"));
            if !settings_enabled {
                ui.label("ジョブ実行中は builder 設定を変更できません。");
            }
        });

    app.builder_request_state.settings_window_open = window_open;

    let mut runtime_changed = false;
    if let Some(path) = selected_python_path {
        app.builder_request_state.python_path_override = Some(path);
        runtime_changed = true;
    }
    if clear_python_override {
        app.builder_request_state.python_path_override = None;
        runtime_changed = true;
    }
    if let Some(path) = selected_input_dir {
        app.builder_request_state.input_dir_path = Some(path);
    }
    if let Some(path) = selected_analysis_db_path {
        app.builder_request_state.analysis_db_path = path;
    }
    if let Some(path) = selected_report_path {
        app.builder_request_state.report_path = Some(path);
    }
    if clear_report_override {
        app.builder_request_state.report_path = None;
    }

    if runtime_changed {
        app.refresh_builder_runtime();
        ctx.request_repaint();
    }
}

fn draw_path_override_row<F>(
    ui: &mut Ui,
    label: &str,
    current_label: &str,
    settings_enabled: bool,
    mut choose_path: F,
    selected_path: &mut Option<PathBuf>,
    clear_action: Option<(&mut bool, &str)>,
) where
    F: FnMut() -> Option<PathBuf>,
{
    ui.label(label);
    ui.horizontal(|ui| {
        let mut displayed_label = current_label.to_string();
        ui.add(
            ime_safe_singleline(&mut displayed_label)
                .desired_width(520.0)
                .interactive(false),
        );
        if ui
            .add_enabled(settings_enabled, egui::Button::new("選択"))
            .clicked()
        {
            *selected_path = choose_path();
        }
        if let Some((clear_flag, clear_label)) = clear_action {
            if ui
                .add_enabled(settings_enabled, egui::Button::new(clear_label))
                .clicked()
            {
                *clear_flag = true;
            }
        }
    });
}
