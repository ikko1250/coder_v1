use crate::analysis_job_manager::AnalysisJobManager;
use crate::analysis_runner::{build_runtime_config, AnalysisWarningMessage};
use crate::app_services::{
    apply_analysis_settings_update, cleanup_analysis_jobs,
    load_condition_editor_from_path as load_condition_editor_document,
    load_csv as load_csv_records, load_db_viewer_context_for_location,
    open_db_viewer_for_selected_record, poll_analysis_job as poll_analysis_jobs,
    request_condition_editor_reload as request_condition_reload,
    resolved_filter_config_path as resolve_filter_config_path_for_state,
    save_annotation_for_selected_record as save_selected_annotation,
    save_condition_editor_document as save_condition_document_for_state,
    start_analysis_job as start_analysis, start_export_job as start_export, AnalysisSettingsUpdate,
    OptionalPathUpdate, PollAnalysisJobResult,
};
use crate::app_state::{
    AnalysisJobStatus, AppState, ConditionEditorConfirmAction, ScrollBehavior, SelectionChange,
};
use crate::app_ui_state::{AppUiState, TreeScrollRequest};
use crate::condition_editor::{build_default_condition_item, FilterConfigDocument};
use crate::condition_editor_view::{
    draw_condition_editor_confirm_overlay as render_condition_editor_confirm_overlay,
    draw_condition_editor_footer_panel as render_condition_editor_footer_panel,
    draw_condition_editor_global_settings as render_condition_editor_global_settings,
    draw_condition_editor_header_panel as render_condition_editor_header_panel,
    draw_condition_editor_list_panel as render_condition_editor_list_panel,
    draw_condition_editor_selected_condition as render_condition_editor_selected_condition,
    ConditionEditorDetailResponse, ConditionEditorFooterResponse, ConditionEditorListResponse,
    ConfirmOverlayResponse,
};
use crate::db::resolve_default_db_path;
use crate::db_viewer_view::render_db_viewer_contents;
use crate::filter::normalize_filter_candidate_search_text;
use crate::filter_panel_view::draw_filter_panel as render_filter_panel;
use crate::manual_annotation_store::first_manual_annotation_line;
use crate::model::{AnalysisRecord, FilterColumn, TextSegment};
use crate::ui_helpers::{ime_safe_multiline, ime_safe_singleline};
use eframe::egui;
use egui::text::{LayoutJob, TextFormat};
use egui::{Color32, RichText, ScrollArea, TextStyle, TextWrapMode, Ui};
use egui_extras::{Column, Size, StripBuilder, TableBuilder};
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::Duration;

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

const DB_VIEWER_VIEWPORT_ID: &str = "db_viewer_viewport";
const CONDITION_EDITOR_VIEWPORT_ID: &str = "condition_editor_viewport";
const RECORD_LIST_PANEL_MIN_WIDTH: f32 = 360.0;
const RECORD_LIST_PANEL_DEFAULT_RATIO: f32 = 0.33;
const RECORD_LIST_PANEL_MAX_RATIO: f32 = 0.85;

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

#[derive(Clone, Debug)]
struct ConditionEditorWindowInputs {
    can_modify: bool,
    resolved_path_result: Result<PathBuf, String>,
    resolved_path_label: String,
    loaded_path_label: String,
    current_confirm_action: Option<ConditionEditorConfirmAction>,
    panel_fill: Color32,
}

pub(crate) struct App {
    state: AppState,
    ui_state: AppUiState,
    job_manager: AnalysisJobManager,
    pub(crate) error_message: Option<String>,
}

impl App {
    pub(crate) fn new(initial_csv_path: Option<PathBuf>) -> Self {
        let analysis_request_state = crate::app_state::AnalysisRequestState::default();
        let runtime = build_runtime_config(&analysis_request_state.runtime_overrides());
        let mut app = Self {
            state: AppState::new(runtime, resolve_default_db_path()),
            ui_state: AppUiState::new(RECORD_LIST_PANEL_DEFAULT_RATIO),
            job_manager: AnalysisJobManager::default(),
            error_message: None,
        };
        app.state.analysis_request_state = analysis_request_state;
        app.try_cleanup_analysis_jobs();
        if let Some(csv_path) = initial_csv_path {
            app.load_csv(csv_path);
        }
        app
    }

    fn load_csv(&mut self, path: PathBuf) {
        match load_csv_records(&mut self.state, &mut self.ui_state, path) {
            Ok(()) => self.error_message = None,
            Err(error) => self.error_message = Some(error),
        }
    }

    fn handle_keyboard_navigation(&mut self, ctx: &egui::Context) {
        if self.error_message.is_some()
            || self.state.filtered_indices.is_empty()
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
            self.state.move_selection_down(&mut self.ui_state);
        } else if up_pressed {
            self.state.move_selection_up(&mut self.ui_state);
        }
    }

    fn save_annotation_for_selected_record(&mut self) {
        if let Err(error) = save_selected_annotation(&mut self.state) {
            self.state.annotation_editor_state.status_message = Some(error);
            self.state.annotation_editor_state.status_is_error = true;
        }
    }

    fn try_cleanup_analysis_jobs(&mut self) {
        cleanup_analysis_jobs(&mut self.state);
    }

    fn resolved_filter_config_path(&self) -> Result<PathBuf, String> {
        resolve_filter_config_path_for_state(&self.state)
    }

    fn focus_condition_editor_viewport(&self, ctx: &egui::Context) {
        let viewport_id = egui::ViewportId::from_hash_of(CONDITION_EDITOR_VIEWPORT_ID);
        ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(false));
        ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Focus);
    }

    fn open_condition_editor(&mut self, ctx: &egui::Context) -> Result<(), String> {
        if self.ui_state.condition_editor.window_open {
            self.state.condition_editor_state.status_message =
                Some("condition editor は既に開いています。".to_string());
            self.state.condition_editor_state.status_is_error = false;
            self.focus_condition_editor_viewport(ctx);
            return Ok(());
        }
        let path = self.resolved_filter_config_path()?;
        self.load_condition_editor_from_path(path, "条件 JSON を読み込みました。")
    }

    fn load_condition_editor_from_path(
        &mut self,
        path: PathBuf,
        status_message: &str,
    ) -> Result<(), String> {
        load_condition_editor_document(&mut self.state, &mut self.ui_state, path, status_message)
    }

    fn mark_condition_editor_dirty(&mut self) {
        self.state.condition_editor_state.is_dirty = true;
        self.state.condition_editor_state.status_message =
            Some("未保存の変更があります。".to_string());
        self.state.condition_editor_state.status_is_error = false;
    }

    fn condition_editor_selection_draft(&self) -> ConditionEditorSelectionDraft {
        ConditionEditorSelectionDraft {
            requested_selection: self.state.condition_editor_state.selected_index,
            requested_group_selection: self.state.condition_editor_state.selected_group_index,
        }
    }

    fn condition_editor_window_inputs(&self, ctx: &egui::Context) -> ConditionEditorWindowInputs {
        let resolved_path_result = self.resolved_filter_config_path();
        let resolved_path_label = resolved_path_result
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let loaded_path_label = self
            .state
            .condition_editor_state
            .loaded_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string());

        ConditionEditorWindowInputs {
            can_modify: !self.job_manager.has_running_job(),
            resolved_path_result,
            resolved_path_label,
            loaded_path_label,
            current_confirm_action: self.ui_state.condition_editor.confirm_action.clone(),
            panel_fill: ctx.style().visuals.panel_fill,
        }
    }

    fn apply_condition_editor_selection_draft(
        &mut self,
        selection_draft: ConditionEditorSelectionDraft,
    ) {
        let selected_index = clamp_condition_index(
            selection_draft.requested_selection,
            self.state
                .condition_editor_state
                .document
                .as_ref()
                .map_or(0, |document| document.cooccurrence_conditions.len()),
        );
        self.state.condition_editor_state.selected_index = selected_index;
        self.state.condition_editor_state.selected_group_index = self
            .state
            .condition_editor_state
            .document
            .as_ref()
            .and_then(|document| {
                clamp_condition_group_selection_for_document(
                    document,
                    selected_index,
                    selection_draft.requested_group_selection,
                )
            });
    }

    fn reload_condition_editor(&mut self, path: PathBuf) -> Result<(), String> {
        self.load_condition_editor_from_path(path, "条件 JSON を再読込しました。")
    }

    fn request_condition_editor_reload(&mut self, path: PathBuf) -> Result<(), String> {
        request_condition_reload(&mut self.state, &mut self.ui_state, path)
    }

    fn save_condition_editor_document(&mut self) -> Result<(), String> {
        save_condition_document_for_state(&mut self.state, &mut self.ui_state)
    }

    fn start_analysis_job(&mut self) -> Result<(), String> {
        start_analysis(&mut self.state, &mut self.ui_state, &mut self.job_manager)
    }

    fn start_export_job(&mut self, output_csv_path: PathBuf) -> Result<(), String> {
        start_export(
            &mut self.state,
            &mut self.ui_state,
            &mut self.job_manager,
            output_csv_path,
        )
    }

    fn poll_analysis_job(&mut self, ctx: &egui::Context) {
        match poll_analysis_jobs(&mut self.state, &mut self.ui_state, &mut self.job_manager) {
            PollAnalysisJobResult::Idle => {}
            PollAnalysisJobResult::Pending => {
                ctx.request_repaint_after(Duration::from_millis(100));
            }
            PollAnalysisJobResult::Updated { error_message } => {
                if let Some(error_message) = error_message {
                    self.error_message = Some(error_message);
                }
                ctx.request_repaint();
            }
        }
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
                lines.push(format!(
                    "combinationCount: {count} / cap {cap} (+{})",
                    count - cap
                ));
            }
            (Some(count), Some(cap)) => {
                lines.push(format!("combinationCount: {count} / cap {cap}"))
            }
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
        if !self.ui_state.warning_details_window_open {
            return;
        }

        let mut window_open = self.ui_state.warning_details_window_open;
        egui::Window::new(format!(
            "警告詳細 ({})",
            self.state.analysis_runtime_state.last_warnings.len()
        ))
        .open(&mut window_open)
        .resizable(true)
        .default_width(620.0)
        .show(ctx, |ui| {
            ScrollArea::vertical()
                .max_height(480.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for (idx, warning) in self
                        .state
                        .analysis_runtime_state
                        .last_warnings
                        .iter()
                        .enumerate()
                    {
                        ui.group(|ui| {
                            ui.label(
                                RichText::new(format!(
                                    "{}. {}",
                                    idx + 1,
                                    self.warning_headline(warning)
                                ))
                                .strong(),
                            );
                            for line in self.warning_detail_lines(warning) {
                                ui.add(egui::Label::new(line).wrap_mode(TextWrapMode::Wrap));
                            }
                        });
                        if idx + 1 < self.state.analysis_runtime_state.last_warnings.len() {
                            ui.add_space(6.0);
                        }
                    }
                });
        });
        self.ui_state.warning_details_window_open = window_open;
    }

    fn guard_root_close_with_dirty_editor(&mut self, ctx: &egui::Context) {
        if !self.state.condition_editor_state.is_dirty {
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
        if self.ui_state.condition_editor.window_open {
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

        let consumed_tree_scroll = self.ui_state.pending_tree_scroll;
        let mut clicked_row = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            clicked_row = self.draw_body(ui, consumed_tree_scroll);
        });

        self.draw_db_viewer_window(ctx);
        self.draw_analysis_settings_window(ctx);
        self.draw_condition_editor_window(ctx);

        if let Some(row_index) = clicked_row {
            if self.state.apply_selection_change(
                &mut self.ui_state,
                SelectionChange::new(Some(row_index), ScrollBehavior::KeepVisible),
            ) {
                ctx.request_repaint();
            }
        }

        if self.ui_state.pending_tree_scroll == consumed_tree_scroll {
            self.ui_state.pending_tree_scroll = None;
        }
    }
}

impl App {
    fn draw_db_viewer_button(&mut self, ui: &mut Ui, enabled: bool) {
        let response = ui.add_enabled(enabled, egui::Button::new("DB参照"));
        if response.clicked() {
            if let Err(error) = open_db_viewer_for_selected_record(&mut self.state) {
                self.error_message = Some(error);
            }
        }
    }

    fn previous_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.state.db_viewer_state.context.as_ref()?;
        let previous_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no < context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .max()?;

        Some((context.center.document_id, previous_paragraph_no))
    }

    fn next_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.state.db_viewer_state.context.as_ref()?;
        let next_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no > context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .min()?;

        Some((context.center.document_id, next_paragraph_no))
    }

    fn draw_db_viewer_window(&mut self, ctx: &egui::Context) {
        if !self.state.db_viewer_state.is_open {
            return;
        }

        let snapshot = self.state.db_viewer_state.clone();
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
            self.state.db_viewer_state.is_open = false;
            return;
        }

        if let Some((document_id, paragraph_no)) = requested_location {
            load_db_viewer_context_for_location(&mut self.state, document_id, paragraph_no);
            ctx.request_repaint();
        }
    }

    fn draw_toolbar(&mut self, ui: &mut Ui) {
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                ui.label("表示元:");
                let path_str = self.state.records_source_label.clone();
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
                    .state
                    .selected_row
                    .map(|idx| idx + 1)
                    .map(|position| position.to_string())
                    .unwrap_or_else(|| "-".to_string());
                ui.label(format!(
                    "総件数: {} 件  抽出後: {} 件  選択: {} / {}",
                    self.state.all_records.len(),
                    self.state.filtered_indices.len(),
                    selected_position,
                    self.state.filtered_indices.len()
                ));
            });

            ui.horizontal_wrapped(|ui| {
                let can_start = self.state.analysis_runtime_state.can_start();
                let can_export = self.state.analysis_runtime_state.can_export();
                let settings_enabled = !self.job_manager.has_running_job();
                let python_label = self
                    .state
                    .analysis_runtime_state
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.python_label.clone())
                    .unwrap_or_else(|| "-".to_string());
                let filter_config_label = self
                    .state
                    .analysis_runtime_state
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.filter_config_path.display().to_string())
                    .unwrap_or_else(|| "-".to_string());
                let annotation_label = self
                    .state
                    .resolved_annotation_csv_path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|_| "-".to_string());
                let db_label = self.state.db_viewer_state.db_path.display().to_string();

                if matches!(
                    self.state.analysis_runtime_state.status,
                    AnalysisJobStatus::RunningAnalysis { .. }
                        | AnalysisJobStatus::RunningExport { .. }
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
                    self.ui_state.analysis_settings_window_open = true;
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
                if self.state.analysis_runtime_state.has_warning_details()
                    && ui.button("警告詳細").clicked()
                {
                    self.ui_state.warning_details_window_open = true;
                }

                let status_text = self.state.analysis_runtime_state.status_text();
                let status_color =
                    analysis_status_color(ui, &self.state.analysis_runtime_state.status);
                ui.label(RichText::new(status_text).color(status_color));
            });
        });
    }

    fn draw_analysis_settings_window(&mut self, ctx: &egui::Context) {
        if !self.ui_state.analysis_settings_window_open {
            return;
        }

        let mut window_open = self.ui_state.analysis_settings_window_open;
        let mut selected_python_path = None;
        let mut selected_filter_config_path = None;
        let mut selected_annotation_csv_path = None;
        let mut clear_python_override = false;
        let mut clear_filter_config_override = false;
        let mut clear_annotation_csv_override = false;
        let settings_enabled = !self.job_manager.has_running_job();
        let python_override_label = self
            .state
            .analysis_request_state
            .python_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "自動解決".to_string());
        let filter_override_label = self
            .state
            .analysis_request_state
            .filter_config_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "既定値 (asset/cooccurrence-conditions.json)".to_string());
        let annotation_override_label = self
            .state
            .analysis_request_state
            .annotation_csv_path_override
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "既定値 (asset/manual-annotations.csv)".to_string());
        let resolved_python_label = self
            .state
            .analysis_runtime_state
            .runtime
            .as_ref()
            .map(|runtime| runtime.python_label.clone())
            .unwrap_or_else(|| "-".to_string());
        let resolved_filter_label = self
            .state
            .analysis_runtime_state
            .runtime
            .as_ref()
            .map(|runtime| runtime.filter_config_path.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        let resolved_annotation_label = self
            .state
            .resolved_annotation_csv_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let status_text = self.state.analysis_runtime_state.status_text();

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

        self.ui_state.analysis_settings_window_open = window_open;

        let runtime_changed = apply_analysis_settings_update(
            &mut self.state,
            &mut self.ui_state,
            AnalysisSettingsUpdate {
                python_path: match (selected_python_path, clear_python_override) {
                    (Some(path), _) => OptionalPathUpdate::Set(path),
                    (None, true) => OptionalPathUpdate::Clear,
                    (None, false) => OptionalPathUpdate::Unchanged,
                },
                filter_config_path: match (
                    selected_filter_config_path,
                    clear_filter_config_override,
                ) {
                    (Some(path), _) => OptionalPathUpdate::Set(path),
                    (None, true) => OptionalPathUpdate::Clear,
                    (None, false) => OptionalPathUpdate::Unchanged,
                },
                annotation_csv_path: match (
                    selected_annotation_csv_path,
                    clear_annotation_csv_override,
                ) {
                    (Some(path), _) => OptionalPathUpdate::Set(path),
                    (None, true) => OptionalPathUpdate::Clear,
                    (None, false) => OptionalPathUpdate::Unchanged,
                },
            },
        );

        if runtime_changed {
            ctx.request_repaint();
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
                    let list_response = render_condition_editor_list_panel(
                        ui,
                        can_modify,
                        self.state.condition_editor_state.document.as_ref(),
                        selection_draft.requested_selection,
                    );
                    self.apply_condition_editor_list_response(
                        selection_draft,
                        command_draft,
                        list_response,
                    );
                });

                strip.cell(|ui| {
                    self.draw_condition_editor_detail_panel(
                        ui,
                        panel_fill,
                        can_modify,
                        selection_draft,
                        command_draft,
                    );
                });
            });
    }

    fn draw_condition_editor_detail_panel(
        &mut self,
        ui: &mut Ui,
        panel_fill: Color32,
        can_modify: bool,
        selection_draft: &mut ConditionEditorSelectionDraft,
        command_draft: &mut ConditionEditorCommandDraft,
    ) {
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
                        if self.draw_condition_editor_detail_contents(
                            ui,
                            can_modify,
                            selection_draft,
                            command_draft,
                        ) {
                            self.mark_condition_editor_dirty();
                        }
                    });
            });
    }

    fn draw_condition_editor_detail_contents(
        &mut self,
        ui: &mut Ui,
        can_modify: bool,
        selection_draft: &mut ConditionEditorSelectionDraft,
        command_draft: &mut ConditionEditorCommandDraft,
    ) -> bool {
        let Some(document) = self.state.condition_editor_state.document.as_mut() else {
            ui.label(RichText::new("条件 JSON 未読込").italics());
            return false;
        };

        let mut changed = false;
        selection_draft.requested_selection = clamp_condition_index(
            selection_draft.requested_selection,
            document.cooccurrence_conditions.len(),
        );
        selection_draft.requested_group_selection = clamp_condition_group_selection_for_document(
            document,
            selection_draft.requested_selection,
            selection_draft.requested_group_selection,
        );

        changed |= render_condition_editor_global_settings(ui, document);

        let Some(selected_index) = selection_draft.requested_selection else {
            ui.label(RichText::new("condition を選択してください").italics());
            return changed;
        };
        let Some(condition) = document.cooccurrence_conditions.get_mut(selected_index) else {
            ui.label(RichText::new("condition を選択してください").italics());
            return changed;
        };

        let detail_response = render_condition_editor_selected_condition(
            ui,
            can_modify,
            selection_draft.requested_group_selection,
            condition,
        );
        self.apply_condition_editor_detail_response(
            selected_index,
            selection_draft,
            command_draft,
            detail_response,
        );
        changed |= detail_response.changed;

        changed
    }

    fn condition_editor_status_message(&self) -> Option<(&str, bool)> {
        self.state
            .condition_editor_state
            .status_message
            .as_deref()
            .map(|message| (message, self.state.condition_editor_state.status_is_error))
    }

    fn condition_editor_save_enabled(&self, can_modify: bool, resolved_path_ok: bool) -> bool {
        can_modify
            && self.state.condition_editor_state.document.is_some()
            && self
                .state
                .condition_editor_state
                .pending_path_sync
                .is_none()
            && resolved_path_ok
    }

    fn condition_editor_confirm_message(confirm_action: &ConditionEditorConfirmAction) -> String {
        match confirm_action {
            ConditionEditorConfirmAction::CloseWindow => {
                "未保存の変更があります。condition editor を閉じますか。".to_string()
            }
            ConditionEditorConfirmAction::ReloadPath(path) => format!(
                "未保存の変更があります。次の条件 JSON を再読込すると変更は破棄されます。\n{}",
                path.display()
            ),
        }
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
                render_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                    self.state
                        .condition_editor_state
                        .pending_path_sync
                        .as_deref(),
                    self.condition_editor_status_message(),
                    self.state
                        .condition_editor_state
                        .projected_legacy_condition_count,
                );
                ui.separator();
                self.draw_condition_editor_body_panel(
                    ui,
                    can_modify,
                    selection_draft,
                    command_draft,
                );
                ui.separator();
                let footer_response = render_condition_editor_footer_panel(
                    ui,
                    self.condition_editor_save_enabled(can_modify, resolved_path_ok),
                    can_modify && resolved_path_ok,
                    self.state.condition_editor_state.is_dirty,
                );
                self.apply_condition_editor_footer_response(command_draft, footer_response);
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
                render_condition_editor_header_panel(
                    ui,
                    can_modify,
                    loaded_path_label,
                    resolved_path_label,
                    self.state
                        .condition_editor_state
                        .pending_path_sync
                        .as_deref(),
                    self.condition_editor_status_message(),
                    self.state
                        .condition_editor_state
                        .projected_legacy_condition_count,
                );
            });

        egui::TopBottomPanel::bottom("condition_editor_viewport_footer")
            .frame(
                egui::Frame::default()
                    .fill(panel_fill)
                    .inner_margin(egui::Margin::same(10)),
            )
            .show(viewport_ctx, |ui| {
                let footer_response = render_condition_editor_footer_panel(
                    ui,
                    self.condition_editor_save_enabled(can_modify, resolved_path_ok),
                    can_modify && resolved_path_ok,
                    self.state.condition_editor_state.is_dirty,
                );
                self.apply_condition_editor_footer_response(command_draft, footer_response);
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

        if self.state.condition_editor_state.is_dirty {
            self.ui_state.condition_editor.confirm_action =
                Some(ConditionEditorConfirmAction::CloseWindow);
        } else {
            self.ui_state.condition_editor.window_open = false;
        }
    }

    fn apply_condition_editor_footer_response(
        &self,
        command_draft: &mut ConditionEditorCommandDraft,
        footer_response: ConditionEditorFooterResponse,
    ) {
        if footer_response.save_clicked {
            command_draft.should_save = true;
        }
        if footer_response.reload_clicked {
            command_draft.should_reload = true;
        }
    }

    fn apply_condition_editor_confirm_overlay_response(
        &self,
        command_draft: &mut ConditionEditorCommandDraft,
        response: Option<ConfirmOverlayResponse>,
    ) {
        command_draft.modal_response = match response {
            Some(ConfirmOverlayResponse::Continue) => Some(ConditionEditorModalResponse::Continue),
            Some(ConfirmOverlayResponse::Cancel) => Some(ConditionEditorModalResponse::Cancel),
            None => command_draft.modal_response,
        };
    }

    fn apply_condition_editor_list_response(
        &self,
        selection_draft: &mut ConditionEditorSelectionDraft,
        command_draft: &mut ConditionEditorCommandDraft,
        list_response: ConditionEditorListResponse,
    ) {
        if list_response.add_clicked {
            command_draft.should_add_condition = true;
        }
        if let Some(selected_index) = list_response.selected_index {
            selection_draft.requested_selection = Some(selected_index);
            selection_draft.requested_group_selection = list_response.selected_group_index;
        }
    }

    fn apply_condition_editor_detail_response(
        &self,
        selected_index: usize,
        selection_draft: &mut ConditionEditorSelectionDraft,
        command_draft: &mut ConditionEditorCommandDraft,
        detail_response: ConditionEditorDetailResponse,
    ) {
        if detail_response.delete_clicked {
            command_draft.should_delete_condition = Some(selected_index);
        }
        selection_draft.requested_group_selection = detail_response.requested_group_selection;
    }

    fn apply_condition_editor_add_request(&mut self, should_add_condition: bool) {
        if !should_add_condition {
            return;
        }

        let mut new_index = None;
        if let Some(document) = self.state.condition_editor_state.document.as_mut() {
            document
                .cooccurrence_conditions
                .push(build_default_condition_item());
            new_index = Some(document.cooccurrence_conditions.len().saturating_sub(1));
        }
        if let Some(index) = new_index {
            self.state.condition_editor_state.selected_index = Some(index);
            self.state.condition_editor_state.selected_group_index = Some(0);
            self.mark_condition_editor_dirty();
        }
    }

    fn apply_condition_editor_delete_request(&mut self, delete_index: Option<usize>) {
        let Some(delete_index) = delete_index else {
            return;
        };

        if let Some(document) = self.state.condition_editor_state.document.as_mut() {
            if delete_index < document.cooccurrence_conditions.len() {
                document.cooccurrence_conditions.remove(delete_index);
                self.state.condition_editor_state.selected_index = clamp_condition_index(
                    Some(delete_index),
                    document.cooccurrence_conditions.len(),
                );
                self.state.condition_editor_state.selected_group_index =
                    clamp_condition_group_selection_for_document(
                        document,
                        self.state.condition_editor_state.selected_index,
                        Some(0),
                    );
                self.mark_condition_editor_dirty();
                self.state.condition_editor_state.status_message =
                    Some("condition を削除しました。".to_string());
                self.state.condition_editor_state.status_is_error = false;
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
            Ok(path) => self.request_condition_editor_reload(path.clone()).err(),
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
                if let Some(confirm_action) = self.ui_state.condition_editor.confirm_action.clone()
                {
                    match confirm_action {
                        ConditionEditorConfirmAction::CloseWindow => {
                            self.ui_state.condition_editor.window_open = false;
                            self.ui_state.condition_editor.confirm_action = None;
                            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Close);
                        }
                        ConditionEditorConfirmAction::ReloadPath(path) => {
                            if let Err(error) = self.reload_condition_editor(path) {
                                self.state.condition_editor_state.status_message = Some(error);
                                self.state.condition_editor_state.status_is_error = true;
                            }
                            self.ui_state.condition_editor.confirm_action = None;
                        }
                    }
                }
            }
            ConditionEditorModalResponse::Cancel => {
                self.ui_state.condition_editor.confirm_action = None;
            }
        }
    }

    fn apply_condition_editor_command_draft(
        &mut self,
        ctx: &egui::Context,
        viewport_id: egui::ViewportId,
        selection_draft: ConditionEditorSelectionDraft,
        command_draft: ConditionEditorCommandDraft,
        resolved_path_result: &Result<PathBuf, String>,
    ) -> Option<String> {
        self.apply_condition_editor_selection_draft(selection_draft);
        self.apply_condition_editor_close_request(command_draft.close_requested);
        self.apply_condition_editor_add_request(command_draft.should_add_condition);
        self.apply_condition_editor_delete_request(command_draft.should_delete_condition);

        let mut save_error = None;
        if command_draft.should_save {
            if let Err(error) = self.save_condition_editor_document() {
                save_error = Some(error);
            }
        }
        let reload_error = self.apply_condition_editor_reload_request(
            command_draft.should_reload,
            resolved_path_result,
        );
        self.apply_condition_editor_modal_response(ctx, viewport_id, command_draft.modal_response);

        save_error.or(reload_error)
    }

    fn draw_condition_editor_window(&mut self, ctx: &egui::Context) {
        if !self.ui_state.condition_editor.window_open {
            return;
        }

        let viewport_id = egui::ViewportId::from_hash_of(CONDITION_EDITOR_VIEWPORT_ID);
        let builder = egui::ViewportBuilder::default()
            .with_title("条件編集")
            .with_inner_size([1120.0, 760.0])
            .with_resizable(true);
        let window_inputs = self.condition_editor_window_inputs(ctx);
        let mut selection_draft = self.condition_editor_selection_draft();
        let mut command_draft = ConditionEditorCommandDraft::default();

        ctx.show_viewport_immediate(viewport_id, builder, |viewport_ctx, class| {
            command_draft.close_requested =
                viewport_ctx.input(|input| input.viewport().close_requested());
            if command_draft.close_requested && self.state.condition_editor_state.is_dirty {
                viewport_ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            }

            match class {
                egui::ViewportClass::Embedded => {
                    self.draw_condition_editor_embedded_window(
                        viewport_ctx,
                        window_inputs.can_modify,
                        &window_inputs.loaded_path_label,
                        &window_inputs.resolved_path_label,
                        window_inputs.resolved_path_result.is_ok(),
                        &mut selection_draft,
                        &mut command_draft,
                    );
                }
                _ => {
                    self.draw_condition_editor_viewport_panels(
                        viewport_ctx,
                        window_inputs.can_modify,
                        &window_inputs.loaded_path_label,
                        &window_inputs.resolved_path_label,
                        window_inputs.resolved_path_result.is_ok(),
                        window_inputs.panel_fill,
                        &mut selection_draft,
                        &mut command_draft,
                    );
                }
            }

            if let Some(confirm_action) = window_inputs.current_confirm_action.as_ref() {
                let message = Self::condition_editor_confirm_message(confirm_action);
                let response = render_condition_editor_confirm_overlay(viewport_ctx, &message);
                self.apply_condition_editor_confirm_overlay_response(&mut command_draft, response);
            }
        });

        if let Some(error) = self.apply_condition_editor_command_draft(
            ctx,
            viewport_id,
            selection_draft,
            command_draft,
            &window_inputs.resolved_path_result,
        ) {
            self.state.condition_editor_state.status_message = Some(error);
            self.state.condition_editor_state.status_is_error = true;
        }
    }

    fn draw_body(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let mut clicked_row = None;
        let available_width = ui.available_width().max(1.0);
        let record_list_panel_width_range = self.record_list_panel_width_range(available_width);
        let default_list_panel_width = (available_width * self.ui_state.record_list_panel_ratio)
            .clamp(
                *record_list_panel_width_range.start(),
                *record_list_panel_width_range.end(),
            );

        let list_panel_response = egui::SidePanel::left("record_list_panel")
            .resizable(true)
            .default_width(default_list_panel_width)
            .min_width(*record_list_panel_width_range.start())
            .max_width(*record_list_panel_width_range.end())
            .show_inside(ui, |ui| {
                self.draw_filters(ui);
                ui.separator();
                clicked_row = self.draw_tree(ui, tree_scroll_request);
            });

        self.ui_state.record_list_panel_ratio =
            (list_panel_response.response.rect.width() / available_width).clamp(
                RECORD_LIST_PANEL_MIN_WIDTH / available_width,
                RECORD_LIST_PANEL_MAX_RATIO,
            );

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.draw_detail(ui);
        });

        clicked_row
    }

    fn record_list_panel_width_range(&self, available_width: f32) -> RangeInclusive<f32> {
        let max_width = (available_width * RECORD_LIST_PANEL_MAX_RATIO)
            .max(RECORD_LIST_PANEL_MIN_WIDTH)
            .min(1600.0);
        RECORD_LIST_PANEL_MIN_WIDTH..=max_width
    }

    fn draw_filters(&mut self, ui: &mut Ui) {
        let active_count: usize = self
            .state
            .selected_filter_values
            .values()
            .map(BTreeSet::len)
            .sum();
        let options = self
            .state
            .filter_options
            .get(&self.state.active_filter_column)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let selected_values = self
            .state
            .selected_filter_values
            .get(&self.state.active_filter_column);
        let candidate_query = self
            .state
            .filter_candidate_queries
            .get(&self.state.active_filter_column)
            .map(String::as_str)
            .unwrap_or("");
        let normalized_query = normalize_filter_candidate_search_text(candidate_query);
        let mut matching_options = Vec::new();
        let mut selected_non_matching_options = Vec::new();
        for option in options {
            let is_selected = selected_values.is_some_and(|values| values.contains(&option.value));
            let matches_query = normalized_query.is_empty()
                || normalize_filter_candidate_search_text(&option.value)
                    .contains(&normalized_query);

            if matches_query {
                matching_options.push(option.clone());
            } else if is_selected {
                selected_non_matching_options.push(option.clone());
            }
        }
        let active_values: Vec<(FilterColumn, String)> = self
            .state
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
        let response = render_filter_panel(
            ui,
            self.state.active_filter_column,
            active_count,
            &matching_options,
            &selected_non_matching_options,
            selected_values,
            &active_values,
            candidate_query,
            !options.is_empty(),
        );

        let response_column = self.state.active_filter_column;
        if let Some(updated_query) = response.updated_query {
            if updated_query.is_empty() {
                self.state.filter_candidate_queries.remove(&response_column);
            } else {
                self.state
                    .filter_candidate_queries
                    .insert(response_column, updated_query);
            }
        }
        if let Some(selected_column) = response.selected_column {
            self.state.active_filter_column = selected_column;
        }
        if response.clear_column_clicked {
            self.state
                .clear_filters_for_column(&mut self.ui_state, self.state.active_filter_column);
        }
        if response.clear_all_clicked {
            self.state.clear_all_filters(&mut self.ui_state);
        }
        for (value, selected) in response.toggled_options {
            self.state.toggle_filter_value(
                &mut self.ui_state,
                self.state.active_filter_column,
                &value,
                selected,
            );
        }
        for (column, value) in response.removed_active_values {
            self.state
                .toggle_filter_value(&mut self.ui_state, column, &value, false);
        }
    }

    fn draw_tree(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let filtered_indices = &self.state.filtered_indices;
        let selected_row = self.state.selected_row;
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
                    let record = &self.state.all_records[filtered_indices[i]];
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
        if let Some(record) = self.state.selected_record().cloned() {
            self.draw_db_viewer_button(ui, record.supports_db_viewer());
            if !record.supports_db_viewer() {
                ui.label("sentence 行では DB viewer は無効です。");
            }
            ui.add_space(6.0);
            self.draw_record_summary(ui, &record);
            ui.separator();

            let detail_job = build_record_text_layout_job(ui, &self.state.get_segments());

            if record.supports_manual_annotation() {
                if self.ui_state.annotation_panel_expanded {
                    egui::TopBottomPanel::bottom("annotation_editor_panel_expanded")
                        .resizable(false)
                        .default_height(230.0)
                        .min_height(200.0)
                        .show_inside(ui, |ui| {
                            self.draw_annotation_editor_panel(ui, &record);
                        });
                } else {
                    egui::TopBottomPanel::bottom("annotation_editor_panel_collapsed")
                        .resizable(false)
                        .min_height(0.0)
                        .show_inside(ui, |ui| {
                            self.draw_annotation_editor_collapsed_bar(ui, &record);
                        });
                }
            } else {
                egui::TopBottomPanel::bottom("annotation_editor_panel_collapsed")
                    .resizable(false)
                    .min_height(0.0)
                    .show_inside(ui, |ui| {
                        self.draw_annotation_editor_collapsed_bar(ui, &record);
                    });
            }

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
                "{} / {} / {}={}",
                record.municipality_name,
                record.ordinance_or_rule,
                record.analysis_unit.id_column_name(),
                record.unit_id()
            ))
            .size(14.0)
            .strong(),
        );

        ui.add_space(6.0);
        ui.label(format!("document_id: {}", record.document_id));
        ui.label(format!("paragraph_id: {}", record.paragraph_id));
        if !record.sentence_id.trim().is_empty() {
            ui.label(format!("sentence_id: {}", record.sentence_id));
        }
        ui.label(format!("doc_type: {}", record.doc_type));
        ui.label(format!("sentence_count: {}", record.sentence_count));
        if !record.sentence_no_in_paragraph.trim().is_empty() {
            ui.label(format!(
                "sentence_no_in_paragraph: {}",
                record.sentence_no_in_paragraph
            ));
        }
        if !record.sentence_no_in_document.trim().is_empty() {
            ui.label(format!(
                "sentence_no_in_document: {}",
                record.sentence_no_in_document
            ));
        }

        ui.add_space(6.0);
        ui.label(format!("categories: {}", record.matched_categories_text));
        ui.label(format!("conditions: {}", record.matched_condition_ids_text));
        ui.label(format!("match_groups: {}", record.match_group_ids_text));
        ui.label(format!(
            "annotated_tokens: {}",
            record.annotated_token_count
        ));
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

    fn draw_record_text_panel(&self, ui: &mut Ui, record: &AnalysisRecord, detail_job: LayoutJob) {
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
                    RichText::new("高度条件の説明を表示中。本文強調は一部未対応です。").italics(),
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

    fn draw_annotation_editor_collapsed_bar(&mut self, ui: &mut Ui, record: &AnalysisRecord) {
        let annotation_supported = record.supports_manual_annotation();

        let response = ui
            .horizontal(|ui| {
                if annotation_supported {
                    ui.label(RichText::new("▶").strong());

                    let count_str = if record.manual_annotation_count == "0"
                        || record.manual_annotation_count.is_empty()
                    {
                        "なし".to_string()
                    } else {
                        format!("{}件", record.manual_annotation_count)
                    };

                    ui.label(RichText::new(format!("annotation 追記 ({})", count_str)).strong());
                } else {
                    ui.label(RichText::new("▶").color(ui.visuals().weak_text_color()));
                    ui.label(
                        RichText::new("sentence 行では manual annotation editor は無効です。")
                            .color(ui.visuals().weak_text_color()),
                    );
                }
            })
            .response;

        if annotation_supported && response.interact(egui::Sense::click()).clicked() {
            self.ui_state.annotation_panel_expanded = true;
        }
    }

    fn draw_annotation_editor_panel(&mut self, ui: &mut Ui, record: &AnalysisRecord) {
        let annotation_supported = record.supports_manual_annotation();
        let annotation_summary = if record.manual_annotation_pairs_text.trim().is_empty() {
            "annotation なし".to_string()
        } else {
            record.manual_annotation_pairs_text.clone()
        };
        let annotation_path_label = self
            .state
            .resolved_annotation_csv_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|error| format!("解決失敗: {error}"));
        let annotation_save_enabled = self.state.annotation_save_enabled();

        ui.group(|ui| {
            let title_response = ui
                .horizontal(|ui| {
                    ui.label(RichText::new("▼").strong());
                    ui.label(RichText::new("annotation 追記").strong());
                })
                .response;

            if title_response.interact(egui::Sense::click()).clicked() {
                self.ui_state.annotation_panel_expanded = false;
            }
            ui.label(format!("保存先: {annotation_path_label}"));
            if !annotation_supported {
                ui.label("sentence 行では manual annotation editor は無効です。");
            }
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
            ui.add_enabled_ui(annotation_save_enabled, |ui| {
                ui.horizontal(|ui| {
                    ui.label("namespace");
                    ui.add(ime_safe_singleline(
                        &mut self.state.annotation_editor_state.namespace_input,
                    ));
                    ui.label("key");
                    ui.add(ime_safe_singleline(
                        &mut self.state.annotation_editor_state.key_input,
                    ));
                });
                ui.horizontal(|ui| {
                    ui.label("tagged_by");
                    ui.add(ime_safe_singleline(
                        &mut self.state.annotation_editor_state.tagged_by_input,
                    ));
                    ui.label("confidence");
                    ui.add(ime_safe_singleline(
                        &mut self.state.annotation_editor_state.confidence_input,
                    ));
                });
                ui.label(RichText::new("改行は Shift+Enter").italics());
                ui.label("value");
                ui.add(
                    ime_safe_multiline(&mut self.state.annotation_editor_state.value_input)
                        .desired_rows(2),
                );
                ui.label("note");
                ui.add(
                    ime_safe_multiline(&mut self.state.annotation_editor_state.note_input)
                        .desired_rows(2),
                );
            });

            ui.horizontal(|ui| {
                if ui
                    .add_enabled(annotation_save_enabled, egui::Button::new("追記"))
                    .clicked()
                {
                    self.save_annotation_for_selected_record();
                }
                if ui.button("入力クリア").clicked() {
                    self.state.clear_annotation_editor_inputs();
                    self.state.clear_annotation_editor_status();
                }
                if !annotation_supported {
                    ui.label("sentence annotation 対応までは paragraph 専用です。");
                } else if !annotation_save_enabled {
                    ui.label("分析ジョブ実行中は保存できません。");
                }
            });

            if let Some(status_message) = &self.state.annotation_editor_state.status_message {
                ui.colored_label(
                    editor_status_color(self.state.annotation_editor_state.status_is_error),
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
