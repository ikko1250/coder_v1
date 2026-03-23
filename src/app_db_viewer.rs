//! DB 参照ウィンドウ・状態準備。親モジュール `app` の子として `App` の非公開状態にアクセスする。
//!
//! **境界（P1-05）**
//! - 本モジュール: `DbViewerState` の更新、SQLite 取得（[`crate::db`]）、ビューポート／`Window` のホスト。
//! - [`crate::db_viewer_view`]: **純粋表示**のみ。`DbViewerState` のスナップショットと前後移動の候補を受け取り、
//!   `requested_location` への書き戻しでナビゲーション意図のみ返す（DB や `App` に触れない）。

const VIEWPORT_ID: &str = "db_viewer_viewport";

use super::App;
use crate::db::{fetch_paragraph_context, fetch_paragraph_context_by_location};
use crate::db_viewer_view::render_db_viewer_contents;
use eframe::egui::{self, Ui};

fn selected_paragraph_id_for_db(app: &App) -> Result<i64, String> {
    let record = app
        .selected_record()
        .ok_or_else(|| "レコードが選択されていません".to_string())?;
    if !record.supports_db_viewer() {
        return Err("sentence 行では DB viewer は未対応です".to_string());
    }

    record.paragraph_id.parse::<i64>().map_err(|error| {
        format!(
            "paragraph_id を数値として解釈できません: {} ({error})",
            record.paragraph_id
        )
    })
}

fn prepare_db_viewer_state(app: &mut App) -> Result<(), String> {
    let selected_record = app
        .selected_record()
        .ok_or_else(|| "レコードが選択されていません".to_string())?;
    if !selected_record.supports_db_viewer() {
        return Err("sentence 行では DB viewer は未対応です".to_string());
    }
    let paragraph_id = selected_paragraph_id_for_db(app)?;
    let source_paragraph_text = selected_record.paragraph_text.clone();

    app.db_viewer_state.is_open = true;
    app.db_viewer_state.source_paragraph_id = Some(paragraph_id);
    app.db_viewer_state.source_paragraph_text = Some(source_paragraph_text);
    app.db_viewer_state.context = None;
    app.db_viewer_state.error_message = None;
    Ok(())
}

pub(super) fn draw_db_viewer_button(app: &mut App, ui: &mut Ui, enabled: bool) {
    let response = ui.add_enabled(enabled, egui::Button::new("DB参照"));
    if response.clicked() {
        if let Err(error) = open_db_viewer_for_selected_record(app) {
            app.error_message = Some(error);
        }
    }
}

pub(super) fn open_db_viewer_for_selected_record(app: &mut App) -> Result<(), String> {
    prepare_db_viewer_state(app)?;
    load_db_viewer_context(app);
    Ok(())
}

pub(super) fn load_db_viewer_context(app: &mut App) {
    let Some(paragraph_id) = app.db_viewer_state.source_paragraph_id else {
        app.db_viewer_state.context = None;
        app.db_viewer_state.error_message = Some("参照元 paragraph_id が未設定です".to_string());
        app.db_viewer_state.is_open = true;
        return;
    };

    match fetch_paragraph_context(&app.db_viewer_state.db_path, paragraph_id) {
        Ok(context) => {
            app.db_viewer_state.context = Some(context);
            app.db_viewer_state.error_message = None;
            app.db_viewer_state.is_open = true;
        }
        Err(error) => {
            app.db_viewer_state.context = None;
            app.db_viewer_state.error_message = Some(error);
            app.db_viewer_state.is_open = true;
        }
    }
}

pub(super) fn load_db_viewer_context_for_location(
    app: &mut App,
    document_id: i64,
    paragraph_no: i64,
) {
    match fetch_paragraph_context_by_location(
        &app.db_viewer_state.db_path,
        document_id,
        paragraph_no,
    ) {
        Ok(context) => {
            app.db_viewer_state.context = Some(context);
            app.db_viewer_state.error_message = None;
        }
        Err(error) => {
            app.db_viewer_state.context = None;
            app.db_viewer_state.error_message = Some(error);
        }
    }
}

pub(super) fn previous_db_viewer_location(app: &App) -> Option<(i64, i64)> {
    let context = app.db_viewer_state.context.as_ref()?;
    let previous_paragraph_no = context
        .paragraphs
        .iter()
        .filter(|paragraph| paragraph.paragraph_no < context.center.paragraph_no)
        .map(|paragraph| paragraph.paragraph_no)
        .max()?;

    Some((context.center.document_id, previous_paragraph_no))
}

pub(super) fn next_db_viewer_location(app: &App) -> Option<(i64, i64)> {
    let context = app.db_viewer_state.context.as_ref()?;
    let next_paragraph_no = context
        .paragraphs
        .iter()
        .filter(|paragraph| paragraph.paragraph_no > context.center.paragraph_no)
        .map(|paragraph| paragraph.paragraph_no)
        .min()?;

    Some((context.center.document_id, next_paragraph_no))
}

pub(super) fn draw_db_viewer_window(app: &mut App, ctx: &egui::Context) {
    if !app.db_viewer_state.is_open {
        return;
    }

    let snapshot = app.db_viewer_state.clone();
    let previous_location = previous_db_viewer_location(app);
    let next_location = next_db_viewer_location(app);
    let mut requested_location = None;
    let mut close_requested = false;
    let viewport_id = egui::ViewportId::from_hash_of(VIEWPORT_ID);
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
        app.db_viewer_state.is_open = false;
        return;
    }

    if let Some((document_id, paragraph_no)) = requested_location {
        load_db_viewer_context_for_location(app, document_id, paragraph_no);
        ctx.request_repaint();
    }
}
