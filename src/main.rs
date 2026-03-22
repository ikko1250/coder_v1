#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod analysis_runner;
mod app;
mod condition_editor;
mod condition_editor_view;
mod csv_loader;
mod db;
mod db_viewer_view;
mod filter;
mod filter_panel_view;
mod font;
mod manual_annotation_store;
mod model;
mod tagged_text;
mod ui_helpers;

use crate::app::App;
use crate::font::configure_japanese_font;
use eframe::egui;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let initial_csv_path = args
        .get(1)
        .filter(|arg| !arg.starts_with("--"))
        .map(std::path::PathBuf::from);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("条例分析ビューア")
            .with_inner_size([1480.0, 920.0]),
        ..Default::default()
    };

    eframe::run_native(
        "条例分析ビューア",
        options,
        Box::new(move |cc| {
            let font_setup_result = configure_japanese_font(&cc.egui_ctx);
            let mut app = App::new(initial_csv_path.clone());

            match font_setup_result {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if app.error_message.is_none() {
                        app.error_message = Some(
                            "日本語フォントが見つからなかったため、日本語テキストが正しく表示されない可能性があります。".to_string(),
                        );
                    }
                }
                Err(err) => {
                    if app.error_message.is_none() {
                        app.error_message = Some(err);
                    }
                }
            }

            Ok(Box::new(app))
        }),
    )
    .unwrap();
}
