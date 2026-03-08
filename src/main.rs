#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod csv_loader;
mod db;
mod filter;
mod font;
mod model;
mod tagged_text;

use crate::app::App;
use crate::font::configure_japanese_font;
use eframe::egui;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let csv_path = if args.len() > 1 && !args[1].starts_with("--") {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("抑制区域_段落_1.0.csv")
    };

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("CSV Highlight Viewer")
            .with_inner_size([1480.0, 920.0]),
        ..Default::default()
    };

    eframe::run_native(
        "CSV Highlight Viewer",
        options,
        Box::new(move |cc| {
            let font_setup_result = configure_japanese_font(&cc.egui_ctx);
            let mut app = App::new(csv_path.clone());

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
