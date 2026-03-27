#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod analysis_process_host;
mod analysis_runner;
mod analysis_session_cache;
mod app;
mod app_host;
mod app_logger;
mod condition_editor;
mod condition_editor_view;
mod csv_loader;
mod db;
mod db_viewer_view;
mod filter;
mod filter_panel_view;
mod file_dialog_host;
mod font;
mod ipc_dto;
mod manual_annotation_store;
mod model;
mod tagged_text;
mod ui_helpers;
mod viewer_core;

use crate::app::App;
use crate::app_host::{apply_host_startup_effects, build_native_options, APP_WINDOW_TITLE};
use crate::ipc_dto::run_ipc_dto_self_check;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--ipc-dto-self-check") {
        match run_ipc_dto_self_check() {
            Ok(output) => {
                println!("{output}");
                return;
            }
            Err(error) => {
                eprintln!("IPC DTO self-check failed: {error}");
                std::process::exit(1);
            }
        }
    }

    let initial_csv_path = args
        .get(1)
        .filter(|arg| !arg.starts_with("--"))
        .map(std::path::PathBuf::from);

    let options = build_native_options();

    eframe::run_native(
        APP_WINDOW_TITLE,
        options,
        Box::new(move |cc| {
            let mut app = App::new(initial_csv_path.clone());
            apply_host_startup_effects(cc, &mut app);

            Ok(Box::new(app))
        }),
    )
    .unwrap();
}
