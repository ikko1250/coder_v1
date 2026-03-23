//! eframe/egui のホスト起動設定（P3-04）。
//!
//! `viewer_core` はここに依存しない。環境依存（タイトル、初期サイズ、フォント初期化）を
//! `main` から呼び出す薄いホスト層に寄せる。

use crate::app::App;
use crate::font::configure_japanese_font;
use eframe::egui;

pub(crate) const APP_WINDOW_TITLE: &str = "条例分析ビューア";
pub(crate) const APP_INNER_SIZE: [f32; 2] = [1480.0, 920.0];
const MISSING_JAPANESE_FONT_WARNING: &str =
    "日本語フォントが見つからなかったため、日本語テキストが正しく表示されない可能性があります。";

pub(crate) fn build_native_options() -> eframe::NativeOptions {
    eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title(APP_WINDOW_TITLE)
            .with_inner_size(APP_INNER_SIZE),
        ..Default::default()
    }
}

pub(crate) fn apply_host_startup_effects(cc: &eframe::CreationContext<'_>, app: &mut App) {
    let font_setup_result = configure_japanese_font(&cc.egui_ctx);
    match font_setup_result {
        Ok(Some(_)) => {}
        Ok(None) => {
            if app.error_message.is_none() {
                app.error_message = Some(MISSING_JAPANESE_FONT_WARNING.to_string());
            }
        }
        Err(err) => {
            if app.error_message.is_none() {
                app.error_message = Some(err);
            }
        }
    }
}
