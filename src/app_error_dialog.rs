//! グローバルエラーメッセージ用モーダル風ウィンドウ。親モジュール `app` の子。

use super::App;
use eframe::egui;

pub(super) fn draw_error_dialog_if_any(app: &mut App, ctx: &egui::Context) {
    if let Some(err) = app.error_message.clone() {
        egui::Window::new("エラー")
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(&err);
                if ui.button("閉じる").clicked() {
                    app.error_message = None;
                }
            });
    }
}
