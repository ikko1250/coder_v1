//! フレーム先頭の入力・副作用（分析ジョブ受信、キーボード一覧移動、終了ガード）。
//! 親モジュール `app` の子として `App` の非公開メソッドにアクセスする。
//!
//! **境界（P1-07）**
//! - [`super::app_analysis_job`] がジョブチャネル・警告・条件エディタ連携の本体。
//! - 本モジュールは **`eframe::App::update` 冒頭**の呼び出し順（ポーリング → キー → 終了ブロック）を一箇所にまとめる。

use super::app_analysis_job;
use super::App;
use crate::viewer_core::ViewerCoreMessage;
use eframe::egui;

/// 1 フレームの UI 描画より前に実行する処理（`poll_analysis_job` → キーボード → 終了ガード）。
pub(super) fn run_update_prelude(app: &mut App, ctx: &egui::Context) {
    app_analysis_job::poll_analysis_job(app, ctx);
    handle_keyboard_navigation(app, ctx);
    app_analysis_job::guard_root_close_with_dirty_editor(app, ctx);
}

fn handle_keyboard_navigation(app: &mut App, ctx: &egui::Context) {
    if app.error_message.is_some()
        || app.core.filtered_indices.is_empty()
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
        if app
            .apply_event(ViewerCoreMessage::SelectionMoveDown)
            .needs_repaint
        {
            ctx.request_repaint();
        }
    } else if up_pressed {
        if app
            .apply_event(ViewerCoreMessage::SelectionMoveUp)
            .needs_repaint
        {
            ctx.request_repaint();
        }
    }
}
