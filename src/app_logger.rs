//! アプリ内ログ出力の最小インタフェース（P3-05）。
//!
//! 実装を差し替え可能にして、呼び出し側は出力先（stderr / tracing 等）に依存しない。

/// ログ出力の抽象。
pub(crate) trait AppLogger {
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
    fn error(&self, message: &str);
}

/// 既定実装。stderr に 1 行ログを出す。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StderrAppLogger;

impl AppLogger for StderrAppLogger {
    fn info(&self, message: &str) {
        eprintln!("[INFO] {message}");
    }

    fn warn(&self, message: &str) {
        eprintln!("[WARN] {message}");
    }

    fn error(&self, message: &str) {
        eprintln!("[ERROR] {message}");
    }
}
