//! ネイティブファイルダイアログの **ホスト側**抽象（P3-01）。
//!
//! UI 層は [`FileDialogHost`] 経由で `PathBuf` のみ受け取り、[`RfdFileDialogHost`] が `rfd` を閉じ込める。
//! [`crate::viewer_core`] はファイルダイアログを知らない。

use std::path::PathBuf;

/// ファイル選択ダイアログ（開く／保存）をホストが提供する。
pub(crate) trait FileDialogHost {
    /// メイン CSV を開く（`.csv` 優先）。
    fn pick_open_csv(&self) -> Option<PathBuf>;
    /// 分析に使う既存の Analysis DB を開く。
    fn pick_open_analysis_db(&self) -> Option<PathBuf>;
    /// Builder 入力フォルダーを選ぶ。
    fn pick_open_folder(&self) -> Option<PathBuf>;
    /// 一覧に表示中の分析結果 CSV 保存。
    fn pick_save_analysis_result_csv(&self) -> Option<PathBuf>;
    /// Analysis DB の保存先。
    fn pick_save_analysis_db(&self) -> Option<PathBuf>;
    /// Builder report JSON の保存先。
    fn pick_save_report_json(&self) -> Option<PathBuf>;
    /// Python 実行ファイルを選ぶ（Windows 想定で `.exe` フィルタあり）。
    fn pick_python_executable(&self) -> Option<PathBuf>;
    /// 条件 JSON を開く。
    fn pick_open_json(&self) -> Option<PathBuf>;
    /// annotation CSV の保存先。
    fn pick_save_annotation_csv(&self) -> Option<PathBuf>;
}

/// [`rfd`] による [`FileDialogHost`] 実装（egui ホスト／デスクトップ用）。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RfdFileDialogHost;

impl FileDialogHost for RfdFileDialogHost {
    fn pick_open_csv(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("CSV files", &["csv"])
            .add_filter("All files", &["*"])
            .pick_file()
    }

    fn pick_open_analysis_db(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("SQLite DB", &["db", "sqlite", "sqlite3"])
            .add_filter("All files", &["*"])
            .pick_file()
    }

    fn pick_open_folder(&self) -> Option<PathBuf> {
        rfd::FileDialog::new().pick_folder()
    }

    fn pick_save_analysis_result_csv(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("CSV files", &["csv"])
            .set_file_name("analysis-result-visible.csv")
            .save_file()
    }

    fn pick_save_analysis_db(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("SQLite DB", &["db", "sqlite", "sqlite3"])
            .set_file_name("analysis.db")
            .save_file()
    }

    fn pick_save_report_json(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("JSON files", &["json"])
            .set_file_name("analysis.db.report.json")
            .save_file()
    }

    fn pick_python_executable(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("Python", &["exe"])
            .add_filter("All files", &["*"])
            .pick_file()
    }

    fn pick_open_json(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("JSON files", &["json"])
            .add_filter("All files", &["*"])
            .pick_file()
    }

    fn pick_save_annotation_csv(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("CSV files", &["csv"])
            .set_file_name("manual-annotations.csv")
            .save_file()
    }
}
