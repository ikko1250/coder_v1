//! ネイティブファイルダイアログの **ホスト側**抽象（P3-01）。
//!
//! UI 層は [`FileDialogHost`] 経由で `PathBuf` のみ受け取り、[`RfdFileDialogHost`] が `rfd` を閉じ込める。
//! [`crate::viewer_core`] はファイルダイアログを知らない。

use std::path::PathBuf;

/// ファイル選択ダイアログ（開く／保存）をホストが提供する。
pub(crate) trait FileDialogHost {
    /// メイン CSV を開く（`.csv` 優先）。
    fn pick_open_csv(&self) -> Option<PathBuf>;
    /// 分析結果の全件 CSV 保存。
    fn pick_save_analysis_result_csv(&self) -> Option<PathBuf>;
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

    fn pick_save_analysis_result_csv(&self) -> Option<PathBuf> {
        rfd::FileDialog::new()
            .add_filter("CSV files", &["csv"])
            .set_file_name("analysis-result.csv")
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
