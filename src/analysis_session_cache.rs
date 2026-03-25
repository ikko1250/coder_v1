//! 分析結果の **セッション内**再利用用キャッシュキー（ディスクへの結果保存は行わない）。

use crate::analysis_runner::{AnalysisRuntimeConfig, AnalysisWarningMessage};
use crate::model::AnalysisRecord;
use std::path::PathBuf;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileStamp {
    pub(crate) canonical_path: PathBuf,
    pub(crate) len: u64,
    pub(crate) mtime_ns: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeStamp {
    pub(crate) python_command: String,
    pub(crate) python_args: Vec<String>,
    pub(crate) project_root: PathBuf,
    pub(crate) script_path: PathBuf,
}

impl From<&AnalysisRuntimeConfig> for AnalysisRuntimeStamp {
    fn from(runtime: &AnalysisRuntimeConfig) -> Self {
        Self {
            python_command: runtime.python_command.to_string_lossy().into_owned(),
            python_args: runtime
                .python_args
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            project_root: runtime.project_root.clone(),
            script_path: runtime.script_path.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisSessionCacheKey {
    pub(crate) db: FileStamp,
    pub(crate) filter_config_sha256: [u8; 32],
    pub(crate) annotation: Option<FileStamp>,
    pub(crate) runtime: AnalysisRuntimeStamp,
}

/// UI 行とバッファを共有しないよう、成功時に **clone** して保持する（設計書 §3.3）。
#[derive(Clone, Debug)]
pub(crate) struct AnalysisResultSnapshot {
    pub(crate) records: Vec<AnalysisRecord>,
    pub(crate) source_label: String,
    pub(crate) last_warnings: Vec<AnalysisWarningMessage>,
    pub(crate) db_path: PathBuf,
    pub(crate) filter_config_path: PathBuf,
    pub(crate) annotation_csv_path: PathBuf,
    /// `AnalysisJobStatus::Succeeded` 用の一行（キャッシュヒット時は接頭辞を付けて表示する）。
    pub(crate) status_summary: String,
}

fn file_stamp(path: &Path) -> Option<FileStamp> {
    let meta = fs::metadata(path).ok()?;
    let len = meta.len();
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|st| st.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let canonical_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    Some(FileStamp {
        canonical_path,
        len,
        mtime_ns,
    })
}

fn sha256_file(path: &Path) -> Option<[u8; 32]> {
    let bytes = fs::read(path).ok()?;
    let digest = Sha256::digest(bytes);
    Some(digest.into())
}

/// annotation が未作成のときは「パス文字列 + 0 バイト」で安定キーにする。
fn annotation_stamp(path: &Path) -> Option<FileStamp> {
    if path.as_os_str().is_empty() {
        return None;
    }
    if path.exists() {
        return file_stamp(path);
    }
    Some(FileStamp {
        canonical_path: fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        len: 0,
        mtime_ns: 0,
    })
}

/// キー構築に失敗した場合はキャッシュ未使用（分析本体は別経路でエラーになりうる）。
pub(crate) fn build_session_cache_key(
    db_path: &Path,
    filter_config_path: &Path,
    annotation_csv_path: &Path,
    runtime: &AnalysisRuntimeConfig,
) -> Option<AnalysisSessionCacheKey> {
    let db = file_stamp(db_path)?;
    let filter_config_sha256 = sha256_file(filter_config_path)?;
    let annotation = annotation_stamp(annotation_csv_path);
    Some(AnalysisSessionCacheKey {
        db,
        filter_config_sha256,
        annotation,
        runtime: AnalysisRuntimeStamp::from(runtime),
    })
}
