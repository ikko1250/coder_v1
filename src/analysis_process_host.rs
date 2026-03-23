//! 分析・エクスポート **プロセス起動**のホスト側抽象（P3-02）。
//!
//! 既定実装 [`ThreadAnalysisProcessHost`] は [`crate::analysis_runner`] の `spawn_*` に委譲する。
//! [`crate::viewer_core`] は子プロセス起動を行わず、ジョブ ID とチャネル受信は UI 層の責務のままである。

use crate::analysis_runner::{
    spawn_analysis_job, spawn_export_job, AnalysisExportRequest, AnalysisJobEvent, AnalysisJobRequest,
};
use std::sync::mpsc::Receiver;

/// 分析ジョブ／エクスポートジョブを起動し、`job_id` と完了イベント受信チャネルを返す。
pub(crate) trait AnalysisProcessHost {
    fn spawn_analysis_job(
        &self,
        request: AnalysisJobRequest,
    ) -> (String, Receiver<AnalysisJobEvent>);

    fn spawn_export_job(
        &self,
        request: AnalysisExportRequest,
    ) -> (String, Receiver<AnalysisJobEvent>);
}

/// 標準ライブラリのスレッド＋[`analysis_runner`] のワーカー起動（デスクトップ用）。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ThreadAnalysisProcessHost;

impl AnalysisProcessHost for ThreadAnalysisProcessHost {
    fn spawn_analysis_job(
        &self,
        request: AnalysisJobRequest,
    ) -> (String, Receiver<AnalysisJobEvent>) {
        spawn_analysis_job(request)
    }

    fn spawn_export_job(
        &self,
        request: AnalysisExportRequest,
    ) -> (String, Receiver<AnalysisJobEvent>) {
        spawn_export_job(request)
    }
}
