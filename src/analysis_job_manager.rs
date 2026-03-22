use crate::analysis_runner::{
    spawn_analysis_job, spawn_export_job, AnalysisExportRequest, AnalysisJobEvent,
    AnalysisJobRequest,
};
use std::sync::mpsc::{Receiver, TryRecvError};

struct RunningAnalysisJob {
    receiver: Receiver<AnalysisJobEvent>,
}

pub(crate) enum JobPollResult {
    Idle,
    Pending,
    Completed(AnalysisJobEvent),
    Disconnected,
}

#[derive(Default)]
pub(crate) struct AnalysisJobManager {
    current_job: Option<RunningAnalysisJob>,
}

impl AnalysisJobManager {
    pub(crate) fn has_running_job(&self) -> bool {
        self.current_job.is_some()
    }

    pub(crate) fn start_analysis_job(
        &mut self,
        request: AnalysisJobRequest,
    ) -> Result<String, String> {
        if self.has_running_job() {
            return Err("分析ジョブは既に実行中です".to_string());
        }
        let (job_id, receiver) = spawn_analysis_job(request);
        self.current_job = Some(RunningAnalysisJob { receiver });
        Ok(job_id)
    }

    pub(crate) fn start_export_job(
        &mut self,
        request: AnalysisExportRequest,
    ) -> Result<String, String> {
        if self.has_running_job() {
            return Err("分析ジョブは既に実行中です".to_string());
        }
        let (job_id, receiver) = spawn_export_job(request);
        self.current_job = Some(RunningAnalysisJob { receiver });
        Ok(job_id)
    }

    pub(crate) fn poll(&mut self) -> JobPollResult {
        let Some(running_job) = self.current_job.as_ref() else {
            return JobPollResult::Idle;
        };

        match running_job.receiver.try_recv() {
            Ok(event) => {
                self.current_job = None;
                JobPollResult::Completed(event)
            }
            Err(TryRecvError::Empty) => JobPollResult::Pending,
            Err(TryRecvError::Disconnected) => {
                self.current_job = None;
                JobPollResult::Disconnected
            }
        }
    }
}
