//! P4-01: Tauri/IPC 向け DTO（serde 可能な Command/Event）。
//!
//! §6 の初期契約を Rust 型として固定する。現時点では egui 版から直接利用しない。

use serde::{Deserialize, Serialize};

/// P4-03: 現行 IPC 契約バージョン（初期固定値）。
pub(crate) const IPC_API_VERSION: &str = "2026-03-23";

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiEnvelope<T> {
    pub(crate) api_version: String,
    pub(crate) payload: T,
}

#[allow(dead_code)]
impl<T> ApiEnvelope<T> {
    /// 現行 API バージョン付きの envelope を作る。
    pub(crate) fn new(payload: T) -> Self {
        Self {
            api_version: IPC_API_VERSION.to_string(),
            payload,
        }
    }

    /// 現行実装で受理できる API バージョンか。
    pub(crate) fn is_supported_api_version(&self) -> bool {
        self.api_version == IPC_API_VERSION
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum IpcCommand {
    LoadCsv {
        path: String,
    },
    SetFilter {
        column: String,
        values: Vec<String>,
    },
    SelectRow {
        index: usize,
    },
    RunAnalysis {
        overrides: AnalysisOverridesDto,
    },
    OpenDbViewer {
        paragraph_id: String,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysisOverridesDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) python_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) filter_config_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) annotation_csv_path: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum IpcEvent {
    AnalysisProgress {
        job_id: String,
        phase: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    AnalysisFinished {
        job_id: String,
        outcome: AnalysisOutcomeDto,
    },
    Error {
        #[serde(flatten)]
        error: IpcErrorDto,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AnalysisOutcomeDto {
    Succeeded,
    Failed,
    Cancelled,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IpcErrorDto {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) job_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        AnalysisOutcomeDto, ApiEnvelope, IpcCommand, IpcErrorDto, IpcEvent, IPC_API_VERSION,
    };

    #[test]
    fn ipc_command_round_trip_json() {
        let command = ApiEnvelope {
            api_version: IPC_API_VERSION.to_string(),
            payload: IpcCommand::RunAnalysis {
                overrides: super::AnalysisOverridesDto {
                    python_path: Some("C:/Python312/python.exe".to_string()),
                    filter_config_path: Some("asset/cooccurrence-conditions.json".to_string()),
                    annotation_csv_path: None,
                },
            },
        };
        let json = serde_json::to_string(&command).unwrap();
        let parsed: ApiEnvelope<IpcCommand> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, command);
    }

    #[test]
    fn ipc_event_round_trip_json() {
        let event = ApiEnvelope {
            api_version: IPC_API_VERSION.to_string(),
            payload: IpcEvent::AnalysisFinished {
                job_id: "job-123".to_string(),
                outcome: AnalysisOutcomeDto::Succeeded,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: ApiEnvelope<IpcEvent> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn ipc_error_event_serialization_omits_job_id_when_none() {
        let event = ApiEnvelope {
            api_version: IPC_API_VERSION.to_string(),
            payload: IpcEvent::Error {
                error: IpcErrorDto {
                    code: "csv_not_found".to_string(),
                    message: "CSV が見つかりません".to_string(),
                    job_id: None,
                },
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"code\":\"csv_not_found\""));
        assert!(!json.contains("\"jobId\""));
        let parsed: ApiEnvelope<IpcEvent> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn ipc_error_event_serialization_keeps_job_id_when_present() {
        let event = ApiEnvelope {
            api_version: IPC_API_VERSION.to_string(),
            payload: IpcEvent::Error {
                error: IpcErrorDto {
                    code: "analysis_failed".to_string(),
                    message: "analysis subprocess exited with code 1".to_string(),
                    job_id: Some("job-999".to_string()),
                },
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"jobId\":\"job-999\""));
        let parsed: ApiEnvelope<IpcEvent> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn api_envelope_new_sets_current_api_version() {
        let envelope = ApiEnvelope::new(IpcCommand::SelectRow { index: 3 });
        assert_eq!(envelope.api_version, IPC_API_VERSION);
    }

    #[test]
    fn api_version_support_check_works() {
        let supported = ApiEnvelope {
            api_version: IPC_API_VERSION.to_string(),
            payload: IpcEvent::AnalysisProgress {
                job_id: "job-1".to_string(),
                phase: "running".to_string(),
                message: None,
            },
        };
        assert!(supported.is_supported_api_version());

        let unsupported = ApiEnvelope {
            api_version: "2026-03-24".to_string(),
            payload: IpcEvent::AnalysisProgress {
                job_id: "job-1".to_string(),
                phase: "running".to_string(),
                message: None,
            },
        };
        assert!(!unsupported.is_supported_api_version());
    }
}
