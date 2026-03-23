//! P4-01: Tauri/IPC 向け DTO（serde 可能な Command/Event）。
//!
//! §6 の初期契約を Rust 型として固定する。現時点では egui 版から直接利用しない。

use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApiEnvelope<T> {
    pub(crate) api_version: String,
    pub(crate) payload: T,
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
        code: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_id: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::{
        AnalysisOutcomeDto, ApiEnvelope, IpcCommand, IpcEvent,
    };

    #[test]
    fn ipc_command_round_trip_json() {
        let command = ApiEnvelope {
            api_version: "2026-03-23".to_string(),
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
            api_version: "2026-03-23".to_string(),
            payload: IpcEvent::AnalysisFinished {
                job_id: "job-123".to_string(),
                outcome: AnalysisOutcomeDto::Succeeded,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: ApiEnvelope<IpcEvent> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, event);
    }
}
