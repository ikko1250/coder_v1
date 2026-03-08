use serde::Deserialize;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_FILTER_CONFIG_RELATIVE_PATH: &str = "asset/cooccurrence-conditions.json";
const DEFAULT_SCRIPT_RELATIVE_PATH: &str = "run-analysis.py";
const DEFAULT_JOBS_RELATIVE_PATH: &str = "runtime/jobs";
const JOB_HISTORY_KEEP_COUNT: usize = 5;
const PYTHON_PATH_ENV_KEY: &str = "CSV_VIEWER_PYTHON";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeConfig {
    pub(crate) python_command: OsString,
    pub(crate) python_label: String,
    pub(crate) script_path: PathBuf,
    pub(crate) filter_config_path: PathBuf,
    pub(crate) jobs_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisJobRequest {
    pub(crate) db_path: PathBuf,
    pub(crate) runtime: AnalysisRuntimeConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnalysisJobSuccess {
    pub(crate) meta: AnalysisMeta,
    pub(crate) output_csv_path: PathBuf,
    pub(crate) stdout: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnalysisJobFailure {
    pub(crate) meta: Option<AnalysisMeta>,
    pub(crate) stderr: String,
    pub(crate) message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AnalysisJobEvent {
    Completed(Result<AnalysisJobSuccess, AnalysisJobFailure>),
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysisMeta {
    pub(crate) job_id: String,
    pub(crate) status: String,
    pub(crate) started_at: String,
    pub(crate) finished_at: String,
    pub(crate) duration_seconds: f64,
    pub(crate) db_path: String,
    pub(crate) filter_config_path: String,
    pub(crate) output_csv_path: String,
    pub(crate) target_paragraph_count: usize,
    pub(crate) selected_paragraph_count: usize,
    pub(crate) warning_messages: Vec<String>,
    pub(crate) error_summary: String,
}

pub(crate) fn build_default_runtime_config() -> Result<AnalysisRuntimeConfig, String> {
    let script_path = resolve_project_file(DEFAULT_SCRIPT_RELATIVE_PATH)
        .ok_or_else(|| format!("分析スクリプトが見つかりません: {DEFAULT_SCRIPT_RELATIVE_PATH}"))?;
    let filter_config_path = resolve_project_file(DEFAULT_FILTER_CONFIG_RELATIVE_PATH).ok_or_else(|| {
        format!(
            "条件 JSON が見つかりません: {DEFAULT_FILTER_CONFIG_RELATIVE_PATH}"
        )
    })?;
    let project_root = script_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let jobs_root = project_root.join(DEFAULT_JOBS_RELATIVE_PATH);
    let (python_command, python_label) = resolve_python_command()?;

    Ok(AnalysisRuntimeConfig {
        python_command,
        python_label,
        script_path,
        filter_config_path,
        jobs_root,
    })
}

pub(crate) fn cleanup_job_directories(jobs_root: &Path) -> Result<(), String> {
    if !jobs_root.exists() {
        return Ok(());
    }

    let mut job_directories: Vec<(SystemTime, PathBuf)> = Vec::new();
    let entries = fs::read_dir(jobs_root)
        .map_err(|error| format!("job ディレクトリ一覧の取得に失敗しました: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("job ディレクトリ読込に失敗しました: {error}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let modified_at = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(UNIX_EPOCH);
        job_directories.push((modified_at, path));
    }

    if job_directories.len() <= JOB_HISTORY_KEEP_COUNT {
        return Ok(());
    }

    job_directories.sort_by_key(|(modified_at, _)| *modified_at);
    let remove_count = job_directories.len().saturating_sub(JOB_HISTORY_KEEP_COUNT);
    for (_, path) in job_directories.into_iter().take(remove_count) {
        fs::remove_dir_all(&path)
            .map_err(|error| format!("古い job ディレクトリ削除に失敗しました ({}): {error}", path.display()))?;
    }

    Ok(())
}

pub(crate) fn spawn_analysis_job(request: AnalysisJobRequest) -> (String, Receiver<AnalysisJobEvent>) {
    let (sender, receiver) = mpsc::channel();
    let job_id = build_job_id();
    let request_job_id = job_id.clone();

    thread::spawn(move || {
        let event = run_analysis_job(request_job_id, request);
        let _ = sender.send(event);
    });

    (job_id, receiver)
}

fn run_analysis_job(job_id: String, request: AnalysisJobRequest) -> AnalysisJobEvent {
    let output_dir = request.runtime.jobs_root.join(&job_id);
    let output_csv_path = output_dir.join("result.csv");
    let output_meta_json_path = output_dir.join("meta.json");

    if let Err(error) = fs::create_dir_all(&output_dir) {
        return AnalysisJobEvent::Completed(Err(AnalysisJobFailure {
            meta: None,
            stderr: String::new(),
            message: format!(
                "job 出力ディレクトリを作成できませんでした ({}): {error}",
                output_dir.display()
            ),
        }));
    }

    let output = Command::new(&request.runtime.python_command)
        .arg(&request.runtime.script_path)
        .arg("--job-id")
        .arg(&job_id)
        .arg("--db-path")
        .arg(&request.db_path)
        .arg("--filter-config-path")
        .arg(&request.runtime.filter_config_path)
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--output-csv-path")
        .arg(&output_csv_path)
        .arg("--output-meta-json-path")
        .arg(&output_meta_json_path)
        .output();

    let output = match output {
        Ok(output) => output,
        Err(error) => {
            return AnalysisJobEvent::Completed(Err(AnalysisJobFailure {
                meta: None,
                stderr: String::new(),
                message: format!("Python CLI の起動に失敗しました: {error}"),
            }));
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let meta = read_meta_json(&output_meta_json_path).ok();

    if output.status.success() {
        let Some(meta) = meta else {
            return AnalysisJobEvent::Completed(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: "meta.json が生成されませんでした".to_string(),
            }));
        };

        let resolved_output_csv_path = PathBuf::from(&meta.output_csv_path);
        if !resolved_output_csv_path.is_file() {
            return AnalysisJobEvent::Completed(Err(AnalysisJobFailure {
                meta: Some(meta),
                stderr,
                message: format!(
                    "出力 CSV が見つかりません: {}",
                    resolved_output_csv_path.display()
                ),
            }));
        }

        return AnalysisJobEvent::Completed(Ok(AnalysisJobSuccess {
            meta,
            output_csv_path: resolved_output_csv_path,
            stdout,
        }));
    }

    let message = meta
        .as_ref()
        .and_then(|meta| (!meta.error_summary.trim().is_empty()).then(|| meta.error_summary.clone()))
        .or_else(|| (!stderr.is_empty()).then_some(stderr.clone()))
        .unwrap_or_else(|| format!("Python CLI が異常終了しました: {}", output.status));

    AnalysisJobEvent::Completed(Err(AnalysisJobFailure {
        meta,
        stderr,
        message,
    }))
}

fn read_meta_json(path: &Path) -> Result<AnalysisMeta, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("meta.json を読めませんでした ({}): {error}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("meta.json の解析に失敗しました ({}): {error}", path.display()))
}

fn build_job_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("job-{millis}")
}

fn resolve_python_command() -> Result<(OsString, String), String> {
    let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let candidate_paths = [
        current_dir.join(".venv").join("Scripts").join("python.exe"),
        current_dir.join(".venv").join("bin").join("python"),
    ];

    for path in candidate_paths {
        if path.is_file() {
            let display = path.display().to_string();
            return Ok((path.into_os_string(), display));
        }
    }

    if let Ok(value) = env::var(PYTHON_PATH_ENV_KEY) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok((OsString::from(trimmed), trimmed.to_string()));
        }
    }

    for candidate in ["python3", "python"] {
        if command_exists(candidate) {
            return Ok((OsString::from(candidate), candidate.to_string()));
        }
    }

    Err("Python 実行系が見つかりません。.venv または CSV_VIEWER_PYTHON を確認してください".to_string())
}

fn command_exists(command: &str) -> bool {
    let Some(paths) = env::var_os("PATH") else {
        return false;
    };

    env::split_paths(&paths).any(|dir| {
        let direct = dir.join(command);
        let windows = dir.join(format!("{command}.exe"));
        direct.is_file() || windows.is_file()
    })
}

fn resolve_project_file(relative_path: &str) -> Option<PathBuf> {
    resolve_project_path(relative_path).filter(|path| path.is_file())
}

fn resolve_project_path(relative_path: &str) -> Option<PathBuf> {
    let relative_path = PathBuf::from(relative_path);
    let cwd_candidate = env::current_dir().ok()?.join(&relative_path);
    if cwd_candidate.exists() {
        return Some(cwd_candidate);
    }

    if let Ok(exe_path) = env::current_exe() {
        for ancestor in exe_path.ancestors().skip(1).take(6) {
            let candidate = ancestor.join(&relative_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    Some(relative_path)
}
