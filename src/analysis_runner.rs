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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeOverrides {
    pub(crate) python_path: Option<PathBuf>,
    pub(crate) filter_config_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeConfig {
    pub(crate) python_command: OsString,
    pub(crate) python_args: Vec<OsString>,
    pub(crate) python_label: String,
    pub(crate) project_root: PathBuf,
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
    build_runtime_config(&AnalysisRuntimeOverrides::default())
}

pub(crate) fn build_runtime_config(
    overrides: &AnalysisRuntimeOverrides,
) -> Result<AnalysisRuntimeConfig, String> {
    let script_path = resolve_project_file(DEFAULT_SCRIPT_RELATIVE_PATH)
        .ok_or_else(|| format!("分析スクリプトが見つかりません: {DEFAULT_SCRIPT_RELATIVE_PATH}"))?;
    let project_root = script_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let filter_config_path = resolve_filter_config_path(overrides)?;
    let jobs_root = project_root.join(DEFAULT_JOBS_RELATIVE_PATH);
    let (python_command, python_args, python_label) =
        resolve_python_command(&project_root, overrides)?;

    Ok(AnalysisRuntimeConfig {
        python_command,
        python_args,
        python_label,
        project_root,
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

    let mut command = Command::new(&request.runtime.python_command);
    command
        .current_dir(&request.runtime.project_root)
        .args(&request.runtime.python_args)
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
        .arg(&output_meta_json_path);

    let output = command.output();

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

fn resolve_filter_config_path(overrides: &AnalysisRuntimeOverrides) -> Result<PathBuf, String> {
    if let Some(path) = overrides.filter_config_path.as_ref() {
        if path.is_file() {
            return Ok(path.clone());
        }

        return Err(format!(
            "条件 JSON が見つかりません: {}",
            path.display()
        ));
    }

    resolve_project_file(DEFAULT_FILTER_CONFIG_RELATIVE_PATH).ok_or_else(|| {
        format!(
            "条件 JSON が見つかりません: {DEFAULT_FILTER_CONFIG_RELATIVE_PATH}"
        )
    })
}

fn resolve_python_command(
    project_root: &Path,
    overrides: &AnalysisRuntimeOverrides,
) -> Result<(OsString, Vec<OsString>, String), String> {
    if let Some(path) = overrides.python_path.as_ref() {
        if path.is_file() {
            let display = path.display().to_string();
            return Ok((path.clone().into_os_string(), Vec::new(), display));
        }

        return Err(format!(
            "Python 実行ファイルが見つかりません: {}",
            path.display()
        ));
    }

    if let Ok(value) = env::var(PYTHON_PATH_ENV_KEY) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok((OsString::from(trimmed), Vec::new(), trimmed.to_string()));
        }
    }

    let project_venv_root = project_root.join(".venv");
    let candidate_paths = [
        project_venv_root.join("Scripts").join("python.exe"),
        project_venv_root.join("bin").join("python"),
    ];

    for path in candidate_paths {
        if path.is_file() {
            let display = path.display().to_string();
            return Ok((path.into_os_string(), Vec::new(), display));
        }
    }

    if project_venv_root.exists() {
        return Err(format!(
            "プロジェクトの .venv は見つかりましたが、この OS 用の Python 実行ファイルを確認できませんでした。\
Windows なら .venv\\\\Scripts\\\\python.exe を作成するか、{PYTHON_PATH_ENV_KEY} で明示指定してください"
        ));
    }

    if project_root.join("pyproject.toml").is_file() && command_exists("uv") {
        return Ok((
            OsString::from("uv"),
            vec![OsString::from("run"), OsString::from("python")],
            "uv run python".to_string(),
        ));
    }

    for candidate in ["python3", "python"] {
        if command_exists(candidate) {
            return Ok((OsString::from(candidate), Vec::new(), candidate.to_string()));
        }
    }

    Err(
        "Python 実行系が見つかりません。.venv、CSV_VIEWER_PYTHON、または uv を確認してください"
            .to_string(),
    )
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
