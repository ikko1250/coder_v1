use crate::model::{AnalysisRecord, AnalysisUnit};
use serde::{Deserialize, Serialize};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_FILTER_CONFIG_RELATIVE_PATH: &str = "asset/cooccurrence-conditions.json";
const DEFAULT_ANNOTATION_CSV_RELATIVE_PATH: &str = "asset/manual-annotations.csv";
const DEFAULT_SCRIPT_RELATIVE_PATH: &str = "run-analysis.py";
const DEFAULT_BUILDER_SCRIPT_RELATIVE_PATH: &str = "docs/build_ordinance_analysis_db.py";
const DEFAULT_JOBS_RELATIVE_PATH: &str = "runtime/jobs";
const JOB_HISTORY_KEEP_COUNT: usize = 5;
const PYTHON_PATH_ENV_KEY: &str = "CSV_VIEWER_PYTHON";
const WORKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10000);
const BUILD_REPORT_PATH_SUFFIX: &str = ".report.json";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeOverrides {
    pub(crate) python_path: Option<PathBuf>,
    pub(crate) filter_config_path: Option<PathBuf>,
    pub(crate) annotation_csv_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisRuntimeConfig {
    pub(crate) python_command: OsString,
    pub(crate) python_args: Vec<OsString>,
    pub(crate) python_label: String,
    pub(crate) project_root: PathBuf,
    pub(crate) script_path: PathBuf,
    pub(crate) filter_config_path: PathBuf,
    pub(crate) annotation_csv_path: PathBuf,
    pub(crate) jobs_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuilderRuntimeConfig {
    pub(crate) python_command: OsString,
    pub(crate) python_args: Vec<OsString>,
    pub(crate) python_label: String,
    pub(crate) project_root: PathBuf,
    pub(crate) builder_script_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisJobRequest {
    pub(crate) db_path: PathBuf,
    pub(crate) runtime: AnalysisRuntimeConfig,
    /// Python worker の DB フレーム読み込みキャッシュを破棄して再読込する（設計書の「再読込分析」レベル B）。
    pub(crate) force_reload_db_frames: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisExportRequest {
    pub(crate) db_path: PathBuf,
    pub(crate) filter_config_path: PathBuf,
    pub(crate) annotation_csv_path: PathBuf,
    pub(crate) output_csv_path: PathBuf,
    pub(crate) runtime: AnalysisRuntimeConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisDbBuildRequest {
    pub(crate) runtime: BuilderRuntimeConfig,
    pub(crate) input_dir: PathBuf,
    pub(crate) analysis_db_path: PathBuf,
    pub(crate) report_path: PathBuf,
    pub(crate) skip_tokenize: bool,
    pub(crate) sudachi_dict: String,
    pub(crate) split_mode: String,
    pub(crate) split_inside_parentheses: bool,
    pub(crate) merge_table_lines: bool,
    pub(crate) purge: bool,
    pub(crate) fresh_db: bool,
    pub(crate) limit: Option<usize>,
    pub(crate) note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnalysisJobSuccess {
    pub(crate) meta: AnalysisMeta,
    pub(crate) records: Vec<AnalysisRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnalysisExportSuccess {
    pub(crate) meta: AnalysisMeta,
    pub(crate) output_csv_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnalysisDbBuildSuccess {
    pub(crate) analysis_db_path: PathBuf,
    pub(crate) report_path: PathBuf,
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
    AnalysisCompleted(Result<AnalysisJobSuccess, AnalysisJobFailure>),
    ExportCompleted(Result<AnalysisExportSuccess, AnalysisJobFailure>),
    BuildCompleted(Result<AnalysisDbBuildSuccess, AnalysisJobFailure>),
}

#[derive(Clone)]
pub(crate) struct BuildJobControl {
    child: Arc<Mutex<Child>>,
    cancelled: Arc<AtomicBool>,
}

impl BuildJobControl {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysisWarningMessage {
    #[serde(default)]
    pub(crate) code: String,
    #[serde(default)]
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) severity: Option<String>,
    #[serde(default)]
    pub(crate) scope: Option<String>,
    #[serde(default)]
    pub(crate) condition_id: Option<String>,
    #[serde(default)]
    pub(crate) field_name: Option<String>,
    #[serde(default)]
    pub(crate) unit_id: Option<i64>,
    #[serde(default)]
    pub(crate) query_name: Option<String>,
    #[serde(default)]
    pub(crate) db_path: Option<String>,
    #[serde(default)]
    pub(crate) requested_mode: Option<String>,
    #[serde(default)]
    pub(crate) used_mode: Option<String>,
    #[serde(default)]
    pub(crate) combination_count: Option<i64>,
    #[serde(default)]
    pub(crate) combination_cap: Option<i64>,
    #[serde(default)]
    pub(crate) safety_limit: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum RawAnalysisWarningMessage {
    Structured(AnalysisWarningMessage),
    LegacyString(String),
}

fn deserialize_warning_messages<'de, D>(
    deserializer: D,
) -> Result<Vec<AnalysisWarningMessage>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw_values = Vec::<RawAnalysisWarningMessage>::deserialize(deserializer)?;
    Ok(raw_values
        .into_iter()
        .map(|raw_value| match raw_value {
            RawAnalysisWarningMessage::Structured(warning) => warning,
            RawAnalysisWarningMessage::LegacyString(message) => AnalysisWarningMessage {
                code: String::new(),
                message,
                severity: None,
                scope: None,
                condition_id: None,
                field_name: None,
                unit_id: None,
                query_name: None,
                db_path: None,
                requested_mode: None,
                used_mode: None,
                combination_count: None,
                combination_cap: None,
                safety_limit: None,
            },
        })
        .collect())
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
    #[serde(default)]
    pub(crate) analysis_unit: AnalysisUnit,
    pub(crate) target_paragraph_count: usize,
    pub(crate) selected_paragraph_count: usize,
    #[serde(default)]
    pub(crate) selected_sentence_count: usize,
    #[serde(default, deserialize_with = "deserialize_warning_messages")]
    pub(crate) warning_messages: Vec<AnalysisWarningMessage>,
    pub(crate) error_summary: String,
}

impl AnalysisMeta {
    fn expected_record_count(&self) -> usize {
        match self.analysis_unit {
            AnalysisUnit::Paragraph => self.selected_paragraph_count,
            AnalysisUnit::Sentence => self.selected_sentence_count,
        }
    }

    pub(crate) fn selected_unit_count(&self) -> usize {
        self.expected_record_count()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct AnalysisJsonRecord {
    #[serde(default)]
    paragraph_id: String,
    #[serde(default)]
    sentence_id: String,
    #[serde(default)]
    document_id: String,
    #[serde(default)]
    category1: String,
    #[serde(default)]
    category2: String,
    #[serde(default)]
    sentence_count: String,
    #[serde(default)]
    sentence_no_in_paragraph: String,
    #[serde(default)]
    sentence_no_in_document: String,
    #[serde(default)]
    sentence_text: String,
    #[serde(default)]
    sentence_text_tagged: String,
    #[serde(default)]
    paragraph_text: String,
    #[serde(default)]
    paragraph_text_tagged: String,
    #[serde(default)]
    matched_condition_ids_text: String,
    #[serde(default)]
    matched_categories_text: String,
    #[serde(default)]
    matched_form_group_ids_text: String,
    #[serde(default)]
    matched_form_group_logics_text: String,
    #[serde(default)]
    form_group_explanations_text: String,
    #[serde(default)]
    text_groups_explanations_text: String,
    #[serde(default)]
    mixed_scope_warning_text: String,
    #[serde(default)]
    match_group_ids_text: String,
    #[serde(default)]
    match_group_count: String,
    #[serde(default)]
    annotated_token_count: String,
    #[serde(default)]
    manual_annotation_count: String,
    #[serde(default)]
    manual_annotation_pairs_text: String,
    #[serde(default)]
    manual_annotation_namespaces_text: String,
}

impl AnalysisJsonRecord {
    fn into_analysis_record(self, row_no: usize, analysis_unit: AnalysisUnit) -> AnalysisRecord {
        AnalysisRecord {
            row_no,
            analysis_unit,
            paragraph_id: self.paragraph_id,
            sentence_id: self.sentence_id,
            document_id: self.document_id,
            category1: self.category1,
            category2: self.category2,
            sentence_count: self.sentence_count,
            sentence_no_in_paragraph: self.sentence_no_in_paragraph,
            sentence_no_in_document: self.sentence_no_in_document,
            sentence_text: self.sentence_text,
            sentence_text_tagged: self.sentence_text_tagged,
            paragraph_text: self.paragraph_text,
            paragraph_text_tagged: self.paragraph_text_tagged,
            matched_condition_ids_text: self.matched_condition_ids_text,
            matched_categories_text: self.matched_categories_text,
            matched_form_group_ids_text: self.matched_form_group_ids_text,
            matched_form_group_logics_text: self.matched_form_group_logics_text,
            form_group_explanations_text: self.form_group_explanations_text,
            text_groups_explanations_text: self.text_groups_explanations_text,
            mixed_scope_warning_text: self.mixed_scope_warning_text,
            match_group_ids_text: self.match_group_ids_text,
            match_group_count: self.match_group_count,
            annotated_token_count: self.annotated_token_count,
            manual_annotation_count: self.manual_annotation_count,
            manual_annotation_pairs_text: self.manual_annotation_pairs_text,
            manual_annotation_namespaces_text: self.manual_annotation_namespaces_text,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkerRuntimeFingerprint {
    python_command: String,
    python_args: Vec<String>,
    project_root: PathBuf,
    script_path: PathBuf,
}

impl WorkerRuntimeFingerprint {
    fn from_runtime(runtime: &AnalysisRuntimeConfig) -> Self {
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

struct WorkerConnection {
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Clone)]
struct WorkerHandle {
    fingerprint: WorkerRuntimeFingerprint,
    child: Arc<Mutex<Child>>,
    connection: Arc<Mutex<WorkerConnection>>,
    stderr_buffer: Arc<Mutex<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerAnalyzeRequest {
    request_id: String,
    request_type: &'static str,
    job_id: String,
    db_path: String,
    filter_config_path: String,
    annotation_csv_path: String,
    limit_rows: Option<i64>,
    force_reload: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerExportRequest {
    request_id: String,
    request_type: &'static str,
    job_id: String,
    db_path: String,
    filter_config_path: String,
    annotation_csv_path: String,
    output_path: String,
    force_reload: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerSimpleRequest {
    request_id: String,
    request_type: &'static str,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct WorkerResponse {
    request_id: String,
    status: String,
    #[serde(default)]
    meta: Option<AnalysisMeta>,
    #[serde(default)]
    records: Vec<AnalysisJsonRecord>,
    #[serde(default)]
    message: String,
}

fn worker_slot() -> &'static Mutex<Option<WorkerHandle>> {
    static WORKER_SLOT: OnceLock<Mutex<Option<WorkerHandle>>> = OnceLock::new();
    WORKER_SLOT.get_or_init(|| Mutex::new(None))
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
    let annotation_csv_path = resolve_annotation_csv_path(overrides)?;
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
        annotation_csv_path,
        jobs_root,
    })
}

pub(crate) fn build_builder_runtime_config(
    overrides: &AnalysisRuntimeOverrides,
) -> Result<BuilderRuntimeConfig, String> {
    let builder_script_path = resolve_project_file(DEFAULT_BUILDER_SCRIPT_RELATIVE_PATH)
        .ok_or_else(|| {
            format!(
                "builder スクリプトが見つかりません: {DEFAULT_BUILDER_SCRIPT_RELATIVE_PATH}"
            )
        })?;
    let project_root = builder_script_path
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let (python_command, python_args, python_label) =
        resolve_python_command(&project_root, overrides)?;

    Ok(BuilderRuntimeConfig {
        python_command,
        python_args,
        python_label,
        project_root,
        builder_script_path,
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
        let entry =
            entry.map_err(|error| format!("job ディレクトリ読込に失敗しました: {error}"))?;
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
        fs::remove_dir_all(&path).map_err(|error| {
            format!(
                "古い job ディレクトリ削除に失敗しました ({}): {error}",
                path.display()
            )
        })?;
    }

    Ok(())
}

pub(crate) fn spawn_analysis_job(
    request: AnalysisJobRequest,
) -> (String, Receiver<AnalysisJobEvent>) {
    let (sender, receiver) = mpsc::channel();
    let job_id = build_job_id();
    let request_job_id = job_id.clone();

    thread::spawn(move || {
        let event = run_analysis_job(request_job_id, request);
        let _ = sender.send(event);
    });

    (job_id, receiver)
}

pub(crate) fn spawn_export_job(
    request: AnalysisExportRequest,
) -> (String, Receiver<AnalysisJobEvent>) {
    let (sender, receiver) = mpsc::channel();
    let job_id = build_job_id();
    let request_job_id = job_id.clone();

    thread::spawn(move || {
        let event = run_export_job(request_job_id, request);
        let _ = sender.send(event);
    });

    (job_id, receiver)
}

pub(crate) fn spawn_build_job(
    request: AnalysisDbBuildRequest,
) -> Result<(String, Receiver<AnalysisJobEvent>, BuildJobControl), String> {
    let mut command = build_builder_command(&request)?;
    let mut child = command
        .spawn()
        .map_err(|error| format!("builder プロセスの起動に失敗しました: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "builder stdout を取得できませんでした".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "builder stderr を取得できませんでした".to_string())?;

    let (sender, receiver) = mpsc::channel();
    let job_id = build_job_id();
    let child = Arc::new(Mutex::new(child));
    let stdout_buffer = Arc::new(Mutex::new(String::new()));
    let stderr_buffer = Arc::new(Mutex::new(String::new()));
    let cancelled = Arc::new(AtomicBool::new(false));
    spawn_output_collector(stdout, stdout_buffer.clone());
    spawn_stderr_collector(stderr, stderr_buffer.clone());

    let control = BuildJobControl {
        child: child.clone(),
        cancelled: cancelled.clone(),
    };

    let request_job_id = job_id.clone();
    thread::spawn(move || {
        let event = run_build_job(
            request_job_id,
            request,
            child,
            stdout_buffer,
            stderr_buffer,
            cancelled,
        );
        let _ = sender.send(event);
    });

    Ok((job_id, receiver, control))
}

fn run_analysis_job(job_id: String, request: AnalysisJobRequest) -> AnalysisJobEvent {
    let worker_handle = match ensure_worker(&request.runtime) {
        Ok(worker_handle) => worker_handle,
        Err(message) => {
            return AnalysisJobEvent::AnalysisCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr: String::new(),
                message,
            }));
        }
    };
    let worker_request = WorkerAnalyzeRequest {
        request_id: job_id.clone(),
        request_type: "analyze",
        job_id,
        db_path: request.db_path.display().to_string(),
        filter_config_path: request.runtime.filter_config_path.display().to_string(),
        annotation_csv_path: request.runtime.annotation_csv_path.display().to_string(),
        limit_rows: None,
        force_reload: request.force_reload_db_frames,
    };
    let worker_handle_for_request = worker_handle.clone();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result = send_analyze_request(worker_handle_for_request, worker_request);
        let _ = sender.send(result);
    });

    match receiver.recv_timeout(WORKER_REQUEST_TIMEOUT) {
        Ok(result) => AnalysisJobEvent::AnalysisCompleted(result),
        Err(RecvTimeoutError::Timeout) => {
            let stderr = worker_stderr_snapshot(&worker_handle);
            shutdown_worker(&worker_handle);
            invalidate_worker_slot(&worker_handle.fingerprint);
            AnalysisJobEvent::AnalysisCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: format!(
                    "Python worker が {} 秒以内に応答しませんでした",
                    WORKER_REQUEST_TIMEOUT.as_secs()
                ),
            }))
        }
        Err(RecvTimeoutError::Disconnected) => {
            let stderr = worker_stderr_snapshot(&worker_handle);
            shutdown_worker(&worker_handle);
            invalidate_worker_slot(&worker_handle.fingerprint);
            AnalysisJobEvent::AnalysisCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: "Python worker 応答待機中にチャネルが切断されました".to_string(),
            }))
        }
    }
}

fn run_export_job(job_id: String, request: AnalysisExportRequest) -> AnalysisJobEvent {
    let worker_handle = match ensure_worker(&request.runtime) {
        Ok(worker_handle) => worker_handle,
        Err(message) => {
            return AnalysisJobEvent::ExportCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr: String::new(),
                message,
            }));
        }
    };
    let worker_request = WorkerExportRequest {
        request_id: job_id.clone(),
        request_type: "export_csv",
        job_id,
        db_path: request.db_path.display().to_string(),
        filter_config_path: request.filter_config_path.display().to_string(),
        annotation_csv_path: request.annotation_csv_path.display().to_string(),
        output_path: request.output_csv_path.display().to_string(),
        force_reload: false,
    };
    let worker_handle_for_request = worker_handle.clone();
    let output_csv_path = request.output_csv_path;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result =
            send_export_request(worker_handle_for_request, worker_request, output_csv_path);
        let _ = sender.send(result);
    });

    match receiver.recv_timeout(WORKER_REQUEST_TIMEOUT) {
        Ok(result) => AnalysisJobEvent::ExportCompleted(result),
        Err(RecvTimeoutError::Timeout) => {
            let stderr = worker_stderr_snapshot(&worker_handle);
            shutdown_worker(&worker_handle);
            invalidate_worker_slot(&worker_handle.fingerprint);
            AnalysisJobEvent::ExportCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: format!(
                    "Python worker が {} 秒以内に CSV 保存へ応答しませんでした",
                    WORKER_REQUEST_TIMEOUT.as_secs()
                ),
            }))
        }
        Err(RecvTimeoutError::Disconnected) => {
            let stderr = worker_stderr_snapshot(&worker_handle);
            shutdown_worker(&worker_handle);
            invalidate_worker_slot(&worker_handle.fingerprint);
            AnalysisJobEvent::ExportCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: "Python worker の CSV 保存応答待機中にチャネルが切断されました"
                    .to_string(),
            }))
        }
    }
}

fn ensure_worker(runtime: &AnalysisRuntimeConfig) -> Result<WorkerHandle, String> {
    let fingerprint = WorkerRuntimeFingerprint::from_runtime(runtime);
    let slot = worker_slot();
    let mut guard = slot
        .lock()
        .map_err(|_| "worker スロットのロック取得に失敗しました".to_string())?;

    if let Some(handle) = guard.as_ref() {
        if handle.fingerprint == fingerprint && worker_is_running(handle) {
            return Ok(handle.clone());
        }
        shutdown_worker(handle);
        *guard = None;
    }

    let handle = spawn_worker(runtime, fingerprint)?;
    *guard = Some(handle.clone());
    Ok(handle)
}

fn spawn_worker(
    runtime: &AnalysisRuntimeConfig,
    fingerprint: WorkerRuntimeFingerprint,
) -> Result<WorkerHandle, String> {
    let mut command = Command::new(&runtime.python_command);
    command
        .current_dir(&runtime.project_root)
        .args(&runtime.python_args)
        .arg(&runtime.script_path)
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| format!("Python worker の起動に失敗しました: {error}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "Python worker stdin を取得できませんでした".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Python worker stdout を取得できませんでした".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Python worker stderr を取得できませんでした".to_string())?;

    let child = Arc::new(Mutex::new(child));
    let stderr_buffer = Arc::new(Mutex::new(String::new()));
    spawn_stderr_collector(stderr, stderr_buffer.clone());

    let handle = WorkerHandle {
        fingerprint,
        child,
        connection: Arc::new(Mutex::new(WorkerConnection {
            stdin,
            stdout: BufReader::new(stdout),
        })),
        stderr_buffer,
    };

    if let Err(message) = send_simple_worker_request(&handle, "health") {
        let stderr = worker_stderr_snapshot(&handle);
        shutdown_worker(&handle);
        return Err(attach_stderr_to_message(message, &stderr));
    }
    Ok(handle)
}

fn spawn_stderr_collector(stderr: ChildStderr, stderr_buffer: Arc<Mutex<String>>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut chunk = [0_u8; 4096];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(bytes_read) => {
                    let text = String::from_utf8_lossy(&chunk[..bytes_read]);
                    if let Ok(mut buffer) = stderr_buffer.lock() {
                        buffer.push_str(&text);
                        if buffer.len() > 32_768 {
                            let remove_len = buffer.len() - 32_768;
                            buffer.drain(..remove_len);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
}

fn spawn_output_collector(stdout: ChildStdout, stdout_buffer: Arc<Mutex<String>>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut chunk = [0_u8; 4096];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(bytes_read) => {
                    let text = String::from_utf8_lossy(&chunk[..bytes_read]);
                    if let Ok(mut buffer) = stdout_buffer.lock() {
                        buffer.push_str(&text);
                        if buffer.len() > 32_768 {
                            let remove_len = buffer.len() - 32_768;
                            buffer.drain(..remove_len);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
}

fn buffer_snapshot(buffer: &Arc<Mutex<String>>) -> String {
    buffer
        .lock()
        .map(|value| value.trim().to_string())
        .unwrap_or_default()
}

fn stderr_buffer_snapshot(stderr_buffer: &Arc<Mutex<String>>) -> String {
    buffer_snapshot(stderr_buffer)
}

fn worker_is_running(handle: &WorkerHandle) -> bool {
    let Ok(mut child) = handle.child.lock() else {
        return false;
    };
    match child.try_wait() {
        Ok(None) => true,
        Ok(Some(_)) => false,
        Err(_) => false,
    }
}

fn shutdown_worker(handle: &WorkerHandle) {
    if let Ok(mut child) = handle.child.lock() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn invalidate_worker_slot(fingerprint: &WorkerRuntimeFingerprint) {
    if let Ok(mut guard) = worker_slot().lock() {
        let should_clear = guard
            .as_ref()
            .map(|handle| &handle.fingerprint == fingerprint)
            .unwrap_or(false);
        if should_clear {
            *guard = None;
        }
    }
}

fn worker_stderr_snapshot(handle: &WorkerHandle) -> String {
    handle
        .stderr_buffer
        .lock()
        .map(|buffer| buffer.trim().to_string())
        .unwrap_or_default()
}

fn attach_stderr_to_message(message: String, stderr: &str) -> String {
    if stderr.trim().is_empty() {
        return message;
    }
    format!("{message}\n\nstderr:\n{stderr}")
}

fn send_simple_worker_request(
    handle: &WorkerHandle,
    request_type: &'static str,
) -> Result<(), String> {
    let request = WorkerSimpleRequest {
        request_id: format!("worker-{request_type}"),
        request_type,
    };
    let response = send_worker_request(handle, &request)?;
    if response.status == "ok" {
        return Ok(());
    }
    Err(build_worker_response_message(
        &response,
        &worker_stderr_snapshot(handle),
        "Python worker 制御要求が失敗しました".to_string(),
    ))
}

fn send_analyze_request(
    handle: WorkerHandle,
    request: WorkerAnalyzeRequest,
) -> Result<AnalysisJobSuccess, AnalysisJobFailure> {
    let response = match send_worker_request(&handle, &request) {
        Ok(response) => response,
        Err(message) => {
            let stderr = worker_stderr_snapshot(&handle);
            shutdown_worker(&handle);
            invalidate_worker_slot(&handle.fingerprint);
            return Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message,
            });
        }
    };

    let stderr = worker_stderr_snapshot(&handle);

    if response.request_id != request.request_id {
        shutdown_worker(&handle);
        invalidate_worker_slot(&handle.fingerprint);
        return Err(AnalysisJobFailure {
            meta: response.meta,
            stderr,
            message: format!(
                "Python worker 応答の requestId が一致しません: expected={}, actual={}",
                request.request_id, response.request_id
            ),
        });
    }

    let meta = response.meta.clone();
    if response.status == "succeeded" {
        let Some(meta) = meta else {
            return Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: "Python worker 成功応答に meta が含まれていません".to_string(),
            });
        };
        if meta.expected_record_count() != response.records.len() {
            let selected_count = meta.expected_record_count();
            let record_count = response.records.len();
            let analysis_unit = meta.analysis_unit;
            return Err(AnalysisJobFailure {
                meta: Some(meta),
                stderr,
                message: format!(
                    "返却件数が選択件数と一致しません: unit={:?}, meta={}, records={}",
                    analysis_unit, selected_count, record_count
                ),
            });
        }
        let analysis_unit = meta.analysis_unit;
        let records = response
            .records
            .into_iter()
            .enumerate()
            .map(|(idx, record)| record.into_analysis_record(idx + 1, analysis_unit))
            .collect();
        return Ok(AnalysisJobSuccess { meta, records });
    }

    Err(AnalysisJobFailure {
        meta,
        stderr,
        message: build_worker_response_message(
            &response,
            &worker_stderr_snapshot(&handle),
            "Python worker が分析要求に失敗しました".to_string(),
        ),
    })
}

fn send_export_request(
    handle: WorkerHandle,
    request: WorkerExportRequest,
    output_csv_path: PathBuf,
) -> Result<AnalysisExportSuccess, AnalysisJobFailure> {
    let response = match send_worker_request(&handle, &request) {
        Ok(response) => response,
        Err(message) => {
            let stderr = worker_stderr_snapshot(&handle);
            shutdown_worker(&handle);
            invalidate_worker_slot(&handle.fingerprint);
            return Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message,
            });
        }
    };

    let stderr = worker_stderr_snapshot(&handle);

    if response.request_id != request.request_id {
        shutdown_worker(&handle);
        invalidate_worker_slot(&handle.fingerprint);
        return Err(AnalysisJobFailure {
            meta: response.meta,
            stderr,
            message: format!(
                "Python worker export 応答の requestId が一致しません: expected={}, actual={}",
                request.request_id, response.request_id
            ),
        });
    }

    let meta = response.meta.clone();
    if response.status == "succeeded" {
        let Some(meta) = meta else {
            return Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: "Python worker export 成功応答に meta が含まれていません".to_string(),
            });
        };
        if !output_csv_path.is_file() {
            return Err(AnalysisJobFailure {
                meta: Some(meta),
                stderr,
                message: format!("保存先 CSV が見つかりません: {}", output_csv_path.display()),
            });
        }
        return Ok(AnalysisExportSuccess {
            meta,
            output_csv_path,
        });
    }

    Err(AnalysisJobFailure {
        meta,
        stderr,
        message: build_worker_response_message(
            &response,
            &worker_stderr_snapshot(&handle),
            "Python worker が CSV 保存要求に失敗しました".to_string(),
        ),
    })
}

fn send_worker_request<T>(handle: &WorkerHandle, payload: &T) -> Result<WorkerResponse, String>
where
    T: Serialize,
{
    let mut connection = handle
        .connection
        .lock()
        .map_err(|_| "worker 接続のロック取得に失敗しました".to_string())?;
    write_framed_json(&mut connection.stdin, payload)?;
    read_framed_json(&mut connection.stdout)
}

fn write_framed_json<T>(writer: &mut ChildStdin, payload: &T) -> Result<(), String>
where
    T: Serialize,
{
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|error| format!("worker 要求 JSON の直列化に失敗しました: {error}"))?;
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| "worker 要求 JSON が大きすぎます".to_string())?;
    writer
        .write_all(&payload_len.to_be_bytes())
        .map_err(|error| format!("worker 要求長の送信に失敗しました: {error}"))?;
    writer
        .write_all(&payload_bytes)
        .map_err(|error| format!("worker 要求本体の送信に失敗しました: {error}"))?;
    writer
        .flush()
        .map_err(|error| format!("worker 要求 flush に失敗しました: {error}"))
}

fn read_framed_json(reader: &mut BufReader<ChildStdout>) -> Result<WorkerResponse, String> {
    let mut length_bytes = [0_u8; 4];
    reader
        .read_exact(&mut length_bytes)
        .map_err(|error| format!("worker 応答長の読込に失敗しました: {error}"))?;
    let payload_len = u32::from_be_bytes(length_bytes) as usize;
    let mut payload_bytes = vec![0_u8; payload_len];
    reader
        .read_exact(&mut payload_bytes)
        .map_err(|error| format!("worker 応答本体の読込に失敗しました: {error}"))?;
    serde_json::from_slice(&payload_bytes)
        .map_err(|error| format!("worker 応答 JSON の解析に失敗しました: {error}"))
}

fn build_worker_response_message(
    response: &WorkerResponse,
    stderr: &str,
    fallback_message: String,
) -> String {
    if let Some(meta) = response.meta.as_ref() {
        if !meta.error_summary.trim().is_empty() {
            return meta.error_summary.clone();
        }
    }
    if !response.message.trim().is_empty() {
        return response.message.clone();
    }
    if !stderr.trim().is_empty() {
        return stderr.to_string();
    }
    fallback_message
}

/// meta.json / 仮想 JSON 応答のテスト用（非 test ビルドではコンパイルしない）。
#[cfg(test)]
mod json_response_tests {
    use super::{AnalysisJsonRecord, AnalysisMeta};
    use serde::Deserialize;
    use std::fs;
    use std::path::Path;

    #[derive(Clone, Debug, Deserialize, PartialEq)]
    pub(super) struct AnalysisJsonResponse {
        pub(super) meta: AnalysisMeta,
        #[serde(default)]
        pub(super) records: Vec<AnalysisJsonRecord>,
    }

    pub(super) fn read_meta_json(path: &Path) -> Result<AnalysisMeta, String> {
        let text = fs::read_to_string(path).map_err(|error| {
            format!("meta.json を読めませんでした ({}): {error}", path.display())
        })?;
        serde_json::from_str(&text).map_err(|error| {
            format!(
                "meta.json の解析に失敗しました ({}): {error}",
                path.display()
            )
        })
    }

    pub(super) fn read_json_response(text: &str) -> Result<AnalysisJsonResponse, String> {
        serde_json::from_str(text).map_err(|error| format!("JSON 応答の解析に失敗しました: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::json_response_tests::{read_json_response, read_meta_json};
    use super::{
        build_builder_command, build_default_builder_report_path, AnalysisDbBuildRequest,
        AnalysisWarningMessage, BuilderRuntimeConfig,
    };
    use std::ffi::OsString;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_meta_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        path.push(format!("csv-viewer-{name}-{nanos}.json"));
        path
    }

    #[test]
    fn read_meta_json_accepts_structured_warning_messages() {
        let path = temp_meta_path("structured-warning");
        let payload = r#"{
  "jobId": "job-1",
  "status": "succeeded",
  "startedAt": "2026-03-13T00:00:00Z",
  "finishedAt": "2026-03-13T00:00:01Z",
  "durationSeconds": 1.0,
  "dbPath": "/tmp/db.sqlite3",
  "filterConfigPath": "/tmp/filter.json",
  "outputCsvPath": "/tmp/result.csv",
  "analysisUnit": "paragraph",
  "targetParagraphCount": 2,
  "selectedParagraphCount": 1,
  "warningMessages": [
    {
      "code": "distance_match_fallback",
      "message": "fallback applied",
      "severity": "warning",
      "scope": "condition",
      "conditionId": "pair",
      "fieldName": "max_token_distance",
      "unitId": 10,
      "queryName": "analysis_tokens",
      "dbPath": "/tmp/db.sqlite3",
      "requestedMode": "auto-approx",
      "usedMode": "approx",
      "combinationCount": 10100,
      "combinationCap": 10000,
      "safetyLimit": 1000000
    }
  ],
  "errorSummary": ""
}"#;
        fs::write(&path, payload).unwrap();

        let meta = read_meta_json(&path).unwrap();

        assert_eq!(meta.warning_messages.len(), 1);
        assert_eq!(
            meta.warning_messages[0],
            AnalysisWarningMessage {
                code: "distance_match_fallback".to_string(),
                message: "fallback applied".to_string(),
                severity: Some("warning".to_string()),
                scope: Some("condition".to_string()),
                condition_id: Some("pair".to_string()),
                field_name: Some("max_token_distance".to_string()),
                unit_id: Some(10),
                query_name: Some("analysis_tokens".to_string()),
                requested_mode: Some("auto-approx".to_string()),
                used_mode: Some("approx".to_string()),
                combination_count: Some(10100),
                combination_cap: Some(10000),
                safety_limit: Some(1000000),
                db_path: Some("/tmp/db.sqlite3".to_string()),
            }
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_meta_json_accepts_legacy_string_warning_messages() {
        let path = temp_meta_path("legacy-warning");
        let payload = r#"{
  "jobId": "job-2",
  "status": "succeeded",
  "startedAt": "2026-03-13T00:00:00Z",
  "finishedAt": "2026-03-13T00:00:01Z",
  "durationSeconds": 1.0,
  "dbPath": "/tmp/db.sqlite3",
  "filterConfigPath": "/tmp/filter.json",
  "outputCsvPath": "/tmp/result.csv",
  "analysisUnit": "paragraph",
  "targetParagraphCount": 2,
  "selectedParagraphCount": 1,
  "warningMessages": ["legacy warning"],
  "errorSummary": ""
}"#;
        fs::write(&path, payload).unwrap();

        let meta = read_meta_json(&path).unwrap();

        assert_eq!(meta.warning_messages.len(), 1);
        assert_eq!(meta.warning_messages[0].message, "legacy warning");
        assert!(meta.warning_messages[0].code.is_empty());
        assert_eq!(meta.warning_messages[0].severity, None);
        assert_eq!(meta.warning_messages[0].scope, None);
        assert_eq!(meta.warning_messages[0].field_name, None);
        assert_eq!(meta.warning_messages[0].query_name, None);
        assert_eq!(meta.warning_messages[0].used_mode, None);
        assert_eq!(meta.warning_messages[0].db_path, None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_meta_json_sets_new_warning_fields_to_none_for_older_structured_payloads() {
        let path = temp_meta_path("older-structured-warning");
        let payload = r#"{
  "jobId": "job-3",
  "status": "succeeded",
  "startedAt": "2026-03-13T00:00:00Z",
  "finishedAt": "2026-03-13T00:00:01Z",
  "durationSeconds": 1.0,
  "dbPath": "/tmp/db.sqlite3",
  "filterConfigPath": "/tmp/filter.json",
  "outputCsvPath": "/tmp/result.csv",
  "analysisUnit": "paragraph",
  "targetParagraphCount": 2,
  "selectedParagraphCount": 1,
  "warningMessages": [
    {
      "code": "distance_match_fallback",
      "message": "fallback applied",
      "conditionId": "pair",
      "requestedMode": "auto-approx",
      "usedMode": "approx"
    }
  ],
  "errorSummary": ""
}"#;
        fs::write(&path, payload).unwrap();

        let meta = read_meta_json(&path).unwrap();

        assert_eq!(meta.warning_messages.len(), 1);
        assert_eq!(meta.warning_messages[0].severity, None);
        assert_eq!(meta.warning_messages[0].scope, None);
        assert_eq!(meta.warning_messages[0].field_name, None);
        assert_eq!(meta.warning_messages[0].query_name, None);
        assert_eq!(meta.warning_messages[0].db_path, None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_json_response_accepts_gui_records_payload() {
        let payload = r#"{
  "meta": {
    "jobId": "job-4",
    "status": "succeeded",
    "startedAt": "2026-03-16T00:00:00Z",
    "finishedAt": "2026-03-16T00:00:01Z",
    "durationSeconds": 1.0,
    "dbPath": "/tmp/db.sqlite3",
    "filterConfigPath": "/tmp/filter.json",
    "outputCsvPath": "/tmp/result.csv",
    "analysisUnit": "paragraph",
    "targetParagraphCount": 1,
    "selectedParagraphCount": 1,
    "warningMessages": [],
    "errorSummary": ""
  },
  "records": [
    {
      "paragraph_id": "1",
      "document_id": "2",
      "category1": "札幌市",
      "category2": "条例",
      "sentence_count": "3",
      "paragraph_text": "本文",
      "paragraph_text_tagged": "<hit>本文</hit>",
      "matched_condition_ids_text": "",
      "matched_categories_text": "抑制区域",
      "match_group_ids_text": "",
      "match_group_count": "1",
      "annotated_token_count": "2",
      "manual_annotation_count": "1",
      "manual_annotation_pairs_text": "zoning:zone_strength=suppression",
      "manual_annotation_namespaces_text": "zoning"
    }
  ]
}"#;

        let response = read_json_response(payload).unwrap();

        assert_eq!(response.meta.analysis_unit, super::AnalysisUnit::Paragraph);
        assert_eq!(response.meta.selected_paragraph_count, 1);
        assert_eq!(response.records.len(), 1);
        let first_record = response
            .records
            .into_iter()
            .next()
            .unwrap()
            .into_analysis_record(1, response.meta.analysis_unit);
        assert_eq!(first_record.row_no, 1);
        assert_eq!(first_record.paragraph_id, "1");
        assert_eq!(first_record.category1, "札幌市");
        assert_eq!(first_record.category2, "条例");
        assert_eq!(first_record.annotated_token_count, "2");
        assert_eq!(first_record.manual_annotation_count, "1");
    }

    #[test]
    fn read_meta_json_accepts_sentence_analysis_payload() {
        let path = temp_meta_path("sentence-meta");
        let payload = r#"{
  "jobId": "job-sentence",
  "status": "succeeded",
  "startedAt": "2026-03-16T00:00:00Z",
  "finishedAt": "2026-03-16T00:00:01Z",
  "durationSeconds": 1.0,
  "dbPath": "/tmp/db.sqlite3",
  "filterConfigPath": "/tmp/filter.json",
  "outputCsvPath": "/tmp/result-sentences.csv",
  "analysisUnit": "sentence",
  "targetParagraphCount": 1,
  "selectedParagraphCount": 1,
  "selectedSentenceCount": 2,
  "warningMessages": [],
  "errorSummary": ""
}"#;
        fs::write(&path, payload).unwrap();

        let meta = read_meta_json(&path).unwrap();

        assert_eq!(meta.analysis_unit, super::AnalysisUnit::Sentence);
        assert_eq!(meta.selected_paragraph_count, 1);
        assert_eq!(meta.selected_sentence_count, 2);
        assert_eq!(meta.selected_unit_count(), 2);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn build_default_builder_report_path_appends_report_suffix() {
        let report_path =
            build_default_builder_report_path(PathBuf::from("/tmp/sample.db").as_path());
        assert_eq!(report_path, PathBuf::from("/tmp/sample.db.report.json"));
    }

    #[test]
    fn build_builder_command_uses_explicit_report_path_and_optional_flags() {
        let request = AnalysisDbBuildRequest {
            runtime: BuilderRuntimeConfig {
                python_command: OsString::from("python"),
                python_args: vec![OsString::from("-X"), OsString::from("utf8")],
                python_label: "python".to_string(),
                project_root: PathBuf::from("/tmp/project"),
                builder_script_path: PathBuf::from("/tmp/project/docs/build_ordinance_analysis_db.py"),
            },
            input_dir: PathBuf::from("/tmp/in"),
            analysis_db_path: PathBuf::from("/tmp/out.db"),
            report_path: PathBuf::from("/tmp/out.db.report.json"),
            skip_tokenize: false,
            sudachi_dict: "core".to_string(),
            split_mode: "C".to_string(),
            split_inside_parentheses: true,
            merge_table_lines: true,
            purge: false,
            fresh_db: true,
            limit: Some(25),
            note: "note".to_string(),
        };

        let command = build_builder_command(&request).unwrap();
        let args: Vec<String> = command
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            command.get_program().to_string_lossy(),
            std::borrow::Cow::Borrowed("python")
        );
        assert!(args.contains(&"/tmp/project/docs/build_ordinance_analysis_db.py".to_string()));
        assert!(args.contains(&"--report-path".to_string()));
        assert!(args.contains(&"/tmp/out.db.report.json".to_string()));
        assert!(args.contains(&"--sudachi-dict".to_string()));
        assert!(args.contains(&"core".to_string()));
        assert!(args.contains(&"--split-mode".to_string()));
        assert!(args.contains(&"C".to_string()));
        assert!(args.contains(&"--split-inside-parentheses".to_string()));
        assert!(args.contains(&"--merge-table-lines".to_string()));
        assert!(args.contains(&"--fresh-db".to_string()));
        assert!(args.contains(&"--limit".to_string()));
        assert!(args.contains(&"25".to_string()));
        assert!(args.contains(&"--note".to_string()));
        assert!(args.contains(&"note".to_string()));
    }

    #[test]
    fn read_json_response_accepts_sentence_records_payload() {
        let payload = r#"{
  "meta": {
    "jobId": "job-5",
    "status": "succeeded",
    "startedAt": "2026-03-16T00:00:00Z",
    "finishedAt": "2026-03-16T00:00:01Z",
    "durationSeconds": 1.0,
    "dbPath": "/tmp/db.sqlite3",
    "filterConfigPath": "/tmp/filter.json",
    "outputCsvPath": "/tmp/result-sentences.csv",
    "analysisUnit": "sentence",
    "targetParagraphCount": 1,
    "selectedParagraphCount": 1,
    "selectedSentenceCount": 1,
    "warningMessages": [],
    "errorSummary": ""
  },
  "records": [
    {
      "sentence_id": "11",
      "paragraph_id": "1",
      "document_id": "2",
      "category1": "札幌市",
      "category2": "条例",
      "sentence_no_in_paragraph": "2",
      "sentence_no_in_document": "5",
      "sentence_text": "文本文",
      "sentence_text_tagged": "<hit>文</hit>本文",
      "matched_condition_ids_text": "",
      "matched_categories_text": "抑制区域",
      "match_group_ids_text": "",
      "match_group_count": "1",
      "annotated_token_count": "2"
    }
  ]
}"#;

        let response = read_json_response(payload).unwrap();

        assert_eq!(response.meta.analysis_unit, super::AnalysisUnit::Sentence);
        assert_eq!(response.meta.selected_unit_count(), 1);
        let first_record = response
            .records
            .into_iter()
            .next()
            .unwrap()
            .into_analysis_record(1, response.meta.analysis_unit);
        assert_eq!(first_record.analysis_unit, super::AnalysisUnit::Sentence);
        assert_eq!(first_record.sentence_id, "11");
        assert_eq!(first_record.sentence_no_in_paragraph, "2");
        assert_eq!(first_record.sentence_text, "文本文");
        assert_eq!(first_record.primary_text_tagged(), "<hit>文</hit>本文");
    }
}

fn run_build_job(
    job_id: String,
    request: AnalysisDbBuildRequest,
    child: Arc<Mutex<Child>>,
    stdout_buffer: Arc<Mutex<String>>,
    stderr_buffer: Arc<Mutex<String>>,
    cancelled: Arc<AtomicBool>,
) -> AnalysisJobEvent {
    let exit_status = loop {
        let status = match child.lock() {
            Ok(mut child_guard) => child_guard.try_wait(),
            Err(_) => {
                return AnalysisJobEvent::BuildCompleted(Err(AnalysisJobFailure {
                    meta: None,
                    stderr: stderr_buffer_snapshot(&stderr_buffer),
                    message: "builder プロセス状態の取得に失敗しました".to_string(),
                }));
            }
        };
        match status {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(error) => {
                return AnalysisJobEvent::BuildCompleted(Err(AnalysisJobFailure {
                    meta: None,
                    stderr: stderr_buffer_snapshot(&stderr_buffer),
                    message: format!("builder プロセス終了待機に失敗しました: {error}"),
                }));
            }
        }
    };

    let stdout = buffer_snapshot(&stdout_buffer);
    let stderr = stderr_buffer_snapshot(&stderr_buffer);
    if cancelled.load(Ordering::SeqCst) {
        return AnalysisJobEvent::BuildCompleted(Err(AnalysisJobFailure {
            meta: None,
            stderr,
            message: "DB 生成ジョブを中止しました".to_string(),
        }));
    }

    if exit_status.success() {
        if !request.analysis_db_path.is_file() {
            return AnalysisJobEvent::BuildCompleted(Err(AnalysisJobFailure {
                meta: None,
                stderr,
                message: format!(
                    "生成後の analysis DB が見つかりません: {}",
                    request.analysis_db_path.display()
                ),
            }));
        }
        return AnalysisJobEvent::BuildCompleted(Ok(AnalysisDbBuildSuccess {
            analysis_db_path: request.analysis_db_path,
            report_path: request.report_path,
            stdout,
        }));
    }

    AnalysisJobEvent::BuildCompleted(Err(AnalysisJobFailure {
        meta: None,
        stderr,
        message: format!("builder プロセスが失敗しました: {job_id}"),
    }))
}

fn build_job_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("job-{millis}")
}

pub(crate) fn build_default_builder_report_path(analysis_db_path: &Path) -> PathBuf {
    let resolved_analysis_db = analysis_db_path.to_path_buf();
    let file_name = resolved_analysis_db
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "analysis.db".to_string());
    resolved_analysis_db.with_file_name(format!("{file_name}{BUILD_REPORT_PATH_SUFFIX}"))
}

fn build_builder_command(request: &AnalysisDbBuildRequest) -> Result<Command, String> {
    let mut command = Command::new(&request.runtime.python_command);
    command.current_dir(&request.runtime.project_root);
    command.args(&request.runtime.python_args);
    command.arg(&request.runtime.builder_script_path);
    command.arg("--input-dir");
    command.arg(&request.input_dir);
    command.arg("--analysis-db");
    command.arg(&request.analysis_db_path);
    command.arg("--report-path");
    command.arg(&request.report_path);
    if request.skip_tokenize {
        command.arg("--skip-tokenize");
    } else {
        command.arg("--sudachi-dict");
        command.arg(OsString::from(request.sudachi_dict.as_str()));
        command.arg("--split-mode");
        command.arg(OsString::from(request.split_mode.as_str()));
    }
    if request.split_inside_parentheses {
        command.arg("--split-inside-parentheses");
    }
    if request.merge_table_lines {
        command.arg("--merge-table-lines");
    }
    if request.purge {
        command.arg("--purge");
    }
    if request.fresh_db {
        command.arg("--fresh-db");
    }
    if let Some(limit) = request.limit {
        command.arg("--limit");
        command.arg(OsString::from(limit.to_string()));
    }
    if !request.note.trim().is_empty() {
        command.arg("--note");
        command.arg(OsString::from(request.note.as_str()));
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.stdin(Stdio::null());
    Ok(command)
}

pub(crate) fn resolve_filter_config_path(
    overrides: &AnalysisRuntimeOverrides,
) -> Result<PathBuf, String> {
    if let Some(path) = overrides.filter_config_path.as_ref() {
        if path.is_file() {
            return Ok(path.clone());
        }

        return Err(format!("条件 JSON が見つかりません: {}", path.display()));
    }

    resolve_project_file(DEFAULT_FILTER_CONFIG_RELATIVE_PATH)
        .ok_or_else(|| format!("条件 JSON が見つかりません: {DEFAULT_FILTER_CONFIG_RELATIVE_PATH}"))
}

pub(crate) fn resolve_annotation_csv_path(
    overrides: &AnalysisRuntimeOverrides,
) -> Result<PathBuf, String> {
    if let Some(path) = overrides.annotation_csv_path.as_ref() {
        return absolutize_path(path);
    }

    let script_path = resolve_project_file(DEFAULT_SCRIPT_RELATIVE_PATH)
        .ok_or_else(|| format!("分析スクリプトが見つかりません: {DEFAULT_SCRIPT_RELATIVE_PATH}"))?;
    let project_root = script_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(project_root.join(DEFAULT_ANNOTATION_CSV_RELATIVE_PATH))
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

fn absolutize_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    env::current_dir()
        .map(|current_dir| current_dir.join(path))
        .map_err(|error| format!("カレントディレクトリを解決できません: {error}"))
}

pub(crate) fn resolve_forbidden_dirs(project_root: &Path) -> Vec<PathBuf> {
    vec![
        project_root.join("asset").join("ocr_manual"),
        project_root.join("asset").join("texts_2nd").join("manual"),
    ]
}

pub(crate) fn classify_forbidden_input_relation(input_dir: &Path, forbidden_dir: &Path) -> Option<&'static str> {
    let input_canon = input_dir.canonicalize().ok()?;
    let forbidden_canon = forbidden_dir.canonicalize().ok()?;

    if input_canon == forbidden_canon {
        return Some("same");
    }
    if input_canon.starts_with(&forbidden_canon) {
        return Some("child");
    }
    if forbidden_canon.starts_with(&input_canon) {
        return Some("parent");
    }
    None
}

pub(crate) fn check_forbidden_input_dir(input_dir: &Path, forbidden_dirs: &[PathBuf]) -> Option<String> {
    for forbidden_dir in forbidden_dirs {
        if let Some(relation) = classify_forbidden_input_relation(input_dir, forbidden_dir) {
            let relation_label = match relation {
                "same" => "本体",
                "child" => "子ディレクトリ",
                "parent" => "親ディレクトリ",
                _ => "不明な関係",
            };
            return Some(format!(
                "OCR 作業領域の{}として選択できません: {} (禁止: {})",
                relation_label,
                input_dir.display(),
                forbidden_dir.display(),
            ));
        }
    }
    None
}
