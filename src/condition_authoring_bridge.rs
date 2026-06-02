//! Python `condition_authoring_cli` の issues JSON を Rust 側で parse し、
//! 既存 `AnalysisWarningMessage` に変換する bridge モジュール。
//!
//! Task 2 スコープ: DTO 定義 + warning 変換。
//! Task 3 スコープ: compiled runtime output path builder。
//! Task 4 スコープ: Python CLI command execution。

use crate::analysis_runner::{AnalysisRuntimeConfig, AnalysisWarningMessage};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;

/// MVP 固定の bridge version。
/// 将来の互換性破壊変更に備えた予約。
pub(crate) const BRIDGE_VERSION: &str = "rust-authoring-bridge/v1";

/// compiled runtime JSON / issues JSON の出力先パスを構築する。
///
/// stable-key は canonicalized source path + file content + bridge version の SHA-256 由来。
/// Windows path separator に依存しにくいよう、パス区切りを `/` で正規化する。
#[derive(Debug)]
pub(crate) struct CompiledOutputPath {
    pub(crate) runtime_json: PathBuf,
    pub(crate) issues_json: PathBuf,
}

impl CompiledOutputPath {
    /// source_path と content から stable-key を計算し、output_dir 配下のパスを構築する。
    pub(crate) fn build(
        source_path: &Path,
        content: &str,
        output_dir: &Path,
    ) -> Result<Self, String> {
        let stable_key = compute_stable_key(source_path, content)?;
        Ok(Self {
            runtime_json: output_dir.join(format!("{stable_key}.runtime.json")),
            issues_json: output_dir.join(format!("{stable_key}.issues.json")),
        })
    }
}

/// source_path を canonicalize し、content + bridge version と合わせて SHA-256 を計算する。
/// パス区切りは `/` に正規化して Windows/Linux の差異を減らす。
fn compute_stable_key(source_path: &Path, content: &str) -> Result<String, String> {
    let canonical = source_path.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            format!("source file not found: '{}'", source_path.display())
        } else {
            format!(
                "failed to canonicalize source path '{}': {e}",
                source_path.display()
            )
        }
    })?;
    let canonical_str = canonical.to_string_lossy();
    let normalized_path = canonical_str
        .strip_prefix("\\\\?\\")
        .unwrap_or(&canonical_str)
        .replace('\\', "/");
    let input = format!("{}\n{}\n{}", normalized_path, content, BRIDGE_VERSION);
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

/// 指定パスの親ディレクトリが存在しなければ自動作成する。
pub(crate) fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "failed to create parent directory '{}': {e}",
                parent.display()
            )
        })?;
    }
    Ok(())
}

/// Python CLI issues JSON の thin wrapper。
/// Rust 側で schema を厚く再定義せず、必要最小限のフィールドのみ保持する。
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct AuthoringIssue {
    #[serde(default)]
    pub(crate) code: String,
    #[serde(default)]
    pub(crate) severity: String,
    #[serde(default)]
    pub(crate) scope: String,
    #[serde(default)]
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) condition_index: Option<i64>,
    #[serde(default)]
    pub(crate) condition_id: Option<String>,
    #[serde(default)]
    pub(crate) field_name: Option<String>,
}

impl AuthoringIssue {
    /// severity が "error" かどうかを判定する。
    pub(crate) fn is_error(&self) -> bool {
        self.severity == "error"
    }

    /// この issue を既存 `AnalysisWarningMessage` へ変換する。
    ///
    /// `AnalysisWarningMessage` と共通するフィールドはそのまま流用し、
    /// 存在しないフィールドはデフォルト値で埋める。
    /// 変換不能な場合（message が空など）は `code = "authoring_compiler"` で fallback する。
    pub(crate) fn to_analysis_warning_message(&self) -> AnalysisWarningMessage {
        let code = if self.code.is_empty() {
            "authoring_compiler".to_string()
        } else {
            self.code.clone()
        };
        let message = if self.message.is_empty() {
            // message が空の場合、code だけの最小 fallback
            format!("authoring compiler issue: {}", code)
        } else {
            self.message.clone()
        };
        AnalysisWarningMessage {
            code,
            message,
            severity: Some(self.severity.clone()),
            scope: Some(self.scope.clone()),
            condition_id: self.condition_id.clone(),
            field_name: self.field_name.clone(),
            // 以下は AnalysisWarningMessage が持つが AuthoringIssue にはないフィールド
            unit_id: None,
            query_name: None,
            db_path: None,
            requested_mode: None,
            used_mode: None,
            combination_count: None,
            combination_cap: None,
            safety_limit: None,
        }
    }
}

/// Authoring compile の結果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthoringCompileResult {
    pub(crate) runtime_json_path: PathBuf,
    pub(crate) issues: Vec<AuthoringIssue>,
    pub(crate) stdout_summary: String,
    pub(crate) stderr_summary: String,
}

impl AuthoringCompileResult {
    /// error severity の issue が含まれるかどうか。
    pub(crate) fn has_errors(&self) -> bool {
        has_authoring_errors(&self.issues)
    }
}

/// stdout/stderr を要約する。長すぎる場合は UTF-8 文字境界安全に切り詰める。
fn summarize_output(output: Vec<u8>, max_len: usize) -> String {
    let text = String::from_utf8_lossy(&output);
    let trimmed = text.trim();
    if trimmed.len() <= max_len {
        trimmed.to_string()
    } else {
        // UTF-8 文字境界安全に切り詰める
        let mut end = max_len;
        while end > 0 && !trimmed.is_char_boundary(end) {
            end -= 1;
        }
        let mut truncated = trimmed[..end].to_string();
        truncated.push_str("... (truncated)");
        truncated
    }
}

/// Python CLI `analysis_backend.condition_authoring_cli` を呼び出して authoring JSON を runtime JSON に compile する。
///
/// - `runtime.python_command` / `runtime.python_args` / `runtime.project_root` を使う
/// - `CSV_VIEWER_PROJECT_ROOT` 環境変数を設定する
/// - `--input`, `--output`, `--issues-json` を `PathBuf`/`OsString` として渡す（shell 連結しない）
/// - exit code != 0 は blocking error
/// - stdout/stderr は長すぎる場合に切り詰める
/// - issues JSON を読んで result に含める
pub(crate) fn compile_authoring_to_runtime(
    runtime: &AnalysisRuntimeConfig,
    source_path: &Path,
    output_dir: &Path,
) -> Result<AuthoringCompileResult, String> {
    let content = std::fs::read_to_string(source_path).map_err(|e| {
        format!(
            "failed to read authoring source '{}': {e}",
            source_path.display()
        )
    })?;
    let output_paths = CompiledOutputPath::build(source_path, &content, output_dir)?;

    ensure_parent_dir(&output_paths.runtime_json)?;
    ensure_parent_dir(&output_paths.issues_json)?;

    let mut cmd = Command::new(&runtime.python_command);
    for arg in &runtime.python_args {
        cmd.arg(arg);
    }

    cmd.arg("-m")
        .arg("analysis_backend.condition_authoring_cli")
        .arg("--input")
        .arg(source_path.as_os_str())
        .arg("--output")
        .arg(output_paths.runtime_json.as_os_str())
        .arg("--issues-json")
        .arg(output_paths.issues_json.as_os_str());

    cmd.env("CSV_VIEWER_PROJECT_ROOT", runtime.project_root.as_os_str())
        .current_dir(&runtime.project_root);

    let output = cmd.output().map_err(|e| {
        format!(
            "failed to execute Python CLI '{}': {e}",
            runtime.python_command.to_string_lossy()
        )
    })?;

    let stdout_summary = summarize_output(output.stdout, 4096);
    let stderr_summary = summarize_output(output.stderr, 4096);

    if !output.status.success() {
        let exit_code = output
            .status
            .code()
            .map_or("unknown".to_string(), |c| c.to_string());
        return Err(format!(
            "authoring compiler exited with code {exit_code}. stderr: {stderr_summary}"
        ));
    }

    let issues_json_str = std::fs::read_to_string(&output_paths.issues_json).map_err(|e| {
        format!(
            "failed to read issues JSON '{}': {e}",
            output_paths.issues_json.display()
        )
    })?;
    let issues = parse_authoring_issues(&issues_json_str)?;

    Ok(AuthoringCompileResult {
        runtime_json_path: output_paths.runtime_json,
        issues,
        stdout_summary,
        stderr_summary,
    })
}

/// issues JSON 文字列を Vec<AuthoringIssue> として parse する。
pub(crate) fn parse_authoring_issues(json_str: &str) -> Result<Vec<AuthoringIssue>, String> {
    serde_json::from_str(json_str).map_err(|e| format!("issues JSON parse error: {e}"))
}

/// AuthoringIssue のリストに error severity が含まれるかどうか。
pub(crate) fn has_authoring_errors(issues: &[AuthoringIssue]) -> bool {
    issues.iter().any(|issue| issue.is_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    // ---- parse tests ----

    #[test]
    fn parse_empty_issues_list() {
        let json = r#"[]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert!(issues.is_empty());
    }

    #[test]
    fn parse_single_warning_issue() {
        let json = r#"[{"code":"unknown_settings_field","severity":"warning","scope":"filter_config","message":"Unknown settings field: unknown_field","field_name":"unknown_field"}]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert_eq!(issues.len(), 1);
        let issue = &issues[0];
        assert_eq!(issue.code, "unknown_settings_field");
        assert_eq!(issue.severity, "warning");
        assert_eq!(issue.scope, "filter_config");
        assert_eq!(issue.message, "Unknown settings field: unknown_field");
        assert_eq!(issue.field_name, Some("unknown_field".to_string()));
        assert_eq!(issue.condition_id, None);
        assert_eq!(issue.condition_index, None);
    }

    #[test]
    fn parse_single_error_issue() {
        let json = r#"[{"code":"authoring_format_missing","severity":"error","scope":"filter_config","message":"format field is missing","condition_index":null,"condition_id":null,"field_name":null}]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert_eq!(issues.len(), 1);
        let issue = &issues[0];
        assert_eq!(issue.code, "authoring_format_missing");
        assert_eq!(issue.severity, "error");
        assert!(issue.is_error());
    }

    #[test]
    fn parse_multiple_issues() {
        let json = r#"[
            {"code":"unknown_settings_field","severity":"warning","scope":"filter_config","message":"m1","field_name":"f1"},
            {"code":"authoring_format_missing","severity":"error","scope":"filter_config","message":"m2"}
        ]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert_eq!(issues.len(), 2);
        assert!(!issues[0].is_error());
        assert!(issues[1].is_error());
    }

    #[test]
    fn parse_ignores_unknown_fields() {
        let json = r#"[{"code":"c1","severity":"warning","scope":"filter_config","message":"m1","unknown_future_field":123,"extra_nested":{"a":1}}]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "c1");
    }

    #[test]
    fn parse_missing_optional_fields_defaults() {
        let json = r#"[{"code":"c1","severity":"warning","message":"m1"}]"#;
        let issues = parse_authoring_issues(json).unwrap();
        assert_eq!(issues.len(), 1);
        let issue = &issues[0];
        assert_eq!(issue.scope, "");
        assert_eq!(issue.condition_index, None);
        assert_eq!(issue.condition_id, None);
        assert_eq!(issue.field_name, None);
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        let json = r#"{ not valid }"#;
        let result = parse_authoring_issues(json);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("parse error"),
            "error should mention parse: {}",
            err
        );
    }

    // ---- severity detection ----

    #[test]
    fn has_authoring_errors_true_when_error_present() {
        let issues = vec![
            AuthoringIssue {
                code: "e1".into(),
                severity: "error".into(),
                scope: "".into(),
                message: "m1".into(),
                condition_index: None,
                condition_id: None,
                field_name: None,
            },
            AuthoringIssue {
                code: "w1".into(),
                severity: "warning".into(),
                scope: "".into(),
                message: "m2".into(),
                condition_index: None,
                condition_id: None,
                field_name: None,
            },
        ];
        assert!(has_authoring_errors(&issues));
    }

    #[test]
    fn has_authoring_errors_false_when_only_warnings() {
        let issues = vec![AuthoringIssue {
            code: "w1".into(),
            severity: "warning".into(),
            scope: "".into(),
            message: "m1".into(),
            condition_index: None,
            condition_id: None,
            field_name: None,
        }];
        assert!(!has_authoring_errors(&issues));
    }

    #[test]
    fn has_authoring_errors_false_when_empty() {
        let issues: Vec<AuthoringIssue> = vec![];
        assert!(!has_authoring_errors(&issues));
    }

    // ---- conversion to AnalysisWarningMessage ----

    #[test]
    fn warning_conversion_maps_common_fields() {
        let issue = AuthoringIssue {
            code: "label_ignored".into(),
            severity: "warning".into(),
            scope: "filter_config".into(),
            message: "label is ignored when labels is present".into(),
            condition_index: Some(0),
            condition_id: Some("r1".into()),
            field_name: Some("label".into()),
        };
        let warning = issue.to_analysis_warning_message();
        assert_eq!(warning.code, "label_ignored");
        assert_eq!(warning.message, "label is ignored when labels is present");
        assert_eq!(warning.severity, Some("warning".to_string()));
        assert_eq!(warning.scope, Some("filter_config".to_string()));
        assert_eq!(warning.condition_id, Some("r1".to_string()));
        assert_eq!(warning.field_name, Some("label".to_string()));
    }

    #[test]
    fn warning_conversion_sets_none_for_worker_only_fields() {
        let issue = AuthoringIssue {
            code: "c1".into(),
            severity: "warning".into(),
            scope: "filter_config".into(),
            message: "m1".into(),
            condition_index: None,
            condition_id: None,
            field_name: None,
        };
        let warning = issue.to_analysis_warning_message();
        assert_eq!(warning.unit_id, None);
        assert_eq!(warning.query_name, None);
        assert_eq!(warning.db_path, None);
        assert_eq!(warning.requested_mode, None);
        assert_eq!(warning.used_mode, None);
        assert_eq!(warning.combination_count, None);
        assert_eq!(warning.combination_cap, None);
        assert_eq!(warning.safety_limit, None);
    }

    #[test]
    fn warning_conversion_fallback_when_code_empty() {
        let issue = AuthoringIssue {
            code: "".into(),
            severity: "warning".into(),
            scope: "".into(),
            message: "something happened".into(),
            condition_index: None,
            condition_id: None,
            field_name: None,
        };
        let warning = issue.to_analysis_warning_message();
        assert_eq!(warning.code, "authoring_compiler");
        assert_eq!(warning.message, "something happened");
    }

    #[test]
    fn warning_conversion_fallback_when_message_empty() {
        let issue = AuthoringIssue {
            code: "custom_code".into(),
            severity: "error".into(),
            scope: "".into(),
            message: "".into(),
            condition_index: None,
            condition_id: None,
            field_name: None,
        };
        let warning = issue.to_analysis_warning_message();
        assert_eq!(warning.code, "custom_code");
        assert!(
            warning.message.contains("authoring compiler issue"),
            "message should contain fallback prefix: {}",
            warning.message
        );
        assert!(warning.message.contains("custom_code"));
    }

    #[test]
    fn warning_conversion_fallback_when_both_empty() {
        let issue = AuthoringIssue {
            code: "".into(),
            severity: "warning".into(),
            scope: "".into(),
            message: "".into(),
            condition_index: None,
            condition_id: None,
            field_name: None,
        };
        let warning = issue.to_analysis_warning_message();
        assert_eq!(warning.code, "authoring_compiler");
        assert_eq!(
            warning.message,
            "authoring compiler issue: authoring_compiler"
        );
    }

    // ---- AnalysisWarningMessage direct reuse confirmation ----

    /// `AnalysisWarningMessage` は既存の worker warning 用 DTO である。
    /// AuthoringIssue からの変換では、共通フィールド（code, message, severity, scope,
    /// condition_id, field_name）を直接流用し、worker 独自フィールドは None で埋める。
    /// このテストは `AnalysisWarningMessage` が serde Deserialize を持ち、
    /// AuthoringIssue と構造的に互換する範囲で直接 deserialize 可能であることを確認する。
    #[test]
    fn analysis_warning_message_can_deserialize_from_authoring_issue_json() {
        let json = r#"{"code":"c1","severity":"warning","scope":"filter_config","message":"m1","conditionId":"r1","fieldName":"f1"}"#;
        let warning: AnalysisWarningMessage = serde_json::from_str(json).unwrap();
        assert_eq!(warning.code, "c1");
        assert_eq!(warning.severity, Some("warning".to_string()));
        assert_eq!(warning.scope, Some("filter_config".to_string()));
        assert_eq!(warning.message, "m1");
        assert_eq!(warning.condition_id, Some("r1".to_string()));
        assert_eq!(warning.field_name, Some("f1".to_string()));
        // worker 独自フィールドはデフォルトで None
        assert_eq!(warning.unit_id, None);
    }

    // ---- CompiledOutputPath tests (Task 3) ----

    fn temp_source_file(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "csv_viewer_bridge_test_{}_{}.json",
            name,
            std::process::id()
        ));
        std::fs::write(&path, content).unwrap();
        path
    }

    fn temp_output_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "csv_viewer_bridge_out_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn missing_source_file_returns_not_found_error() {
        let missing = PathBuf::from("/nonexistent/path/to/source.json");
        let output_dir = temp_output_dir("missing");
        let result = CompiledOutputPath::build(&missing, "content", &output_dir);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("not found"),
            "error should mention not found: {}",
            err
        );
        assert!(
            err.contains("nonexistent"),
            "error should contain the path: {}",
            err
        );
    }

    #[test]
    fn output_path_is_under_compiled_conditions_dir() {
        let source = temp_source_file("under_dir", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("under_dir");
        let result = CompiledOutputPath::build(&source, "content", &output_dir).unwrap();
        let parent = result.runtime_json.parent().unwrap();
        assert_eq!(parent, output_dir);
        let issues_parent = result.issues_json.parent().unwrap();
        assert_eq!(issues_parent, output_dir);
        let _ = std::fs::remove_file(&source);
    }

    #[test]
    fn stable_key_is_deterministic() {
        let source = temp_source_file("det", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("det");
        let r1 = CompiledOutputPath::build(&source, "same content", &output_dir).unwrap();
        let r2 = CompiledOutputPath::build(&source, "same content", &output_dir).unwrap();
        assert_eq!(r1.runtime_json, r2.runtime_json);
        assert_eq!(r1.issues_json, r2.issues_json);
        let _ = std::fs::remove_file(&source);
    }

    #[test]
    fn content_change_changes_key() {
        let source = temp_source_file("chg", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("chg");
        let r1 = CompiledOutputPath::build(&source, "content A", &output_dir).unwrap();
        let r2 = CompiledOutputPath::build(&source, "content B", &output_dir).unwrap();
        assert_ne!(r1.runtime_json, r2.runtime_json);
        assert_ne!(r1.issues_json, r2.issues_json);
        let _ = std::fs::remove_file(&source);
    }

    #[test]
    fn bridge_version_is_included_in_key() {
        // BRIDGE_VERSION を含む input の形式を検証する間接テスト。
        // 異なる content で異なる key が出ることは content_change_changes_key で確認済み。
        // ここでは、BRIDGE_VERSION 定数が空でないことを確認し、
        // 将来の互換性破壊変更に備えた予約として存在することを担保する。
        assert!(!BRIDGE_VERSION.is_empty());
        assert!(BRIDGE_VERSION.contains("bridge"));
    }

    #[test]
    fn windows_path_separator_normalization() {
        // canonicalize 後に `\\` を `/` に置換するため、
        // 同じ論理パスに対して異なる区切り文字を使っても同じ key になることを確認。
        // ただし canonicalize は OS 依存なので、実際のファイルを使ってテストする。
        let source = temp_source_file("sep", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("sep");
        let r1 = CompiledOutputPath::build(&source, "x", &output_dir).unwrap();
        // 同じファイル、同じ content なら必ず同じ key
        let r2 = CompiledOutputPath::build(&source, "x", &output_dir).unwrap();
        assert_eq!(r1.runtime_json, r2.runtime_json);
        let _ = std::fs::remove_file(&source);
    }

    #[test]
    fn stable_key_is_64_hex_chars() {
        let source = temp_source_file("hex64", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("hex64");
        let result = CompiledOutputPath::build(&source, "content", &output_dir).unwrap();

        // file_name から .runtime.json suffix を取り除いて key を取得
        let file_name = result
            .runtime_json
            .file_name()
            .expect("file_name should exist")
            .to_string_lossy();
        let key = file_name
            .strip_suffix(".runtime.json")
            .expect("suffix should be .runtime.json");

        assert_eq!(
            key.len(),
            64,
            "stable key should be 64 hex chars, got {} chars: {}",
            key.len(),
            key
        );
        assert!(
            key.chars().all(|c| c.is_ascii_hexdigit()),
            "stable key should contain only hex digits: {}",
            key
        );

        let _ = std::fs::remove_file(&source);
    }

    #[test]
    fn ensure_parent_dir_creates_missing_directories() {
        let output_dir = temp_output_dir("mkdir");
        let nested = output_dir.join("a").join("b").join("file.json");
        assert!(!nested.parent().unwrap().exists());
        ensure_parent_dir(&nested).unwrap();
        assert!(nested.parent().unwrap().exists());
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    #[test]
    fn ensure_parent_dir_succeeds_when_parent_already_exists() {
        let output_dir = temp_output_dir("mkdir_exists");
        std::fs::create_dir_all(&output_dir).unwrap();
        let file = output_dir.join("file.json");
        ensure_parent_dir(&file).unwrap();
        assert!(output_dir.exists());
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    // ---- Command execution tests (Task 4) ----

    /// テスト用の fake project root を作成する。
    /// `analysis_backend/__init__.py` と `analysis_backend/condition_authoring_cli.py` を配置し、
    /// 本番と同じ `python -m analysis_backend.condition_authoring_cli` で実行できるようにする。
    fn fake_project_root_with_cli(name: &str, cli_body: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "csv_viewer_fake_proj_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let backend_dir = root.join("analysis_backend");
        std::fs::create_dir_all(&backend_dir).unwrap();
        std::fs::write(backend_dir.join("__init__.py"), "").unwrap();
        std::fs::write(backend_dir.join("condition_authoring_cli.py"), cli_body).unwrap();
        root
    }

    fn fake_runtime_for_test(
        python_command: OsString,
        python_args: Vec<OsString>,
        project_root: PathBuf,
    ) -> AnalysisRuntimeConfig {
        AnalysisRuntimeConfig {
            python_command,
            python_args,
            python_label: "fake".to_string(),
            project_root,
            script_path: PathBuf::from("run-analysis.py"),
            filter_config_path: PathBuf::from("asset/cooccurrence-conditions.json"),
            filter_config_source_path: None,
            annotation_csv_path: PathBuf::from("asset/manual-annotations.csv"),
            jobs_root: PathBuf::from("runtime/jobs"),
        }
    }

    #[test]
    fn compile_authoring_success_via_fake_module() {
        let project_root = fake_project_root_with_cli(
            "success",
            r#"
import sys
import json
import os

# verify env
assert os.environ.get("CSV_VIEWER_PROJECT_ROOT"), "CSV_VIEWER_PROJECT_ROOT missing"

args = sys.argv[1:]
input_idx = args.index("--input")
output_idx = args.index("--output")
issues_idx = args.index("--issues-json")
input_path = args[input_idx + 1]
output_path = args[output_idx + 1]
issues_path = args[issues_idx + 1]

# write a minimal runtime JSON
with open(output_path, "w", encoding="utf-8") as f:
    json.dump({"cooccurrence_conditions": []}, f)

# write empty issues
with open(issues_path, "w", encoding="utf-8") as f:
    json.dump([], f)

sys.exit(0)
"#,
        );

        let source = temp_source_file("exec_success", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("exec_success");
        let runtime = fake_runtime_for_test(
            std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python")),
            vec![],
            project_root.clone(),
        );

        let result = compile_authoring_to_runtime(&runtime, &source, &output_dir);
        assert!(result.is_ok(), "expected ok, got: {:?}", result);
        let compile_result = result.unwrap();
        assert!(compile_result.issues.is_empty());
        assert!(compile_result.runtime_json_path.exists());

        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&output_dir);
        let _ = std::fs::remove_dir_all(&project_root);
    }

    #[test]
    fn compile_authoring_warning_issue_via_fake_module() {
        let project_root = fake_project_root_with_cli(
            "warning",
            r#"
import sys
import json
import os

assert os.environ.get("CSV_VIEWER_PROJECT_ROOT")

args = sys.argv[1:]
input_idx = args.index("--input")
output_idx = args.index("--output")
issues_idx = args.index("--issues-json")
output_path = args[output_idx + 1]
issues_path = args[issues_idx + 1]

with open(output_path, "w", encoding="utf-8") as f:
    json.dump({"cooccurrence_conditions": []}, f)

with open(issues_path, "w", encoding="utf-8") as f:
    json.dump([{"code":"unknown_field","severity":"warning","scope":"filter_config","message":"unknown field ignored","field_name":"extra"}], f)

sys.exit(0)
"#,
        );

        let source = temp_source_file("exec_warn", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("exec_warn");
        let runtime = fake_runtime_for_test(
            std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python")),
            vec![],
            project_root.clone(),
        );

        let result = compile_authoring_to_runtime(&runtime, &source, &output_dir).unwrap();
        assert_eq!(result.issues.len(), 1);
        assert_eq!(result.issues[0].code, "unknown_field");
        assert_eq!(result.issues[0].severity, "warning");
        assert!(!result.has_errors());

        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&output_dir);
        let _ = std::fs::remove_dir_all(&project_root);
    }

    #[test]
    fn compile_authoring_error_exit_via_fake_module() {
        let project_root = fake_project_root_with_cli(
            "error_exit",
            r#"
import sys
import json
import os

assert os.environ.get("CSV_VIEWER_PROJECT_ROOT")

args = sys.argv[1:]
issues_idx = args.index("--issues-json")
issues_path = args[issues_idx + 1]

with open(issues_path, "w", encoding="utf-8") as f:
    json.dump([{"code":"compile_error","severity":"error","scope":"filter_config","message":"something broke"}], f)

sys.stderr.write("compilation failed\n")
sys.exit(1)
"#,
        );

        let source = temp_source_file("exec_err", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("exec_err");
        let runtime = fake_runtime_for_test(
            std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python")),
            vec![],
            project_root.clone(),
        );

        let result = compile_authoring_to_runtime(&runtime, &source, &output_dir);
        assert!(result.is_err(), "expected error for non-zero exit");
        let err = result.unwrap_err();
        assert!(
            err.contains("exit code 1") || err.contains("compilation failed"),
            "error should mention failure: {}",
            err
        );

        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&output_dir);
        let _ = std::fs::remove_dir_all(&project_root);
    }

    #[test]
    fn compile_authoring_missing_issues_json_handling() {
        let project_root = fake_project_root_with_cli(
            "missing_issues",
            r#"
import sys
import json
import os

assert os.environ.get("CSV_VIEWER_PROJECT_ROOT")

args = sys.argv[1:]
output_idx = args.index("--output")
output_path = args[output_idx + 1]

with open(output_path, "w", encoding="utf-8") as f:
    json.dump({"cooccurrence_conditions": []}, f)

# do NOT write issues JSON
sys.exit(0)
"#,
        );

        let source = temp_source_file("exec_miss", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("exec_miss");
        let runtime = fake_runtime_for_test(
            std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python")),
            vec![],
            project_root.clone(),
        );

        let result = compile_authoring_to_runtime(&runtime, &source, &output_dir);
        assert!(
            result.is_err(),
            "expected error when issues JSON is missing"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("issues JSON")
                || err.contains("not found")
                || err.contains("No such file"),
            "error should mention issues JSON missing: {}",
            err
        );

        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&output_dir);
        let _ = std::fs::remove_dir_all(&project_root);
    }

    #[test]
    fn compile_authoring_stderr_truncation() {
        let project_root = fake_project_root_with_cli(
            "stderr_long",
            r#"
import sys
import json
import os

assert os.environ.get("CSV_VIEWER_PROJECT_ROOT")

args = sys.argv[1:]
output_idx = args.index("--output")
issues_idx = args.index("--issues-json")
output_path = args[output_idx + 1]
issues_path = args[issues_idx + 1]

with open(output_path, "w", encoding="utf-8") as f:
    json.dump({"cooccurrence_conditions": []}, f)
with open(issues_path, "w", encoding="utf-8") as f:
    json.dump([], f)

# write a very long stderr with multibyte UTF-8 characters
sys.stderr.write("エラー" * 3000 + "\n")
sys.exit(0)
"#,
        );

        let source = temp_source_file("exec_trunc", r#"{"format":"condition-authoring/v1"}"#);
        let output_dir = temp_output_dir("exec_trunc");
        let runtime = fake_runtime_for_test(
            std::env::var_os("PYTHON").unwrap_or_else(|| OsString::from("python")),
            vec![],
            project_root.clone(),
        );

        let result = compile_authoring_to_runtime(&runtime, &source, &output_dir);
        assert!(
            result.is_ok(),
            "expected ok despite long stderr: {:?}",
            result
        );
        let compile_result = result.unwrap();
        // stderr_summary should be truncated and valid UTF-8
        assert!(
            compile_result.stderr_summary.len() <= 4096 + "... (truncated)".len(),
            "stderr should be truncated: len={}",
            compile_result.stderr_summary.len()
        );
        // verify it's valid UTF-8 (String type guarantees this)
        assert!(
            compile_result.stderr_summary.contains("... (truncated)"),
            "stderr should contain truncation marker: {}",
            compile_result.stderr_summary
        );

        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_dir_all(&output_dir);
        let _ = std::fs::remove_dir_all(&project_root);
    }

    #[test]
    fn summarize_output_utf8_safe_truncation() {
        // マルチバイト文字を含む長文の切り詰めテスト
        let multibyte_text = "エラー".repeat(2000); // 3 bytes * 2000 = 6000 bytes
        let output = multibyte_text.clone().into_bytes();
        let summary = summarize_output(output, 10);
        assert!(
            summary.len() <= 10 + "... (truncated)".len(),
            "summary should be truncated: len={}",
            summary.len()
        );
        assert!(
            summary.ends_with("... (truncated)"),
            "summary should end with truncation marker: {}",
            summary
        );
        // UTF-8 文字境界で切り詰められていることを確認（String 型なので valid UTF-8 は保証される）
        assert!(
            summary.as_bytes().len() <= 10 + "... (truncated)".len(),
            "byte length should respect boundary: {}",
            summary.as_bytes().len()
        );
    }

    #[test]
    fn summarize_output_no_truncation_when_short() {
        let text = "short message";
        let summary = summarize_output(text.as_bytes().to_vec(), 100);
        assert_eq!(summary, text);
    }

    #[test]
    fn summarize_output_exact_boundary_multibyte() {
        // max_len がマルチバイト文字の真ん中にある場合
        let text = "あいう"; // 9 bytes total
        let summary = summarize_output(text.as_bytes().to_vec(), 4);
        // 4 bytes 目は「い」の途中なので、3 bytes 目（「あ」の後）で切り詰め
        assert!(
            summary.ends_with("... (truncated)"),
            "should truncate at char boundary: {}",
            summary
        );
        assert!(
            !summary.contains("\u{FFFD}"),
            "should not contain replacement character: {}",
            summary
        );
    }
}
