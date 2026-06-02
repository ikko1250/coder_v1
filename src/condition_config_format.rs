use serde_json::Value;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConditionConfigFormat {
    Runtime,
    AuthoringV1,
    UnsupportedYaml,
    Invalid(String),
}

/// 条件ファイルの種別を判定する。
/// ファイルは read-only で読み込み、一切変更しない。
pub(crate) fn detect_condition_config_format(
    path: &Path,
) -> Result<ConditionConfigFormat, String> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase());

    match extension.as_deref() {
        Some("yaml") | Some("yml") => return Ok(ConditionConfigFormat::UnsupportedYaml),
        _ => {}
    }

    let content = fs::read_to_string(path)
        .map_err(|e| format!("ファイル読み込み失敗: {}", e))?;

    let json_value: Value = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            return Ok(ConditionConfigFormat::Invalid(e.to_string()));
        }
    };

    if let Some(Value::String(format)) = json_value.get("format") {
        if format == "condition-authoring/v1" {
            return Ok(ConditionConfigFormat::AuthoringV1);
        }
    }

    // runtime JSON の判定
    if json_value.get("cooccurrence_conditions").is_some() {
        return Ok(ConditionConfigFormat::Runtime);
    }

    // FilterConfigDocument として deserialize 可能か試す
    if serde_json::from_str::<crate::condition_editor::FilterConfigDocument>(&content).is_ok() {
        return Ok(ConditionConfigFormat::Runtime);
    }

    Ok(ConditionConfigFormat::Invalid("unknown JSON format".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    fn write_temp_file(name: &str, content: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("csv_viewer_test_{}_{}", name, std::process::id()));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    fn write_temp_file_with_ext(name: &str, ext: &str, content: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "csv_viewer_test_{}_{}.{}",
            name,
            std::process::id(),
            ext
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn detect_authoring_v1_json() {
        let json = r#"{"format": "condition-authoring/v1", "conditions": []}"#;
        let path = write_temp_file("authoring", json);
        let result = detect_condition_config_format(&path).unwrap();
        assert_eq!(result, ConditionConfigFormat::AuthoringV1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_runtime_json_with_cooccurrence_conditions() {
        let json = r#"{"cooccurrence_conditions": [{"condition_id": "c1"}]}"#;
        let path = write_temp_file("runtime_cooc", json);
        let result = detect_condition_config_format(&path).unwrap();
        assert_eq!(result, ConditionConfigFormat::Runtime);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_runtime_json_as_filter_config_document() {
        let json = r#"{"condition_match_logic": "AND", "cooccurrence_conditions": []}"#;
        let path = write_temp_file("runtime_fcd", json);
        let result = detect_condition_config_format(&path).unwrap();
        assert_eq!(result, ConditionConfigFormat::Runtime);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_unsupported_yaml() {
        let yaml = "format: condition-authoring/v1\nconditions: []";
        let path = write_temp_file_with_ext("authoring", "yaml", yaml);
        let result = detect_condition_config_format(&path).unwrap();
        assert_eq!(result, ConditionConfigFormat::UnsupportedYaml);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_unsupported_yml() {
        let yaml = "format: condition-authoring/v1\nconditions: []";
        let path = write_temp_file_with_ext("authoring", "yml", yaml);
        let result = detect_condition_config_format(&path).unwrap();
        assert_eq!(result, ConditionConfigFormat::UnsupportedYaml);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_invalid_json() {
        let json = "{ not valid json }";
        let path = write_temp_file("invalid", json);
        let result = detect_condition_config_format(&path).unwrap();
        assert!(
            matches!(result, ConditionConfigFormat::Invalid(_)),
            "expected Invalid variant, got {:?}",
            result
        );
        if let ConditionConfigFormat::Invalid(msg) = result {
            assert!(
                !msg.is_empty(),
                "error message should not be empty, got: {}",
                msg
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detect_missing_file_error() {
        let path = std::env::temp_dir().join(format!("csv_viewer_missing_{}", std::process::id()));
        let result = detect_condition_config_format(&path);
        assert!(result.is_err());
        let _ = std::fs::remove_file(&path);
    }
}
