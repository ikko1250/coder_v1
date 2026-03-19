use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct FilterConfigDocument {
    #[serde(default)]
    pub(crate) condition_match_logic: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u32_from_any")]
    pub(crate) max_reconstructed_paragraphs: Option<u32>,
    #[serde(default)]
    pub(crate) distance_matching_mode: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u32_from_any")]
    pub(crate) distance_match_combination_cap: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_optional_u32_from_any")]
    pub(crate) distance_match_strict_safety_limit: Option<u32>,
    #[serde(default)]
    pub(crate) cooccurrence_conditions: Vec<ConditionEditorItem>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct ConditionEditorItem {
    #[serde(default)]
    pub(crate) condition_id: String,
    #[serde(default)]
    pub(crate) categories: Vec<String>,
    #[serde(default)]
    pub(crate) overall_search_scope: Option<String>,
    #[serde(default)]
    pub(crate) form_groups: Vec<FormGroupEditorItem>,
    #[serde(default)]
    pub(crate) forms: Vec<String>,
    #[serde(default)]
    pub(crate) form_match_logic: Option<String>,
    #[serde(default)]
    pub(crate) search_scope: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_i64_from_any")]
    pub(crate) max_token_distance: Option<i64>,
    #[serde(default)]
    pub(crate) annotation_filters: Vec<AnnotationFilterItem>,
    #[serde(default)]
    pub(crate) required_categories_all: Vec<String>,
    #[serde(default)]
    pub(crate) required_categories_any: Vec<String>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct FormGroupEditorItem {
    #[serde(default)]
    pub(crate) forms: Vec<String>,
    #[serde(default)]
    pub(crate) match_logic: Option<String>,
    #[serde(default)]
    pub(crate) anchor_form: Option<String>,
    #[serde(default)]
    pub(crate) combine_logic: Option<String>,
    #[serde(default)]
    pub(crate) search_scope: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_i64_from_any")]
    pub(crate) max_token_distance: Option<i64>,
    #[serde(default)]
    pub(crate) exclude_forms_any: Vec<String>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct AnnotationFilterItem {
    #[serde(default)]
    pub(crate) namespace: String,
    #[serde(default)]
    pub(crate) key: String,
    #[serde(default)]
    pub(crate) value: String,
    #[serde(default)]
    pub(crate) operator: Option<String>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

pub(crate) fn load_condition_document(path: &Path) -> Result<FilterConfigDocument, String> {
    let json_text = fs::read_to_string(path)
        .map_err(|error| format!("条件 JSON を読めませんでした: {} ({error})", path.display()))?;
    serde_json::from_str::<FilterConfigDocument>(&json_text).map_err(|error| {
        format!(
            "条件 JSON の読込に失敗しました: {} ({error})",
            path.display()
        )
    })
}

pub(crate) fn save_condition_document_atomic(
    path: &Path,
    document: &FilterConfigDocument,
) -> Result<(), String> {
    let mut sanitized_document = document.clone();
    sanitize_document_for_save(&mut sanitized_document)?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "条件 JSON の親ディレクトリを作成できません: {} ({error})",
                parent.display()
            )
        })?;
    }

    let temp_path = build_temp_path(path);
    let write_result = write_condition_document(&temp_path, &sanitized_document);
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result?;

    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        format!(
            "条件 JSON の差し替えに失敗しました: {} ({error})",
            path.display()
        )
    })?;
    Ok(())
}

pub(crate) fn sanitize_document_for_save(
    document: &mut FilterConfigDocument,
) -> Result<(), String> {
    sanitize_optional_string(&mut document.condition_match_logic);
    sanitize_optional_string(&mut document.distance_matching_mode);

    for (index, condition) in document.cooccurrence_conditions.iter_mut().enumerate() {
        condition.condition_id = condition.condition_id.trim().to_string();
        if condition.condition_id.is_empty() {
            return Err(format!(
                "condition_id が空の condition は保存できません (index: {})",
                index + 1
            ));
        }

        sanitize_optional_string(&mut condition.overall_search_scope);
        sanitize_optional_string(&mut condition.form_match_logic);
        sanitize_optional_string(&mut condition.search_scope);
        sanitize_string_list(&mut condition.categories);
        sanitize_string_list(&mut condition.forms);
        sanitize_string_list(&mut condition.required_categories_all);
        sanitize_string_list(&mut condition.required_categories_any);
        sanitize_form_groups(&mut condition.form_groups);

        let mut sanitized_filters = Vec::new();
        for filter in &mut condition.annotation_filters {
            filter.namespace = filter.namespace.trim().to_string();
            filter.key = filter.key.trim().to_string();
            filter.value = filter.value.trim().to_string();
            sanitize_optional_string(&mut filter.operator);
            if filter.namespace.is_empty() && filter.key.is_empty() && filter.value.is_empty() {
                continue;
            }
            sanitized_filters.push(filter.clone());
        }
        condition.annotation_filters = sanitized_filters;
    }

    Ok(())
}

pub(crate) fn build_default_condition_item() -> ConditionEditorItem {
    ConditionEditorItem {
        condition_id: "new_condition".to_string(),
        overall_search_scope: Some("paragraph".to_string()),
        form_match_logic: Some("all".to_string()),
        search_scope: Some("paragraph".to_string()),
        ..Default::default()
    }
}

fn write_condition_document(path: &Path, document: &FilterConfigDocument) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|error| format!("条件 JSON の temp file を作成できません: {} ({error})", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, document)
        .map_err(|error| format!("条件 JSON の書き込みに失敗しました: {error}"))?;
    writer
        .write_all(b"\n")
        .map_err(|error| format!("条件 JSON の改行追加に失敗しました: {error}"))?;
    writer
        .flush()
        .map_err(|error| format!("条件 JSON の flush に失敗しました: {error}"))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| format!("条件 JSON の sync に失敗しました: {error}"))?;
    Ok(())
}

fn build_temp_path(path: &Path) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("cooccurrence-conditions.json");
    let temp_name = format!(".{file_name}.tmp-{}-{timestamp}", process::id());
    path.with_file_name(temp_name)
}

fn sanitize_optional_string(value: &mut Option<String>) {
    if let Some(text) = value.as_mut() {
        *text = text.trim().to_string();
        if text.is_empty() {
            *value = None;
        }
    }
}

fn sanitize_string_list(values: &mut Vec<String>) {
    *values = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect();
}

fn sanitize_form_groups(groups: &mut Vec<FormGroupEditorItem>) {
    let mut sanitized_groups = Vec::new();
    for group in groups.iter_mut() {
        sanitize_string_list(&mut group.forms);
        sanitize_optional_string(&mut group.match_logic);
        sanitize_optional_string(&mut group.anchor_form);
        sanitize_optional_string(&mut group.combine_logic);
        sanitize_optional_string(&mut group.search_scope);
        sanitize_string_list(&mut group.exclude_forms_any);
        if group.forms.is_empty()
            && group.match_logic.is_none()
            && group.anchor_form.is_none()
            && group.combine_logic.is_none()
            && group.search_scope.is_none()
            && group.max_token_distance.is_none()
            && group.exclude_forms_any.is_empty()
        {
            continue;
        }
        sanitized_groups.push(group.clone());
    }
    *groups = sanitized_groups;
}

fn deserialize_optional_u32_from_any<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_integer_from_any(deserializer).and_then(|value| {
        value
            .map(|parsed| {
                u32::try_from(parsed).map_err(|_| D::Error::custom("expected positive u32 value"))
            })
            .transpose()
    })
}

fn deserialize_optional_i64_from_any<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_integer_from_any(deserializer)
}

fn deserialize_optional_integer_from_any<'de, D>(
    deserializer: D,
) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw_value = Option::<Value>::deserialize(deserializer)?;
    match raw_value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .ok_or_else(|| D::Error::custom("expected integer number"))
            .map(Some),
        Some(Value::String(text)) => {
            let trimmed_text = text.trim();
            if trimmed_text.is_empty() {
                return Ok(None);
            }
            trimmed_text
                .parse::<i64>()
                .map(Some)
                .map_err(|_| D::Error::custom("expected integer string"))
        }
        Some(other) => Err(D::Error::custom(format!(
            "expected integer-compatible value, got {other}"
        ))),
    }
}
