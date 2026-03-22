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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) condition_match_logic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) analysis_unit: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_from_any",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) max_reconstructed_paragraphs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) distance_matching_mode: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_from_any",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) distance_match_combination_cap: Option<u32>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_from_any",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) distance_match_strict_safety_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) cooccurrence_conditions: Vec<ConditionEditorItem>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct ConditionEditorItem {
    #[serde(default)]
    pub(crate) condition_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) categories: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) overall_search_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) form_groups: Vec<FormGroupEditorItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) forms: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) form_match_logic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) search_scope: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_i64_from_any",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) max_token_distance: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) annotation_filters: Vec<AnnotationFilterItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) required_categories_all: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) required_categories_any: Vec<String>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
    #[serde(skip)]
    pub(crate) projected_from_legacy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub(crate) struct FormGroupEditorItem {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) forms: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) match_logic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) anchor_form: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) combine_logic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) search_scope: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_i64_from_any",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) max_token_distance: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) operator: Option<String>,
    #[serde(default, flatten)]
    pub(crate) extra_fields: HashMap<String, Value>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ConditionDocumentLoadInfo {
    pub(crate) projected_legacy_condition_count: usize,
}

pub(crate) fn load_condition_document(
    path: &Path,
) -> Result<(FilterConfigDocument, ConditionDocumentLoadInfo), String> {
    let json_text = fs::read_to_string(path)
        .map_err(|error| format!("条件 JSON を読めませんでした: {} ({error})", path.display()))?;
    let mut document =
        serde_json::from_str::<FilterConfigDocument>(&json_text).map_err(|error| {
            format!(
                "条件 JSON の読込に失敗しました: {} ({error})",
                path.display()
            )
        })?;
    let load_info = project_legacy_conditions_for_editor(&mut document);
    Ok((document, load_info))
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
    sanitize_analysis_unit(&mut document.analysis_unit);
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
        remove_condition_schema_keys_from_extra_fields(condition);

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

        if !condition.annotation_filters.is_empty() {
            if !condition.form_groups.is_empty() {
                for group in &mut condition.form_groups {
                    group.search_scope = Some("paragraph".to_string());
                }
            } else if condition.search_scope.as_deref() != Some("paragraph") {
                condition.search_scope = Some("paragraph".to_string());
            }
        }

        normalize_condition_schema_for_save(condition, index + 1)?;

        let has_token_clause = !condition.forms.is_empty() || !condition.form_groups.is_empty();
        let has_annotation_clause = !condition.annotation_filters.is_empty();
        let has_reference_clause = !condition.required_categories_all.is_empty()
            || !condition.required_categories_any.is_empty();
        if !has_token_clause && !has_annotation_clause && !has_reference_clause {
            return Err(format!(
                "condition の clause が空です: {}",
                condition.condition_id
            ));
        }
    }

    Ok(())
}

pub(crate) fn build_default_condition_item() -> ConditionEditorItem {
    ConditionEditorItem {
        condition_id: "new_condition".to_string(),
        overall_search_scope: Some("paragraph".to_string()),
        form_groups: vec![FormGroupEditorItem {
            match_logic: Some("and".to_string()),
            forms: vec![String::new()],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn write_condition_document(path: &Path, document: &FilterConfigDocument) -> Result<(), String> {
    let file = File::create(path).map_err(|error| {
        format!(
            "条件 JSON の temp file を作成できません: {} ({error})",
            path.display()
        )
    })?;
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

fn sanitize_analysis_unit(value: &mut Option<String>) {
    sanitize_optional_string(value);
    let normalized = match value.as_deref() {
        Some(raw_value) if raw_value.eq_ignore_ascii_case("sentence") => "sentence",
        Some(_) | None => "paragraph",
    };
    *value = Some(normalized.to_string());
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

fn project_legacy_conditions_for_editor(
    document: &mut FilterConfigDocument,
) -> ConditionDocumentLoadInfo {
    let mut projected_legacy_condition_count = 0usize;

    sanitize_analysis_unit(&mut document.analysis_unit);

    for condition in &mut document.cooccurrence_conditions {
        sanitize_optional_string(&mut condition.overall_search_scope);
        sanitize_optional_string(&mut condition.form_match_logic);
        sanitize_optional_string(&mut condition.search_scope);
        sanitize_string_list(&mut condition.forms);
        sanitize_form_groups(&mut condition.form_groups);

        if condition.form_groups.is_empty() && legacy_token_clause_used(condition) {
            condition.form_groups = vec![FormGroupEditorItem {
                forms: condition.forms.clone(),
                match_logic: Some(match condition.form_match_logic.as_deref() {
                    Some("any") => "or".to_string(),
                    _ => "and".to_string(),
                }),
                search_scope: condition
                    .search_scope
                    .clone()
                    .or_else(|| condition.overall_search_scope.clone()),
                max_token_distance: condition.max_token_distance,
                ..Default::default()
            }];
            condition.projected_from_legacy = true;
            projected_legacy_condition_count += 1;
        }
    }

    ConditionDocumentLoadInfo {
        projected_legacy_condition_count,
    }
}

fn legacy_token_clause_used(condition: &ConditionEditorItem) -> bool {
    !condition.forms.is_empty()
        || condition.form_match_logic.is_some()
        || condition.search_scope.is_some()
        || condition.max_token_distance.is_some()
}

fn remove_condition_schema_keys_from_extra_fields(condition: &mut ConditionEditorItem) {
    condition.extra_fields.remove("forms");
    condition.extra_fields.remove("form_match_logic");
    condition.extra_fields.remove("search_scope");
    condition.extra_fields.remove("max_token_distance");
    condition.extra_fields.remove("form_groups");
}

fn normalize_condition_schema_for_save(
    condition: &mut ConditionEditorItem,
    condition_index: usize,
) -> Result<(), String> {
    if let Some(distance) = condition.max_token_distance {
        if distance < 0 {
            return Err(format!(
                "condition {} で max_token_distance に負値は使えません",
                condition.condition_id
            ));
        }
    }

    for (group_index, group) in condition.form_groups.iter_mut().enumerate() {
        if let Some(distance) = group.max_token_distance {
            if distance < 0 {
                return Err(format!(
                    "condition {} の group {} で max_token_distance に負値は使えません",
                    condition.condition_id,
                    group_index + 1
                ));
            }
        }

        if let Some(anchor_form) = group.anchor_form.as_ref() {
            if !group.forms.iter().any(|form| form == anchor_form) {
                return Err(format!(
                    "condition {} の group {} で anchor_form が forms に含まれていません",
                    condition.condition_id,
                    group_index + 1
                ));
            }
        }
    }

    if !condition.form_groups.is_empty() {
        if should_save_as_legacy(condition) {
            let first_group = condition.form_groups.first().cloned().ok_or_else(|| {
                format!(
                    "condition {} の group 変換に失敗しました",
                    condition.condition_id
                )
            })?;
            condition.forms = first_group.forms;
            condition.form_match_logic = Some(match first_group.match_logic.as_deref() {
                Some("or") => "any".to_string(),
                _ => "all".to_string(),
            });
            condition.search_scope = first_group.search_scope;
            condition.max_token_distance = first_group.max_token_distance;
            condition.form_groups.clear();
        } else {
            condition.forms.clear();
            condition.form_match_logic = None;
            condition.search_scope = None;
            condition.max_token_distance = None;

            for (group_index, group) in condition.form_groups.iter().enumerate() {
                let match_logic = group.match_logic.as_deref().unwrap_or("and");
                if group_index == 0 && match_logic == "not" {
                    return Err(format!(
                        "condition {} の group 1 では match_logic=not を保存できません",
                        condition.condition_id
                    ));
                }
                if match_logic == "and"
                    && group.max_token_distance.is_some()
                    && group.anchor_form.is_none()
                {
                    return Err(format!(
                        "condition {} の group {} は anchor_form が必要です",
                        condition.condition_id,
                        group_index + 1
                    ));
                }
            }
        }
    } else {
        condition.form_groups.clear();
    }

    condition.projected_from_legacy = false;

    if condition.condition_id.trim().is_empty() {
        return Err(format!(
            "condition_id が空の condition は保存できません (index: {})",
            condition_index
        ));
    }

    Ok(())
}

fn should_save_as_legacy(condition: &ConditionEditorItem) -> bool {
    if !condition.projected_from_legacy || condition.form_groups.len() != 1 {
        return false;
    }

    let Some(group) = condition.form_groups.first() else {
        return false;
    };

    matches!(
        group.match_logic.as_deref(),
        None | Some("and") | Some("or")
    ) && group.combine_logic.is_none()
        && group.anchor_form.is_none()
        && group.exclude_forms_any.is_empty()
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

fn deserialize_optional_integer_from_any<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_document_sets_default_analysis_unit_to_paragraph() {
        let mut document = FilterConfigDocument {
            cooccurrence_conditions: vec![ConditionEditorItem {
                condition_id: "condition_1".to_string(),
                forms: vec!["term".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        sanitize_document_for_save(&mut document).expect("document should sanitize");

        assert_eq!(document.analysis_unit.as_deref(), Some("paragraph"));
    }

    #[test]
    fn sanitize_document_normalizes_invalid_analysis_unit_to_paragraph() {
        let mut document = FilterConfigDocument {
            analysis_unit: Some(" token ".to_string()),
            cooccurrence_conditions: vec![ConditionEditorItem {
                condition_id: "condition_1".to_string(),
                forms: vec!["term".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        sanitize_document_for_save(&mut document).expect("document should sanitize");

        assert_eq!(document.analysis_unit.as_deref(), Some("paragraph"));
    }

    #[test]
    fn save_and_load_round_trip_preserves_sentence_analysis_unit() {
        let path = std::env::temp_dir().join(format!(
            "condition-editor-roundtrip-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        ));
        let document = FilterConfigDocument {
            analysis_unit: Some("sentence".to_string()),
            cooccurrence_conditions: vec![ConditionEditorItem {
                condition_id: "condition_1".to_string(),
                forms: vec!["term".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        save_condition_document_atomic(&path, &document).expect("document should save");
        let saved_text = fs::read_to_string(&path).expect("saved document should be readable");
        assert!(saved_text.contains(r#""analysis_unit": "sentence""#));

        let (loaded_document, _load_info) =
            load_condition_document(&path).expect("document should load");
        assert_eq!(loaded_document.analysis_unit.as_deref(), Some("sentence"));

        fs::remove_file(&path).expect("temp file should be removable");
    }
}
