use csv::WriterBuilder;
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::path::Path;

const MANUAL_ANNOTATION_HEADERS: [&str; 9] = [
    "target_type",
    "target_id",
    "label_namespace",
    "label_key",
    "label_value",
    "tagged_by",
    "tagged_at",
    "confidence",
    "note",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManualAnnotationAppendRow {
    pub(crate) target_type: String,
    pub(crate) target_id: String,
    pub(crate) label_namespace: String,
    pub(crate) label_key: String,
    pub(crate) label_value: String,
    pub(crate) tagged_by: String,
    pub(crate) tagged_at: String,
    pub(crate) confidence: String,
    pub(crate) note: String,
}

impl ManualAnnotationAppendRow {
    fn as_record(&self) -> [&str; 9] {
        [
            &self.target_type,
            &self.target_id,
            &self.label_namespace,
            &self.label_key,
            &self.label_value,
            &self.tagged_by,
            &self.tagged_at,
            &self.confidence,
            &self.note,
        ]
    }
}

pub(crate) fn append_manual_annotation_row(
    annotation_csv_path: &Path,
    row: &ManualAnnotationAppendRow,
) -> Result<(), String> {
    if let Some(parent) = annotation_csv_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "annotation CSV の親ディレクトリを作成できません: {} ({error})",
                parent.display()
            )
        })?;
    }

    let needs_header = match fs::metadata(annotation_csv_path) {
        Ok(metadata) => metadata.len() == 0,
        Err(_) => true,
    };

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(annotation_csv_path)
        .map_err(|error| {
            format!(
                "annotation CSV を開けませんでした: {} ({error})",
                annotation_csv_path.display()
            )
        })?;

    let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
    if needs_header {
        writer
            .write_record(MANUAL_ANNOTATION_HEADERS)
            .map_err(|error| format!("annotation CSV のヘッダー書き込みに失敗しました: {error}"))?;
    }
    writer
        .write_record(row.as_record())
        .map_err(|error| format!("annotation CSV への追記に失敗しました: {error}"))?;
    writer
        .flush()
        .map_err(|error| format!("annotation CSV の flush に失敗しました: {error}"))?;
    Ok(())
}

pub(crate) fn build_manual_annotation_pair(namespace: &str, key: &str, value: &str) -> String {
    format!("{}:{}={}", namespace.trim(), key.trim(), value.trim())
}

pub(crate) fn append_manual_annotation_pairs_text(existing: &str, pair: &str) -> String {
    let trimmed_existing = existing.trim();
    let trimmed_pair = pair.trim();
    if trimmed_pair.is_empty() {
        return trimmed_existing.to_string();
    }
    if trimmed_existing.is_empty() {
        return trimmed_pair.to_string();
    }
    format!("{trimmed_existing}\n{trimmed_pair}")
}

pub(crate) fn append_manual_annotation_namespaces_text(existing: &str, namespace: &str) -> String {
    let trimmed_namespace = namespace.trim();
    let mut namespaces = BTreeSet::new();
    for value in existing.split(',') {
        let trimmed_value = value.trim();
        if !trimmed_value.is_empty() {
            namespaces.insert(trimmed_value.to_string());
        }
    }
    if !trimmed_namespace.is_empty() {
        namespaces.insert(trimmed_namespace.to_string());
    }
    namespaces.into_iter().collect::<Vec<_>>().join(", ")
}

pub(crate) fn increment_manual_annotation_count(existing: &str) -> String {
    existing
        .trim()
        .parse::<u32>()
        .unwrap_or(0)
        .saturating_add(1)
        .to_string()
}

pub(crate) fn first_manual_annotation_line(text: &str) -> String {
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .to_string()
}
