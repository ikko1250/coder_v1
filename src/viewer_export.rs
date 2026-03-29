use crate::model::{AnalysisRecord, AnalysisUnit};
use csv::{Terminator, WriterBuilder};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const UTF8_BOM: &[u8; 3] = b"\xEF\xBB\xBF";

const PARAGRAPH_HEADERS: &[&str] = &[
    "paragraph_id",
    "document_id",
    "category1",
    "category2",
    "sentence_count",
    "paragraph_text",
    "paragraph_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "matched_form_group_ids_text",
    "matched_form_group_logics_text",
    "form_group_explanations_text",
    "text_groups_explanations_text",
    "mixed_scope_warning_text",
    "match_group_ids_text",
    "match_group_count",
    "annotated_token_count",
    "manual_annotation_count",
    "manual_annotation_pairs_text",
    "manual_annotation_namespaces_text",
];

const SENTENCE_HEADERS: &[&str] = &[
    "sentence_id",
    "paragraph_id",
    "document_id",
    "category1",
    "category2",
    "sentence_no_in_paragraph",
    "sentence_no_in_document",
    "sentence_text",
    "sentence_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "matched_form_group_ids_text",
    "matched_form_group_logics_text",
    "form_group_explanations_text",
    "text_groups_explanations_text",
    "mixed_scope_warning_text",
    "match_group_ids_text",
    "match_group_count",
    "annotated_token_count",
];

pub(crate) fn write_visible_records_csv(
    path: &Path,
    records: &[AnalysisRecord],
) -> Result<(), String> {
    let analysis_unit = validate_records(records)?;
    let temp_path = build_temp_path(path);

    write_visible_records_csv_to_path(&temp_path, records, analysis_unit)?;

    replace_file(&temp_path, path)
}

fn validate_records(records: &[AnalysisRecord]) -> Result<AnalysisUnit, String> {
    let Some(first_record) = records.first() else {
        return Err("保存対象の表示レコードがありません".to_string());
    };
    let analysis_unit = first_record.analysis_unit;
    if records
        .iter()
        .any(|record| record.analysis_unit != analysis_unit)
    {
        return Err("表示レコードに paragraph / sentence が混在しています".to_string());
    }
    Ok(analysis_unit)
}

fn write_visible_records_csv_to_path(
    path: &Path,
    records: &[AnalysisRecord],
    analysis_unit: AnalysisUnit,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("保存先ディレクトリを作成できませんでした: {error}"))?;
    }

    let file = File::create(path)
        .map_err(|error| format!("CSV ファイルを作成できませんでした: {error}"))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(UTF8_BOM)
        .map_err(|error| format!("CSV BOM の書き込みに失敗しました: {error}"))?;

    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .terminator(Terminator::CRLF)
        .from_writer(writer);

    match analysis_unit {
        AnalysisUnit::Paragraph => {
            csv_writer
                .write_record(PARAGRAPH_HEADERS)
                .map_err(|error| format!("CSV ヘッダーの書き込みに失敗しました: {error}"))?;
            for record in records {
                csv_writer
                    .write_record(paragraph_record_fields(record))
                    .map_err(|error| format!("CSV 行の書き込みに失敗しました: {error}"))?;
            }
        }
        AnalysisUnit::Sentence => {
            csv_writer
                .write_record(SENTENCE_HEADERS)
                .map_err(|error| format!("CSV ヘッダーの書き込みに失敗しました: {error}"))?;
            for record in records {
                csv_writer
                    .write_record(sentence_record_fields(record))
                    .map_err(|error| format!("CSV 行の書き込みに失敗しました: {error}"))?;
            }
        }
    }

    csv_writer
        .flush()
        .map_err(|error| format!("CSV 書き込みの flush に失敗しました: {error}"))?;
    Ok(())
}

fn paragraph_record_fields(record: &AnalysisRecord) -> [&str; 20] {
    [
        record.paragraph_id.as_str(),
        record.document_id.as_str(),
        record.category1.as_str(),
        record.category2.as_str(),
        record.sentence_count.as_str(),
        record.paragraph_text.as_str(),
        record.paragraph_text_tagged.as_str(),
        record.matched_condition_ids_text.as_str(),
        record.matched_categories_text.as_str(),
        record.matched_form_group_ids_text.as_str(),
        record.matched_form_group_logics_text.as_str(),
        record.form_group_explanations_text.as_str(),
        record.text_groups_explanations_text.as_str(),
        record.mixed_scope_warning_text.as_str(),
        record.match_group_ids_text.as_str(),
        record.match_group_count.as_str(),
        record.annotated_token_count.as_str(),
        manual_annotation_count_value(record),
        record.manual_annotation_pairs_text.as_str(),
        record.manual_annotation_namespaces_text.as_str(),
    ]
}

fn sentence_record_fields(record: &AnalysisRecord) -> [&str; 19] {
    [
        record.sentence_id.as_str(),
        record.paragraph_id.as_str(),
        record.document_id.as_str(),
        record.category1.as_str(),
        record.category2.as_str(),
        record.sentence_no_in_paragraph.as_str(),
        record.sentence_no_in_document.as_str(),
        record.sentence_text.as_str(),
        record.sentence_text_tagged.as_str(),
        record.matched_condition_ids_text.as_str(),
        record.matched_categories_text.as_str(),
        record.matched_form_group_ids_text.as_str(),
        record.matched_form_group_logics_text.as_str(),
        record.form_group_explanations_text.as_str(),
        record.text_groups_explanations_text.as_str(),
        record.mixed_scope_warning_text.as_str(),
        record.match_group_ids_text.as_str(),
        record.match_group_count.as_str(),
        record.annotated_token_count.as_str(),
    ]
}

fn manual_annotation_count_value(record: &AnalysisRecord) -> &str {
    if record.manual_annotation_count.is_empty() {
        "0"
    } else {
        record.manual_annotation_count.as_str()
    }
}

fn build_temp_path(target_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "analysis-result-visible.csv".to_string());
    let temp_file_name = format!(".{file_name}.tmp-{nanos}");
    target_path.with_file_name(temp_file_name)
}

fn replace_file(temp_path: &Path, target_path: &Path) -> Result<(), String> {
    match fs::rename(temp_path, target_path) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            if target_path.exists() {
                fs::remove_file(target_path).map_err(|remove_error| {
                    format!(
                        "既存 CSV の置換に失敗しました: rename={rename_error}; remove={remove_error}"
                    )
                })?;
                fs::rename(temp_path, target_path).map_err(|second_rename_error| {
                    format!(
                        "既存 CSV の置換に失敗しました: rename={rename_error}; retry={second_rename_error}"
                    )
                })
            } else {
                Err(format!("CSV ファイルの配置に失敗しました: {rename_error}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::write_visible_records_csv;
    use crate::csv_loader::load_records;
    use crate::model::{AnalysisRecord, AnalysisUnit};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_csv_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        path.push(format!("csv-viewer-export-{name}-{nanos}.csv"));
        path
    }

    fn paragraph_record() -> AnalysisRecord {
        AnalysisRecord {
            row_no: 7,
            analysis_unit: AnalysisUnit::Paragraph,
            paragraph_id: "10".to_string(),
            sentence_id: String::new(),
            document_id: "3".to_string(),
            category1: "札幌市".to_string(),
            category2: "条例".to_string(),
            sentence_count: "2".to_string(),
            sentence_no_in_paragraph: String::new(),
            sentence_no_in_document: String::new(),
            sentence_text: String::new(),
            sentence_text_tagged: String::new(),
            paragraph_text: "本文".to_string(),
            paragraph_text_tagged: "[[HIT condition_ids=\"c1\" categories=\"cat\" groups=\"g1\"]]本[[/HIT]]文".to_string(),
            matched_condition_ids_text: "c1".to_string(),
            matched_categories_text: "cat".to_string(),
            matched_form_group_ids_text: "g1".to_string(),
            matched_form_group_logics_text: "g1=and".to_string(),
            form_group_explanations_text: "exp".to_string(),
            text_groups_explanations_text: "text-exp".to_string(),
            mixed_scope_warning_text: String::new(),
            match_group_ids_text: "g1".to_string(),
            match_group_count: "1".to_string(),
            annotated_token_count: "2".to_string(),
            manual_annotation_count: String::new(),
            manual_annotation_pairs_text: "ns:key=value".to_string(),
            manual_annotation_namespaces_text: "ns".to_string(),
        }
    }

    fn sentence_record() -> AnalysisRecord {
        AnalysisRecord {
            row_no: 9,
            analysis_unit: AnalysisUnit::Sentence,
            paragraph_id: "10".to_string(),
            sentence_id: "11".to_string(),
            document_id: "3".to_string(),
            category1: "札幌市".to_string(),
            category2: "条例".to_string(),
            sentence_count: String::new(),
            sentence_no_in_paragraph: "1".to_string(),
            sentence_no_in_document: "8".to_string(),
            sentence_text: "文本文".to_string(),
            sentence_text_tagged: "[[HIT condition_ids=\"c1\" categories=\"cat\" groups=\"g1\"]]文[[/HIT]]本文".to_string(),
            paragraph_text: String::new(),
            paragraph_text_tagged: String::new(),
            matched_condition_ids_text: "c1".to_string(),
            matched_categories_text: "cat".to_string(),
            matched_form_group_ids_text: String::new(),
            matched_form_group_logics_text: String::new(),
            form_group_explanations_text: String::new(),
            text_groups_explanations_text: String::new(),
            mixed_scope_warning_text: String::new(),
            match_group_ids_text: "g1".to_string(),
            match_group_count: "1".to_string(),
            annotated_token_count: "1".to_string(),
            manual_annotation_count: "9".to_string(),
            manual_annotation_pairs_text: "ignored".to_string(),
            manual_annotation_namespaces_text: "ignored".to_string(),
        }
    }

    #[test]
    fn write_paragraph_records_round_trips_via_csv_loader() {
        let path = temp_csv_path("paragraph");
        write_visible_records_csv(&path, &[paragraph_record()]).expect("paragraph export should succeed");

        let bytes = fs::read(&path).expect("output csv should be readable");
        assert!(bytes.starts_with(b"\xEF\xBB\xBF"));

        let loaded = load_records(&path).expect("output csv should round-trip");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].analysis_unit, AnalysisUnit::Paragraph);
        assert_eq!(loaded[0].paragraph_id, "10");
        assert_eq!(loaded[0].manual_annotation_count, "0");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_sentence_records_round_trips_via_csv_loader() {
        let path = temp_csv_path("sentence");
        write_visible_records_csv(&path, &[sentence_record()]).expect("sentence export should succeed");

        let loaded = load_records(&path).expect("output csv should round-trip");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].analysis_unit, AnalysisUnit::Sentence);
        assert_eq!(loaded[0].sentence_id, "11");
        assert_eq!(loaded[0].manual_annotation_count, "0");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_visible_records_csv_rejects_mixed_units() {
        let path = temp_csv_path("mixed");
        let error = write_visible_records_csv(&path, &[paragraph_record(), sentence_record()])
            .expect_err("mixed-unit export should fail");
        assert!(error.contains("混在"));
    }
}
