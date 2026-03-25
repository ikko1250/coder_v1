use crate::model::{AnalysisRecord, AnalysisUnit};
use std::collections::HashSet;
use std::path::PathBuf;

const PARAGRAPH_REQUIRED_COLUMNS: &[&str] = &[
    "paragraph_id",
    "document_id",
    "municipality_name",
    "ordinance_or_rule",
    "doc_type",
    "sentence_count",
    "paragraph_text",
    "paragraph_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "match_group_count",
    "annotated_token_count",
];

const SENTENCE_REQUIRED_COLUMNS: &[&str] = &[
    "sentence_id",
    "paragraph_id",
    "document_id",
    "municipality_name",
    "ordinance_or_rule",
    "doc_type",
    "sentence_no_in_paragraph",
    "sentence_no_in_document",
    "sentence_text",
    "sentence_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "match_group_count",
    "annotated_token_count",
];

pub(crate) fn load_records(path: &PathBuf) -> Result<Vec<AnalysisRecord>, String> {
    if !path.exists() {
        return Err(format!("CSV ファイルが見つかりません: {}", path.display()));
    }

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| format!("CSV を開けませんでした: {e}"))?;

    let headers = rdr
        .headers()
        .map_err(|e| format!("ヘッダー読み込みエラー: {e}"))?
        .clone();

    let header_names: Vec<String> = headers
        .iter()
        .map(|h| h.trim_start_matches('\u{feff}').to_string())
        .collect();
    let header_set: HashSet<&str> = header_names.iter().map(String::as_str).collect();
    let analysis_unit = detect_analysis_unit(&header_set)?;

    let idx = |name: &str| -> Option<usize> { header_names.iter().position(|h| h == name) };

    let get = |record: &csv::StringRecord, name: &str| -> String {
        idx(name)
            .and_then(|column_index| record.get(column_index))
            .unwrap_or("")
            .to_string()
    };

    let mut records = Vec::new();
    for (row_no, result) in rdr.records().enumerate() {
        let row = result.map_err(|e| format!("行 {} の読み込みエラー: {e}", row_no + 1))?;
        records.push(AnalysisRecord {
            row_no: row_no + 1,
            analysis_unit,
            paragraph_id: get(&row, "paragraph_id"),
            sentence_id: get(&row, "sentence_id"),
            document_id: get(&row, "document_id"),
            municipality_name: get(&row, "municipality_name"),
            ordinance_or_rule: get(&row, "ordinance_or_rule"),
            doc_type: get(&row, "doc_type"),
            sentence_count: get(&row, "sentence_count"),
            sentence_no_in_paragraph: get(&row, "sentence_no_in_paragraph"),
            sentence_no_in_document: get(&row, "sentence_no_in_document"),
            sentence_text: get(&row, "sentence_text"),
            sentence_text_tagged: get(&row, "sentence_text_tagged"),
            paragraph_text: get(&row, "paragraph_text"),
            paragraph_text_tagged: get(&row, "paragraph_text_tagged"),
            matched_condition_ids_text: get(&row, "matched_condition_ids_text"),
            matched_categories_text: get(&row, "matched_categories_text"),
            matched_form_group_ids_text: get(&row, "matched_form_group_ids_text"),
            matched_form_group_logics_text: get(&row, "matched_form_group_logics_text"),
            form_group_explanations_text: get(&row, "form_group_explanations_text"),
            text_groups_explanations_text: get(&row, "text_groups_explanations_text"),
            mixed_scope_warning_text: get(&row, "mixed_scope_warning_text"),
            match_group_ids_text: get(&row, "match_group_ids_text"),
            match_group_count: get(&row, "match_group_count"),
            annotated_token_count: get(&row, "annotated_token_count"),
            manual_annotation_count: default_zero_if_empty(get(&row, "manual_annotation_count")),
            manual_annotation_pairs_text: get(&row, "manual_annotation_pairs_text"),
            manual_annotation_namespaces_text: get(&row, "manual_annotation_namespaces_text"),
        });
    }

    Ok(records)
}

fn detect_analysis_unit(headers: &HashSet<&str>) -> Result<AnalysisUnit, String> {
    let paragraph_missing = missing_columns(headers, PARAGRAPH_REQUIRED_COLUMNS);
    if paragraph_missing.is_empty() {
        return Ok(AnalysisUnit::Paragraph);
    }

    let sentence_missing = missing_columns(headers, SENTENCE_REQUIRED_COLUMNS);
    if sentence_missing.is_empty() {
        return Ok(AnalysisUnit::Sentence);
    }

    Err(format!(
        "CSV に必要な列が不足しています: paragraph=[{}], sentence=[{}]",
        paragraph_missing.join(", "),
        sentence_missing.join(", ")
    ))
}

fn missing_columns(headers: &HashSet<&str>, required_columns: &[&str]) -> Vec<String> {
    required_columns
        .iter()
        .filter(|column| !headers.contains(**column))
        .map(|column| column.to_string())
        .collect()
}

fn default_zero_if_empty(value: String) -> String {
    if value.is_empty() {
        "0".to_string()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::load_records;
    use crate::model::AnalysisUnit;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_csv_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        path.push(format!("csv-viewer-{name}-{nanos}.csv"));
        path
    }

    #[test]
    fn load_records_supports_paragraph_csv() {
        let path = temp_csv_path("paragraph");
        fs::write(
            &path,
            concat!(
                "paragraph_id,document_id,municipality_name,ordinance_or_rule,doc_type,sentence_count,",
                "paragraph_text,paragraph_text_tagged,matched_condition_ids_text,matched_categories_text,",
                "match_group_count,annotated_token_count\n",
                "1,2,札幌市,条例,,3,本文,<hit>本文</hit>,cond-1,抑制区域,1,2\n"
            ),
        )
        .unwrap();

        let records = load_records(&path).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].analysis_unit, AnalysisUnit::Paragraph);
        assert_eq!(records[0].paragraph_id, "1");
        assert_eq!(records[0].paragraph_text, "本文");
        assert_eq!(records[0].manual_annotation_count, "0");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_records_supports_sentence_csv() {
        let path = temp_csv_path("sentence");
        fs::write(
            &path,
            concat!(
                "sentence_id,paragraph_id,document_id,municipality_name,ordinance_or_rule,doc_type,",
                "sentence_no_in_paragraph,sentence_no_in_document,sentence_text,sentence_text_tagged,",
                "matched_condition_ids_text,matched_categories_text,match_group_ids_text,match_group_count,annotated_token_count\n",
                "11,1,2,札幌市,条例,,2,5,文本文,<hit>文</hit>本文,cond-1,抑制区域,group-1,1,2\n"
            ),
        )
        .unwrap();

        let records = load_records(&path).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].analysis_unit, AnalysisUnit::Sentence);
        assert_eq!(records[0].sentence_id, "11");
        assert_eq!(records[0].paragraph_id, "1");
        assert_eq!(records[0].sentence_text_tagged, "<hit>文</hit>本文");
        assert_eq!(records[0].manual_annotation_count, "0");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_records_errors_when_file_missing() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "csv-viewer-missing-{}.csv",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = fs::remove_file(&path);
        let err = load_records(&path).unwrap_err();
        assert!(
            err.contains("見つかりません") || err.contains("not found"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn load_records_errors_on_insufficient_columns() {
        let path = temp_csv_path("bad-headers");
        fs::write(
            &path,
            "foo,bar\n1,2\n",
        )
        .unwrap();
        let err = load_records(&path).unwrap_err();
        assert!(
            err.contains("必要な列") || err.contains("不足"),
            "unexpected message: {err}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_records_accepts_bom_on_first_header_cell() {
        let path = temp_csv_path("bom-paragraph");
        let content = format!(
            "{}{}",
            '\u{feff}',
            concat!(
                "paragraph_id,document_id,municipality_name,ordinance_or_rule,doc_type,sentence_count,",
                "paragraph_text,paragraph_text_tagged,matched_condition_ids_text,matched_categories_text,",
                "match_group_count,annotated_token_count\n",
                "1,2,市,条例,,3,本文,,cond-1,cat,1,0\n"
            )
        );
        fs::write(&path, content).unwrap();

        let records = load_records(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].analysis_unit, AnalysisUnit::Paragraph);
        assert_eq!(records[0].paragraph_id, "1");

        let _ = fs::remove_file(path);
    }
}
