use crate::model::AnalysisRecord;
use std::path::PathBuf;

const REQUIRED_COLUMNS: &[&str] = &[
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
    let missing: Vec<&str> = REQUIRED_COLUMNS
        .iter()
        .filter(|&&col| !header_names.iter().any(|h| h == col))
        .copied()
        .collect();

    if !missing.is_empty() {
        return Err(format!(
            "CSV に必要な列が不足しています: {}",
            missing.join(", ")
        ));
    }

    let idx = |name: &str| -> usize {
        header_names.iter().position(|h| h == name).unwrap_or(0)
    };

    let get = |record: &csv::StringRecord, name: &str| -> String {
        record.get(idx(name)).unwrap_or("").to_string()
    };

    let mut records = Vec::new();
    for (row_no, result) in rdr.records().enumerate() {
        let row = result.map_err(|e| format!("行 {} の読み込みエラー: {e}", row_no + 1))?;
        records.push(AnalysisRecord {
            row_no: row_no + 1,
            paragraph_id: get(&row, "paragraph_id"),
            document_id: get(&row, "document_id"),
            municipality_name: get(&row, "municipality_name"),
            ordinance_or_rule: get(&row, "ordinance_or_rule"),
            doc_type: get(&row, "doc_type"),
            sentence_count: get(&row, "sentence_count"),
            paragraph_text: get(&row, "paragraph_text"),
            paragraph_text_tagged: get(&row, "paragraph_text_tagged"),
            matched_condition_ids_text: get(&row, "matched_condition_ids_text"),
            matched_categories_text: get(&row, "matched_categories_text"),
            matched_form_group_ids_text: get(&row, "matched_form_group_ids_text"),
            matched_form_group_logics_text: get(&row, "matched_form_group_logics_text"),
            form_group_explanations_text: get(&row, "form_group_explanations_text"),
            mixed_scope_warning_text: get(&row, "mixed_scope_warning_text"),
            match_group_ids_text: get(&row, "match_group_ids_text"),
            match_group_count: get(&row, "match_group_count"),
            annotated_token_count: get(&row, "annotated_token_count"),
            manual_annotation_count: {
                let value = get(&row, "manual_annotation_count");
                if value.is_empty() { "0".to_string() } else { value }
            },
            manual_annotation_pairs_text: get(&row, "manual_annotation_pairs_text"),
            manual_annotation_namespaces_text: get(&row, "manual_annotation_namespaces_text"),
        });
    }

    Ok(records)
}
