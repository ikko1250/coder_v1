use crate::model::{AnalysisRecord, FilterColumn, FilterOption};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug)]
enum FilterValueKind {
    Single(fn(&AnalysisRecord) -> String),
    Multi(fn(&AnalysisRecord) -> Vec<String>),
}

#[derive(Clone, Copy, Debug)]
enum FilterSortKind {
    Text,
    Numeric,
}

#[derive(Clone, Copy, Debug)]
struct FilterColumnSpec {
    column: FilterColumn,
    label: &'static str,
    value_kind: FilterValueKind,
    sort_kind: FilterSortKind,
}

const FILTER_COLUMN_ORDER: &[FilterColumn] = &[
    FilterColumn::MatchedCategories,
    FilterColumn::MunicipalityName,
    FilterColumn::OrdinanceOrRule,
    FilterColumn::DocType,
    FilterColumn::ParagraphId,
    FilterColumn::DocumentId,
    FilterColumn::SentenceCount,
    FilterColumn::MatchedConditions,
    FilterColumn::MatchGroupIds,
    FilterColumn::MatchGroupCount,
    FilterColumn::AnnotatedTokenCount,
];

const FILTER_COLUMN_SPECS: &[FilterColumnSpec] = &[
    FilterColumnSpec {
        column: FilterColumn::MatchedCategories,
        label: "カテゴリ",
        value_kind: FilterValueKind::Multi(record_matched_categories_values),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MunicipalityName,
        label: "自治体",
        value_kind: FilterValueKind::Single(record_municipality_name_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::OrdinanceOrRule,
        label: "条例/規則",
        value_kind: FilterValueKind::Single(record_ordinance_or_rule_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::DocType,
        label: "doc_type",
        value_kind: FilterValueKind::Single(record_doc_type_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::ParagraphId,
        label: "unit_id",
        value_kind: FilterValueKind::Single(record_paragraph_id_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::DocumentId,
        label: "document_id",
        value_kind: FilterValueKind::Single(record_document_id_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::SentenceCount,
        label: "sentence_count",
        value_kind: FilterValueKind::Single(record_sentence_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchedConditions,
        label: "conditions",
        value_kind: FilterValueKind::Multi(record_matched_condition_ids_values),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchGroupIds,
        label: "match_groups",
        value_kind: FilterValueKind::Multi(record_match_group_ids_values),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchGroupCount,
        label: "match_group_count",
        value_kind: FilterValueKind::Single(record_match_group_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
    FilterColumnSpec {
        column: FilterColumn::AnnotatedTokenCount,
        label: "annotated_tokens",
        value_kind: FilterValueKind::Single(record_annotated_token_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
];

impl FilterColumnSpec {
    fn values(self, record: &AnalysisRecord) -> Vec<String> {
        match self.value_kind {
            FilterValueKind::Single(extract) => vec![extract(record)],
            FilterValueKind::Multi(extract) => extract(record),
        }
    }

    fn matches(self, record: &AnalysisRecord, selected: &BTreeSet<String>) -> bool {
        if selected.is_empty() {
            return true;
        }

        self.values(record)
            .into_iter()
            .any(|value| selected.contains(&value))
    }

    fn compare_values(self, left: &str, right: &str) -> Ordering {
        compare_filter_values(left, right, self.sort_kind)
    }
}

impl FilterColumn {
    pub(crate) fn all() -> &'static [Self] {
        FILTER_COLUMN_ORDER
    }

    pub(crate) fn label(self) -> &'static str {
        filter_column_spec(self).label
    }

    pub(crate) fn matches(self, record: &AnalysisRecord, selected: &BTreeSet<String>) -> bool {
        filter_column_spec(self).matches(record, selected)
    }
}

pub(crate) fn build_filter_options(
    records: &[AnalysisRecord],
) -> HashMap<FilterColumn, Vec<FilterOption>> {
    let mut options = HashMap::new();

    for spec in FILTER_COLUMN_SPECS {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for record in records {
            for value in spec.values(record) {
                *counts.entry(value).or_insert(0) += 1;
            }
        }

        let mut column_options: Vec<FilterOption> = counts
            .into_iter()
            .map(|(value, count)| FilterOption { value, count })
            .collect();
        column_options.sort_by(|a, b| spec.compare_values(&a.value, &b.value));
        options.insert(spec.column, column_options);
    }

    options
}

pub(crate) fn display_filter_value(value: &str) -> String {
    if value.is_empty() {
        "(空)".to_string()
    } else {
        value.to_string()
    }
}

pub(crate) fn normalize_filter_candidate_search_text(value: &str) -> String {
    value.trim().to_lowercase()
}

fn filter_column_spec(column: FilterColumn) -> &'static FilterColumnSpec {
    FILTER_COLUMN_SPECS
        .iter()
        .find(|spec| spec.column == column)
        .expect("missing FilterColumnSpec")
}

fn normalize_single_filter_value(value: &str) -> String {
    value.trim().to_string()
}

fn record_paragraph_id_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(record.unit_id())
}

fn record_document_id_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.document_id)
}

fn record_municipality_name_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.municipality_name)
}

fn record_ordinance_or_rule_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.ordinance_or_rule)
}

fn record_doc_type_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.doc_type)
}

fn record_sentence_count_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.sentence_count)
}

fn record_matched_categories_values(record: &AnalysisRecord) -> Vec<String> {
    category_values(&record.matched_categories_text)
}

fn record_matched_condition_ids_values(record: &AnalysisRecord) -> Vec<String> {
    category_values(&record.matched_condition_ids_text)
}

fn record_match_group_ids_values(record: &AnalysisRecord) -> Vec<String> {
    category_values(&record.match_group_ids_text)
}

fn record_match_group_count_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.match_group_count)
}

fn record_annotated_token_count_value(record: &AnalysisRecord) -> String {
    normalize_single_filter_value(&record.annotated_token_count)
}

fn category_values(raw: &str) -> Vec<String> {
    let mut values = BTreeSet::new();
    for part in raw.split(|c| c == ',' || c == '、' || c == ';' || c == '\n') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            values.insert(trimmed.to_string());
        }
    }

    if values.is_empty() {
        values.insert(String::new());
    }

    values.into_iter().collect()
}

fn compare_filter_values(left: &str, right: &str, sort_kind: FilterSortKind) -> Ordering {
    match sort_kind {
        FilterSortKind::Text => display_filter_value(left)
            .cmp(&display_filter_value(right))
            .then_with(|| left.cmp(right)),
        FilterSortKind::Numeric => compare_numeric_filter_values(left, right),
    }
}

fn compare_numeric_filter_values(left: &str, right: &str) -> Ordering {
    let left_trimmed = left.trim();
    let right_trimmed = right.trim();

    match (left_trimmed.is_empty(), right_trimmed.is_empty()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        (false, false) => {}
    }

    match (left_trimmed.parse::<i64>(), right_trimmed.parse::<i64>()) {
        (Ok(left_value), Ok(right_value)) => left_value
            .cmp(&right_value)
            .then_with(|| left_trimmed.cmp(right_trimmed)),
        (Ok(_), Err(_)) => Ordering::Less,
        (Err(_), Ok(_)) => Ordering::Greater,
        (Err(_), Err(_)) => display_filter_value(left_trimmed)
            .cmp(&display_filter_value(right_trimmed))
            .then_with(|| left_trimmed.cmp(right_trimmed)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AnalysisUnit;
    use std::collections::BTreeSet;

    fn empty_paragraph_record(row_no: usize) -> AnalysisRecord {
        AnalysisRecord {
            row_no,
            analysis_unit: AnalysisUnit::Paragraph,
            paragraph_id: String::new(),
            sentence_id: String::new(),
            document_id: String::new(),
            municipality_name: String::new(),
            ordinance_or_rule: String::new(),
            doc_type: String::new(),
            sentence_count: String::new(),
            sentence_no_in_paragraph: String::new(),
            sentence_no_in_document: String::new(),
            sentence_text: String::new(),
            sentence_text_tagged: String::new(),
            paragraph_text: String::new(),
            paragraph_text_tagged: String::new(),
            matched_condition_ids_text: String::new(),
            matched_categories_text: String::new(),
            matched_form_group_ids_text: String::new(),
            matched_form_group_logics_text: String::new(),
            form_group_explanations_text: String::new(),
            mixed_scope_warning_text: String::new(),
            match_group_ids_text: String::new(),
            match_group_count: String::new(),
            annotated_token_count: String::new(),
            manual_annotation_count: String::new(),
            manual_annotation_pairs_text: String::new(),
            manual_annotation_namespaces_text: String::new(),
        }
    }

    #[test]
    fn display_filter_value_empty_shows_placeholder() {
        assert_eq!(display_filter_value(""), "(空)");
        assert_eq!(display_filter_value("x"), "x");
    }

    #[test]
    fn normalize_filter_candidate_search_text_trims_and_lowercases() {
        assert_eq!(
            normalize_filter_candidate_search_text("  AbC  "),
            "abc"
        );
    }

    #[test]
    fn filter_column_matches_empty_selection_is_always_true() {
        let record = empty_paragraph_record(1);
        let empty: BTreeSet<String> = BTreeSet::new();
        assert!(FilterColumn::MunicipalityName.matches(&record, &empty));
    }

    #[test]
    fn filter_column_municipality_matches_selected_value() {
        let mut a = empty_paragraph_record(1);
        a.municipality_name = "札幌市".to_string();
        let mut b = empty_paragraph_record(2);
        b.municipality_name = "旭川市".to_string();
        let selected: BTreeSet<String> = ["札幌市".to_string()].into_iter().collect();
        assert!(FilterColumn::MunicipalityName.matches(&a, &selected));
        assert!(!FilterColumn::MunicipalityName.matches(&b, &selected));
    }

    #[test]
    fn filter_column_matched_categories_any_token_matches() {
        let mut record = empty_paragraph_record(1);
        record.matched_categories_text = "A, B、C".to_string();
        let selected: BTreeSet<String> = ["B".to_string()].into_iter().collect();
        assert!(FilterColumn::MatchedCategories.matches(&record, &selected));
        let selected2: BTreeSet<String> = ["Z".to_string()].into_iter().collect();
        assert!(!FilterColumn::MatchedCategories.matches(&record, &selected2));
    }

    #[test]
    fn filter_column_matched_conditions_matches_any_condition_id() {
        let mut record = empty_paragraph_record(1);
        record.matched_condition_ids_text = "A, B".to_string();
        let selected: BTreeSet<String> = ["B".to_string()].into_iter().collect();
        assert!(FilterColumn::MatchedConditions.matches(&record, &selected));
        let selected2: BTreeSet<String> = ["Z".to_string()].into_iter().collect();
        assert!(!FilterColumn::MatchedConditions.matches(&record, &selected2));
    }

    #[test]
    fn build_filter_options_splits_match_group_ids() {
        let mut a = empty_paragraph_record(1);
        a.match_group_ids_text = "g1, g2".to_string();
        let mut b = empty_paragraph_record(2);
        b.match_group_ids_text = "g2".to_string();
        let opts = build_filter_options(&[a, b]);
        let groups = opts.get(&FilterColumn::MatchGroupIds).unwrap();
        let values: Vec<_> = groups.iter().map(|option| (option.value.as_str(), option.count)).collect();
        assert_eq!(values, vec![("g1", 1), ("g2", 2)]);
    }

    #[test]
    fn build_filter_options_counts_distinct_municipalities() {
        let mut a = empty_paragraph_record(1);
        a.municipality_name = "札幌市".to_string();
        let mut b = empty_paragraph_record(2);
        b.municipality_name = "札幌市".to_string();
        let mut c = empty_paragraph_record(3);
        c.municipality_name = "旭川市".to_string();
        let opts = build_filter_options(&[a, b, c]);
        let muni = opts.get(&FilterColumn::MunicipalityName).unwrap();
        assert_eq!(muni.len(), 2);
        let total: usize = muni.iter().map(|o| o.count).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn build_filter_options_sorts_numeric_columns_numerically() {
        let mut r1 = empty_paragraph_record(1);
        r1.match_group_count = "10".to_string();
        let mut r2 = empty_paragraph_record(2);
        r2.match_group_count = "2".to_string();
        let opts = build_filter_options(&[r1, r2]);
        let counts = opts.get(&FilterColumn::MatchGroupCount).unwrap();
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0].value, "2");
        assert_eq!(counts[1].value, "10");
    }
}
