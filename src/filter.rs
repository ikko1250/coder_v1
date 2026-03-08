use crate::model::{CsvRecord, FilterColumn, FilterOption};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug)]
enum FilterValueKind {
    Single(fn(&CsvRecord) -> String),
    Multi(fn(&CsvRecord) -> Vec<String>),
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
        label: "paragraph_id",
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
        value_kind: FilterValueKind::Single(record_matched_condition_ids_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchGroupIds,
        label: "match_groups",
        value_kind: FilterValueKind::Single(record_match_group_ids_value),
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
    fn values(self, record: &CsvRecord) -> Vec<String> {
        match self.value_kind {
            FilterValueKind::Single(extract) => vec![extract(record)],
            FilterValueKind::Multi(extract) => extract(record),
        }
    }

    fn matches(self, record: &CsvRecord, selected: &BTreeSet<String>) -> bool {
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

    pub(crate) fn matches(self, record: &CsvRecord, selected: &BTreeSet<String>) -> bool {
        filter_column_spec(self).matches(record, selected)
    }
}

pub(crate) fn build_filter_options(records: &[CsvRecord]) -> HashMap<FilterColumn, Vec<FilterOption>> {
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

fn filter_column_spec(column: FilterColumn) -> &'static FilterColumnSpec {
    FILTER_COLUMN_SPECS
        .iter()
        .find(|spec| spec.column == column)
        .expect("missing FilterColumnSpec")
}

fn normalize_single_filter_value(value: &str) -> String {
    value.trim().to_string()
}

fn record_paragraph_id_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.paragraph_id)
}

fn record_document_id_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.document_id)
}

fn record_municipality_name_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.municipality_name)
}

fn record_ordinance_or_rule_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.ordinance_or_rule)
}

fn record_doc_type_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.doc_type)
}

fn record_sentence_count_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.sentence_count)
}

fn record_matched_categories_values(record: &CsvRecord) -> Vec<String> {
    category_values(&record.matched_categories_text)
}

fn record_matched_condition_ids_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.matched_condition_ids_text)
}

fn record_match_group_ids_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.match_group_ids_text)
}

fn record_match_group_count_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.match_group_count)
}

fn record_annotated_token_count_value(record: &CsvRecord) -> String {
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
