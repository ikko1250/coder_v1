use crate::condition_editor::ConditionEditorItem;
use crate::model::FilterOption;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ConditionListFilterColumn {
    ConditionId,
    Categories,
    Scope,
    GroupCount,
    FormCount,
    TextGroupCount,
    AnnotationFilterCount,
    ReferenceCount,
    LegacyProjection,
}

impl Default for ConditionListFilterColumn {
    fn default() -> Self {
        Self::Categories
    }
}

#[derive(Clone, Copy, Debug)]
enum ConditionListFilterValueKind {
    Single(fn(&ConditionEditorItem) -> String),
    Multi(fn(&ConditionEditorItem) -> Vec<String>),
}

#[derive(Clone, Copy, Debug)]
enum ConditionListFilterSortKind {
    Text,
    Numeric,
}

#[derive(Clone, Copy, Debug)]
struct ConditionListFilterSpec {
    column: ConditionListFilterColumn,
    label: &'static str,
    value_kind: ConditionListFilterValueKind,
    sort_kind: ConditionListFilterSortKind,
}

const CONDITION_LIST_FILTER_COLUMN_ORDER: &[ConditionListFilterColumn] = &[
    ConditionListFilterColumn::Categories,
    ConditionListFilterColumn::Scope,
    ConditionListFilterColumn::ConditionId,
    ConditionListFilterColumn::GroupCount,
    ConditionListFilterColumn::FormCount,
    ConditionListFilterColumn::TextGroupCount,
    ConditionListFilterColumn::AnnotationFilterCount,
    ConditionListFilterColumn::ReferenceCount,
    ConditionListFilterColumn::LegacyProjection,
];

const CONDITION_LIST_FILTER_SPECS: &[ConditionListFilterSpec] = &[
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::Categories,
        label: "categories",
        value_kind: ConditionListFilterValueKind::Multi(condition_categories_values),
        sort_kind: ConditionListFilterSortKind::Text,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::Scope,
        label: "scope",
        value_kind: ConditionListFilterValueKind::Single(condition_scope_value),
        sort_kind: ConditionListFilterSortKind::Text,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::ConditionId,
        label: "condition_id",
        value_kind: ConditionListFilterValueKind::Single(condition_id_value),
        sort_kind: ConditionListFilterSortKind::Text,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::GroupCount,
        label: "groups",
        value_kind: ConditionListFilterValueKind::Single(condition_group_count_value),
        sort_kind: ConditionListFilterSortKind::Numeric,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::FormCount,
        label: "forms",
        value_kind: ConditionListFilterValueKind::Single(condition_form_count_value),
        sort_kind: ConditionListFilterSortKind::Numeric,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::TextGroupCount,
        label: "text_groups",
        value_kind: ConditionListFilterValueKind::Single(condition_text_group_count_value),
        sort_kind: ConditionListFilterSortKind::Numeric,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::AnnotationFilterCount,
        label: "filters",
        value_kind: ConditionListFilterValueKind::Single(condition_annotation_filter_count_value),
        sort_kind: ConditionListFilterSortKind::Numeric,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::ReferenceCount,
        label: "refs",
        value_kind: ConditionListFilterValueKind::Single(condition_reference_count_value),
        sort_kind: ConditionListFilterSortKind::Numeric,
    },
    ConditionListFilterSpec {
        column: ConditionListFilterColumn::LegacyProjection,
        label: "legacy",
        value_kind: ConditionListFilterValueKind::Single(condition_legacy_projection_value),
        sort_kind: ConditionListFilterSortKind::Text,
    },
];

impl ConditionListFilterSpec {
    fn values(self, condition: &ConditionEditorItem) -> Vec<String> {
        match self.value_kind {
            ConditionListFilterValueKind::Single(extract) => vec![extract(condition)],
            ConditionListFilterValueKind::Multi(extract) => extract(condition),
        }
    }

    fn matches(self, condition: &ConditionEditorItem, selected: &BTreeSet<String>) -> bool {
        if selected.is_empty() {
            return true;
        }

        self.values(condition)
            .into_iter()
            .any(|value| selected.contains(&value))
    }

    fn compare_values(self, left: &str, right: &str) -> Ordering {
        compare_filter_values(left, right, self.sort_kind)
    }
}

impl ConditionListFilterColumn {
    pub(crate) fn all() -> &'static [Self] {
        CONDITION_LIST_FILTER_COLUMN_ORDER
    }

    pub(crate) fn label(self) -> &'static str {
        condition_list_filter_spec(self).label
    }

    pub(crate) fn matches(
        self,
        condition: &ConditionEditorItem,
        selected: &BTreeSet<String>,
    ) -> bool {
        condition_list_filter_spec(self).matches(condition, selected)
    }
}

pub(crate) fn build_condition_list_filter_options(
    conditions: &[ConditionEditorItem],
) -> HashMap<ConditionListFilterColumn, Vec<FilterOption>> {
    let mut options = HashMap::new();

    for spec in CONDITION_LIST_FILTER_SPECS {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for condition in conditions {
            for value in spec.values(condition) {
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

pub(crate) fn condition_matches_list_filters(
    condition: &ConditionEditorItem,
    selected_filter_values: &HashMap<ConditionListFilterColumn, BTreeSet<String>>,
) -> bool {
    selected_filter_values
        .iter()
        .all(|(column, selected)| column.matches(condition, selected))
}

pub(crate) fn normalize_condition_list_filter_search_text(value: &str) -> String {
    value.trim().to_lowercase()
}

pub(crate) fn condition_reference_count(condition: &ConditionEditorItem) -> usize {
    condition.required_categories_all.len()
        + condition.required_categories_any.len()
        + condition.required_condition_ids_all.len()
        + condition.required_condition_ids_any.len()
        + condition.excluded_condition_ids_any.len()
}

pub(crate) fn condition_effective_scope_label(condition: &ConditionEditorItem) -> String {
    let mut scopes = BTreeSet::new();

    if let Some(scope) = non_empty_trimmed(condition.overall_search_scope.as_deref()) {
        scopes.insert(scope.to_string());
    }
    if let Some(scope) = non_empty_trimmed(condition.search_scope.as_deref()) {
        scopes.insert(scope.to_string());
    }
    for group in &condition.form_groups {
        if let Some(scope) = non_empty_trimmed(group.search_scope.as_deref()) {
            scopes.insert(scope.to_string());
        }
    }
    for group in &condition.text_groups {
        if let Some(scope) = non_empty_trimmed(group.search_scope.as_deref()) {
            scopes.insert(scope.to_string());
        }
    }

    match scopes.len() {
        0 => String::new(),
        1 => scopes.into_iter().next().unwrap_or_default(),
        _ => "mixed".to_string(),
    }
}

pub(crate) fn condition_group_count(condition: &ConditionEditorItem) -> usize {
    if condition.form_groups.is_empty() {
        usize::from(!condition.forms.is_empty())
    } else {
        condition.form_groups.len()
    }
}

pub(crate) fn condition_form_count(condition: &ConditionEditorItem) -> usize {
    if !condition.form_groups.is_empty() {
        condition
            .form_groups
            .iter()
            .map(|group| group.forms.len())
            .sum()
    } else {
        condition.forms.len()
    }
}

fn condition_list_filter_spec(column: ConditionListFilterColumn) -> &'static ConditionListFilterSpec {
    CONDITION_LIST_FILTER_SPECS
        .iter()
        .find(|spec| spec.column == column)
        .expect("missing ConditionListFilterSpec")
}

fn condition_id_value(condition: &ConditionEditorItem) -> String {
    normalize_single_filter_value(&condition.condition_id)
}

fn condition_categories_values(condition: &ConditionEditorItem) -> Vec<String> {
    normalize_multi_filter_values(&condition.categories)
}

fn condition_scope_value(condition: &ConditionEditorItem) -> String {
    normalize_single_filter_value(&condition_effective_scope_label(condition))
}

fn condition_group_count_value(condition: &ConditionEditorItem) -> String {
    condition_group_count(condition).to_string()
}

fn condition_form_count_value(condition: &ConditionEditorItem) -> String {
    condition_form_count(condition).to_string()
}

fn condition_text_group_count_value(condition: &ConditionEditorItem) -> String {
    condition.text_groups.len().to_string()
}

fn condition_annotation_filter_count_value(condition: &ConditionEditorItem) -> String {
    condition.annotation_filters.len().to_string()
}

fn condition_reference_count_value(condition: &ConditionEditorItem) -> String {
    condition_reference_count(condition).to_string()
}

fn condition_legacy_projection_value(condition: &ConditionEditorItem) -> String {
    if condition.projected_from_legacy {
        "legacy".to_string()
    } else {
        String::new()
    }
}

fn normalize_single_filter_value(value: &str) -> String {
    value.trim().to_string()
}

fn normalize_multi_filter_values(values: &[String]) -> Vec<String> {
    let mut normalized = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            normalized.insert(trimmed.to_string());
        }
    }

    if normalized.is_empty() {
        normalized.insert(String::new());
    }

    normalized.into_iter().collect()
}

fn compare_filter_values(
    left: &str,
    right: &str,
    sort_kind: ConditionListFilterSortKind,
) -> Ordering {
    match sort_kind {
        ConditionListFilterSortKind::Text => left.cmp(right),
        ConditionListFilterSortKind::Numeric => compare_numeric_filter_values(left, right),
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
        (Err(_), Err(_)) => left_trimmed.cmp(right_trimmed),
    }
}

fn non_empty_trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_condition(condition_id: &str) -> ConditionEditorItem {
        ConditionEditorItem {
            condition_id: condition_id.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn effective_scope_aggregates_nested_scopes() {
        let mut condition = empty_condition("c1");
        condition.form_groups.push(crate::condition_editor::FormGroupEditorItem {
            search_scope: Some("paragraph".to_string()),
            ..Default::default()
        });
        assert_eq!(condition_effective_scope_label(&condition), "paragraph");
    }

    #[test]
    fn effective_scope_marks_mixed_when_multiple_scopes_exist() {
        let mut condition = empty_condition("c1");
        condition.overall_search_scope = Some("paragraph".to_string());
        condition.text_groups.push(crate::condition_editor::TextGroupEditorItem {
            search_scope: Some("sentence".to_string()),
            ..Default::default()
        });
        assert_eq!(condition_effective_scope_label(&condition), "mixed");
    }

    #[test]
    fn condition_filters_match_selected_category() {
        let mut condition = empty_condition("c1");
        condition.categories = vec!["A".to_string(), "B".to_string()];
        let selected = HashMap::from([(
            ConditionListFilterColumn::Categories,
            ["B".to_string()].into_iter().collect(),
        )]);
        assert!(condition_matches_list_filters(&condition, &selected));
    }

    #[test]
    fn build_filter_options_counts_numeric_values() {
        let mut a = empty_condition("a");
        a.text_groups.push(Default::default());
        let b = empty_condition("b");
        let options = build_condition_list_filter_options(&[a, b]);
        let text_groups = options
            .get(&ConditionListFilterColumn::TextGroupCount)
            .expect("missing text group options");
        assert_eq!(text_groups[0].value, "0");
        assert_eq!(text_groups[0].count, 1);
        assert_eq!(text_groups[1].value, "1");
        assert_eq!(text_groups[1].count, 1);
    }
}
