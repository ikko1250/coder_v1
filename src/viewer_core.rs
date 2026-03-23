//! 条例分析ビューアのドメインコア（P2）。
//!
//! **egui / eframe に依存しない**型・状態のみを置く。UI アダプタ（[`crate::app`]）が [`ViewerCoreState`] を保持する。
//!
//! - **P2-01**: 足場のみ。
//! - **P2-02**: レコード・フィルタ・選択の状態を本構造体へ集約。

use crate::model::{AnalysisRecord, FilterColumn, FilterOption};
use std::collections::{BTreeSet, HashMap};

/// 一覧・フィルタ・選択の状態（`App` が保持）。
#[derive(Debug)]
pub struct ViewerCoreState {
    pub(crate) all_records: Vec<AnalysisRecord>,
    pub(crate) filtered_indices: Vec<usize>,
    pub(crate) filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    pub(crate) selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    pub(crate) filter_candidate_queries: HashMap<FilterColumn, String>,
    pub(crate) active_filter_column: FilterColumn,
    pub(crate) selected_row: Option<usize>,
}

impl Default for ViewerCoreState {
    fn default() -> Self {
        Self {
            all_records: Vec::new(),
            filtered_indices: Vec::new(),
            filter_options: HashMap::new(),
            selected_filter_values: HashMap::new(),
            filter_candidate_queries: HashMap::new(),
            active_filter_column: FilterColumn::MatchedCategories,
            selected_row: None,
        }
    }
}

/// フィルタ後の行インデックスを `filtered_len` の範囲にクランプする。
pub(crate) fn clamp_selected_row(selected_row: Option<usize>, filtered_len: usize) -> Option<usize> {
    match (selected_row, filtered_len) {
        (_, 0) => None,
        (Some(idx), len) => Some(idx.min(len - 1)),
        (None, _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{clamp_selected_row, ViewerCoreState};

    #[test]
    fn viewer_core_state_defaults() {
        let core = ViewerCoreState::default();
        assert!(core.all_records.is_empty());
        assert_eq!(clamp_selected_row(Some(5), 3), Some(2));
    }
}
