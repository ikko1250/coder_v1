//! 条例分析ビューアのドメインコア（P2）。
//!
//! **egui / eframe に依存しない**型・状態のみを置く。UI アダプタ（[`crate::app`]）が [`ViewerCoreState`] を保持する。
//!
//! - **P2-01**: 足場のみ。
//! - **P2-02**: レコード・フィルタ・選択の状態を本構造体へ集約。
//! - **P2-03**: [`ViewerCoreMessage`] でユーザー操作・ジョブ完了に伴うコア更新を型で表現。
//! - **P2-04**: `App::apply_event` の戻り値 [`CoreOutput`] に `needs_repaint`（設計 §5.5）。
//! - **P2-05**: `expected_job_id` で非同期ジョブの **有効 ID** を保持し、完了イベントの検証に使う（設計 §5.3）。
//! - **P2-06**: `can_close` / `CloseBlockReason`（設計 §5.4）。
//! - **P2-07**: `SegmentCacheInvalidateReason` と `invalidate_detail_segment_cache`（詳細ペインのセグメントキャッシュ）。
//! - **P2-08**: `recompute_filtered_indices` と `clamp_selected_row_to_filtered_len`。
//! - **P2-09**: `data_source_generation`（CSV／分析結果の世代カウンタ、§11.4）。

use crate::model::{AnalysisRecord, FilterColumn, FilterOption, TextSegment};
use std::collections::{BTreeSet, HashMap};

/// コア更新後にホスト（egui）へ伝える **副作用ヒント**（再描画など）。`egui` 非依存。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CoreOutput {
    /// 論理状態が変化し、当該フレームまたは直後に再描画するとよい場合に `true`。
    pub needs_repaint: bool,
}

/// P2-04: [`ViewerCoreMessage`] と同じ型。`apply_event` の引数名に合わせたエイリアス。
pub type ViewerCoreEvent = ViewerCoreMessage;

/// 終了をブロックする理由（ホストは `egui` のキャンセル等で対応する）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloseBlockReason {
    UnsavedConditionEditor,
}

/// ホストが収集して [`ViewerCoreState::can_close`] に渡す、終了判定に必要な入力（egui 非依存）。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ViewerCoreCloseInput {
    pub(crate) condition_editor_dirty: bool,
}

/// 詳細ペインの `detail_segment_cache` を無効化する **経路**（P2-07 / P1-09）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentCacheInvalidateReason {
    /// `replace_records`（CSV・分析ジョブ完了など）
    ReplaceRecords,
    /// 一覧の選択行が変わった
    SelectionChanged,
    /// フィルタ再計算で一覧の行集合が変わる
    FilterApplied,
    /// 手動アノテーションを選択行に反映した直後
    AnnotationSaved,
}

/// 一覧・フィルタ・選択まわりの **意図**（UI・ホストからコアへ）。
///
/// `egui` 型を含まない。ファイル I/O・子プロセス起動などは [`crate::app::App`] 側の責務。
#[derive(Debug)]
pub enum ViewerCoreMessage {
    /// CSV 読込成功・分析ジョブ完了など、全レコードを差し替える。
    ReplaceRecords {
        records: Vec<AnalysisRecord>,
        source_label: String,
    },
    /// 一覧のキーボード ↑（抽出行インデックス）。
    SelectionMoveUp,
    /// 一覧のキーボード ↓。
    SelectionMoveDown,
    /// 一覧行クリック（`filtered_indices` 上のインデックス）。
    SelectionSetFilteredRow { filtered_index: usize },
    /// フィルタ候補のオン/オフ。
    FilterToggle {
        column: FilterColumn,
        value: String,
        selected: bool,
    },
    /// 現在列など、列単位でフィルタ解除。
    FilterClearColumn(FilterColumn),
    /// 全フィルタ解除。
    FilterClearAll,
}

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
    /// 現在「この ID の完了だけ受け入れる」分析／エクスポートジョブ。CSV 再読込などで [`Self::clear_expected_job_id`] する。
    pub(crate) expected_job_id: Option<String>,
    /// 詳細ペイン用。第 1 要素は `AnalysisRecord::row_no`。[`Self::invalidate_detail_segment_cache`] で明示的に無効化する。
    pub(crate) detail_segment_cache: Option<(usize, Vec<TextSegment>)>,
    /// メインのレコード集合が置き換わるたびに増える（§11.4）。サブウィンドウは読み込み時の値と比較する。
    pub(crate) data_source_generation: u64,
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
            expected_job_id: None,
            detail_segment_cache: None,
            data_source_generation: 0,
        }
    }
}

impl ViewerCoreState {
    pub(crate) fn set_expected_job_id(&mut self, job_id: String) {
        self.expected_job_id = Some(job_id);
    }

    pub(crate) fn clear_expected_job_id(&mut self) {
        self.expected_job_id = None;
    }

    pub(crate) fn job_id_matches_expected(&self, job_id: &str) -> bool {
        self.expected_job_id.as_deref() == Some(job_id)
    }

    /// `meta` が無い失敗は、期待 ID が無いときは破棄（遅延メッセージ）、あるときは同一チャネル由来とみなして受理する。
    pub(crate) fn accept_failure_without_meta_job_id(&self) -> bool {
        self.expected_job_id.is_some()
    }

    /// アプリ終了がドメイン上許容されるか。ブロック時は [`CloseBlockReason`] を返す（§5.4）。
    #[allow(clippy::unused_self)]
    pub(crate) fn can_close(&self, input: &ViewerCoreCloseInput) -> Result<(), CloseBlockReason> {
        if input.condition_editor_dirty {
            return Err(CloseBlockReason::UnsavedConditionEditor);
        }
        Ok(())
    }

    /// 詳細ペインのタグ付きテキストセグメントキャッシュを無効化する。理由はログ・デバッグ用に列挙子で渡す。
    pub(crate) fn invalidate_detail_segment_cache(&mut self, reason: SegmentCacheInvalidateReason) {
        let _ = reason;
        self.detail_segment_cache = None;
    }

    pub(crate) fn set_detail_segment_cache(&mut self, row_no: usize, segments: Vec<TextSegment>) {
        self.detail_segment_cache = Some((row_no, segments));
    }

    /// 現在のフィルタ選択に基づき `filtered_indices` を再計算する（P2-08）。
    pub(crate) fn recompute_filtered_indices(&mut self) {
        self.filtered_indices = self
            .all_records
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| self.record_matches_filters(record).then_some(idx))
            .collect();
    }

    pub(crate) fn record_matches_filters(&self, record: &AnalysisRecord) -> bool {
        self.selected_filter_values
            .iter()
            .all(|(column, selected)| column.matches(record, selected))
    }

    /// `selected_row` を `filtered_indices.len()` に整合させる（`None` または `0..len`）。
    pub(crate) fn clamp_selected_row_to_filtered_len(&mut self) {
        self.selected_row = clamp_selected_row(self.selected_row, self.filtered_indices.len());
    }

    /// CSV／分析結果で `all_records` を差し替えた直後に呼ぶ。単調に増加する。
    pub(crate) fn bump_data_source_generation(&mut self) {
        self.data_source_generation = self.data_source_generation.wrapping_add(1);
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
    use super::{
        clamp_selected_row, CloseBlockReason, CoreOutput, SegmentCacheInvalidateReason,
        ViewerCoreCloseInput, ViewerCoreMessage, ViewerCoreState,
    };
    use crate::model::AnalysisRecord;
    use crate::model::AnalysisUnit;
    use crate::model::FilterColumn;
    use crate::model::TextSegment;
    use std::collections::HashMap;

    fn make_minimal_paragraph_record(row_no: usize) -> AnalysisRecord {
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

    fn dummy_segment(text: &str) -> TextSegment {
        TextSegment {
            text: text.to_string(),
            is_hit: false,
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn viewer_core_state_defaults() {
        let core = ViewerCoreState::default();
        assert!(core.all_records.is_empty());
        assert!(core.expected_job_id.is_none());
        assert!(core.detail_segment_cache.is_none());
        assert_eq!(core.data_source_generation, 0);
        assert_eq!(clamp_selected_row(Some(5), 3), Some(2));
    }

    #[test]
    fn stale_job_id_is_rejected() {
        let mut core = ViewerCoreState::default();
        core.set_expected_job_id("job-b".into());
        assert!(!core.job_id_matches_expected("job-a"));
    }

    #[test]
    fn matching_job_id_is_accepted() {
        let mut core = ViewerCoreState::default();
        core.set_expected_job_id("job-a".into());
        assert!(core.job_id_matches_expected("job-a"));
    }

    #[test]
    fn clear_expected_invalidates_job() {
        let mut core = ViewerCoreState::default();
        core.set_expected_job_id("x".into());
        core.clear_expected_job_id();
        assert!(!core.job_id_matches_expected("x"));
    }

    #[test]
    fn can_close_when_condition_editor_clean() {
        let core = ViewerCoreState::default();
        let input = ViewerCoreCloseInput {
            condition_editor_dirty: false,
        };
        assert!(core.can_close(&input).is_ok());
    }

    #[test]
    fn cannot_close_when_condition_editor_dirty() {
        let core = ViewerCoreState::default();
        let input = ViewerCoreCloseInput {
            condition_editor_dirty: true,
        };
        assert_eq!(
            core.can_close(&input),
            Err(CloseBlockReason::UnsavedConditionEditor)
        );
    }

    #[test]
    fn core_output_default_is_no_repaint() {
        assert!(!CoreOutput::default().needs_repaint);
    }

    #[test]
    fn viewer_core_message_variants_constructible() {
        let _: ViewerCoreMessage = ViewerCoreMessage::ReplaceRecords {
            records: Vec::new(),
            source_label: "test".into(),
        };
        let _: ViewerCoreMessage = ViewerCoreMessage::SelectionMoveUp;
        let _: ViewerCoreMessage = ViewerCoreMessage::SelectionMoveDown;
        let _: ViewerCoreMessage = ViewerCoreMessage::SelectionSetFilteredRow { filtered_index: 0 };
        let _: ViewerCoreMessage = ViewerCoreMessage::FilterToggle {
            column: FilterColumn::MatchedCategories,
            value: "a".into(),
            selected: true,
        };
        let _: ViewerCoreMessage = ViewerCoreMessage::FilterClearColumn(FilterColumn::MunicipalityName);
        let _: ViewerCoreMessage = ViewerCoreMessage::FilterClearAll;
    }

    #[test]
    fn detail_segment_cache_cleared_on_invalidate_does_not_reuse_stale_row() {
        let mut core = ViewerCoreState::default();
        core.set_detail_segment_cache(1, vec![dummy_segment("row1-only")]);
        assert!(core.detail_segment_cache.as_ref().is_some_and(|(r, _)| *r == 1));

        core.invalidate_detail_segment_cache(SegmentCacheInvalidateReason::SelectionChanged);
        assert!(core.detail_segment_cache.is_none());

        // 別行を表示するときに、古い row_no のキャッシュが残っていない
        core.set_detail_segment_cache(2, vec![dummy_segment("row2")]);
        assert_eq!(
            core.detail_segment_cache.as_ref().map(|(r, _)| *r),
            Some(2)
        );
    }

    #[test]
    fn segment_cache_invalidate_reasons_are_distinct_paths() {
        let mut core = ViewerCoreState::default();
        core.set_detail_segment_cache(1, vec![dummy_segment("x")]);
        core.invalidate_detail_segment_cache(SegmentCacheInvalidateReason::ReplaceRecords);
        assert!(core.detail_segment_cache.is_none());

        core.set_detail_segment_cache(1, vec![dummy_segment("y")]);
        core.invalidate_detail_segment_cache(SegmentCacheInvalidateReason::FilterApplied);
        assert!(core.detail_segment_cache.is_none());

        core.set_detail_segment_cache(1, vec![dummy_segment("z")]);
        core.invalidate_detail_segment_cache(SegmentCacheInvalidateReason::AnnotationSaved);
        assert!(core.detail_segment_cache.is_none());
    }

    #[test]
    fn recompute_filtered_indices_empty_records() {
        let mut core = ViewerCoreState::default();
        core.recompute_filtered_indices();
        assert!(core.filtered_indices.is_empty());
    }

    #[test]
    fn recompute_filtered_indices_with_no_active_filters_includes_all_indices() {
        let mut core = ViewerCoreState::default();
        core.all_records = vec![
            make_minimal_paragraph_record(1),
            make_minimal_paragraph_record(2),
        ];
        core.recompute_filtered_indices();
        assert_eq!(core.filtered_indices, vec![0, 1]);
    }

    #[test]
    fn clamp_selected_row_respects_filtered_len_invariant() {
        let mut core = ViewerCoreState::default();
        core.filtered_indices = vec![10, 20, 30, 40, 50];
        core.selected_row = Some(100);
        core.clamp_selected_row_to_filtered_len();
        assert_eq!(core.selected_row, Some(4));
    }

    #[test]
    fn clamp_selected_row_none_when_filtered_empty() {
        let mut core = ViewerCoreState::default();
        core.filtered_indices = vec![];
        core.selected_row = Some(0);
        core.clamp_selected_row_to_filtered_len();
        assert_eq!(core.selected_row, None);
    }

    #[test]
    fn recompute_then_clamp_keeps_selected_index_in_range() {
        let mut core = ViewerCoreState::default();
        core.all_records = vec![make_minimal_paragraph_record(1)];
        core.selected_row = Some(100);
        core.recompute_filtered_indices();
        assert_eq!(core.filtered_indices, vec![0]);
        core.clamp_selected_row_to_filtered_len();
        assert_eq!(core.selected_row, Some(0));
    }

    #[test]
    fn data_source_generation_bumps_monotonically() {
        let mut core = ViewerCoreState::default();
        assert_eq!(core.data_source_generation, 0);
        core.bump_data_source_generation();
        assert_eq!(core.data_source_generation, 1);
        core.bump_data_source_generation();
        assert_eq!(core.data_source_generation, 2);
    }
}
