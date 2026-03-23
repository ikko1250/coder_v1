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

use crate::model::{AnalysisRecord, FilterColumn, FilterOption};
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
        clamp_selected_row, CloseBlockReason, CoreOutput, ViewerCoreCloseInput, ViewerCoreMessage,
        ViewerCoreState,
    };
    use crate::model::FilterColumn;

    #[test]
    fn viewer_core_state_defaults() {
        let core = ViewerCoreState::default();
        assert!(core.all_records.is_empty());
        assert!(core.expected_job_id.is_none());
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
}
