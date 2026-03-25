//! 条例分析ビューアのメイン UI（egui）。
//!
//! **`impl App` の機能別メソッド一覧と切り出し候補モジュール**は P1-01 として
//! [`docs/p1-01-app-impl-inventory.md`](../docs/p1-01-app-impl-inventory.md) に記載する。
//! **`all_records` / `filtered_indices` / `selected_row` の変更経路**は P1-08 として
//! [`docs/p1-08-record-selection-mutation-paths.md`](../docs/p1-08-record-selection-mutation-paths.md) に記載する。
//! **詳細ペインのセグメントキャッシュ**は P1-09 として
//! [`docs/p1-09-cache-invalidation-paths.md`](../docs/p1-09-cache-invalidation-paths.md) に記載し、P2-07 で
//! [`docs/p2-07-segment-cache-invalidation.md`](../docs/p2-07-segment-cache-invalidation.md) のとおりコア側で無効化経路を明示する。
//! **副作用の境界（コア候補 / ホスト必須）**は P1-10 として
//! [`docs/p1-10-side-effect-boundaries.md`](../docs/p1-10-side-effect-boundaries.md) に記載する。
//! **公開 API・可視性の整理**は P1-11 として
//! [`docs/p1-11-public-api-review.md`](../docs/p1-11-public-api-review.md) に記載する。
//! **ドメインコア（egui 非依存）の足場**は P2-01 として [`crate::viewer_core`]（`src/viewer_core.rs`）、
//! 説明は [`docs/p2-01-viewer-core.md`](../docs/p2-01-viewer-core.md)。
//! **一覧・フィルタ・選択のドメイン状態の集約**は P2-02 として [`docs/p2-02-viewer-core-domain-state.md`](../docs/p2-02-viewer-core-domain-state.md)。
//! **コア更新の列挙型（`ViewerCoreMessage`）**は P2-03 として [`docs/p2-03-viewer-core-message.md`](../docs/p2-03-viewer-core-message.md)。
//! **`apply_event` → `CoreOutput`（`needs_repaint`）**は P2-04 として [`docs/p2-04-core-output.md`](../docs/p2-04-core-output.md)。
//! **ジョブ ID と `ViewerCoreState::expected_job_id`** は P2-05 として [`docs/p2-05-job-id-validation.md`](../docs/p2-05-job-id-validation.md)。
//! **`can_close` / `CloseBlockReason`** は P2-06 として [`docs/p2-06-can-close.md`](../docs/p2-06-can-close.md)。
//! **詳細ペインの `detail_segment_cache` 無効化**は P2-07 として [`docs/p2-07-segment-cache-invalidation.md`](../docs/p2-07-segment-cache-invalidation.md)。
//! **`filtered_indices` 再計算と選択クランプ**は P2-08 として [`docs/p2-08-filter-selection-core.md`](../docs/p2-08-filter-selection-core.md)。
//! **データソース世代 `data_source_generation`** は P2-09 として [`docs/p2-09-data-source-generation.md`](../docs/p2-09-data-source-generation.md)。
//! **`filter` / `csv_loader` のユニットテスト** は P2-10 として [`docs/p2-10-filter-csv-loader-tests.md`](../docs/p2-10-filter-csv-loader-tests.md)。
//! **ファイルダイアログのホスト抽象（`rfd` の閉じ込め）** は P3-01 として [`docs/p3-01-file-dialog-host.md`](../docs/p3-01-file-dialog-host.md)。
//! **分析ジョブ起動のホスト抽象**（`spawn_analysis_job` 等の集約）は P3-02 として [`docs/p3-02-analysis-process-host.md`](../docs/p3-02-analysis-process-host.md)。
//! **ジョブ受信→`apply_event` パイプラインの一本化**は P3-03 として [`docs/p3-03-update-event-pipeline.md`](../docs/p3-03-update-event-pipeline.md)。
//! **ホスト起動設定（フォント／ウィンドウタイトル）の集約**は P3-04 として [`docs/p3-04-host-startup-config.md`](../docs/p3-04-host-startup-config.md)。
//! **ログ出力インタフェースの導入**は P3-05 として [`docs/p3-05-app-logger-interface.md`](../docs/p3-05-app-logger-interface.md)。
//! **IPC DTO（Command/Event）の serde 定義**は P4-01 として [`docs/p4-01-ipc-dto.md`](../docs/p4-01-ipc-dto.md)。
//! **IPC エラー DTO（`code + message + job_id?`）**は P4-02 として [`docs/p4-02-ipc-error-shape.md`](../docs/p4-02-ipc-error-shape.md)。
//! **`api_version` 運用方針と互換判定**は P4-03 として [`docs/p4-03-api-version-policy.md`](../docs/p4-03-api-version-policy.md)。
//! **Tauri 未導入時の DTO 自己検証 CLI** は P4-04 として [`docs/p4-04-ipc-dto-self-check-cli.md`](../docs/p4-04-ipc-dto-self-check-cli.md)。
//! **ブレークチェンジ時の運用手順**は P4-05 として [`docs/p4-05-breaking-change-procedure.md`](../docs/p4-05-breaking-change-procedure.md)。
//! **workspace 構成（ルート + `src-tauri`）**は P5-01 として [`docs/p5-01-workspace-layout.md`](../docs/p5-01-workspace-layout.md)。
//! **最小フロント + invoke で P4 DTO 読み取り**は P5-02 として [`docs/p5-02-minimal-front-invoke.md`](../docs/p5-02-minimal-front-invoke.md)。
//! **Windows 開発時のビルド手順**は P5-03 として [`docs/p5-03-windows-dev-build-steps.md`](../docs/p5-03-windows-dev-build-steps.md)。
//! **本番移行判断のレビュー記録**は P5-04 として [`docs/p5-04-production-migration-review.md`](../docs/p5-04-production-migration-review.md)。
//!
//! トップツールバーは [`app_toolbar`](app_toolbar) サブモジュール（`src/app_toolbar.rs`）。
//! DB 参照ウィンドウは [`app_db_viewer`](app_db_viewer) サブモジュール（`src/app_db_viewer.rs`）。
//! 分析設定ウィンドウは [`app_analysis_settings`](app_analysis_settings)（`src/app_analysis_settings.rs`）。
//! 分析ジョブ・警告一覧は [`app_analysis_job`](app_analysis_job)（`src/app_analysis_job.rs`）。
//! 中央ペイン（フィルタ・一覧・詳細）は [`app_main_layout`](app_main_layout)（`src/app_main_layout.rs`）。
//! エラーダイアログは [`app_error_dialog`](app_error_dialog)（`src/app_error_dialog.rs`）。
//! 条件 JSON エディタは [`app_condition_editor`](app_condition_editor)（`src/app_condition_editor.rs`）。
//! フレーム先頭（ジョブポーリング・キーボード・終了ガード）は [`app_lifecycle`](app_lifecycle)（`src/app_lifecycle.rs`）。

#[path = "app_toolbar.rs"]
mod app_toolbar;

#[path = "app_db_viewer.rs"]
mod app_db_viewer;

#[path = "app_analysis_settings.rs"]
mod app_analysis_settings;

#[path = "app_analysis_job.rs"]
mod app_analysis_job;

#[path = "app_main_layout.rs"]
mod app_main_layout;

#[path = "app_error_dialog.rs"]
mod app_error_dialog;

#[path = "app_condition_editor.rs"]
mod app_condition_editor;

#[path = "app_lifecycle.rs"]
mod app_lifecycle;

use crate::analysis_runner::{
    build_runtime_config, resolve_annotation_csv_path, AnalysisJobEvent, AnalysisRuntimeConfig,
    AnalysisRuntimeOverrides, AnalysisWarningMessage,
};
use crate::analysis_session_cache::{AnalysisResultSnapshot, AnalysisSessionCacheKey};
use crate::analysis_process_host::{AnalysisProcessHost, ThreadAnalysisProcessHost};
use crate::app_logger::{AppLogger, StderrAppLogger};
use crate::csv_loader::load_records;
use crate::file_dialog_host::{FileDialogHost, RfdFileDialogHost};
use crate::db::resolve_default_db_path;
use crate::filter::build_filter_options;
use crate::manual_annotation_store::{
    append_manual_annotation_namespaces_text, append_manual_annotation_pairs_text,
    append_manual_annotation_row, build_manual_annotation_pair, first_manual_annotation_line,
    increment_manual_annotation_count, ManualAnnotationAppendRow,
};
use crate::model::{AnalysisRecord, DbViewerState, FilterColumn, TextSegment};
use crate::tagged_text::parse_tagged_text;
use crate::viewer_core::{
    clamp_selected_row, CoreOutput, SegmentCacheInvalidateReason, ViewerCoreEvent, ViewerCoreMessage,
    ViewerCoreState,
};
use eframe::egui;
use egui::Ui;
use egui_extras::Column;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TreeScrollRequest {
    row_index: usize,
    align: Option<egui::Align>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScrollBehavior {
    None,
    KeepVisible,
    AlignMin,
    AlignMax,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectionChange {
    selected_row: Option<usize>,
    scroll_behavior: ScrollBehavior,
}

#[derive(Clone, Copy)]
struct TreeColumnSpec {
    header: &'static str,
    build_column: fn() -> Column,
    value: fn(&AnalysisRecord) -> String,
}

const TREE_COLUMN_SPECS: &[TreeColumnSpec] = &[
    TreeColumnSpec {
        header: "No",
        build_column: build_tree_row_no_column,
        value: tree_row_no_value,
    },
    TreeColumnSpec {
        header: "unit_id",
        build_column: build_tree_paragraph_id_column,
        value: tree_paragraph_id_value,
    },
    TreeColumnSpec {
        header: "自治体",
        build_column: build_tree_municipality_column,
        value: tree_municipality_value,
    },
    TreeColumnSpec {
        header: "条例/規則",
        build_column: build_tree_ordinance_column,
        value: tree_ordinance_value,
    },
    TreeColumnSpec {
        header: "カテゴリ",
        build_column: build_tree_category_column,
        value: tree_category_value,
    },
    TreeColumnSpec {
        header: "annotation",
        build_column: build_tree_annotation_column,
        value: tree_annotation_value,
    },
    TreeColumnSpec {
        header: "強調token数",
        build_column: build_tree_annotated_token_count_column,
        value: tree_annotated_token_count_value,
    },
];

const RECORD_LIST_PANEL_MIN_WIDTH: f32 = 360.0;
const RECORD_LIST_PANEL_DEFAULT_RATIO: f32 = 0.33;
const RECORD_LIST_PANEL_MAX_RATIO: f32 = 0.85;

struct RunningAnalysisJob {
    receiver: Receiver<AnalysisJobEvent>,
}

#[derive(Clone)]
struct AnalysisExportContext {
    db_path: PathBuf,
    filter_config_path: PathBuf,
    annotation_csv_path: PathBuf,
}

enum AnalysisJobStatus {
    Idle,
    RunningAnalysis { job_id: String },
    RunningExport { job_id: String },
    Succeeded { summary: String },
    Failed { summary: String },
}

struct AnalysisRuntimeState {
    runtime: Option<AnalysisRuntimeConfig>,
    current_job: Option<RunningAnalysisJob>,
    status: AnalysisJobStatus,
    last_warnings: Vec<AnalysisWarningMessage>,
    warning_window_open: bool,
    last_export_context: Option<AnalysisExportContext>,
    /// 同一セッション内の分析結果再利用（`document/session-scoped-analysis-result-reuse-design.md`）。
    session_analysis_cache: Option<(AnalysisSessionCacheKey, AnalysisResultSnapshot)>,
}

impl AnalysisRuntimeState {
    fn from_runtime(runtime: Result<AnalysisRuntimeConfig, String>) -> Self {
        match runtime {
            Ok(runtime) => Self {
                runtime: Some(runtime),
                current_job: None,
                status: AnalysisJobStatus::Idle,
                last_warnings: Vec::new(),
                warning_window_open: false,
                last_export_context: None,
                session_analysis_cache: None,
            },
            Err(error) => Self {
                runtime: None,
                current_job: None,
                status: AnalysisJobStatus::Failed { summary: error },
                last_warnings: Vec::new(),
                warning_window_open: false,
                last_export_context: None,
                session_analysis_cache: None,
            },
        }
    }

    fn can_start(&self) -> bool {
        self.runtime.is_some() && self.current_job.is_none()
    }

    fn status_text(&self) -> String {
        match &self.status {
            AnalysisJobStatus::Idle => "分析待機中".to_string(),
            AnalysisJobStatus::RunningAnalysis { job_id } => format!("分析実行中: {job_id}"),
            AnalysisJobStatus::RunningExport { job_id } => format!("CSV 保存中: {job_id}"),
            AnalysisJobStatus::Succeeded { summary } => format!("分析成功: {summary}"),
            AnalysisJobStatus::Failed { summary } => format!("分析失敗: {summary}"),
        }
    }

    fn has_warning_details(&self) -> bool {
        !self.last_warnings.is_empty()
    }

    fn can_export(&self) -> bool {
        self.runtime.is_some() && self.current_job.is_none() && self.last_export_context.is_some()
    }
}

#[derive(Default)]
struct AnalysisRequestState {
    python_path_override: Option<PathBuf>,
    filter_config_path_override: Option<PathBuf>,
    annotation_csv_path_override: Option<PathBuf>,
    settings_window_open: bool,
}

impl AnalysisRequestState {
    fn runtime_overrides(&self) -> AnalysisRuntimeOverrides {
        AnalysisRuntimeOverrides {
            python_path: self.python_path_override.clone(),
            filter_config_path: self.filter_config_path_override.clone(),
            annotation_csv_path: self.annotation_csv_path_override.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AnnotationEditorState {
    namespace_input: String,
    key_input: String,
    value_input: String,
    tagged_by_input: String,
    confidence_input: String,
    note_input: String,
    status_message: Option<String>,
    status_is_error: bool,
}

impl SelectionChange {
    fn new(selected_row: Option<usize>, scroll_behavior: ScrollBehavior) -> Self {
        Self {
            selected_row,
            scroll_behavior,
        }
    }

    fn first_filtered_row(filtered_len: usize, scroll_behavior: ScrollBehavior) -> Self {
        Self::new((filtered_len > 0).then_some(0), scroll_behavior)
    }
}

fn build_tree_scroll_request(
    selected_row: Option<usize>,
    scroll_behavior: ScrollBehavior,
) -> Option<TreeScrollRequest> {
    match scroll_behavior {
        ScrollBehavior::None => None,
        ScrollBehavior::KeepVisible => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: None,
        }),
        ScrollBehavior::AlignMin => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: Some(egui::Align::Min),
        }),
        ScrollBehavior::AlignMax => selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align: Some(egui::Align::Max),
        }),
    }
}

pub(crate) struct App {
    file_dialog_host: Box<dyn FileDialogHost>,
    analysis_process_host: Box<dyn AnalysisProcessHost>,
    pub(crate) logger: Box<dyn AppLogger>,
    records_source_label: String,
    db_viewer_state: DbViewerState,
    analysis_request_state: AnalysisRequestState,
    analysis_runtime_state: AnalysisRuntimeState,
    pub(crate) core: ViewerCoreState,
    pending_tree_scroll: Option<TreeScrollRequest>,
    pub(crate) error_message: Option<String>,
    annotation_editor_state: AnnotationEditorState,
    condition_editor_state: app_condition_editor::ConditionEditorState,
    record_list_panel_ratio: f32,
    annotation_panel_expanded: bool,
}

impl App {
    pub(crate) fn new(initial_csv_path: Option<PathBuf>) -> Self {
        let analysis_request_state = AnalysisRequestState::default();
        let runtime = build_runtime_config(&analysis_request_state.runtime_overrides());
        let mut app = Self {
            file_dialog_host: Box::new(RfdFileDialogHost),
            analysis_process_host: Box::new(ThreadAnalysisProcessHost),
            logger: Box::new(StderrAppLogger),
            records_source_label: "分析結果なし".to_string(),
            db_viewer_state: DbViewerState::new(resolve_default_db_path()),
            analysis_request_state,
            analysis_runtime_state: AnalysisRuntimeState::from_runtime(runtime),
            core: ViewerCoreState::default(),
            pending_tree_scroll: None,
            error_message: None,
            annotation_editor_state: AnnotationEditorState::default(),
            condition_editor_state: app_condition_editor::ConditionEditorState::default(),
            record_list_panel_ratio: RECORD_LIST_PANEL_DEFAULT_RATIO,
            annotation_panel_expanded: false,
        };
        app.try_cleanup_analysis_jobs();
        if let Some(csv_path) = initial_csv_path {
            let _ = app.load_csv(csv_path);
        }
        app
    }

    /// CSV 読込成功時は [`CoreOutput`] を返す（失敗時は `None`）。ホストが `needs_repaint` に応じて `request_repaint` できる。
    fn load_csv(&mut self, path: PathBuf) -> Option<CoreOutput> {
        match load_records(&path) {
            Ok(records) => {
                // 進行中ジョブの遅延完了で上書きされないよう、データソース切替前に期待 ID を無効化（§5.3）。
                self.core.clear_expected_job_id();
                let out = self.apply_event(ViewerCoreMessage::ReplaceRecords {
                    records,
                    source_label: path.display().to_string(),
                });
                self.analysis_runtime_state.last_export_context = None;
                app_analysis_job::invalidate_session_analysis_cache(self);
                Some(out)
            }
            Err(e) => {
                self.error_message = Some(e);
                None
            }
        }
    }

    /// P2-04: 一覧・フィルタ・選択の **型付き**更新。[`crate::viewer_core::CoreOutput`] の `needs_repaint` で再描画要否を返す（設計 §5.5）。
    pub(crate) fn apply_event(&mut self, event: ViewerCoreEvent) -> CoreOutput {
        let needs_repaint = match event {
            ViewerCoreMessage::ReplaceRecords {
                records,
                source_label,
            } => {
                self.replace_records(records, source_label);
                true
            }
            ViewerCoreMessage::SelectionMoveUp => self.move_selection_up(),
            ViewerCoreMessage::SelectionMoveDown => self.move_selection_down(),
            ViewerCoreMessage::SelectionSetFilteredRow { filtered_index } => self.apply_selection_change(
                SelectionChange::new(Some(filtered_index), ScrollBehavior::KeepVisible),
            ),
            ViewerCoreMessage::FilterToggle {
                column,
                value,
                selected,
            } => self.toggle_filter_value(column, &value, selected),
            ViewerCoreMessage::FilterClearColumn(column) => self.clear_filters_for_column(column),
            ViewerCoreMessage::FilterClearAll => self.clear_all_filters(),
        };
        CoreOutput { needs_repaint }
    }

    fn replace_records(&mut self, records: Vec<AnalysisRecord>, source_label: String) {
        self.core.all_records = records;
        self.records_source_label = source_label;
        self.db_viewer_state.reset_loaded_state();
        self.core.filter_options = build_filter_options(&self.core.all_records);
        self.core.selected_filter_values.clear();
        self.core.filter_candidate_queries.clear();
        self.core.recompute_filtered_indices();
        self.core.bump_data_source_generation();
        self.core
            .invalidate_detail_segment_cache(SegmentCacheInvalidateReason::ReplaceRecords);
        self.apply_selection_change(SelectionChange::first_filtered_row(
            self.core.filtered_indices.len(),
            ScrollBehavior::AlignMin,
        ));
        self.error_message = None;
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    fn apply_selection_change(&mut self, change: SelectionChange) -> bool {
        let next = clamp_selected_row(change.selected_row, self.core.filtered_indices.len());
        let selection_changed = self.core.selected_row != next;
        if selection_changed {
            self.core.selected_row = next;
            self.core
                .invalidate_detail_segment_cache(SegmentCacheInvalidateReason::SelectionChanged);
            self.clear_annotation_editor_status();
        }

        let next_scroll_request = build_tree_scroll_request(next, change.scroll_behavior);
        let scroll_changed = self.pending_tree_scroll != next_scroll_request;
        self.pending_tree_scroll = next_scroll_request;

        selection_changed || scroll_changed
    }

    fn select_first_filtered_row(&mut self, scroll_behavior: ScrollBehavior) -> bool {
        self.apply_selection_change(SelectionChange::first_filtered_row(
            self.core.filtered_indices.len(),
            scroll_behavior,
        ))
    }

    fn move_selection_up(&mut self) -> bool {
        if self.core.filtered_indices.is_empty() {
            return false;
        }

        match self.core.selected_row {
            Some(idx) if idx > 0 => self.apply_selection_change(SelectionChange::new(
                Some(idx - 1),
                ScrollBehavior::KeepVisible,
            )),
            None => self.select_first_filtered_row(ScrollBehavior::AlignMin),
            _ => false,
        }
    }

    fn move_selection_down(&mut self) -> bool {
        let current_len = self.core.filtered_indices.len();
        if current_len == 0 {
            return false;
        }

        match self.core.selected_row {
            Some(idx) if idx + 1 < current_len => self.apply_selection_change(SelectionChange::new(
                Some(idx + 1),
                ScrollBehavior::KeepVisible,
            )),
            None => self.select_first_filtered_row(ScrollBehavior::AlignMin),
            _ => false,
        }
    }

    fn selected_record(&self) -> Option<&AnalysisRecord> {
        let filtered_idx = self.core.selected_row?;
        let record_idx = *self.core.filtered_indices.get(filtered_idx)?;
        self.core.all_records.get(record_idx)
    }

    fn selected_record_index(&self) -> Option<usize> {
        let filtered_idx = self.core.selected_row?;
        self.core.filtered_indices.get(filtered_idx).copied()
    }

    fn selected_record_mut(&mut self) -> Option<&mut AnalysisRecord> {
        let record_idx = self.selected_record_index()?;
        self.core.all_records.get_mut(record_idx)
    }

    fn resolved_annotation_csv_path(&self) -> Result<PathBuf, String> {
        resolve_annotation_csv_path(&self.analysis_request_state.runtime_overrides())
    }

    fn annotation_save_enabled(&self) -> bool {
        self.selected_record()
            .is_some_and(AnalysisRecord::supports_manual_annotation)
            && self.analysis_runtime_state.current_job.is_none()
    }

    fn clear_annotation_editor_status(&mut self) {
        self.annotation_editor_state.status_message = None;
        self.annotation_editor_state.status_is_error = false;
    }

    fn clear_annotation_editor_inputs(&mut self) {
        self.annotation_editor_state.value_input.clear();
        self.annotation_editor_state.confidence_input.clear();
        self.annotation_editor_state.note_input.clear();
    }

    fn build_annotation_append_row(&self) -> Result<ManualAnnotationAppendRow, String> {
        let record = self
            .selected_record()
            .ok_or_else(|| "レコードが選択されていません".to_string())?;
        if !record.supports_manual_annotation() {
            return Err("manual annotation は paragraph 行のみ対応です".to_string());
        }

        let paragraph_id = record.paragraph_id.trim();
        if paragraph_id.is_empty() {
            return Err("paragraph_id が空のため annotation を保存できません".to_string());
        }

        let namespace = self.annotation_editor_state.namespace_input.trim();
        if namespace.is_empty() {
            return Err("namespace を入力してください".to_string());
        }

        let key = self.annotation_editor_state.key_input.trim();
        if key.is_empty() {
            return Err("key を入力してください".to_string());
        }

        let value = self.annotation_editor_state.value_input.trim();
        if value.is_empty() {
            return Err("value を入力してください".to_string());
        }

        Ok(ManualAnnotationAppendRow {
            target_type: "paragraph".to_string(),
            target_id: paragraph_id.to_string(),
            label_namespace: namespace.to_string(),
            label_key: key.to_string(),
            label_value: value.to_string(),
            tagged_by: self
                .annotation_editor_state
                .tagged_by_input
                .trim()
                .to_string(),
            tagged_at: String::new(),
            confidence: self
                .annotation_editor_state
                .confidence_input
                .trim()
                .to_string(),
            note: self.annotation_editor_state.note_input.trim().to_string(),
        })
    }

    fn apply_saved_annotation_to_selected_record(
        &mut self,
        annotation_row: &ManualAnnotationAppendRow,
    ) -> Result<(), String> {
        let pair = build_manual_annotation_pair(
            &annotation_row.label_namespace,
            &annotation_row.label_key,
            &annotation_row.label_value,
        );
        {
            let updated_record = self
                .selected_record_mut()
                .ok_or_else(|| "レコードが選択されていません".to_string())?;
            updated_record.manual_annotation_count =
                increment_manual_annotation_count(&updated_record.manual_annotation_count);
            updated_record.manual_annotation_pairs_text = append_manual_annotation_pairs_text(
                &updated_record.manual_annotation_pairs_text,
                &pair,
            );
            updated_record.manual_annotation_namespaces_text =
                append_manual_annotation_namespaces_text(
                    &updated_record.manual_annotation_namespaces_text,
                    &annotation_row.label_namespace,
                );
        }
        self.core.filter_options = build_filter_options(&self.core.all_records);
        self.core
            .invalidate_detail_segment_cache(SegmentCacheInvalidateReason::AnnotationSaved);
        Ok(())
    }

    fn save_annotation_for_selected_record(&mut self) {
        self.clear_annotation_editor_status();
        let annotation_row = match self.build_annotation_append_row() {
            Ok(annotation_row) => annotation_row,
            Err(error) => {
                self.annotation_editor_state.status_message = Some(error);
                self.annotation_editor_state.status_is_error = true;
                return;
            }
        };

        let annotation_csv_path = match self.resolved_annotation_csv_path() {
            Ok(annotation_csv_path) => annotation_csv_path,
            Err(error) => {
                self.annotation_editor_state.status_message = Some(error);
                self.annotation_editor_state.status_is_error = true;
                return;
            }
        };

        if let Err(error) = append_manual_annotation_row(&annotation_csv_path, &annotation_row) {
            self.annotation_editor_state.status_message = Some(error);
            self.annotation_editor_state.status_is_error = true;
            return;
        }

        if let Err(error) = self.apply_saved_annotation_to_selected_record(&annotation_row) {
            self.annotation_editor_state.status_message = Some(error);
            self.annotation_editor_state.status_is_error = true;
            return;
        }

        self.clear_annotation_editor_inputs();
        self.annotation_editor_state.status_message = Some(format!(
            "annotation を追記しました: {}",
            annotation_csv_path.display()
        ));
        self.annotation_editor_state.status_is_error = false;
        app_analysis_job::invalidate_session_analysis_cache(self);
    }

    fn apply_filters(&mut self) {
        self.core.recompute_filtered_indices();
        self.core
            .invalidate_detail_segment_cache(SegmentCacheInvalidateReason::FilterApplied);
        self.select_first_filtered_row(ScrollBehavior::AlignMin);
    }

    fn clear_filters_for_column(&mut self, column: FilterColumn) -> bool {
        if self.core.selected_filter_values.remove(&column).is_some() {
            self.apply_filters();
            true
        } else {
            false
        }
    }

    fn clear_all_filters(&mut self) -> bool {
        if !self.core.selected_filter_values.is_empty() {
            self.core.selected_filter_values.clear();
            self.apply_filters();
            true
        } else {
            false
        }
    }

    fn toggle_filter_value(&mut self, column: FilterColumn, value: &str, selected: bool) -> bool {
        let changed = {
            let entry = self.core.selected_filter_values.entry(column).or_default();
            if selected {
                entry.insert(value.to_string())
            } else {
                entry.remove(value)
            }
        };

        if self
            .core
            .selected_filter_values
            .get(&column)
            .is_some_and(BTreeSet::is_empty)
        {
            self.core.selected_filter_values.remove(&column);
        }

        if changed {
            self.apply_filters();
        }
        changed
    }

    fn get_segments(&mut self) -> Vec<TextSegment> {
        if let Some(record) = self.selected_record() {
            let row_no = record.row_no;
            if let Some((cached_row, ref segs)) = self.core.detail_segment_cache {
                if cached_row == row_no {
                    return segs.clone();
                }
            }
            let tagged = if record.primary_text_tagged().trim().is_empty() {
                record.primary_text().to_string()
            } else {
                record.primary_text_tagged().to_string()
            };
            let segs = parse_tagged_text(&tagged);
            self.core.set_detail_segment_cache(row_no, segs.clone());
            segs
        } else {
            Vec::new()
        }
    }

    fn try_cleanup_analysis_jobs(&mut self) {
        app_analysis_job::try_cleanup_analysis_jobs(self);
    }

    fn refresh_analysis_runtime(&mut self) {
        app_analysis_job::refresh_analysis_runtime(self);
    }

    fn focus_condition_editor_viewport(&self, ctx: &egui::Context) {
        app_condition_editor::focus_condition_editor_viewport(self, ctx);
    }

    fn open_condition_editor(&mut self, ctx: &egui::Context) -> Result<(), String> {
        app_condition_editor::open_condition_editor(self, ctx)
    }

    fn sync_condition_editor_with_runtime_path(&mut self) {
        app_condition_editor::sync_condition_editor_with_runtime_path(self);
    }

    fn start_analysis_job(&mut self) -> Result<(), String> {
        app_analysis_job::start_analysis_job(self)
    }

    fn start_analysis_job_force_rerun(&mut self) -> Result<(), String> {
        app_analysis_job::start_analysis_job_with_mode(
            self,
            app_analysis_job::AnalysisStartMode::ForceWorkerRun,
        )
    }

    fn start_analysis_job_force_reload(&mut self) -> Result<(), String> {
        app_analysis_job::start_analysis_job_with_mode(
            self,
            app_analysis_job::AnalysisStartMode::ForceWorkerRunAndReloadDb,
        )
    }

    fn start_export_job(&mut self, output_csv_path: PathBuf) -> Result<(), String> {
        app_analysis_job::start_export_job(self, output_csv_path)
    }

    fn draw_warning_details_window(&mut self, ctx: &egui::Context) {
        app_analysis_job::draw_warning_details_window(self, ctx);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        app_lifecycle::run_update_prelude(self, ctx);

        app_error_dialog::draw_error_dialog_if_any(self, ctx);

        self.draw_warning_details_window(ctx);

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            self.draw_toolbar(ui);
        });

        let consumed_tree_scroll = self.pending_tree_scroll;
        let mut clicked_row = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            clicked_row = self.draw_body(ui, consumed_tree_scroll);
        });

        self.draw_db_viewer_window(ctx);
        self.draw_analysis_settings_window(ctx);
        self.draw_condition_editor_window(ctx);

        if let Some(row_index) = clicked_row {
            if self
                .apply_event(ViewerCoreMessage::SelectionSetFilteredRow {
                    filtered_index: row_index,
                })
                .needs_repaint
            {
                ctx.request_repaint();
            }
        }

        if self.pending_tree_scroll == consumed_tree_scroll {
            self.pending_tree_scroll = None;
        }
    }
}

impl App {
    fn draw_db_viewer_button(&mut self, ui: &mut Ui, enabled: bool) {
        app_db_viewer::draw_db_viewer_button(self, ui, enabled);
    }

    fn draw_db_viewer_window(&mut self, ctx: &egui::Context) {
        app_db_viewer::draw_db_viewer_window(self, ctx);
    }

    fn draw_toolbar(&mut self, ui: &mut Ui) {
        app_toolbar::draw_toolbar(self, ui);
    }

    fn draw_analysis_settings_window(&mut self, ctx: &egui::Context) {
        app_analysis_settings::draw_analysis_settings_window(self, ctx);
    }

    fn draw_condition_editor_window(&mut self, ctx: &egui::Context) {
        app_condition_editor::draw_condition_editor_window(self, ctx);
    }

    fn draw_body(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        app_main_layout::draw_body(self, ui, tree_scroll_request)
    }
}

fn build_tree_row_no_column() -> Column {
    Column::initial(56.0).at_least(48.0).clip(true)
}

fn build_tree_paragraph_id_column() -> Column {
    Column::initial(140.0).at_least(96.0).clip(true)
}

fn build_tree_municipality_column() -> Column {
    Column::initial(128.0).at_least(96.0).clip(true)
}

fn build_tree_ordinance_column() -> Column {
    Column::initial(120.0).at_least(88.0).clip(true)
}

fn build_tree_category_column() -> Column {
    Column::remainder().at_least(140.0).clip(true)
}

fn build_tree_annotation_column() -> Column {
    Column::initial(220.0).at_least(140.0).clip(true)
}

fn build_tree_annotated_token_count_column() -> Column {
    Column::initial(92.0).at_least(72.0).clip(true)
}

fn tree_row_no_value(record: &AnalysisRecord) -> String {
    record.row_no.to_string()
}

fn tree_paragraph_id_value(record: &AnalysisRecord) -> String {
    record.unit_id().to_string()
}

fn tree_municipality_value(record: &AnalysisRecord) -> String {
    record.municipality_name.clone()
}

fn tree_ordinance_value(record: &AnalysisRecord) -> String {
    record.ordinance_or_rule.clone()
}

fn tree_category_value(record: &AnalysisRecord) -> String {
    record.matched_categories_text.clone()
}

fn tree_annotation_value(record: &AnalysisRecord) -> String {
    first_manual_annotation_line(&record.manual_annotation_pairs_text)
}

fn tree_annotated_token_count_value(record: &AnalysisRecord) -> String {
    record.annotated_token_count.clone()
}
