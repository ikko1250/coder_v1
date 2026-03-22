use crate::app_state::ConditionEditorConfirmAction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TreeScrollRequest {
    pub(crate) row_index: usize,
    pub(crate) align: Option<egui::Align>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ConditionEditorUiState {
    pub(crate) window_open: bool,
    pub(crate) confirm_action: Option<ConditionEditorConfirmAction>,
}

#[derive(Clone, Debug)]
pub(crate) struct AppUiState {
    pub(crate) pending_tree_scroll: Option<TreeScrollRequest>,
    pub(crate) record_list_panel_ratio: f32,
    pub(crate) annotation_panel_expanded: bool,
    pub(crate) analysis_settings_window_open: bool,
    pub(crate) warning_details_window_open: bool,
    pub(crate) condition_editor: ConditionEditorUiState,
}

impl AppUiState {
    pub(crate) fn new(record_list_panel_default_ratio: f32) -> Self {
        Self {
            pending_tree_scroll: None,
            record_list_panel_ratio: record_list_panel_default_ratio,
            annotation_panel_expanded: false,
            analysis_settings_window_open: false,
            warning_details_window_open: false,
            condition_editor: ConditionEditorUiState::default(),
        }
    }
}
