use crate::app_state::{AppState, ConditionEditorConfirmAction};
use crate::app_ui_state::AppUiState;
use crate::condition_editor::{build_default_condition_item, FilterConfigDocument};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConditionEditorSelectionDraft {
    pub(crate) requested_selection: Option<usize>,
    pub(crate) requested_group_selection: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConditionEditorCommandDraft {
    pub(crate) should_save: bool,
    pub(crate) should_reload: bool,
    pub(crate) should_add_condition: bool,
    pub(crate) should_delete_condition: Option<usize>,
    pub(crate) close_requested: bool,
    pub(crate) modal_response: Option<ConditionEditorModalResponse>,
    pub(crate) document_edited: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConditionEditorModalResponse {
    Continue,
    Cancel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConditionEditorAction {
    ApplySelectionDraft(ConditionEditorSelectionDraft),
    RequestClose,
    AddCondition,
    DeleteCondition {
        index: usize,
    },
    DocumentEdited,
    RequestSave,
    RequestReload {
        resolved_path: Result<PathBuf, String>,
    },
    ResolveModal(ConditionEditorModalResponse),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConditionEditorEffect {
    SaveDocument,
    ReloadDocument(PathBuf),
    CloseWindowViewport,
    ShowError(String),
}

pub(crate) fn reduce_condition_editor_action(
    state: &mut AppState,
    ui_state: &mut AppUiState,
    action: ConditionEditorAction,
) -> Vec<ConditionEditorEffect> {
    match action {
        ConditionEditorAction::ApplySelectionDraft(selection_draft) => {
            apply_selection_draft(state, selection_draft);
            Vec::new()
        }
        ConditionEditorAction::RequestClose => {
            if state.condition_editor_state.is_dirty {
                ui_state.condition_editor.confirm_action =
                    Some(ConditionEditorConfirmAction::CloseWindow);
                Vec::new()
            } else {
                ui_state.condition_editor.window_open = false;
                vec![ConditionEditorEffect::CloseWindowViewport]
            }
        }
        ConditionEditorAction::AddCondition => {
            apply_add_condition(state);
            Vec::new()
        }
        ConditionEditorAction::DeleteCondition { index } => {
            apply_delete_condition(state, index);
            Vec::new()
        }
        ConditionEditorAction::DocumentEdited => {
            mark_condition_editor_dirty(state, "未保存の変更があります。");
            Vec::new()
        }
        ConditionEditorAction::RequestSave => vec![ConditionEditorEffect::SaveDocument],
        ConditionEditorAction::RequestReload { resolved_path } => match resolved_path {
            Ok(path) => {
                if state.condition_editor_state.is_dirty {
                    ui_state.condition_editor.confirm_action =
                        Some(ConditionEditorConfirmAction::ReloadPath(path));
                    Vec::new()
                } else {
                    vec![ConditionEditorEffect::ReloadDocument(path)]
                }
            }
            Err(error) => vec![ConditionEditorEffect::ShowError(error)],
        },
        ConditionEditorAction::ResolveModal(response) => match response {
            ConditionEditorModalResponse::Continue => {
                match ui_state.condition_editor.confirm_action.take() {
                    Some(ConditionEditorConfirmAction::CloseWindow) => {
                        ui_state.condition_editor.window_open = false;
                        vec![ConditionEditorEffect::CloseWindowViewport]
                    }
                    Some(ConditionEditorConfirmAction::ReloadPath(path)) => {
                        vec![ConditionEditorEffect::ReloadDocument(path)]
                    }
                    None => Vec::new(),
                }
            }
            ConditionEditorModalResponse::Cancel => {
                ui_state.condition_editor.confirm_action = None;
                Vec::new()
            }
        },
    }
}

pub(crate) fn apply_footer_response(
    command_draft: &mut ConditionEditorCommandDraft,
    save_clicked: bool,
    reload_clicked: bool,
) {
    if save_clicked {
        command_draft.should_save = true;
    }
    if reload_clicked {
        command_draft.should_reload = true;
    }
}

pub(crate) fn apply_confirm_overlay_response(
    command_draft: &mut ConditionEditorCommandDraft,
    response: Option<ConditionEditorModalResponse>,
) {
    if let Some(response) = response {
        command_draft.modal_response = Some(response);
    }
}

pub(crate) fn apply_list_response(
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
    add_clicked: bool,
    selected_index: Option<usize>,
    selected_group_index: Option<usize>,
) {
    if add_clicked {
        command_draft.should_add_condition = true;
    }
    if let Some(selected_index) = selected_index {
        selection_draft.requested_selection = Some(selected_index);
        selection_draft.requested_group_selection = selected_group_index;
    }
}

pub(crate) fn apply_detail_response(
    selection_draft: &mut ConditionEditorSelectionDraft,
    command_draft: &mut ConditionEditorCommandDraft,
    selected_index: usize,
    requested_group_selection: Option<usize>,
    delete_clicked: bool,
    changed: bool,
) {
    if delete_clicked {
        command_draft.should_delete_condition = Some(selected_index);
    }
    if changed {
        command_draft.document_edited = true;
    }
    selection_draft.requested_group_selection = requested_group_selection;
}

pub(crate) fn build_condition_editor_actions(
    selection_draft: ConditionEditorSelectionDraft,
    command_draft: ConditionEditorCommandDraft,
    resolved_path_result: Result<PathBuf, String>,
) -> Vec<ConditionEditorAction> {
    let mut actions = vec![ConditionEditorAction::ApplySelectionDraft(selection_draft)];

    if command_draft.close_requested {
        actions.push(ConditionEditorAction::RequestClose);
    }
    if command_draft.should_add_condition {
        actions.push(ConditionEditorAction::AddCondition);
    }
    if let Some(index) = command_draft.should_delete_condition {
        actions.push(ConditionEditorAction::DeleteCondition { index });
    }
    if command_draft.document_edited {
        actions.push(ConditionEditorAction::DocumentEdited);
    }
    if command_draft.should_save {
        actions.push(ConditionEditorAction::RequestSave);
    }
    if command_draft.should_reload {
        actions.push(ConditionEditorAction::RequestReload {
            resolved_path: resolved_path_result,
        });
    }
    if let Some(response) = command_draft.modal_response {
        actions.push(ConditionEditorAction::ResolveModal(response));
    }

    actions
}

fn apply_selection_draft(state: &mut AppState, selection_draft: ConditionEditorSelectionDraft) {
    let selected_index = clamp_condition_index(
        selection_draft.requested_selection,
        state
            .condition_editor_state
            .document
            .as_ref()
            .map_or(0, |document| document.cooccurrence_conditions.len()),
    );
    state.condition_editor_state.selected_index = selected_index;
    state.condition_editor_state.selected_group_index = state
        .condition_editor_state
        .document
        .as_ref()
        .and_then(|document| {
            clamp_condition_group_selection_for_document(
                document,
                selected_index,
                selection_draft.requested_group_selection,
            )
        });
}

fn apply_add_condition(state: &mut AppState) {
    let mut new_index = None;
    if let Some(document) = state.condition_editor_state.document.as_mut() {
        document
            .cooccurrence_conditions
            .push(build_default_condition_item());
        new_index = Some(document.cooccurrence_conditions.len().saturating_sub(1));
    }
    if let Some(index) = new_index {
        state.condition_editor_state.selected_index = Some(index);
        state.condition_editor_state.selected_group_index = Some(0);
        mark_condition_editor_dirty(state, "未保存の変更があります。");
    }
}

fn apply_delete_condition(state: &mut AppState, delete_index: usize) {
    if let Some(document) = state.condition_editor_state.document.as_mut() {
        if delete_index < document.cooccurrence_conditions.len() {
            document.cooccurrence_conditions.remove(delete_index);
            state.condition_editor_state.selected_index =
                clamp_condition_index(Some(delete_index), document.cooccurrence_conditions.len());
            state.condition_editor_state.selected_group_index =
                clamp_condition_group_selection_for_document(
                    document,
                    state.condition_editor_state.selected_index,
                    Some(0),
                );
            mark_condition_editor_dirty(state, "condition を削除しました。");
        }
    }
}

fn mark_condition_editor_dirty(state: &mut AppState, status_message: &str) {
    state.condition_editor_state.is_dirty = true;
    state.condition_editor_state.status_message = Some(status_message.to_string());
    state.condition_editor_state.status_is_error = false;
}

fn clamp_condition_index(selected_index: Option<usize>, len: usize) -> Option<usize> {
    match (selected_index, len) {
        (_, 0) => None,
        (Some(index), _) => Some(index.min(len - 1)),
        (None, _) => Some(0),
    }
}

fn clamp_condition_group_selection_for_document(
    document: &FilterConfigDocument,
    condition_index: Option<usize>,
    selected_group_index: Option<usize>,
) -> Option<usize> {
    let Some(condition_index) = condition_index else {
        return None;
    };
    let Some(condition) = document.cooccurrence_conditions.get(condition_index) else {
        return None;
    };
    match (selected_group_index, condition.form_groups.len()) {
        (_, 0) => None,
        (Some(index), len) => Some(index.min(len - 1)),
        (None, len) => Some(len - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::condition_editor::FilterConfigDocument;
    use std::path::PathBuf;

    fn build_state() -> (AppState, AppUiState) {
        let mut state = AppState::new(
            Err("runtime unavailable".to_string()),
            PathBuf::from("test.db"),
        );
        state.condition_editor_state.document = Some(FilterConfigDocument::default());
        state.condition_editor_state.loaded_path = Some(PathBuf::from("conditions.json"));
        let ui_state = AppUiState::new(0.33);
        (state, ui_state)
    }

    #[test]
    fn request_close_sets_confirm_action_when_editor_is_dirty() {
        let (mut state, mut ui_state) = build_state();
        state.condition_editor_state.is_dirty = true;
        ui_state.condition_editor.window_open = true;

        let effects = reduce_condition_editor_action(
            &mut state,
            &mut ui_state,
            ConditionEditorAction::RequestClose,
        );

        assert!(effects.is_empty());
        assert_eq!(
            ui_state.condition_editor.confirm_action,
            Some(ConditionEditorConfirmAction::CloseWindow)
        );
        assert!(ui_state.condition_editor.window_open);
    }

    #[test]
    fn request_reload_emits_effect_when_editor_is_clean() {
        let (mut state, mut ui_state) = build_state();

        let effects = reduce_condition_editor_action(
            &mut state,
            &mut ui_state,
            ConditionEditorAction::RequestReload {
                resolved_path: Ok(PathBuf::from("next.json")),
            },
        );

        assert_eq!(
            effects,
            vec![ConditionEditorEffect::ReloadDocument(PathBuf::from(
                "next.json"
            ))]
        );
        assert_eq!(ui_state.condition_editor.confirm_action, None);
    }

    #[test]
    fn confirm_continue_for_reload_returns_reload_effect() {
        let (mut state, mut ui_state) = build_state();
        ui_state.condition_editor.confirm_action = Some(ConditionEditorConfirmAction::ReloadPath(
            PathBuf::from("next.json"),
        ));

        let effects = reduce_condition_editor_action(
            &mut state,
            &mut ui_state,
            ConditionEditorAction::ResolveModal(ConditionEditorModalResponse::Continue),
        );

        assert_eq!(
            effects,
            vec![ConditionEditorEffect::ReloadDocument(PathBuf::from(
                "next.json"
            ))]
        );
        assert_eq!(ui_state.condition_editor.confirm_action, None);
    }

    #[test]
    fn add_condition_updates_selection_and_dirty_status() {
        let (mut state, mut ui_state) = build_state();

        reduce_condition_editor_action(
            &mut state,
            &mut ui_state,
            ConditionEditorAction::AddCondition,
        );

        assert_eq!(
            state
                .condition_editor_state
                .document
                .as_ref()
                .map(|document| document.cooccurrence_conditions.len()),
            Some(1)
        );
        assert_eq!(state.condition_editor_state.selected_index, Some(0));
        assert_eq!(state.condition_editor_state.selected_group_index, Some(0));
        assert!(state.condition_editor_state.is_dirty);
    }

    #[test]
    fn document_edited_marks_editor_dirty() {
        let (mut state, mut ui_state) = build_state();

        reduce_condition_editor_action(
            &mut state,
            &mut ui_state,
            ConditionEditorAction::DocumentEdited,
        );

        assert!(state.condition_editor_state.is_dirty);
        assert_eq!(
            state.condition_editor_state.status_message.as_deref(),
            Some("未保存の変更があります。")
        );
    }

    #[test]
    fn build_condition_editor_actions_preserves_expected_execution_order() {
        let actions = build_condition_editor_actions(
            ConditionEditorSelectionDraft {
                requested_selection: Some(1),
                requested_group_selection: Some(2),
            },
            ConditionEditorCommandDraft {
                should_save: true,
                should_reload: true,
                should_add_condition: true,
                should_delete_condition: Some(3),
                close_requested: true,
                modal_response: Some(ConditionEditorModalResponse::Continue),
                document_edited: true,
            },
            Ok(PathBuf::from("next.json")),
        );

        assert_eq!(
            actions,
            vec![
                ConditionEditorAction::ApplySelectionDraft(ConditionEditorSelectionDraft {
                    requested_selection: Some(1),
                    requested_group_selection: Some(2),
                }),
                ConditionEditorAction::RequestClose,
                ConditionEditorAction::AddCondition,
                ConditionEditorAction::DeleteCondition { index: 3 },
                ConditionEditorAction::DocumentEdited,
                ConditionEditorAction::RequestSave,
                ConditionEditorAction::RequestReload {
                    resolved_path: Ok(PathBuf::from("next.json")),
                },
                ConditionEditorAction::ResolveModal(ConditionEditorModalResponse::Continue),
            ]
        );
    }
}
