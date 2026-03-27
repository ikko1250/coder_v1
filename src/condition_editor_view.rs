use crate::condition_editor::{
    AnnotationFilterItem, ConditionEditorItem, FilterConfigDocument, FormGroupEditorItem,
    TextGroupEditorItem,
};
use crate::ui_helpers::ime_safe_singleline;
use egui::{Color32, RichText, ScrollArea, Ui};
use std::path::Path;

/// 条件エディターヘッダーのパス表示幅（分析設定の条件 JSON 行に合わせる）。
pub(crate) const CONDITION_EDITOR_HEADER_PATH_FIELD_WIDTH: f32 = 460.0;

pub(crate) const CONDITION_EDITOR_FIELD_LABEL_WIDTH: f32 = 156.0;
pub(crate) const CONDITION_EDITOR_TEXT_INPUT_WIDTH: f32 = 280.0;
pub(crate) const CONDITION_EDITOR_CHOICE_WIDTH: f32 = 140.0;
const CONDITION_EDITOR_NUMBER_WIDTH: f32 = 120.0;
const CONDITION_EDITOR_LIST_INPUT_WIDTH: f32 = 280.0;
const CONDITION_EDITOR_FILTER_OPERATOR_WIDTH: f32 = 120.0;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConditionEditorHeaderResponse {
    pub(crate) select_clicked: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConditionEditorFooterResponse {
    pub(crate) save_clicked: bool,
    pub(crate) reload_clicked: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConditionEditorListResponse {
    pub(crate) add_clicked: bool,
    pub(crate) selected_index: Option<usize>,
    pub(crate) selected_group_index: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConditionEditorDetailResponse {
    pub(crate) delete_clicked: bool,
    pub(crate) requested_group_selection: Option<usize>,
    pub(crate) changed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfirmOverlayResponse {
    Continue,
    Cancel,
}

pub(crate) fn draw_condition_editor_header_panel(
    ui: &mut Ui,
    can_modify: bool,
    loaded_path_label: &str,
    resolved_path_label: &str,
    pending_path: Option<&Path>,
    status_message: Option<(&str, bool)>,
    projected_legacy_condition_count: usize,
    data_source_stale: bool,
) -> ConditionEditorHeaderResponse {
    let mut response = ConditionEditorHeaderResponse::default();

    ui.horizontal(|ui| {
        ui.label("読込中");
        let mut loaded_display = loaded_path_label.to_string();
        ui.add(
            ime_safe_singleline(&mut loaded_display)
                .desired_width(CONDITION_EDITOR_HEADER_PATH_FIELD_WIDTH)
                .interactive(false),
        );
        if ui
            .add_enabled(can_modify, egui::Button::new("選択"))
            .on_hover_text("別の条件 JSON を開き、分析実行でもそのパスを使います。")
            .clicked()
        {
            response.select_clicked = true;
        }
    });
    ui.horizontal(|ui| {
        ui.label("現在の解決先");
        let mut resolved_display = resolved_path_label.to_string();
        ui.add(
            ime_safe_singleline(&mut resolved_display)
                .desired_width(CONDITION_EDITOR_HEADER_PATH_FIELD_WIDTH)
                .interactive(false),
        );
    });

    if data_source_stale {
        ui.colored_label(
            Color32::from_rgb(200, 120, 40),
            "メインのデータソース（CSV／分析結果）が読み込み以降に更新されました。必要に応じて条件 JSON を再読込してください。",
        );
    }

    if let Some(pending_path) = pending_path {
        ui.colored_label(
            Color32::from_rgb(200, 64, 64),
            format!(
                "分析設定で条件 JSON の解決先が変更されています。保存前に再読込してください: {}",
                pending_path.display()
            ),
        );
    }

    if let Some((status_message, status_is_error)) = status_message {
        ui.colored_label(editor_status_color(status_is_error), status_message);
    }

    if projected_legacy_condition_count > 0 {
        ui.colored_label(
            Color32::from_rgb(180, 120, 40),
            format!(
                "legacy 投影: {} 件の condition を group editor 表示へ変換中",
                projected_legacy_condition_count
            ),
        );
    }

    if !can_modify {
        ui.label("分析ジョブ実行中は条件 JSON の保存・再読込・ファイルの選択はできません。");
    }

    response
}

pub(crate) fn draw_condition_editor_footer_panel(
    ui: &mut Ui,
    save_enabled: bool,
    reload_enabled: bool,
    is_dirty: bool,
) -> ConditionEditorFooterResponse {
    let mut response = ConditionEditorFooterResponse::default();
    ui.horizontal(|ui| {
        if ui
            .add_enabled(save_enabled, egui::Button::new("保存"))
            .clicked()
        {
            response.save_clicked = true;
        }
        if ui
            .add_enabled(reload_enabled, egui::Button::new("再読込"))
            .clicked()
        {
            response.reload_clicked = true;
        }
        if is_dirty {
            ui.label("未保存");
        } else {
            ui.label("保存済み");
        }
    });
    response
}

pub(crate) fn draw_condition_editor_confirm_overlay(
    viewport_ctx: &egui::Context,
    message: &str,
) -> Option<ConfirmOverlayResponse> {
    let mut response = None;
    let screen_rect = viewport_ctx.screen_rect();

    egui::Area::new(egui::Id::new("condition_editor_confirm_overlay"))
        .order(egui::Order::Foreground)
        .fixed_pos(screen_rect.min)
        .show(viewport_ctx, |ui| {
            ui.set_min_size(screen_rect.size());
            ui.painter()
                .rect_filled(ui.max_rect(), 0.0, Color32::from_black_alpha(160));
            ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                ui.add_space((screen_rect.height() * 0.22).max(80.0));
                egui::Frame::window(ui.style()).show(ui, |ui| {
                    ui.set_min_width(420.0);
                    ui.label(message);
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("続行").clicked() {
                            response = Some(ConfirmOverlayResponse::Continue);
                        }
                        if ui.button("キャンセル").clicked() {
                            response = Some(ConfirmOverlayResponse::Cancel);
                        }
                    });
                });
            });
        });

    response
}

pub(crate) fn draw_condition_editor_list_panel(
    ui: &mut Ui,
    can_modify: bool,
    document: Option<&FilterConfigDocument>,
    current_selection: Option<usize>,
) -> ConditionEditorListResponse {
    let mut response = ConditionEditorListResponse::default();
    ui.vertical(|ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new("condition 一覧").strong());
            if ui
                .add_enabled(can_modify, egui::Button::new("追加"))
                .clicked()
            {
                response.add_clicked = true;
            }
        });

        ScrollArea::vertical()
            .id_salt("condition_editor_list_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let Some(document) = document else {
                    ui.label(RichText::new("条件 JSON 未読込").italics());
                    return;
                };
                if document.cooccurrence_conditions.is_empty() {
                    ui.label(RichText::new("condition がありません").italics());
                } else {
                    for (index, condition) in document.cooccurrence_conditions.iter().enumerate() {
                        let selected = current_selection == Some(index);
                        let categories_preview = summarize_condition_list(&condition.categories, 2);
                        let label = format!(
                            "{}. {} [{}] fg:{} forms:{} tg:{} filters:{} refs:{}",
                            index + 1,
                            condition.condition_id,
                            categories_preview,
                            condition_group_count(condition),
                            total_forms_count(condition),
                            condition.text_groups.len(),
                            condition.annotation_filters.len(),
                            condition.required_categories_all.len()
                                + condition.required_categories_any.len()
                                + condition.required_condition_ids_all.len()
                                + condition.required_condition_ids_any.len()
                                + condition.excluded_condition_ids_any.len()
                        );
                        if ui.selectable_label(selected, label).clicked() {
                            response.selected_index = Some(index);
                            response.selected_group_index =
                                clamp_editor_index(Some(0), condition.form_groups.len());
                        }
                    }
                }
            });
    });
    response
}

pub(crate) fn draw_condition_editor_selected_condition(
    ui: &mut Ui,
    can_modify: bool,
    current_group_selection: Option<usize>,
    condition: &mut ConditionEditorItem,
) -> ConditionEditorDetailResponse {
    let mut response = ConditionEditorDetailResponse {
        requested_group_selection: current_group_selection,
        ..Default::default()
    };

    ui.horizontal(|ui| {
        ui.label(RichText::new("condition 詳細").strong());
        if ui
            .add_enabled(can_modify, egui::Button::new("condition削除"))
            .clicked()
        {
            response.delete_clicked = true;
        }
    });
    ui.horizontal(|ui| {
        ui.add_sized(
            [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
            egui::Label::new("condition_id"),
        );
        let edit_response = ui.add_sized(
            [CONDITION_EDITOR_TEXT_INPUT_WIDTH, 0.0],
            ime_safe_singleline(&mut condition.condition_id),
        );
        response.changed |= edit_response.changed();
    });

    ui.horizontal(|ui| {
        ui.add_sized(
            [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
            egui::Label::new("overall_search_scope"),
        );
        response.changed |= edit_optional_choice(
            ui,
            &mut condition.overall_search_scope,
            &["paragraph", "sentence"],
            CONDITION_EDITOR_CHOICE_WIDTH,
        );
    });

    ui.add_space(8.0);
    response.changed |= draw_string_list_editor(ui, "categories", &mut condition.categories);
    response.changed |= draw_condition_editor_form_groups_section(
        ui,
        &mut response.requested_group_selection,
        condition,
    );
    response.changed |= draw_condition_editor_text_groups_section(ui, condition);

    if condition.projected_from_legacy {
        ui.colored_label(
            Color32::from_rgb(180, 120, 40),
            "legacy 条件を group editor 表示へ投影中です。保存時に互換形式または新形式へ正規化されます。",
        );
    }

    response.changed |= draw_annotation_filter_editor(ui, &mut condition.annotation_filters);
    response.changed |= draw_string_list_editor(
        ui,
        "required_categories_all",
        &mut condition.required_categories_all,
    );
    response.changed |= draw_string_list_editor(
        ui,
        "required_categories_any",
        &mut condition.required_categories_any,
    );
    response.changed |= draw_string_list_editor(
        ui,
        "required_condition_ids_all",
        &mut condition.required_condition_ids_all,
    );
    response.changed |= draw_string_list_editor(
        ui,
        "required_condition_ids_any",
        &mut condition.required_condition_ids_any,
    );
    response.changed |= draw_string_list_editor(
        ui,
        "excluded_condition_ids_any",
        &mut condition.excluded_condition_ids_any,
    );

    response
}

pub(crate) fn draw_condition_editor_global_settings(
    ui: &mut Ui,
    document: &mut FilterConfigDocument,
) -> bool {
    let mut changed = false;

    ui.group(|ui| {
        ui.label(RichText::new("全体設定").strong());
        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("analysis_unit"),
            );
            changed |= edit_analysis_unit_choice(
                ui,
                &mut document.analysis_unit,
                CONDITION_EDITOR_CHOICE_WIDTH,
            );
        });
        ui.small(
            "analysis_unit は出力単位の設定です。condition / group の search_scope とは別に扱われます。",
        );
        if document.analysis_unit.as_deref() == Some("sentence") {
            ui.small(
                "sentence を選ぶと出力は文単位になります。paragraph 前提の結果確認や downstream 処理では差分に注意してください。",
            );
        }
    });
    ui.add_space(8.0);

    changed
}

pub(crate) fn summarize_condition_list(values: &[String], preview_count: usize) -> String {
    if values.is_empty() {
        return "-".to_string();
    }
    let mut preview = values
        .iter()
        .take(preview_count)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    if preview.is_empty() {
        return "-".to_string();
    }
    if values.len() > preview_count {
        preview.push(format!("+{}", values.len() - preview_count));
    }
    preview.join(", ")
}

pub(crate) fn total_forms_count(condition: &ConditionEditorItem) -> usize {
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

pub(crate) fn condition_group_count(condition: &ConditionEditorItem) -> usize {
    if condition.form_groups.is_empty() {
        usize::from(!condition.forms.is_empty())
    } else {
        condition.form_groups.len()
    }
}

pub(crate) fn summarize_form_group_label(
    group: &FormGroupEditorItem,
    group_index: usize,
) -> String {
    let logic_label = display_combine_logic(
        group.match_logic.as_deref(),
        group.combine_logic.as_deref(),
        group_index,
    );
    let forms_preview = summarize_condition_list(&group.forms, 2);
    format!(
        "g{} [{}] forms:{} {}",
        group_index + 1,
        logic_label,
        group.forms.len(),
        forms_preview
    )
}

pub(crate) fn draw_form_group_editor(
    ui: &mut Ui,
    group_index: usize,
    overall_search_scope: Option<&str>,
    search_scope_locked: bool,
    group: &mut FormGroupEditorItem,
) -> bool {
    let mut changed = false;
    let logic_options_group1 = ["and", "or"];
    let logic_options_group_n = ["and", "and or", "and not", "or and", "or", "or not"];

    ui.add_space(8.0);
    ui.group(|ui| {
        ui.label(RichText::new(format!("group {}", group_index + 1)).strong());

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("logic"),
            );
            let selected_logic = display_combine_logic(
                group.match_logic.as_deref(),
                group.combine_logic.as_deref(),
                group_index,
            );
            egui::ComboBox::from_id_salt(("form_group_logic", group_index))
                .width(CONDITION_EDITOR_CHOICE_WIDTH)
                .selected_text(selected_logic)
                .show_ui(ui, |ui| {
                    let options: &[&str] = if group_index == 0 {
                        &logic_options_group1
                    } else {
                        &logic_options_group_n
                    };
                    for option in options {
                        if ui
                            .selectable_label(selected_logic == *option, *option)
                            .clicked()
                        {
                            set_combine_logic(
                                &mut group.match_logic,
                                &mut group.combine_logic,
                                group_index,
                                option,
                            );
                            changed = true;
                        }
                    }
                });
        });

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("search_scope"),
            );
            if search_scope_locked {
                let mut locked_search_scope = "paragraph".to_string();
                ui.add_enabled(
                    false,
                    ime_safe_singleline(&mut locked_search_scope)
                        .desired_width(CONDITION_EDITOR_CHOICE_WIDTH),
                );
            } else {
                changed |= edit_group_scope_choice(
                    ui,
                    &mut group.search_scope,
                    overall_search_scope,
                    CONDITION_EDITOR_CHOICE_WIDTH,
                );
            }
        });

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("max_token_distance"),
            );
            let distance_disabled = group.match_logic.as_deref() == Some("not");
            if distance_disabled {
                if group.max_token_distance.is_some() {
                    group.max_token_distance = None;
                    changed = true;
                }
                ui.add_enabled(false, egui::Label::new("not group では無効"));
            } else {
                changed |= edit_optional_i64(
                    ui,
                    &mut group.max_token_distance,
                    5,
                    CONDITION_EDITOR_NUMBER_WIDTH,
                );
            }
        });

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("anchor_form"),
            );
            changed |= edit_anchor_form_choice(
                ui,
                &mut group.anchor_form,
                &group.forms,
                CONDITION_EDITOR_CHOICE_WIDTH,
            );
        });
        if let Some(anchor_form) = group.anchor_form.as_ref() {
            if !group.forms.iter().any(|form| form == anchor_form) {
                ui.colored_label(
                    Color32::from_rgb(200, 64, 64),
                    "anchor_form が forms に含まれていません。保存前に修正が必要です。",
                );
            } else if group.match_logic.as_deref() != Some("and") {
                ui.colored_label(
                    Color32::from_rgb(200, 64, 64),
                    "anchor_form は and group でのみ保存できます。",
                );
            }
        }
        if group_index == 0 && group.match_logic.as_deref() == Some("not") {
            ui.colored_label(
                Color32::from_rgb(200, 64, 64),
                "group 1 では match_logic=not を保存できません。",
            );
        }

        changed |= draw_string_list_editor(ui, "forms", &mut group.forms);
        changed |= draw_string_list_editor(ui, "exclude_forms_any", &mut group.exclude_forms_any);
    });

    changed
}

pub(crate) fn edit_optional_choice(
    ui: &mut Ui,
    value: &mut Option<String>,
    options: &[&str],
    width: f32,
) -> bool {
    let current_value = value.clone().unwrap_or_default();
    let selected_text = if current_value.trim().is_empty() {
        "(未設定)".to_string()
    } else {
        current_value.clone()
    };
    let mut changed = false;

    egui::ComboBox::from_id_salt(ui.next_auto_id())
        .width(width)
        .selected_text(selected_text)
        .show_ui(ui, |ui| {
            if ui
                .selectable_label(current_value.trim().is_empty(), "(未設定)")
                .clicked()
            {
                *value = None;
                changed = true;
            }
            for option in options {
                if ui
                    .selectable_label(current_value == *option, *option)
                    .clicked()
                {
                    *value = Some((*option).to_string());
                    changed = true;
                }
            }
        });

    changed
}

fn edit_analysis_unit_choice(ui: &mut Ui, value: &mut Option<String>, width: f32) -> bool {
    let current_value = match value.as_deref() {
        Some(raw_value) if raw_value.eq_ignore_ascii_case("sentence") => "sentence",
        _ => "paragraph",
    };
    let mut changed = false;

    egui::ComboBox::from_id_salt(ui.next_auto_id())
        .width(width)
        .selected_text(current_value)
        .show_ui(ui, |ui| {
            for option in ["paragraph", "sentence"] {
                if ui
                    .selectable_label(current_value == option, option)
                    .clicked()
                {
                    *value = Some(option.to_string());
                    changed = true;
                }
            }
        });

    changed
}

pub(crate) fn draw_string_list_editor(ui: &mut Ui, label: &str, values: &mut Vec<String>) -> bool {
    let mut changed = false;
    let mut remove_index = None;

    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new(label).strong());
            if ui.button("行追加").clicked() {
                values.push(String::new());
                changed = true;
            }
        });

        if values.is_empty() {
            ui.label(RichText::new("未設定").italics());
        }
        for (index, value) in values.iter_mut().enumerate() {
            ui.horizontal(|ui| {
                ui.label(format!("{:02}", index + 1));
                let response = ui.add_sized(
                    [CONDITION_EDITOR_LIST_INPUT_WIDTH, 0.0],
                    ime_safe_singleline(value),
                );
                if response.changed() {
                    changed = true;
                }
                if ui.button("削除").clicked() {
                    remove_index = Some(index);
                }
            });
        }
    });

    if let Some(index) = remove_index {
        values.remove(index);
        changed = true;
    }

    changed
}

pub(crate) fn draw_annotation_filter_editor(
    ui: &mut Ui,
    filters: &mut Vec<AnnotationFilterItem>,
) -> bool {
    let mut changed = false;
    let mut remove_index = None;

    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new("annotation_filters").strong());
            if ui.button("行追加").clicked() {
                filters.push(AnnotationFilterItem::default());
                changed = true;
            }
        });

        if filters.is_empty() {
            ui.label(RichText::new("未設定").italics());
        }
        for (index, filter) in filters.iter_mut().enumerate() {
            ui.group(|ui| {
                ui.horizontal(|ui| {
                    ui.label(format!("{:02}", index + 1));
                    if ui.button("削除").clicked() {
                        remove_index = Some(index);
                    }
                });
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                        egui::Label::new("namespace"),
                    );
                    if ui
                        .add_sized(
                            [CONDITION_EDITOR_TEXT_INPUT_WIDTH, 0.0],
                            ime_safe_singleline(&mut filter.namespace),
                        )
                        .changed()
                    {
                        changed = true;
                    }
                });
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                        egui::Label::new("key"),
                    );
                    if ui
                        .add_sized(
                            [CONDITION_EDITOR_TEXT_INPUT_WIDTH, 0.0],
                            ime_safe_singleline(&mut filter.key),
                        )
                        .changed()
                    {
                        changed = true;
                    }
                });
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                        egui::Label::new("value"),
                    );
                    if ui
                        .add_sized(
                            [CONDITION_EDITOR_TEXT_INPUT_WIDTH, 0.0],
                            ime_safe_singleline(&mut filter.value),
                        )
                        .changed()
                    {
                        changed = true;
                    }
                });
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                        egui::Label::new("operator"),
                    );
                    let operator_text = filter.operator.get_or_insert_with(|| "eq".to_string());
                    if ui
                        .add_sized(
                            [CONDITION_EDITOR_FILTER_OPERATOR_WIDTH, 0.0],
                            ime_safe_singleline(operator_text),
                        )
                        .changed()
                    {
                        changed = true;
                    }
                });
            });
        }
    });

    if let Some(index) = remove_index {
        filters.remove(index);
        changed = true;
    }

    changed
}

fn display_combine_logic(
    match_logic: Option<&str>,
    combine_logic: Option<&str>,
    group_index: usize,
) -> &'static str {
    let match_logic = match_logic.unwrap_or("and");
    if group_index == 0 {
        return match match_logic {
            "or" => "or",
            "not" => "not",
            _ => "and",
        };
    }

    match (combine_logic.unwrap_or("and"), match_logic) {
        ("and", "or") => "and or",
        ("and", "not") => "and not",
        ("or", "and") => "or and",
        ("or", "not") => "or not",
        ("or", _) => "or",
        _ => "and",
    }
}

fn set_combine_logic(
    match_logic: &mut Option<String>,
    combine_logic: &mut Option<String>,
    group_index: usize,
    selected: &str,
) {
    if group_index == 0 {
        *combine_logic = None;
        *match_logic = Some(match selected {
            "or" => "or".to_string(),
            "not" => "not".to_string(),
            _ => "and".to_string(),
        });
        return;
    }

    let (combine, matched) = match selected {
        "and or" => ("and", "or"),
        "and not" => ("and", "not"),
        "or and" => ("or", "and"),
        "or" => ("or", "or"),
        "or not" => ("or", "not"),
        _ => ("and", "and"),
    };
    *combine_logic = Some(combine.to_string());
    *match_logic = Some(matched.to_string());
}

fn edit_group_scope_choice(
    ui: &mut Ui,
    value: &mut Option<String>,
    overall_search_scope: Option<&str>,
    width: f32,
) -> bool {
    let inherited_label = overall_search_scope.unwrap_or("paragraph");
    let current_value = value.clone();
    let selected_text = current_value
        .clone()
        .unwrap_or_else(|| format!("(全体設定に従う: {inherited_label})"));
    let mut changed = false;

    egui::ComboBox::from_id_salt(ui.next_auto_id())
        .width(width)
        .selected_text(selected_text)
        .show_ui(ui, |ui| {
            if ui
                .selectable_label(
                    value.is_none(),
                    format!("(全体設定に従う: {inherited_label})"),
                )
                .clicked()
            {
                *value = None;
                changed = true;
            }
            for option in ["paragraph", "sentence"] {
                if ui
                    .selectable_label(current_value.as_deref() == Some(option), option)
                    .clicked()
                {
                    *value = Some(option.to_string());
                    changed = true;
                }
            }
        });

    changed
}

fn edit_anchor_form_choice(
    ui: &mut Ui,
    value: &mut Option<String>,
    forms: &[String],
    width: f32,
) -> bool {
    let forms = forms
        .iter()
        .filter(|form| !form.trim().is_empty())
        .map(|form| form.trim().to_string())
        .collect::<Vec<_>>();
    let current_value = value.clone();
    let selected_text = current_value
        .clone()
        .unwrap_or_else(|| "(未設定)".to_string());
    let mut changed = false;

    egui::ComboBox::from_id_salt(ui.next_auto_id())
        .width(width)
        .selected_text(selected_text)
        .show_ui(ui, |ui| {
            if ui
                .selectable_label(current_value.is_none(), "(未設定)")
                .clicked()
            {
                *value = None;
                changed = true;
            }
            for form in &forms {
                if ui
                    .selectable_label(current_value.as_deref() == Some(form.as_str()), form)
                    .clicked()
                {
                    *value = Some(form.clone());
                    changed = true;
                }
            }
        });

    changed
}

fn edit_optional_i64(ui: &mut Ui, value: &mut Option<i64>, default_value: i64, width: f32) -> bool {
    let mut changed = false;
    let mut enabled = value.is_some();
    if ui.checkbox(&mut enabled, "有効").changed() {
        if enabled {
            *value = Some(value.unwrap_or(default_value));
        } else {
            *value = None;
        }
        changed = true;
    }

    let mut number = value.unwrap_or(default_value);
    if ui
        .add_enabled_ui(enabled, |ui| {
            ui.add_sized(
                [width, 0.0],
                egui::DragValue::new(&mut number).range(0..=999_999),
            )
        })
        .inner
        .changed()
    {
        *value = Some(number);
        changed = true;
    }

    changed
}

fn editor_status_color(is_error: bool) -> Color32 {
    if is_error {
        Color32::from_rgb(200, 64, 64)
    } else {
        Color32::from_rgb(70, 130, 70)
    }
}

fn clamp_editor_index(selected_index: Option<usize>, len: usize) -> Option<usize> {
    match (selected_index, len) {
        (_, 0) => None,
        (Some(index), _) => Some(index.min(len - 1)),
        (None, _) => Some(0),
    }
}

fn summarize_text_group_label(group: &TextGroupEditorItem, group_index: usize) -> String {
    let logic_label = display_combine_logic(
        group.match_logic.as_deref(),
        group.combine_logic.as_deref(),
        group_index,
    );
    let texts_preview = summarize_condition_list(&group.texts, 2);
    format!(
        "tg{} [{}] texts:{} {}",
        group_index + 1,
        logic_label,
        group.texts.len(),
        texts_preview
    )
}

fn draw_text_group_editor(
    ui: &mut Ui,
    group_index: usize,
    overall_search_scope: Option<&str>,
    search_scope_locked: bool,
    group: &mut TextGroupEditorItem,
) -> bool {
    let mut changed = false;
    let logic_options_group1 = ["and", "or", "not"];
    let logic_options_group_n = ["and", "and or", "and not", "or and", "or", "or not"];

    ui.add_space(8.0);
    ui.group(|ui| {
        ui.label(RichText::new(format!("text group {}", group_index + 1)).strong());

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("logic"),
            );
            let selected_logic = display_combine_logic(
                group.match_logic.as_deref(),
                group.combine_logic.as_deref(),
                group_index,
            );
            egui::ComboBox::from_id_salt(("text_group_logic", group_index))
                .width(CONDITION_EDITOR_CHOICE_WIDTH)
                .selected_text(selected_logic)
                .show_ui(ui, |ui| {
                    let options: &[&str] = if group_index == 0 {
                        &logic_options_group1
                    } else {
                        &logic_options_group_n
                    };
                    for option in options {
                        if ui
                            .selectable_label(selected_logic == *option, *option)
                            .clicked()
                        {
                            set_combine_logic(
                                &mut group.match_logic,
                                &mut group.combine_logic,
                                group_index,
                                option,
                            );
                            changed = true;
                        }
                    }
                });
        });

        ui.horizontal(|ui| {
            ui.add_sized(
                [CONDITION_EDITOR_FIELD_LABEL_WIDTH, 0.0],
                egui::Label::new("search_scope"),
            );
            if search_scope_locked {
                let mut locked_search_scope = "paragraph".to_string();
                ui.add_enabled(
                    false,
                    ime_safe_singleline(&mut locked_search_scope)
                        .desired_width(CONDITION_EDITOR_CHOICE_WIDTH),
                );
            } else {
                changed |= edit_group_scope_choice(
                    ui,
                    &mut group.search_scope,
                    overall_search_scope,
                    CONDITION_EDITOR_CHOICE_WIDTH,
                );
            }
        });

        changed |= draw_string_list_editor(ui, "texts", &mut group.texts);
    });

    changed
}

fn draw_condition_editor_text_groups_section(
    ui: &mut Ui,
    condition: &mut ConditionEditorItem,
) -> bool {
    let mut changed = false;

    egui::CollapsingHeader::new("text_groups（フルテキスト）")
        .default_open(false)
        .show(ui, |ui| {
            ui.small(
                "条件 JSON の text_groups。トークン無しでも保存可。annotation 併用時はスコープは paragraph に固定されます。",
            );

            if ui.button("text group 追加").clicked() {
                condition
                    .text_groups
                    .push(build_default_text_group_item(condition.text_groups.len()));
                changed = true;
            }

            if condition.text_groups.is_empty() {
                ui.label(RichText::new("text group がありません").italics());
                return;
            }

            let mut remove_group_index = None;
            for (group_index, group) in condition.text_groups.iter().enumerate() {
                ui.horizontal(|ui| {
                    let label = summarize_text_group_label(group, group_index);
                    ui.label(RichText::new(label).small());
                    if ui.button("削除").clicked() {
                        remove_group_index = Some(group_index);
                    }
                });
            }
            if let Some(group_index) = remove_group_index {
                condition.text_groups.remove(group_index);
                changed = true;
            }

            let overall_search_scope = condition.overall_search_scope.clone();
            let search_scope_locked = !condition.annotation_filters.is_empty();
            for (group_index, group) in condition.text_groups.iter_mut().enumerate() {
                changed |= draw_text_group_editor(
                    ui,
                    group_index,
                    overall_search_scope.as_deref(),
                    search_scope_locked,
                    group,
                );
            }
        });

    changed
}

fn draw_condition_editor_form_groups_section(
    ui: &mut Ui,
    requested_group_selection: &mut Option<usize>,
    condition: &mut ConditionEditorItem,
) -> bool {
    let mut changed = false;

    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new("form_groups").strong());
            if ui.button("group追加").clicked() {
                condition
                    .form_groups
                    .push(build_default_form_group_item(condition.form_groups.len()));
                *requested_group_selection = clamp_editor_index(
                    Some(condition.form_groups.len() - 1),
                    condition.form_groups.len(),
                );
                changed = true;
            }
        });

        if condition.form_groups.is_empty() {
            ui.label(RichText::new("group がありません").italics());
        } else {
            let mut remove_group_index = None;
            for (group_index, group) in condition.form_groups.iter().enumerate() {
                ui.horizontal(|ui| {
                    let label = summarize_form_group_label(group, group_index);
                    if ui
                        .selectable_label(*requested_group_selection == Some(group_index), label)
                        .clicked()
                    {
                        *requested_group_selection = Some(group_index);
                    }
                    if ui.button("削除").clicked() {
                        remove_group_index = Some(group_index);
                    }
                });
            }

            if let Some(group_index) = remove_group_index {
                condition.form_groups.remove(group_index);
                *requested_group_selection = clamp_editor_index(
                    requested_group_selection.map(|current| {
                        if current > group_index {
                            current - 1
                        } else {
                            current
                        }
                    }),
                    condition.form_groups.len(),
                );
                changed = true;
            }

            *requested_group_selection =
                clamp_editor_index(*requested_group_selection, condition.form_groups.len());
            let overall_search_scope = condition.overall_search_scope.clone();
            let search_scope_locked = !condition.annotation_filters.is_empty();
            if let Some(group_index) = *requested_group_selection {
                if let Some(group) = condition.form_groups.get_mut(group_index) {
                    changed |= draw_form_group_editor(
                        ui,
                        group_index,
                        overall_search_scope.as_deref(),
                        search_scope_locked,
                        group,
                    );
                }
            }
        }
    });

    changed
}

fn build_default_form_group_item(group_index: usize) -> FormGroupEditorItem {
    FormGroupEditorItem {
        match_logic: Some(if group_index == 0 { "and" } else { "or" }.to_string()),
        combine_logic: (group_index > 0).then_some("and".to_string()),
        forms: vec![String::new()],
        ..Default::default()
    }
}

fn build_default_text_group_item(group_index: usize) -> TextGroupEditorItem {
    TextGroupEditorItem {
        match_logic: Some(if group_index == 0 { "and" } else { "or" }.to_string()),
        combine_logic: (group_index > 0).then_some("and".to_string()),
        texts: vec![String::new()],
        ..Default::default()
    }
}
