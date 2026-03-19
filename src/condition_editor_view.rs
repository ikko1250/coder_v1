use crate::condition_editor::{AnnotationFilterItem, ConditionEditorItem, FormGroupEditorItem};
use crate::ui_helpers::ime_safe_singleline;
use egui::{Color32, RichText, Ui};

pub(crate) const CONDITION_EDITOR_FIELD_LABEL_WIDTH: f32 = 156.0;
pub(crate) const CONDITION_EDITOR_TEXT_INPUT_WIDTH: f32 = 280.0;
pub(crate) const CONDITION_EDITOR_CHOICE_WIDTH: f32 = 140.0;
const CONDITION_EDITOR_NUMBER_WIDTH: f32 = 120.0;
const CONDITION_EDITOR_LIST_INPUT_WIDTH: f32 = 280.0;
const CONDITION_EDITOR_FILTER_OPERATOR_WIDTH: f32 = 120.0;

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
    let logic_label = display_form_group_logic(group, group_index);
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
            let selected_logic = display_form_group_logic(group, group_index);
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
                        if ui.selectable_label(selected_logic == *option, *option).clicked() {
                            set_form_group_logic(group, group_index, option);
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
                if ui.selectable_label(current_value == *option, *option).clicked() {
                    *value = Some((*option).to_string());
                    changed = true;
                }
            }
        });

    changed
}

pub(crate) fn draw_string_list_editor(
    ui: &mut Ui,
    label: &str,
    values: &mut Vec<String>,
) -> bool {
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

fn display_form_group_logic(group: &FormGroupEditorItem, group_index: usize) -> &'static str {
    let match_logic = group.match_logic.as_deref().unwrap_or("and");
    if group_index == 0 {
        return match match_logic {
            "or" => "or",
            "not" => "not",
            _ => "and",
        };
    }

    match (group.combine_logic.as_deref().unwrap_or("and"), match_logic) {
        ("and", "or") => "and or",
        ("and", "not") => "and not",
        ("or", "and") => "or and",
        ("or", "not") => "or not",
        ("or", _) => "or",
        _ => "and",
    }
}

fn set_form_group_logic(group: &mut FormGroupEditorItem, group_index: usize, selected: &str) {
    if group_index == 0 {
        group.combine_logic = None;
        group.match_logic = Some(match selected {
            "or" => "or".to_string(),
            "not" => "not".to_string(),
            _ => "and".to_string(),
        });
        return;
    }

    let (combine_logic, match_logic) = match selected {
        "and or" => ("and", "or"),
        "and not" => ("and", "not"),
        "or and" => ("or", "and"),
        "or" => ("or", "or"),
        "or not" => ("or", "not"),
        _ => ("and", "and"),
    };
    group.combine_logic = Some(combine_logic.to_string());
    group.match_logic = Some(match_logic.to_string());
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

fn edit_optional_i64(
    ui: &mut Ui,
    value: &mut Option<i64>,
    default_value: i64,
    width: f32,
) -> bool {
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
