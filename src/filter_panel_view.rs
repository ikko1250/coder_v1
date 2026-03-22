use crate::filter::display_filter_value;
use crate::model::{FilterColumn, FilterOption};
use egui::{RichText, ScrollArea, Ui};
use std::collections::BTreeSet;

const FILTER_OPTION_COUNT_WIDTH: f32 = 56.0;
const FILTER_OPTION_CHECKBOX_WIDTH: f32 = 24.0;
const FILTER_OPTION_SCROLLBAR_MARGIN: f32 = 16.0;

#[derive(Clone, Debug, Default)]
pub(crate) struct FilterPanelResponse {
    pub(crate) selected_column: Option<FilterColumn>,
    pub(crate) clear_column_clicked: bool,
    pub(crate) clear_all_clicked: bool,
    pub(crate) toggled_options: Vec<(String, bool)>,
    pub(crate) removed_active_values: Vec<(FilterColumn, String)>,
}

pub(crate) fn draw_filter_panel(
    ui: &mut Ui,
    active_column: FilterColumn,
    active_count: usize,
    options: &[FilterOption],
    selected_values: Option<&BTreeSet<String>>,
    active_values: &[(FilterColumn, String)],
) -> FilterPanelResponse {
    let mut response = FilterPanelResponse::default();
    let mut selected_column = active_column;

    egui::CollapsingHeader::new(format!("Filters ({})", active_count))
        .id_salt("filters_panel")
        .default_open(true)
        .show(ui, |ui| {
            draw_filter_header(
                ui,
                active_column,
                active_count,
                &mut selected_column,
                &mut response,
            );

            draw_fixed_column_filter_options(ui, options, selected_values, &mut response);
            draw_active_filter_values(ui, active_values, &mut response);
        });

    if selected_column != active_column {
        response.selected_column = Some(selected_column);
    }

    response
}

fn draw_filter_header(
    ui: &mut Ui,
    active_column: FilterColumn,
    active_count: usize,
    selected_column: &mut FilterColumn,
    response: &mut FilterPanelResponse,
) {
    ui.horizontal(|ui| {
        ui.label("フィルター対象:");
        egui::ComboBox::from_id_salt("filter_column_selector")
            .selected_text(active_column.label())
            .show_ui(ui, |ui| {
                for &column in FilterColumn::all() {
                    ui.selectable_value(selected_column, column, column.label());
                }
            });
        ui.label(format!("適用中: {} 件", active_count));
    });

    ui.horizontal(|ui| {
        if ui.button("現在の列をクリア").clicked() {
            response.clear_column_clicked = true;
        }
        if ui.button("全解除").clicked() {
            response.clear_all_clicked = true;
        }
    });
}

fn draw_fixed_column_filter_options(
    ui: &mut Ui,
    options: &[FilterOption],
    selected_values: Option<&BTreeSet<String>>,
    response: &mut FilterPanelResponse,
) {
    ScrollArea::vertical()
        .id_salt("filter_options_scroll")
        .max_height(180.0)
        .show(ui, |ui| {
            if options.is_empty() {
                ui.label(RichText::new("候補なし").italics());
                return;
            }

            let safe_width = (ui.available_width() - FILTER_OPTION_SCROLLBAR_MARGIN).max(0.0);
            let column_count = filter_option_column_count(safe_width);
            let spacing_x = ui.spacing().item_spacing.x;
            let item_width = if column_count <= 1 {
                safe_width
            } else {
                (safe_width - (column_count - 1) as f32 * spacing_x) / column_count as f32
            };

            // Tighter vertical spacing for filter options
            ui.spacing_mut().item_spacing.y = 4.0;

            for row_options in options.chunks(column_count) {
                ui.horizontal(|ui| {
                    for option in row_options {
                        let is_selected =
                            selected_values.is_some_and(|values| values.contains(&option.value));
                        if let Some(next_checked) =
                            draw_filter_option_item(ui, option, is_selected, item_width)
                        {
                            response
                                .toggled_options
                                .push((option.value.clone(), next_checked));
                        }
                    }

                    for _ in row_options.len()..column_count {
                        ui.allocate_space(egui::vec2(item_width, 0.0));
                    }
                });
            }
        });
}

fn filter_option_column_count(available_width: f32) -> usize {
    if available_width < 290.0 {
        1
    } else if available_width < 435.0 {
        2
    } else if available_width < 580.0 {
        3
    } else {
        4
    }
}

fn draw_filter_option_item(
    ui: &mut Ui,
    option: &FilterOption,
    is_selected: bool,
    item_width: f32,
) -> Option<bool> {
    let mut checked = is_selected;
    let mut changed = false;
    let label_text = display_filter_value(&option.value);
    let full_label = format!("{} ({})", label_text, option.count);
    let spacing_x = ui.spacing().item_spacing.x;
    let label_width =
        (item_width - FILTER_OPTION_CHECKBOX_WIDTH - FILTER_OPTION_COUNT_WIDTH - spacing_x * 2.0)
            .max(24.0);

    // To prevent line spacing from being too wide, override spacing for this specific widget
    let response = ui
        .push_id(("filter_option", option.value.as_str()), |ui| {
            let layout_height = ui.spacing().interact_size.y;

            ui.allocate_ui_with_layout(
                egui::vec2(item_width, layout_height),
                egui::Layout::left_to_right(egui::Align::Center),
                |ui| {
                    ui.set_width(item_width);

                    // We need to vertically center everything manually since ui.add_sized can be weird
                    let checkbox_response = ui.allocate_ui_with_layout(
                        egui::vec2(FILTER_OPTION_CHECKBOX_WIDTH, layout_height),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| ui.add(egui::Checkbox::without_text(&mut checked)),
                    ).inner;
                    if checkbox_response.changed() {
                        changed = true;
                    }

                    let label_response = ui.allocate_ui_with_layout(
                        egui::vec2(label_width, layout_height),
                        egui::Layout::left_to_right(egui::Align::Center),
                        |ui| {
                            // Using add_sized ensures the clickable area fills the width,
                            // while left-to-right alignment automatically aligns the text to the left.
                            let r = ui.add_sized(
                                [label_width, layout_height],
                                egui::Label::new(label_text.as_str())
                                    .truncate()
                                    .selectable(false)
                                    .halign(egui::Align::LEFT)
                                    .sense(egui::Sense::click()),
                            );
                            r
                        },
                    ).inner;
                    if label_response.clicked() {
                        checked = !checked;
                        changed = true;
                    }

                    ui.allocate_ui_with_layout(
                        egui::vec2(FILTER_OPTION_COUNT_WIDTH, layout_height),
                        egui::Layout::right_to_left(egui::Align::Center),
                        |ui| {
                            ui.label(format!("({})", option.count));
                        },
                    );

                    checkbox_response.union(label_response)
                },
            )
            .inner
        })
        .inner;
    response.on_hover_text(full_label);

    changed.then_some(checked)
}

fn draw_active_filter_values(
    ui: &mut Ui,
    active_values: &[(FilterColumn, String)],
    response: &mut FilterPanelResponse,
) {
    if active_values.is_empty() {
        return;
    }

    ui.add_space(4.0);
    ui.horizontal_wrapped(|ui| {
        ui.label("適用中:");
        for (column, value) in active_values {
            let button_label =
                format!("{}: {} ×", column.label(), display_filter_value(value));
            if ui.small_button(button_label).clicked() {
                response
                    .removed_active_values
                    .push((*column, value.clone()));
            }
        }
    });
}
