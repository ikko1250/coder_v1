use crate::filter::display_filter_value;
use crate::model::{FilterColumn, FilterOption};
use egui::{RichText, ScrollArea, Ui};
use std::collections::BTreeSet;

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
            ui.horizontal_wrapped(|ui| {
                ui.label("フィルター対象:");
                egui::ComboBox::from_id_salt("filter_column_selector")
                    .selected_text(active_column.label())
                    .show_ui(ui, |ui| {
                        for &column in FilterColumn::all() {
                            ui.selectable_value(&mut selected_column, column, column.label());
                        }
                    });
                ui.label(format!("適用中: {} 件", active_count));
                if ui.button("現在の列をクリア").clicked() {
                    response.clear_column_clicked = true;
                }
                if ui.button("全解除").clicked() {
                    response.clear_all_clicked = true;
                }
            });

            draw_wrapped_filter_options(ui, options, selected_values, &mut response);
            draw_active_filter_values(ui, active_values, &mut response);
        });

    if selected_column != active_column {
        response.selected_column = Some(selected_column);
    }

    response
}

fn draw_wrapped_filter_options(
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

            let max_item_width = ui.available_width() * 0.5;
            ui.horizontal_wrapped(|ui| {
                for option in options {
                    let is_selected =
                        selected_values.is_some_and(|values| values.contains(&option.value));
                    if let Some(next_checked) =
                        draw_filter_option_item(ui, option, is_selected, max_item_width)
                    {
                        response
                            .toggled_options
                            .push((option.value.clone(), next_checked));
                    }
                }
            });
        });
}

fn draw_filter_option_item(
    ui: &mut Ui,
    option: &FilterOption,
    is_selected: bool,
    max_item_width: f32,
) -> Option<bool> {
    let mut checked = is_selected;
    let mut changed = false;
    let full_label = format!(
        "{} ({})",
        display_filter_value(&option.value),
        option.count
    );

    let response = ui
        .push_id(("filter_option", option.value.as_str()), |ui| {
            ui.scope(|ui| {
                ui.set_max_width(max_item_width);
                ui.horizontal(|ui| {
                    let checkbox_response = ui.checkbox(&mut checked, "");
                    if checkbox_response.changed() {
                        changed = true;
                    }

                    let label_response = ui.add(
                        egui::Label::new(full_label.as_str())
                            .truncate()
                            .sense(egui::Sense::click()),
                    );
                    if label_response.clicked() {
                        checked = !checked;
                        changed = true;
                    }

                    checkbox_response.union(label_response)
                })
                .inner
            })
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
