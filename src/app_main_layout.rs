//! 中央レイアウト: 左ペイン（フィルタ・一覧）と詳細ペイン。親モジュール `app` の子。

use super::{App, TreeScrollRequest};
use crate::filter::normalize_filter_candidate_search_text;
use crate::filter_panel_view::draw_filter_panel as render_filter_panel;
use crate::model::{AnalysisRecord, FilterColumn, TextSegment};
use crate::viewer_core::ViewerCoreMessage;
use crate::ui_helpers::{ime_safe_multiline, ime_safe_singleline};
use eframe::egui;
use egui::text::{LayoutJob, TextFormat};
use egui::{Color32, RichText, ScrollArea, TextStyle, TextWrapMode, Ui};
use egui_extras::TableBuilder;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;

pub(super) fn draw_body(
    app: &mut App,
    ui: &mut Ui,
    tree_scroll_request: Option<TreeScrollRequest>,
) -> Option<usize> {
    let mut clicked_row = None;
    let available_width = ui.available_width().max(1.0);
    let record_list_panel_width_range = record_list_panel_width_range(available_width);
    let default_list_panel_width = (available_width * app.record_list_panel_ratio).clamp(
        *record_list_panel_width_range.start(),
        *record_list_panel_width_range.end(),
    );

    let list_panel_response = egui::SidePanel::left("record_list_panel")
        .resizable(true)
        .default_width(default_list_panel_width)
        .min_width(*record_list_panel_width_range.start())
        .max_width(*record_list_panel_width_range.end())
        .show_inside(ui, |ui| {
            draw_filters(app, ui);
            ui.separator();
            clicked_row = draw_tree(app, ui, tree_scroll_request);
        });

    app.record_list_panel_ratio = (list_panel_response.response.rect.width() / available_width)
        .clamp(
            super::RECORD_LIST_PANEL_MIN_WIDTH / available_width,
            super::RECORD_LIST_PANEL_MAX_RATIO,
        );

    egui::CentralPanel::default().show_inside(ui, |ui| {
        draw_detail(app, ui);
    });

    clicked_row
}

fn record_list_panel_width_range(available_width: f32) -> RangeInclusive<f32> {
    let max_width = (available_width * super::RECORD_LIST_PANEL_MAX_RATIO)
        .max(super::RECORD_LIST_PANEL_MIN_WIDTH)
        .min(1600.0);
    super::RECORD_LIST_PANEL_MIN_WIDTH..=max_width
}

fn draw_filters(app: &mut App, ui: &mut Ui) {
    let active_count: usize = app.core.selected_filter_values.values().map(BTreeSet::len).sum();
    let options = app
        .core
        .filter_options
        .get(&app.core.active_filter_column)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let selected_values = app.core.selected_filter_values.get(&app.core.active_filter_column);
    let candidate_query = app
        .core
        .filter_candidate_queries
        .get(&app.core.active_filter_column)
        .map(String::as_str)
        .unwrap_or("");
    let normalized_query = normalize_filter_candidate_search_text(candidate_query);
    let mut matching_options = Vec::new();
    let mut selected_non_matching_options = Vec::new();
    for option in options {
        let is_selected = selected_values.is_some_and(|values| values.contains(&option.value));
        let matches_query = normalized_query.is_empty()
            || normalize_filter_candidate_search_text(&option.value).contains(&normalized_query);

        if matches_query {
            matching_options.push(option.clone());
        } else if is_selected {
            selected_non_matching_options.push(option.clone());
        }
    }
    let active_values: Vec<(FilterColumn, String)> = app
        .core
        .selected_filter_values
        .iter()
        .flat_map(|(column, values)| {
            values
                .iter()
                .cloned()
                .map(|value| (*column, value))
                .collect::<Vec<_>>()
        })
        .collect();
    let response = render_filter_panel(
        ui,
        app.core.active_filter_column,
        active_count,
        &matching_options,
        &selected_non_matching_options,
        selected_values,
        &active_values,
        candidate_query,
        !options.is_empty(),
    );

    let response_column = app.core.active_filter_column;
    if let Some(updated_query) = response.updated_query {
        if updated_query.is_empty() {
            app.core.filter_candidate_queries.remove(&response_column);
        } else {
            app.core.filter_candidate_queries
                .insert(response_column, updated_query);
        }
    }
    if let Some(selected_column) = response.selected_column {
        app.core.active_filter_column = selected_column;
    }
    if response.clear_column_clicked {
        if app
            .apply_event(ViewerCoreMessage::FilterClearColumn(
                app.core.active_filter_column,
            ))
            .needs_repaint
        {
            ui.ctx().request_repaint();
        }
    }
    if response.clear_all_clicked {
        if app.apply_event(ViewerCoreMessage::FilterClearAll).needs_repaint {
            ui.ctx().request_repaint();
        }
    }
    for (value, selected) in response.toggled_options {
        if app
            .apply_event(ViewerCoreMessage::FilterToggle {
                column: app.core.active_filter_column,
                value,
                selected,
            })
            .needs_repaint
        {
            ui.ctx().request_repaint();
        }
    }
    for (column, value) in response.removed_active_values {
        if app
            .apply_event(ViewerCoreMessage::FilterToggle {
                column,
                value,
                selected: false,
            })
            .needs_repaint
        {
            ui.ctx().request_repaint();
        }
    }
}

fn draw_tree(
    app: &mut App,
    ui: &mut Ui,
    tree_scroll_request: Option<TreeScrollRequest>,
) -> Option<usize> {
    let filtered_indices = &app.core.filtered_indices;
    let selected_row = app.core.selected_row;
    let mut clicked_row = None;
    let selected_fill = Color32::from_rgb(70, 130, 180);
    let mut table = TableBuilder::new(ui)
        .striped(true)
        .resizable(true)
        .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

    for spec in super::TREE_COLUMN_SPECS {
        table = table.column((spec.build_column)());
    }

    if let Some(scroll_request) = tree_scroll_request {
        if scroll_request.row_index < filtered_indices.len() {
            table = table.scroll_to_row(scroll_request.row_index, scroll_request.align);
        }
    }

    table
        .header(24.0, |mut header| {
            for spec in super::TREE_COLUMN_SPECS {
                header.col(|ui| {
                    ui.strong(spec.header);
                });
            }
        })
        .body(|body| {
            body.rows(22.0, filtered_indices.len(), |mut row| {
                let i = row.index();
                let record = &app.core.all_records[filtered_indices[i]];
                let is_selected = selected_row == Some(i);

                let mut row_clicked = false;
                for spec in super::TREE_COLUMN_SPECS {
                    let value = (spec.value)(record);
                    row.col(|ui| {
                        let cell_rect = ui.max_rect();
                        if is_selected {
                            ui.painter().rect_filled(cell_rect, 0.0, selected_fill);
                        }

                        let cell_response = ui.interact(
                            cell_rect,
                            ui.id().with("cell_click"),
                            egui::Sense::click(),
                        );

                        let rich_text = if is_selected {
                            RichText::new(value).color(Color32::WHITE)
                        } else {
                            RichText::new(value)
                        };

                        let label_response = ui.add(
                            egui::Label::new(rich_text)
                                .truncate()
                                .sense(egui::Sense::click()),
                        );
                        if (cell_response | label_response).clicked() {
                            row_clicked = true;
                        }
                    });
                }

                if row_clicked {
                    clicked_row = Some(i);
                }
            });
        });

    clicked_row
}

fn draw_detail(app: &mut App, ui: &mut Ui) {
    if let Some(record) = app.selected_record().cloned() {
        app.draw_db_viewer_button(ui, record.supports_db_viewer());
        if !record.supports_db_viewer() {
            ui.label("sentence 行では DB viewer は無効です。");
        }
        ui.add_space(6.0);
        draw_record_summary(ui, &record);
        ui.separator();

        let detail_job = build_record_text_layout_job(ui, &app.get_segments());

        if record.supports_manual_annotation() {
            if app.annotation_panel_expanded {
                egui::TopBottomPanel::bottom("annotation_editor_panel_expanded")
                    .resizable(false)
                    .default_height(230.0)
                    .min_height(200.0)
                    .show_inside(ui, |ui| {
                        draw_annotation_editor_panel(app, ui, &record);
                    });
            } else {
                egui::TopBottomPanel::bottom("annotation_editor_panel_collapsed")
                    .resizable(false)
                    .min_height(0.0)
                    .show_inside(ui, |ui| {
                        draw_annotation_editor_collapsed_bar(app, ui, &record);
                    });
            }
        } else {
            egui::TopBottomPanel::bottom("annotation_editor_panel_collapsed")
                .resizable(false)
                .min_height(0.0)
                .show_inside(ui, |ui| {
                    draw_annotation_editor_collapsed_bar(app, ui, &record);
                });
        }

        egui::CentralPanel::default().show_inside(ui, |ui| {
            draw_record_text_panel(ui, &record, detail_job);
        });
    } else {
        app.draw_db_viewer_button(ui, false);
        ui.add_space(6.0);
        ui.label(RichText::new("レコード未選択").italics());
    }
}

fn draw_record_summary(ui: &mut Ui, record: &AnalysisRecord) {
    ui.label(
        RichText::new(format!(
            "{} / {} / {}={}",
            record.municipality_name,
            record.ordinance_or_rule,
            record.analysis_unit.id_column_name(),
            record.unit_id()
        ))
        .size(14.0)
        .strong(),
    );

    ui.add_space(6.0);
    ui.label(format!("document_id: {}", record.document_id));
    ui.label(format!("paragraph_id: {}", record.paragraph_id));
    if !record.sentence_id.trim().is_empty() {
        ui.label(format!("sentence_id: {}", record.sentence_id));
    }
    ui.label(format!("doc_type: {}", record.doc_type));
    ui.label(format!("sentence_count: {}", record.sentence_count));
    if !record.sentence_no_in_paragraph.trim().is_empty() {
        ui.label(format!(
            "sentence_no_in_paragraph: {}",
            record.sentence_no_in_paragraph
        ));
    }
    if !record.sentence_no_in_document.trim().is_empty() {
        ui.label(format!(
            "sentence_no_in_document: {}",
            record.sentence_no_in_document
        ));
    }

    ui.add_space(6.0);
    ui.label(format!("categories: {}", record.matched_categories_text));
    ui.label(format!("conditions: {}", record.matched_condition_ids_text));
    ui.label(format!("match_groups: {}", record.match_group_ids_text));
    ui.label(format!(
        "annotated_tokens: {}",
        record.annotated_token_count
    ));
    ui.label(format!(
        "manual_annotations: {}",
        record.manual_annotation_count
    ));
    if !record.mixed_scope_warning_text.trim().is_empty() {
        ui.colored_label(
            Color32::from_rgb(196, 110, 0),
            format!("promotion warning: {}", record.mixed_scope_warning_text),
        );
    }
}

fn draw_record_text_panel(ui: &mut Ui, record: &AnalysisRecord, detail_job: LayoutJob) {
    ScrollArea::vertical()
        .id_salt("detail_scroll")
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.add(egui::Label::new(detail_job).wrap());
            draw_form_group_explanations_panel(ui, record);
        });
}

fn draw_form_group_explanations_panel(ui: &mut Ui, record: &AnalysisRecord) {
    let has_form = !record.form_group_explanations_text.trim().is_empty();
    let has_text_groups = !record.text_groups_explanations_text.trim().is_empty();
    if !has_form && !has_text_groups {
        return;
    }

    ui.add_space(10.0);
    egui::CollapsingHeader::new("高度条件の説明")
        .default_open(false)
        .show(ui, |ui| {
            ui.label(RichText::new("高度条件の説明を表示中。本文強調は一部未対応です。").italics());
            ui.small(
                "analysis_unit=sentence でも paragraph 条件を評価します。paragraph 一致時は同一 paragraph の sentence を表示に含めます。",
            );
            ui.small("本文強調は直接ヒット token のみです。paragraph 展開で含まれた sentence は非強調で表示されます。");
            ui.small("text_groups のみ一致の段落ではトークン強調が付かないことがあります。下の text 説明を参照してください。");
            if !record.matched_form_group_ids_text.trim().is_empty() {
                ui.label(format!("group_ids: {}", record.matched_form_group_ids_text));
            }
            if !record.matched_form_group_logics_text.trim().is_empty() {
                ui.label(format!(
                    "group_logics: {}",
                    record.matched_form_group_logics_text
                ));
            }
            if !record.mixed_scope_warning_text.trim().is_empty() {
                ui.colored_label(
                    Color32::from_rgb(196, 110, 0),
                    record.mixed_scope_warning_text.as_str(),
                );
            }
            if has_form {
                ui.strong("form_groups");
                ScrollArea::vertical()
                    .id_salt("form_group_explanations_scroll")
                    .max_height(160.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(record.form_group_explanations_text.as_str())
                                .wrap_mode(TextWrapMode::Wrap),
                        );
                    });
            }
            if has_text_groups {
                ui.add_space(6.0);
                ui.strong("text_groups");
                ScrollArea::vertical()
                    .id_salt("text_groups_explanations_scroll")
                    .max_height(160.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(record.text_groups_explanations_text.as_str())
                                .wrap_mode(TextWrapMode::Wrap),
                        );
                    });
            }
        });
}

fn draw_annotation_editor_collapsed_bar(app: &mut App, ui: &mut Ui, record: &AnalysisRecord) {
    let annotation_supported = record.supports_manual_annotation();

    let response = ui
        .horizontal(|ui| {
            if annotation_supported {
                ui.label(RichText::new("▶").strong());

                let count_str = if record.manual_annotation_count == "0"
                    || record.manual_annotation_count.is_empty()
                {
                    "なし".to_string()
                } else {
                    format!("{}件", record.manual_annotation_count)
                };

                ui.label(RichText::new(format!("annotation 追記 ({})", count_str)).strong());
            } else {
                ui.label(RichText::new("▶").color(ui.visuals().weak_text_color()));
                ui.label(
                    RichText::new("sentence 行では manual annotation editor は無効です。")
                        .color(ui.visuals().weak_text_color()),
                );
            }
        })
        .response;

    if annotation_supported && response.interact(egui::Sense::click()).clicked() {
        app.annotation_panel_expanded = true;
    }
}

fn draw_annotation_editor_panel(app: &mut App, ui: &mut Ui, record: &AnalysisRecord) {
    let annotation_supported = record.supports_manual_annotation();
    let annotation_summary = if record.manual_annotation_pairs_text.trim().is_empty() {
        "annotation なし".to_string()
    } else {
        record.manual_annotation_pairs_text.clone()
    };
    let annotation_path_label = app
        .resolved_annotation_csv_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|error| format!("解決失敗: {error}"));
    let annotation_save_enabled = app.annotation_save_enabled();

    ui.group(|ui| {
        let title_response = ui
            .horizontal(|ui| {
                ui.label(RichText::new("▼").strong());
                ui.label(RichText::new("annotation 追記").strong());
            })
            .response;

        if title_response.interact(egui::Sense::click()).clicked() {
            app.annotation_panel_expanded = false;
        }
        ui.label(format!("保存先: {annotation_path_label}"));
        if !annotation_supported {
            ui.label("sentence 行では manual annotation editor は無効です。");
        }
        ui.label(format!(
            "現在件数: {} / namespaces: {}",
            record.manual_annotation_count,
            if record.manual_annotation_namespaces_text.trim().is_empty() {
                "(なし)"
            } else {
                &record.manual_annotation_namespaces_text
            }
        ));

        ScrollArea::vertical()
            .id_salt("annotation_summary_scroll")
            .max_height(56.0)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.add(egui::Label::new(annotation_summary.as_str()).wrap());
            });

        ui.add_space(6.0);
        ui.add_enabled_ui(annotation_save_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("namespace");
                ui.add(ime_safe_singleline(
                    &mut app.annotation_editor_state.namespace_input,
                ));
                ui.label("key");
                ui.add(ime_safe_singleline(
                    &mut app.annotation_editor_state.key_input,
                ));
            });
            ui.horizontal(|ui| {
                ui.label("tagged_by");
                ui.add(ime_safe_singleline(
                    &mut app.annotation_editor_state.tagged_by_input,
                ));
                ui.label("confidence");
                ui.add(ime_safe_singleline(
                    &mut app.annotation_editor_state.confidence_input,
                ));
            });
            ui.label(RichText::new("改行は Shift+Enter").italics());
            ui.label("value");
            ui.add(
                ime_safe_multiline(&mut app.annotation_editor_state.value_input).desired_rows(2),
            );
            ui.label("note");
            ui.add(ime_safe_multiline(&mut app.annotation_editor_state.note_input).desired_rows(2));
        });

        ui.horizontal(|ui| {
            if ui
                .add_enabled(annotation_save_enabled, egui::Button::new("追記"))
                .clicked()
            {
                app.save_annotation_for_selected_record();
            }
            if ui.button("入力クリア").clicked() {
                app.clear_annotation_editor_inputs();
                app.clear_annotation_editor_status();
            }
            if !annotation_supported {
                ui.label("sentence annotation 対応までは paragraph 専用です。");
            } else if !annotation_save_enabled {
                ui.label("分析ジョブ実行中は保存できません。");
            }
        });

        if let Some(status_message) = &app.annotation_editor_state.status_message {
            ui.colored_label(
                editor_status_color(app.annotation_editor_state.status_is_error),
                status_message,
            );
        }
    });
}

fn build_record_text_layout_job(ui: &Ui, segments: &[TextSegment]) -> LayoutJob {
    let mut job = LayoutJob::default();
    let normal_format = TextFormat {
        font_id: TextStyle::Body.resolve(ui.style()),
        color: ui.visuals().text_color(),
        ..Default::default()
    };
    let hit_format = TextFormat {
        background: Color32::from_rgb(255, 224, 138),
        ..normal_format.clone()
    };

    for seg in segments {
        if seg.text.is_empty() {
            continue;
        }
        let format = if seg.is_hit {
            hit_format.clone()
        } else {
            normal_format.clone()
        };
        job.append(&seg.text, 0.0, format);
    }

    job
}

fn editor_status_color(is_error: bool) -> Color32 {
    if is_error {
        Color32::from_rgb(200, 64, 64)
    } else {
        Color32::from_rgb(70, 130, 70)
    }
}
