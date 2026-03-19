use crate::model::DbViewerState;
use egui::{Color32, RichText, ScrollArea, Ui};

pub(crate) fn render_db_viewer_contents(
    ui: &mut Ui,
    state: &DbViewerState,
    previous_location: Option<(i64, i64)>,
    next_location: Option<(i64, i64)>,
    requested_location: &mut Option<(i64, i64)>,
) {
    let db_file_label = state
        .db_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ordinance_analysis3.db");
    let source_text = state.source_paragraph_text.clone().unwrap_or_default();
    let comparison_label = match state.context.as_ref() {
        Some(context) if context.center.paragraph_text != source_text => {
            RichText::new("表示中テキスト と DB 中心段落は不一致")
                .color(Color32::from_rgb(200, 64, 64))
        }
        Some(_) => {
            RichText::new("表示中テキスト と DB 中心段落は一致")
                .color(Color32::from_rgb(70, 130, 70))
        }
        None => RichText::new("DB 中心段落未取得").italics(),
    };
    let body_scroll_id = state
        .context
        .as_ref()
        .map(|context| format!("db_viewer_body_{}", context.center.paragraph_id))
        .or_else(|| {
            state
                .source_paragraph_id
                .map(|paragraph_id| format!("db_viewer_body_source_{}", paragraph_id))
        })
        .unwrap_or_else(|| "db_viewer_body_unknown".to_string());

    ui.label(format!("DB: {}", db_file_label));
    ui.label(format!("パス: {}", state.db_path.display()));

    if let Some(context) = &state.context {
        ui.label(format!(
            "paragraph_id: {} / document_id: {} / paragraph_no: {}",
            context.center.paragraph_id,
            context.center.document_id,
            context.center.paragraph_no
        ));
    } else if let Some(paragraph_id) = state.source_paragraph_id {
        ui.label(format!("paragraph_id: {}", paragraph_id));
    }

    ui.separator();

    ui.horizontal(|ui| {
        if ui
            .add_enabled(previous_location.is_some(), egui::Button::new("◀ 前へ"))
            .clicked()
        {
            *requested_location = previous_location;
        }

        let center_label = state
            .context
            .as_ref()
            .map(|context| format!("中心段落番号: {}", context.center.paragraph_no))
            .unwrap_or_else(|| "中心段落番号: -".to_string());
        ui.label(center_label);

        if ui
            .add_enabled(next_location.is_some(), egui::Button::new("次へ ▶"))
            .clicked()
        {
            *requested_location = next_location;
        }
    });

    ui.separator();

    if let Some(error) = &state.error_message {
        ui.colored_label(Color32::from_rgb(200, 64, 64), error);
        return;
    }

    ScrollArea::vertical()
        .id_salt(body_scroll_id)
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.group(|ui| {
                ui.label(comparison_label.clone());
            });

            ui.add_space(6.0);
            egui::CollapsingHeader::new("表示中テキスト/DB 比較")
                .id_salt("db_viewer_comparison_header")
                .default_open(false)
                .show(ui, |ui| {
                    ui.label(RichText::new("表示中テキスト").strong());
                    ui.label(&source_text);

                    ui.add_space(6.0);
                    ui.label(RichText::new("DB 中心段落").strong());
                    if let Some(context) = &state.context {
                        ui.group(|ui| {
                            ui.label(format!("paragraph_no: {}", context.center.paragraph_no));
                            ui.label(&context.center.paragraph_text);
                        });
                    } else {
                        ui.label(RichText::new("DB 中心段落未取得").italics());
                    }
                });

            ui.separator();
            ui.label(RichText::new("前後コンテキスト").strong());

            if let Some(context) = &state.context {
                for paragraph in &context.paragraphs {
                    let is_center = paragraph.paragraph_id == context.center.paragraph_id;
                    if is_center {
                        ui.group(|ui| {
                            ui.label(
                                RichText::new(format!("段落 {} (中心)", paragraph.paragraph_no))
                                    .strong()
                                    .color(Color32::from_rgb(200, 120, 40)),
                            );
                            ui.label(&paragraph.paragraph_text);
                        });
                    } else {
                        ui.label(
                            RichText::new(format!("段落 {}", paragraph.paragraph_no)).strong(),
                        );
                        ui.label(&paragraph.paragraph_text);
                    }
                    ui.add_space(8.0);
                }
            } else {
                ui.label(RichText::new("DB コンテキスト未取得").italics());
            }
        });
}
