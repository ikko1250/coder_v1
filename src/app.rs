use crate::csv_loader::load_records;
use crate::db::{
    fetch_paragraph_context, fetch_paragraph_context_by_location, resolve_default_db_path,
};
use crate::filter::{build_filter_options, display_filter_value};
use crate::model::{CsvRecord, DbViewerState, FilterColumn, FilterOption, TextSegment};
use crate::tagged_text::parse_tagged_text;
use eframe::egui;
use egui::text::{LayoutJob, TextFormat};
use egui::{Color32, RichText, ScrollArea, TextStyle, Ui};
use egui_extras::{Column, TableBuilder};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreeScrollRequest {
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
    value: fn(&CsvRecord) -> String,
}

const TREE_COLUMN_SPECS: &[TreeColumnSpec] = &[
    TreeColumnSpec {
        header: "No",
        build_column: build_tree_row_no_column,
        value: tree_row_no_value,
    },
    TreeColumnSpec {
        header: "paragraph_id",
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
        header: "強調token数",
        build_column: build_tree_annotated_token_count_column,
        value: tree_annotated_token_count_value,
    },
];

const DB_VIEWER_VIEWPORT_ID: &str = "db_viewer_viewport";

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

fn clamp_selected_row(selected_row: Option<usize>, filtered_len: usize) -> Option<usize> {
    match (selected_row, filtered_len) {
        (_, 0) => None,
        (Some(idx), len) => Some(idx.min(len - 1)),
        (None, _) => None,
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
    csv_path: PathBuf,
    db_viewer_state: DbViewerState,
    all_records: Vec<CsvRecord>,
    filtered_indices: Vec<usize>,
    filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    active_filter_column: FilterColumn,
    selected_row: Option<usize>,
    pending_tree_scroll: Option<TreeScrollRequest>,
    pub(crate) error_message: Option<String>,
    cached_segments: Option<(usize, Vec<TextSegment>)>,
}

impl App {
    pub(crate) fn new(csv_path: PathBuf) -> Self {
        let mut app = Self {
            csv_path: csv_path.clone(),
            db_viewer_state: DbViewerState::new(resolve_default_db_path()),
            all_records: Vec::new(),
            filtered_indices: Vec::new(),
            filter_options: HashMap::new(),
            selected_filter_values: HashMap::new(),
            active_filter_column: FilterColumn::MatchedCategories,
            selected_row: None,
            pending_tree_scroll: None,
            error_message: None,
            cached_segments: None,
        };
        app.load_csv(csv_path);
        app
    }

    fn load_csv(&mut self, path: PathBuf) {
        match load_records(&path) {
            Ok(records) => {
                self.all_records = records;
                self.csv_path = path;
                self.db_viewer_state.reset_loaded_state();
                self.filter_options = build_filter_options(&self.all_records);
                self.selected_filter_values.clear();
                self.filtered_indices = (0..self.all_records.len()).collect();
                self.cached_segments = None;
                self.apply_selection_change(SelectionChange::first_filtered_row(
                    self.filtered_indices.len(),
                    ScrollBehavior::AlignMin,
                ));
                self.error_message = None;
            }
            Err(e) => {
                self.error_message = Some(e);
            }
        }
    }

    #[allow(dead_code)]
    fn db_viewer_state(&self) -> &DbViewerState {
        &self.db_viewer_state
    }

    #[allow(dead_code)]
    fn db_viewer_state_mut(&mut self) -> &mut DbViewerState {
        &mut self.db_viewer_state
    }

    #[allow(dead_code)]
    fn selected_paragraph_id_for_db(&self) -> Result<i64, String> {
        let record = self
            .selected_record()
            .ok_or_else(|| "レコードが選択されていません".to_string())?;

        record.paragraph_id.parse::<i64>().map_err(|error| {
            format!(
                "paragraph_id を数値として解釈できません: {} ({error})",
                record.paragraph_id
            )
        })
    }

    #[allow(dead_code)]
    fn prepare_db_viewer_state(&mut self) -> Result<(), String> {
        let paragraph_id = self.selected_paragraph_id_for_db()?;
        let csv_paragraph_text = self
            .selected_record()
            .map(|record| record.paragraph_text.clone())
            .ok_or_else(|| "レコードが選択されていません".to_string())?;

        self.db_viewer_state.is_open = true;
        self.db_viewer_state.source_paragraph_id = Some(paragraph_id);
        self.db_viewer_state.csv_paragraph_text = Some(csv_paragraph_text);
        self.db_viewer_state.context = None;
        self.db_viewer_state.error_message = None;
        Ok(())
    }

    fn apply_selection_change(&mut self, change: SelectionChange) -> bool {
        let next = clamp_selected_row(change.selected_row, self.filtered_indices.len());
        let selection_changed = self.selected_row != next;
        if selection_changed {
            self.selected_row = next;
            self.cached_segments = None;
        }

        let next_scroll_request = build_tree_scroll_request(next, change.scroll_behavior);
        let scroll_changed = self.pending_tree_scroll != next_scroll_request;
        self.pending_tree_scroll = next_scroll_request;

        selection_changed || scroll_changed
    }

    fn select_first_filtered_row(&mut self, scroll_behavior: ScrollBehavior) -> bool {
        self.apply_selection_change(SelectionChange::first_filtered_row(
            self.filtered_indices.len(),
            scroll_behavior,
        ))
    }

    fn move_selection_up(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        match self.selected_row {
            Some(idx) if idx > 0 => {
                self.apply_selection_change(SelectionChange::new(
                    Some(idx - 1),
                    ScrollBehavior::KeepVisible,
                ));
            }
            None => {
                self.select_first_filtered_row(ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    fn move_selection_down(&mut self) {
        let current_len = self.filtered_indices.len();
        if current_len == 0 {
            return;
        }

        match self.selected_row {
            Some(idx) if idx + 1 < current_len => {
                self.apply_selection_change(SelectionChange::new(
                    Some(idx + 1),
                    ScrollBehavior::KeepVisible,
                ));
            }
            None => {
                self.select_first_filtered_row(ScrollBehavior::AlignMin);
            }
            _ => {}
        }
    }

    fn handle_keyboard_navigation(&mut self, ctx: &egui::Context) {
        if self.error_message.is_some()
            || self.filtered_indices.is_empty()
            || ctx.wants_keyboard_input()
        {
            return;
        }

        let (up_pressed, down_pressed) = ctx.input(|i| {
            (
                i.key_pressed(egui::Key::ArrowUp),
                i.key_pressed(egui::Key::ArrowDown),
            )
        });

        if down_pressed {
            self.move_selection_down();
        } else if up_pressed {
            self.move_selection_up();
        }
    }

    fn selected_record(&self) -> Option<&CsvRecord> {
        let filtered_idx = self.selected_row?;
        let record_idx = *self.filtered_indices.get(filtered_idx)?;
        self.all_records.get(record_idx)
    }

    fn apply_filters(&mut self) {
        self.filtered_indices = self
            .all_records
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| self.record_matches_filters(record).then_some(idx))
            .collect();
        self.cached_segments = None;
        self.select_first_filtered_row(ScrollBehavior::AlignMin);
    }

    fn record_matches_filters(&self, record: &CsvRecord) -> bool {
        self.selected_filter_values
            .iter()
            .all(|(column, selected)| column.matches(record, selected))
    }

    fn clear_filters_for_column(&mut self, column: FilterColumn) {
        if self.selected_filter_values.remove(&column).is_some() {
            self.apply_filters();
        }
    }

    fn clear_all_filters(&mut self) {
        if !self.selected_filter_values.is_empty() {
            self.selected_filter_values.clear();
            self.apply_filters();
        }
    }

    fn toggle_filter_value(&mut self, column: FilterColumn, value: &str, selected: bool) {
        let changed = {
            let entry = self.selected_filter_values.entry(column).or_default();
            if selected {
                entry.insert(value.to_string())
            } else {
                entry.remove(value)
            }
        };

        if self
            .selected_filter_values
            .get(&column)
            .is_some_and(BTreeSet::is_empty)
        {
            self.selected_filter_values.remove(&column);
        }

        if changed {
            self.apply_filters();
        }
    }

    fn get_segments(&mut self) -> Vec<TextSegment> {
        if let Some(record) = self.selected_record() {
            let row_no = record.row_no;
            if let Some((cached_row, ref segs)) = self.cached_segments {
                if cached_row == row_no {
                    return segs.clone();
                }
            }
            let tagged = record.paragraph_text_tagged.clone();
            let segs = parse_tagged_text(&tagged);
            self.cached_segments = Some((row_no, segs.clone()));
            segs
        } else {
            Vec::new()
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_keyboard_navigation(ctx);

        if let Some(err) = self.error_message.clone() {
            egui::Window::new("エラー")
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label(&err);
                    if ui.button("閉じる").clicked() {
                        self.error_message = None;
                    }
                });
        }

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            self.draw_toolbar(ui);
        });

        let consumed_tree_scroll = self.pending_tree_scroll;
        let mut clicked_row = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            clicked_row = self.draw_body(ui, consumed_tree_scroll);
        });

        self.draw_db_viewer_window(ctx);

        if let Some(row_index) = clicked_row {
            if self.apply_selection_change(SelectionChange::new(
                Some(row_index),
                ScrollBehavior::KeepVisible,
            )) {
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
        let response = ui.add_enabled(enabled, egui::Button::new("DB参照"));
        if response.clicked() {
            if let Err(error) = self.open_db_viewer_for_selected_record() {
                self.error_message = Some(error);
            }
        }
    }

    fn open_db_viewer_for_selected_record(&mut self) -> Result<(), String> {
        self.prepare_db_viewer_state()?;
        self.load_db_viewer_context();
        Ok(())
    }

    fn load_db_viewer_context(&mut self) {
        let Some(paragraph_id) = self.db_viewer_state.source_paragraph_id else {
            self.db_viewer_state.context = None;
            self.db_viewer_state.error_message = Some("参照元 paragraph_id が未設定です".to_string());
            self.db_viewer_state.is_open = true;
            return;
        };

        match fetch_paragraph_context(&self.db_viewer_state.db_path, paragraph_id) {
            Ok(context) => {
                self.db_viewer_state.context = Some(context);
                self.db_viewer_state.error_message = None;
                self.db_viewer_state.is_open = true;
            }
            Err(error) => {
                self.db_viewer_state.context = None;
                self.db_viewer_state.error_message = Some(error);
                self.db_viewer_state.is_open = true;
            }
        }
    }

    fn load_db_viewer_context_for_location(&mut self, document_id: i64, paragraph_no: i64) {
        match fetch_paragraph_context_by_location(
            &self.db_viewer_state.db_path,
            document_id,
            paragraph_no,
        ) {
            Ok(context) => {
                self.db_viewer_state.context = Some(context);
                self.db_viewer_state.error_message = None;
            }
            Err(error) => {
                self.db_viewer_state.context = None;
                self.db_viewer_state.error_message = Some(error);
            }
        }
    }

    fn previous_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.db_viewer_state.context.as_ref()?;
        let previous_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no < context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .max()?;

        Some((context.center.document_id, previous_paragraph_no))
    }

    fn next_db_viewer_location(&self) -> Option<(i64, i64)> {
        let context = self.db_viewer_state.context.as_ref()?;
        let next_paragraph_no = context
            .paragraphs
            .iter()
            .filter(|paragraph| paragraph.paragraph_no > context.center.paragraph_no)
            .map(|paragraph| paragraph.paragraph_no)
            .min()?;

        Some((context.center.document_id, next_paragraph_no))
    }

    fn draw_db_viewer_window(&mut self, ctx: &egui::Context) {
        if !self.db_viewer_state.is_open {
            return;
        }

        let snapshot = self.db_viewer_state.clone();
        let previous_location = self.previous_db_viewer_location();
        let next_location = self.next_db_viewer_location();
        let mut requested_location = None;
        let mut close_requested = false;
        let viewport_id = egui::ViewportId::from_hash_of(DB_VIEWER_VIEWPORT_ID);
        let builder = egui::ViewportBuilder::default()
            .with_title("DB コンテキスト参照")
            .with_inner_size([760.0, 820.0])
            .with_resizable(true);

        ctx.show_viewport_immediate(viewport_id, builder, |viewport_ctx, class| {
            close_requested = viewport_ctx.input(|input| input.viewport().close_requested());

            match class {
                egui::ViewportClass::Embedded => {
                    let mut fallback_open = true;
                    egui::Window::new("DB コンテキスト参照")
                        .open(&mut fallback_open)
                        .default_width(760.0)
                        .default_height(820.0)
                        .resizable(true)
                        .show(viewport_ctx, |ui| {
                            render_db_viewer_contents(
                                ui,
                                &snapshot,
                                previous_location,
                                next_location,
                                &mut requested_location,
                            );
                        });

                    if !fallback_open {
                        close_requested = true;
                    }
                }
                _ => {
                    egui::CentralPanel::default().show(viewport_ctx, |ui| {
                        render_db_viewer_contents(
                            ui,
                            &snapshot,
                            previous_location,
                            next_location,
                            &mut requested_location,
                        );
                    });
                }
            }
        });

        if close_requested {
            self.db_viewer_state.is_open = false;
            return;
        }

        if let Some((document_id, paragraph_no)) = requested_location {
            self.load_db_viewer_context_for_location(document_id, paragraph_no);
            ctx.request_repaint();
        }
    }

    fn draw_toolbar(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.label("CSV:");
            let path_str = self.csv_path.display().to_string();
            ui.add(
                egui::TextEdit::singleline(&mut path_str.as_str())
                    .desired_width(600.0)
                    .interactive(false),
            );

            if ui.button("開く").clicked() {
                if let Some(path) = rfd::FileDialog::new()
                    .add_filter("CSV files", &["csv"])
                    .add_filter("All files", &["*"])
                    .pick_file()
                {
                    self.load_csv(path);
                }
            }

            ui.separator();
            let selected_position = self
                .selected_row
                .map(|idx| idx + 1)
                .map(|position| position.to_string())
                .unwrap_or_else(|| "-".to_string());
            ui.label(format!(
                "総件数: {} 件  抽出後: {} 件  選択: {} / {}",
                self.all_records.len(),
                self.filtered_indices.len(),
                selected_position,
                self.filtered_indices.len()
            ));
        });
    }

    fn draw_body(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let mut clicked_row = None;
        egui::SidePanel::left("record_list_panel")
            .resizable(true)
            .default_width(620.0)
            .min_width(360.0)
            .show_inside(ui, |ui| {
                self.draw_filters(ui);
                ui.separator();
                clicked_row = self.draw_tree(ui, tree_scroll_request);
            });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.draw_detail(ui);
        });

        clicked_row
    }

    fn draw_filters(&mut self, ui: &mut Ui) {
        let active_count: usize = self
            .selected_filter_values
            .values()
            .map(BTreeSet::len)
            .sum();

        egui::CollapsingHeader::new(format!("Filters ({})", active_count))
            .id_salt("filters_panel")
            .default_open(true)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label("フィルター対象:");
                    egui::ComboBox::from_id_salt("filter_column_selector")
                        .selected_text(self.active_filter_column.label())
                        .show_ui(ui, |ui| {
                            for &column in FilterColumn::all() {
                                ui.selectable_value(
                                    &mut self.active_filter_column,
                                    column,
                                    column.label(),
                                );
                            }
                        });
                    ui.label(format!("適用中: {} 件", active_count));
                    if ui.button("現在の列をクリア").clicked() {
                        self.clear_filters_for_column(self.active_filter_column);
                    }
                    if ui.button("全解除").clicked() {
                        self.clear_all_filters();
                    }
                });

                let options = self
                    .filter_options
                    .get(&self.active_filter_column)
                    .cloned()
                    .unwrap_or_default();

                ScrollArea::vertical()
                    .id_salt("filter_options_scroll")
                    .max_height(180.0)
                    .show(ui, |ui| {
                        if options.is_empty() {
                            ui.label(RichText::new("候補なし").italics());
                        } else {
                            for option in options {
                                let is_selected = self
                                    .selected_filter_values
                                    .get(&self.active_filter_column)
                                    .is_some_and(|values| values.contains(&option.value));
                                let mut checked = is_selected;
                                let label = format!(
                                    "{} ({})",
                                    display_filter_value(&option.value),
                                    option.count
                                );
                                if ui.checkbox(&mut checked, label).changed() {
                                    self.toggle_filter_value(
                                        self.active_filter_column,
                                        &option.value,
                                        checked,
                                    );
                                }
                            }
                        }
                    });

                if !self.selected_filter_values.is_empty() {
                    ui.add_space(4.0);
                    ui.horizontal_wrapped(|ui| {
                        ui.label("適用中:");
                        let active_values: Vec<(FilterColumn, String)> = self
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
                        for (column, value) in active_values {
                            let button_label =
                                format!("{}: {} ×", column.label(), display_filter_value(&value));
                            if ui.small_button(button_label).clicked() {
                                self.toggle_filter_value(column, &value, false);
                            }
                        }
                    });
                }
            });
    }

    fn draw_tree(
        &mut self,
        ui: &mut Ui,
        tree_scroll_request: Option<TreeScrollRequest>,
    ) -> Option<usize> {
        let filtered_indices = &self.filtered_indices;
        let selected_row = self.selected_row;
        let mut clicked_row = None;
        let selected_fill = Color32::from_rgb(70, 130, 180);
        let mut table = TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

        for spec in TREE_COLUMN_SPECS {
            table = table.column((spec.build_column)());
        }

        if let Some(scroll_request) = tree_scroll_request {
            if scroll_request.row_index < filtered_indices.len() {
                table = table.scroll_to_row(scroll_request.row_index, scroll_request.align);
            }
        }

        table
            .header(24.0, |mut header| {
                for spec in TREE_COLUMN_SPECS {
                    header.col(|ui| {
                        ui.strong(spec.header);
                    });
                }
            })
            .body(|body| {
                body.rows(22.0, filtered_indices.len(), |mut row| {
                    let i = row.index();
                    let record = &self.all_records[filtered_indices[i]];
                    let is_selected = selected_row == Some(i);

                    let mut row_clicked = false;
                    for spec in TREE_COLUMN_SPECS {
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

    fn draw_detail(&mut self, ui: &mut Ui) {
        if let Some(record) = self.selected_record().cloned() {
            self.draw_db_viewer_button(ui, true);
            ui.add_space(6.0);

            ui.label(
                RichText::new(format!(
                    "{} / {} / paragraph_id={}",
                    record.municipality_name, record.ordinance_or_rule, record.paragraph_id
                ))
                .size(14.0)
                .strong(),
            );

            ui.add_space(6.0);
            ui.label(format!("document_id: {}", record.document_id));
            ui.label(format!("doc_type: {}", record.doc_type));
            ui.label(format!("sentence_count: {}", record.sentence_count));

            ui.add_space(6.0);
            ui.label(format!("categories: {}", record.matched_categories_text));
            ui.label(format!("conditions: {}", record.matched_condition_ids_text));
            ui.label(format!("match_groups: {}", record.match_group_ids_text));
            ui.label(format!("annotated_tokens: {}", record.annotated_token_count));

            ui.separator();

            let segments = self.get_segments();
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

            for seg in &segments {
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

            ScrollArea::vertical()
                .id_salt("detail_scroll")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(egui::Label::new(job).wrap());
                });
        } else {
            self.draw_db_viewer_button(ui, false);
            ui.add_space(6.0);
            ui.label(RichText::new("レコード未選択").italics());
        }
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

fn build_tree_annotated_token_count_column() -> Column {
    Column::initial(92.0).at_least(72.0).clip(true)
}

fn tree_row_no_value(record: &CsvRecord) -> String {
    record.row_no.to_string()
}

fn tree_paragraph_id_value(record: &CsvRecord) -> String {
    record.paragraph_id.clone()
}

fn tree_municipality_value(record: &CsvRecord) -> String {
    record.municipality_name.clone()
}

fn tree_ordinance_value(record: &CsvRecord) -> String {
    record.ordinance_or_rule.clone()
}

fn tree_category_value(record: &CsvRecord) -> String {
    record.matched_categories_text.clone()
}

fn tree_annotated_token_count_value(record: &CsvRecord) -> String {
    record.annotated_token_count.clone()
}

fn render_db_viewer_contents(
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
    let csv_text = state.csv_paragraph_text.clone().unwrap_or_default();
    let mismatch = state
        .context
        .as_ref()
        .map(|context| context.center.paragraph_text != csv_text)
        .unwrap_or(false);

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
            .add_enabled(
                previous_location.is_some(),
                egui::Button::new("◀ 前へ"),
            )
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

    ui.label(RichText::new("CSV テキスト").strong());
    ScrollArea::vertical()
        .id_salt("db_viewer_csv_text")
        .max_height(140.0)
        .show(ui, |ui| {
            ui.label(&csv_text);
        });

    ui.add_space(6.0);
    ui.label(RichText::new("DB 中心段落").strong());
    if let Some(context) = &state.context {
        ui.group(|ui| {
            ui.label(format!("paragraph_no: {}", context.center.paragraph_no));
            ui.label(&context.center.paragraph_text);
        });
    }

    ui.add_space(4.0);
    let compare_label = if mismatch {
        RichText::new("CSV と DB 中心段落は不一致").color(Color32::from_rgb(200, 64, 64))
    } else {
        RichText::new("CSV と DB 中心段落は一致").color(Color32::from_rgb(70, 130, 70))
    };
    ui.label(compare_label);

    ui.separator();
    ui.label(RichText::new("前後コンテキスト").strong());

    ScrollArea::vertical()
        .id_salt("db_viewer_context")
        .auto_shrink([false, false])
        .show(ui, |ui| {
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
