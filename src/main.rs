#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use egui::text::{LayoutJob, TextFormat};
use egui::{Color32, FontData, FontDefinitions, FontFamily, RichText, ScrollArea, TextStyle, Ui};
use egui_extras::{Column, TableBuilder};
use regex::Regex;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

// ─── 定数 ────────────────────────────────────────────────────────────────────

const DEFAULT_PAGE_SIZE: usize = 100;
const JAPANESE_FONT_NAME: &str = "jp_ui_font";
const TREE_HEADERS: [&str; 6] = [
    "No",
    "paragraph_id",
    "自治体",
    "条例/規則",
    "カテゴリ",
    "強調token数",
];

const REQUIRED_COLUMNS: &[&str] = &[
    "paragraph_id",
    "document_id",
    "municipality_name",
    "ordinance_or_rule",
    "doc_type",
    "sentence_count",
    "paragraph_text",
    "paragraph_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "match_group_count",
    "annotated_token_count",
];

// ─── 正規表現（遅延初期化） ────────────────────────────────────────────────

fn hit_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?s)\[\[HIT (?P<attrs>.*?)\]\](?P<text>.*?)\[\[/HIT\]\]").unwrap()
    })
}

fn attr_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"([a-zA-Z0-9_]+)="((?:\\.|[^"])*)""#).unwrap())
}

// ─── データ構造 ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct CsvRecord {
    row_no: usize,
    paragraph_id: String,
    document_id: String,
    municipality_name: String,
    ordinance_or_rule: String,
    doc_type: String,
    sentence_count: String,
    paragraph_text: String,
    paragraph_text_tagged: String,
    matched_condition_ids_text: String,
    matched_categories_text: String,
    match_group_ids_text: String,
    match_group_count: String,
    annotated_token_count: String,
}

#[derive(Clone, Debug)]
struct TextSegment {
    text: String,
    is_hit: bool,
    #[allow(dead_code)]
    attributes: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct FilterOption {
    value: String,
    count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreeScrollRequest {
    row_index: usize,
    align: Option<egui::Align>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum FilterColumn {
    ParagraphId,
    DocumentId,
    MunicipalityName,
    OrdinanceOrRule,
    DocType,
    SentenceCount,
    MatchedCategories,
    MatchedConditions,
    MatchGroupIds,
    MatchGroupCount,
    AnnotatedTokenCount,
}

impl FilterColumn {
    fn all() -> &'static [Self] {
        &[
            Self::MatchedCategories,
            Self::MunicipalityName,
            Self::OrdinanceOrRule,
            Self::DocType,
            Self::ParagraphId,
            Self::DocumentId,
            Self::SentenceCount,
            Self::MatchedConditions,
            Self::MatchGroupIds,
            Self::MatchGroupCount,
            Self::AnnotatedTokenCount,
        ]
    }

    fn label(self) -> &'static str {
        match self {
            Self::ParagraphId => "paragraph_id",
            Self::DocumentId => "document_id",
            Self::MunicipalityName => "自治体",
            Self::OrdinanceOrRule => "条例/規則",
            Self::DocType => "doc_type",
            Self::SentenceCount => "sentence_count",
            Self::MatchedCategories => "カテゴリ",
            Self::MatchedConditions => "conditions",
            Self::MatchGroupIds => "match_groups",
            Self::MatchGroupCount => "match_group_count",
            Self::AnnotatedTokenCount => "annotated_tokens",
        }
    }
}

// ─── テキスト解析 ─────────────────────────────────────────────────────────────

fn unescape_attribute(value: &str) -> String {
    value.replace("\\\"", "\"").replace("\\\\", "\\")
}

fn parse_hit_attributes(raw: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for cap in attr_pattern().captures_iter(raw) {
        let key = cap[1].to_string();
        let val = unescape_attribute(&cap[2]);
        map.insert(key, val);
    }
    map
}

fn parse_tagged_text(tagged: &str) -> Vec<TextSegment> {
    let mut segments = Vec::new();
    let mut last_end = 0;

    for cap in hit_pattern().captures_iter(tagged) {
        let m = cap.get(0).unwrap();
        if m.start() > last_end {
            segments.push(TextSegment {
                text: tagged[last_end..m.start()].to_string(),
                is_hit: false,
                attributes: HashMap::new(),
            });
        }
        segments.push(TextSegment {
            text: cap["text"].to_string(),
            is_hit: true,
            attributes: parse_hit_attributes(&cap["attrs"]),
        });
        last_end = m.end();
    }

    if last_end < tagged.len() {
        segments.push(TextSegment {
            text: tagged[last_end..].to_string(),
            is_hit: false,
            attributes: HashMap::new(),
        });
    }

    if segments.is_empty() {
        segments.push(TextSegment {
            text: tagged.to_string(),
            is_hit: false,
            attributes: HashMap::new(),
        });
    }

    segments
}

// ─── CSV 読み込み ─────────────────────────────────────────────────────────────

fn load_records(path: &PathBuf) -> Result<Vec<CsvRecord>, String> {
    if !path.exists() {
        return Err(format!("CSV ファイルが見つかりません: {}", path.display()));
    }

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| format!("CSV を開けませんでした: {e}"))?;

    // ヘッダー検証
    let headers = rdr
        .headers()
        .map_err(|e| format!("ヘッダー読み込みエラー: {e}"))?
        .clone();

    let header_names: Vec<String> = headers
        .iter()
        .map(|h| h.trim_start_matches('\u{feff}').to_string())
        .collect();
    let missing: Vec<&str> = REQUIRED_COLUMNS
        .iter()
        .filter(|&&col| !header_names.iter().any(|h| h == col))
        .copied()
        .collect();

    if !missing.is_empty() {
        return Err(format!(
            "CSV に必要な列が不足しています: {}",
            missing.join(", ")
        ));
    }

    // 列インデックスを事前に取得
    let idx = |name: &str| -> usize {
        header_names.iter().position(|h| h == name).unwrap_or(0)
    };

    let get = |record: &csv::StringRecord, name: &str| -> String {
        record.get(idx(name)).unwrap_or("").to_string()
    };

    let mut records = Vec::new();
    for (row_no, result) in rdr.records().enumerate() {
        let row = result.map_err(|e| format!("行 {} の読み込みエラー: {e}", row_no + 1))?;
        records.push(CsvRecord {
            row_no: row_no + 1,
            paragraph_id: get(&row, "paragraph_id"),
            document_id: get(&row, "document_id"),
            municipality_name: get(&row, "municipality_name"),
            ordinance_or_rule: get(&row, "ordinance_or_rule"),
            doc_type: get(&row, "doc_type"),
            sentence_count: get(&row, "sentence_count"),
            paragraph_text: get(&row, "paragraph_text"),
            paragraph_text_tagged: get(&row, "paragraph_text_tagged"),
            matched_condition_ids_text: get(&row, "matched_condition_ids_text"),
            matched_categories_text: get(&row, "matched_categories_text"),
            match_group_ids_text: get(&row, "match_group_ids_text"),
            match_group_count: get(&row, "match_group_count"),
            annotated_token_count: get(&row, "annotated_token_count"),
        });
    }

    Ok(records)
}

// ─── アプリケーション状態 ─────────────────────────────────────────────────────

struct App {
    csv_path: PathBuf,
    page_size: usize,
    all_records: Vec<CsvRecord>,
    filtered_indices: Vec<usize>,
    filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    active_filter_column: FilterColumn,
    page_index: usize,
    selected_row: Option<usize>, // current_page_indices 内のインデックス
    pending_tree_scroll: Option<TreeScrollRequest>,
    error_message: Option<String>,
    // キャッシュ: 選択中レコードのセグメント
    cached_segments: Option<(usize, Vec<TextSegment>)>, // (row_no, segments)
}

impl App {
    fn new(csv_path: PathBuf, page_size: usize) -> Self {
        let mut app = Self {
            csv_path: csv_path.clone(),
            page_size,
            all_records: Vec::new(),
            filtered_indices: Vec::new(),
            filter_options: HashMap::new(),
            selected_filter_values: HashMap::new(),
            active_filter_column: FilterColumn::MatchedCategories,
            page_index: 0,
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
                self.filter_options = build_filter_options(&self.all_records);
                self.selected_filter_values.clear();
                self.filtered_indices = (0..self.all_records.len()).collect();
                self.page_index = 0;
                self.cached_segments = None;
                self.pending_tree_scroll = None;
                self.set_selected_row(None);
                self.select_first_row_on_current_page(Some(egui::Align::Min));
                self.error_message = None;
            }
            Err(e) => {
                self.error_message = Some(e);
            }
        }
    }

    fn total_pages(&self) -> usize {
        if self.filtered_indices.is_empty() {
            return 1;
        }
        (self.filtered_indices.len() + self.page_size - 1) / self.page_size
    }

    fn current_page_indices(&self) -> &[usize] {
        let start = self.page_index * self.page_size;
        let end = (start + self.page_size).min(self.filtered_indices.len());
        &self.filtered_indices[start..end]
    }

    fn set_selected_row(&mut self, selected_row: Option<usize>) {
        let next = selected_row.filter(|&idx| idx < self.current_page_indices().len());
        if self.selected_row != next {
            self.selected_row = next;
            self.cached_segments = None;
        }
    }

    fn request_tree_scroll_to_selected_row(&mut self, align: Option<egui::Align>) {
        self.pending_tree_scroll = self.selected_row.map(|row_index| TreeScrollRequest {
            row_index,
            align,
        });
    }

    fn select_first_row_on_current_page(&mut self, align: Option<egui::Align>) {
        let next = if self.current_page_indices().is_empty() {
            None
        } else {
            Some(0)
        };
        self.set_selected_row(next);
        self.request_tree_scroll_to_selected_row(align);
    }

    fn select_last_row_on_current_page(&mut self, align: Option<egui::Align>) {
        self.set_selected_row(self.current_page_indices().len().checked_sub(1));
        self.request_tree_scroll_to_selected_row(align);
    }

    fn go_to_previous_page(&mut self) {
        if self.page_index > 0 {
            self.page_index -= 1;
            self.cached_segments = None;
            self.select_last_row_on_current_page(Some(egui::Align::Max));
        }
    }

    fn go_to_next_page(&mut self) {
        if self.page_index + 1 < self.total_pages() {
            self.page_index += 1;
            self.cached_segments = None;
            self.select_first_row_on_current_page(Some(egui::Align::Min));
        }
    }

    fn move_selection_up(&mut self) {
        if self.filtered_indices.is_empty() || self.current_page_indices().is_empty() {
            return;
        }

        match self.selected_row {
            Some(idx) if idx > 0 => {
                self.set_selected_row(Some(idx - 1));
                self.request_tree_scroll_to_selected_row(None);
            }
            Some(_) if self.page_index > 0 => {
                self.page_index -= 1;
                self.select_last_row_on_current_page(Some(egui::Align::Max));
            }
            None => self.select_first_row_on_current_page(Some(egui::Align::Min)),
            _ => {}
        }
    }

    fn move_selection_down(&mut self) {
        let current_len = self.current_page_indices().len();
        if self.filtered_indices.is_empty() || current_len == 0 {
            return;
        }

        match self.selected_row {
            Some(idx) if idx + 1 < current_len => {
                self.set_selected_row(Some(idx + 1));
                self.request_tree_scroll_to_selected_row(None);
            }
            Some(_) if self.page_index + 1 < self.total_pages() => {
                self.page_index += 1;
                self.select_first_row_on_current_page(Some(egui::Align::Min));
            }
            None => self.select_first_row_on_current_page(Some(egui::Align::Min)),
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
        let page_idx = self.selected_row?;
        let record_idx = *self.current_page_indices().get(page_idx)?;
        self.all_records.get(record_idx)
    }

    fn apply_filters(&mut self) {
        self.filtered_indices = self
            .all_records
            .iter()
            .enumerate()
            .filter_map(|(idx, record)| self.record_matches_filters(record).then_some(idx))
            .collect();
        self.page_index = 0;
        self.pending_tree_scroll = None;
        self.set_selected_row(None);
        self.select_first_row_on_current_page(Some(egui::Align::Min));
    }

    fn record_matches_filters(&self, record: &CsvRecord) -> bool {
        self.selected_filter_values.iter().all(|(column, selected)| {
            if selected.is_empty() {
                return true;
            }

            match column {
                FilterColumn::MatchedCategories => category_values(&record.matched_categories_text)
                    .into_iter()
                    .any(|value| selected.contains(&value)),
                _ => record_filter_value(record, *column)
                    .map(|value| selected.contains(value))
                    .unwrap_or(false),
            }
        })
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
        let mut changed = false;

        {
            let entry = self.selected_filter_values.entry(column).or_default();
            if selected {
                changed = entry.insert(value.to_string());
            } else {
                changed = entry.remove(value);
            }
        }

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

// ─── GUI 描画 ─────────────────────────────────────────────────────────────────

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_keyboard_navigation(ctx);

        // エラーダイアログ
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

        egui::CentralPanel::default().show(ctx, |ui| {
            self.draw_body(ui);
        });
    }
}

impl App {
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

            let can_prev = self.page_index > 0;
            if ui
                .add_enabled(can_prev, egui::Button::new("◀ 前の100件"))
                .clicked()
            {
                self.go_to_previous_page();
            }

            let can_next = self.page_index + 1 < self.total_pages();
            if ui
                .add_enabled(can_next, egui::Button::new("次の100件 ▶"))
                .clicked()
            {
                self.go_to_next_page();
            }

            let total = self.total_pages();
            ui.label(format!("{} / {}", self.page_index + 1, total));
            ui.separator();
            ui.label(format!(
                "総件数: {} 件  抽出後: {} 件  表示: {} 件",
                self.all_records.len(),
                self.filtered_indices.len(),
                self.current_page_indices().len()
            ));
        });
    }

    fn draw_body(&mut self, ui: &mut Ui) {
        egui::SidePanel::left("record_list_panel")
            .resizable(true)
            .default_width(620.0)
            .min_width(360.0)
            .show_inside(ui, |ui| {
                self.draw_filters(ui);
                ui.separator();
                self.draw_tree(ui);
            });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.draw_detail(ui);
        });
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

    fn draw_tree(&mut self, ui: &mut Ui) {
        let page_record_indices = self.current_page_indices();
        let selected_row = self.selected_row;
        let pending_tree_scroll = self.pending_tree_scroll;
        let mut new_selected = selected_row;
        let selected_fill = Color32::from_rgb(70, 130, 180);
        let mut table = TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
            .column(Column::initial(56.0).at_least(48.0).clip(true))
            .column(Column::initial(140.0).at_least(96.0).clip(true))
            .column(Column::initial(128.0).at_least(96.0).clip(true))
            .column(Column::initial(120.0).at_least(88.0).clip(true))
            .column(Column::remainder().at_least(140.0).clip(true))
            .column(Column::initial(92.0).at_least(72.0).clip(true));

        if let Some(scroll_request) = pending_tree_scroll {
            if scroll_request.row_index < page_record_indices.len() {
                table = table.scroll_to_row(scroll_request.row_index, scroll_request.align);
            }
        }

        table
            .header(24.0, |mut header| {
                for label in TREE_HEADERS {
                    header.col(|ui| {
                        ui.strong(label);
                    });
                }
            })
            .body(|mut body| {
                body.rows(22.0, page_record_indices.len(), |mut row| {
                    let i = row.index();
                    let record = &self.all_records[page_record_indices[i]];
                    let is_selected = selected_row == Some(i);

                    let values = [
                        record.row_no.to_string(),
                        record.paragraph_id.clone(),
                        record.municipality_name.clone(),
                        record.ordinance_or_rule.clone(),
                        record.matched_categories_text.clone(),
                        record.annotated_token_count.clone(),
                    ];

                    let mut row_clicked = false;
                    for value in values {
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
                        new_selected = Some(i);
                    }
                });
            });

        if new_selected != self.selected_row {
            self.set_selected_row(new_selected);
        }
        if pending_tree_scroll.is_some() {
            self.pending_tree_scroll = None;
        }
    }

    fn draw_detail(&mut self, ui: &mut Ui) {
        if let Some(record) = self.selected_record().cloned() {
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

            // ハイライトテキスト
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
            ui.label(RichText::new("レコード未選択").italics());
        }
    }
}

// ─── ユーティリティ ───────────────────────────────────────────────────────────

fn build_filter_options(records: &[CsvRecord]) -> HashMap<FilterColumn, Vec<FilterOption>> {
    let mut options = HashMap::new();

    for &column in FilterColumn::all() {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for record in records {
            for value in record_filter_values(record, column) {
                *counts.entry(value).or_insert(0) += 1;
            }
        }

        let mut column_options: Vec<FilterOption> = counts
            .into_iter()
            .map(|(value, count)| FilterOption { value, count })
            .collect();
        column_options.sort_by(|a, b| {
            display_filter_value(&a.value)
                .cmp(&display_filter_value(&b.value))
                .then_with(|| a.value.cmp(&b.value))
        });
        options.insert(column, column_options);
    }

    options
}

fn record_filter_values(record: &CsvRecord, column: FilterColumn) -> Vec<String> {
    match column {
        FilterColumn::MatchedCategories => category_values(&record.matched_categories_text),
        _ => vec![record_filter_value(record, column).unwrap_or("").trim().to_string()],
    }
}

fn record_filter_value<'a>(record: &'a CsvRecord, column: FilterColumn) -> Option<&'a str> {
    match column {
        FilterColumn::ParagraphId => Some(&record.paragraph_id),
        FilterColumn::DocumentId => Some(&record.document_id),
        FilterColumn::MunicipalityName => Some(&record.municipality_name),
        FilterColumn::OrdinanceOrRule => Some(&record.ordinance_or_rule),
        FilterColumn::DocType => Some(&record.doc_type),
        FilterColumn::SentenceCount => Some(&record.sentence_count),
        FilterColumn::MatchedCategories => Some(&record.matched_categories_text),
        FilterColumn::MatchedConditions => Some(&record.matched_condition_ids_text),
        FilterColumn::MatchGroupIds => Some(&record.match_group_ids_text),
        FilterColumn::MatchGroupCount => Some(&record.match_group_count),
        FilterColumn::AnnotatedTokenCount => Some(&record.annotated_token_count),
    }
}

fn category_values(raw: &str) -> Vec<String> {
    let mut values = BTreeSet::new();
    for part in raw.split(|c| c == ',' || c == '、' || c == ';' || c == '\n') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            values.insert(trimmed.to_string());
        }
    }

    if values.is_empty() {
        values.insert(String::new());
    }

    values.into_iter().collect()
}

fn display_filter_value(value: &str) -> String {
    if value.is_empty() {
        "(空)".to_string()
    } else {
        value.to_string()
    }
}

fn japanese_font_candidates() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/usr/share/fonts/opentype/ipaexfont-gothic/ipaexg.ttf"),
        PathBuf::from("/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        PathBuf::from("/mnt/c/Windows/Fonts/BIZ-UDGothicR.ttc"),
        PathBuf::from("/mnt/c/Windows/Fonts/meiryo.ttc"),
        PathBuf::from("/mnt/c/Windows/Fonts/msgothic.ttc"),
        PathBuf::from("C:/Windows/Fonts/BIZ-UDGothicR.ttc"),
        PathBuf::from("C:/Windows/Fonts/meiryo.ttc"),
        PathBuf::from("C:/Windows/Fonts/msgothic.ttc"),
    ]
}

fn configure_japanese_font(ctx: &egui::Context) -> Result<Option<PathBuf>, String> {
    let Some(font_path) = japanese_font_candidates()
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(None);
    };

    let font_bytes = fs::read(&font_path)
        .map_err(|e| format!("日本語フォントを読み込めませんでした ({}): {e}", font_path.display()))?;

    let mut fonts = FontDefinitions::default();
    fonts.font_data.insert(
        JAPANESE_FONT_NAME.to_owned(),
        Arc::new(FontData::from_owned(font_bytes)),
    );

    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .insert(0, JAPANESE_FONT_NAME.to_owned());
    }

    ctx.set_fonts(fonts);
    Ok(Some(font_path))
}

// ─── エントリポイント ─────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let csv_path = if args.len() > 1 && !args[1].starts_with("--") {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("抑制区域_段落_1.0.csv")
    };

    let page_size = args
        .windows(2)
        .find(|w| w[0] == "--page-size")
        .and_then(|w| w[1].parse::<usize>().ok())
        .unwrap_or(DEFAULT_PAGE_SIZE);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("CSV Highlight Viewer")
            .with_inner_size([1480.0, 920.0]),
        ..Default::default()
    };

    eframe::run_native(
        "CSV Highlight Viewer",
        options,
        Box::new(move |cc| {
            let font_setup_result = configure_japanese_font(&cc.egui_ctx);
            let mut app = App::new(csv_path.clone(), page_size);

            match font_setup_result {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if app.error_message.is_none() {
                        app.error_message = Some(
                            "日本語フォントが見つからなかったため、日本語テキストが正しく表示されない可能性があります。".to_string(),
                        );
                    }
                }
                Err(err) => {
                    if app.error_message.is_none() {
                        app.error_message = Some(err);
                    }
                }
            }

            Ok(Box::new(app))
        }),
    )
    .unwrap();
}
