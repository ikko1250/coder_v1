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

const JAPANESE_FONT_NAME: &str = "jp_ui_font";

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

#[derive(Clone, Copy, Debug)]
enum FilterValueKind {
    Single(fn(&CsvRecord) -> String),
    Multi(fn(&CsvRecord) -> Vec<String>),
}

#[derive(Clone, Copy, Debug)]
enum FilterSortKind {
    Text,
    Numeric,
}

#[derive(Clone, Copy, Debug)]
struct FilterColumnSpec {
    column: FilterColumn,
    label: &'static str,
    value_kind: FilterValueKind,
    sort_kind: FilterSortKind,
}

#[derive(Clone, Copy)]
struct TreeColumnSpec {
    header: &'static str,
    build_column: fn() -> Column,
    value: fn(&CsvRecord) -> String,
}

const FILTER_COLUMN_ORDER: &[FilterColumn] = &[
    FilterColumn::MatchedCategories,
    FilterColumn::MunicipalityName,
    FilterColumn::OrdinanceOrRule,
    FilterColumn::DocType,
    FilterColumn::ParagraphId,
    FilterColumn::DocumentId,
    FilterColumn::SentenceCount,
    FilterColumn::MatchedConditions,
    FilterColumn::MatchGroupIds,
    FilterColumn::MatchGroupCount,
    FilterColumn::AnnotatedTokenCount,
];

const FILTER_COLUMN_SPECS: &[FilterColumnSpec] = &[
    FilterColumnSpec {
        column: FilterColumn::MatchedCategories,
        label: "カテゴリ",
        value_kind: FilterValueKind::Multi(record_matched_categories_values),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MunicipalityName,
        label: "自治体",
        value_kind: FilterValueKind::Single(record_municipality_name_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::OrdinanceOrRule,
        label: "条例/規則",
        value_kind: FilterValueKind::Single(record_ordinance_or_rule_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::DocType,
        label: "doc_type",
        value_kind: FilterValueKind::Single(record_doc_type_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::ParagraphId,
        label: "paragraph_id",
        value_kind: FilterValueKind::Single(record_paragraph_id_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::DocumentId,
        label: "document_id",
        value_kind: FilterValueKind::Single(record_document_id_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::SentenceCount,
        label: "sentence_count",
        value_kind: FilterValueKind::Single(record_sentence_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchedConditions,
        label: "conditions",
        value_kind: FilterValueKind::Single(record_matched_condition_ids_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchGroupIds,
        label: "match_groups",
        value_kind: FilterValueKind::Single(record_match_group_ids_value),
        sort_kind: FilterSortKind::Text,
    },
    FilterColumnSpec {
        column: FilterColumn::MatchGroupCount,
        label: "match_group_count",
        value_kind: FilterValueKind::Single(record_match_group_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
    FilterColumnSpec {
        column: FilterColumn::AnnotatedTokenCount,
        label: "annotated_tokens",
        value_kind: FilterValueKind::Single(record_annotated_token_count_value),
        sort_kind: FilterSortKind::Numeric,
    },
];

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

impl FilterColumn {
    fn all() -> &'static [Self] {
        FILTER_COLUMN_ORDER
    }

    fn label(self) -> &'static str {
        self.spec().label
    }

    fn spec(self) -> &'static FilterColumnSpec {
        FILTER_COLUMN_SPECS
            .iter()
            .find(|spec| spec.column == self)
            .expect("missing FilterColumnSpec")
    }
}

impl FilterColumnSpec {
    fn values(self, record: &CsvRecord) -> Vec<String> {
        match self.value_kind {
            FilterValueKind::Single(extract) => vec![extract(record)],
            FilterValueKind::Multi(extract) => extract(record),
        }
    }

    fn matches(self, record: &CsvRecord, selected: &BTreeSet<String>) -> bool {
        if selected.is_empty() {
            return true;
        }

        self.values(record)
            .into_iter()
            .any(|value| selected.contains(&value))
    }

    fn compare_values(self, left: &str, right: &str) -> std::cmp::Ordering {
        compare_filter_values(left, right, self.sort_kind)
    }
}

fn compare_filter_values(left: &str, right: &str, sort_kind: FilterSortKind) -> std::cmp::Ordering {
    match sort_kind {
        FilterSortKind::Text => display_filter_value(left)
            .cmp(&display_filter_value(right))
            .then_with(|| left.cmp(right)),
        FilterSortKind::Numeric => compare_numeric_filter_values(left, right),
    }
}

fn compare_numeric_filter_values(left: &str, right: &str) -> std::cmp::Ordering {
    let left_trimmed = left.trim();
    let right_trimmed = right.trim();

    match (left_trimmed.is_empty(), right_trimmed.is_empty()) {
        (true, true) => return std::cmp::Ordering::Equal,
        (true, false) => return std::cmp::Ordering::Less,
        (false, true) => return std::cmp::Ordering::Greater,
        (false, false) => {}
    }

    match (
        left_trimmed.parse::<i64>(),
        right_trimmed.parse::<i64>(),
    ) {
        (Ok(left_value), Ok(right_value)) => left_value
            .cmp(&right_value)
            .then_with(|| left_trimmed.cmp(right_trimmed)),
        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
        (Err(_), Err(_)) => display_filter_value(left_trimmed)
            .cmp(&display_filter_value(right_trimmed))
            .then_with(|| left_trimmed.cmp(right_trimmed)),
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
    all_records: Vec<CsvRecord>,
    filtered_indices: Vec<usize>,
    filter_options: HashMap<FilterColumn, Vec<FilterOption>>,
    selected_filter_values: HashMap<FilterColumn, BTreeSet<String>>,
    active_filter_column: FilterColumn,
    selected_row: Option<usize>, // filtered_indices 内のインデックス
    pending_tree_scroll: Option<TreeScrollRequest>,
    error_message: Option<String>,
    // キャッシュ: 選択中レコードのセグメント
    cached_segments: Option<(usize, Vec<TextSegment>)>, // (row_no, segments)
}

impl App {
    fn new(csv_path: PathBuf) -> Self {
        let mut app = Self {
            csv_path: csv_path.clone(),
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
                self.filter_options = build_filter_options(&self.all_records);
                self.selected_filter_values.clear();
                self.filtered_indices = (0..self.all_records.len()).collect();
                self.cached_segments = None;
                self.pending_tree_scroll = None;
                self.set_selected_row(None);
                self.select_first_filtered_row(Some(egui::Align::Min));
                self.error_message = None;
            }
            Err(e) => {
                self.error_message = Some(e);
            }
        }
    }

    fn set_selected_row(&mut self, selected_row: Option<usize>) {
        let next = selected_row.filter(|&idx| idx < self.filtered_indices.len());
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

    fn select_first_filtered_row(&mut self, align: Option<egui::Align>) {
        let next = if self.filtered_indices.is_empty() {
            None
        } else {
            Some(0)
        };
        self.set_selected_row(next);
        self.request_tree_scroll_to_selected_row(align);
    }

    fn move_selection_up(&mut self) {
        if self.filtered_indices.is_empty() {
            return;
        }

        match self.selected_row {
            Some(idx) if idx > 0 => {
                self.set_selected_row(Some(idx - 1));
                self.request_tree_scroll_to_selected_row(None);
            }
            None => self.select_first_filtered_row(Some(egui::Align::Min)),
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
                self.set_selected_row(Some(idx + 1));
                self.request_tree_scroll_to_selected_row(None);
            }
            None => self.select_first_filtered_row(Some(egui::Align::Min)),
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
        self.pending_tree_scroll = None;
        self.set_selected_row(None);
        self.select_first_filtered_row(Some(egui::Align::Min));
    }

    fn record_matches_filters(&self, record: &CsvRecord) -> bool {
        self.selected_filter_values.iter().all(|(column, selected)| {
            column.spec().matches(record, selected)
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
        let filtered_indices = &self.filtered_indices;
        let selected_row = self.selected_row;
        let pending_tree_scroll = self.pending_tree_scroll;
        let mut new_selected = selected_row;
        let selected_fill = Color32::from_rgb(70, 130, 180);
        let mut table = TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center));

        for spec in TREE_COLUMN_SPECS {
            table = table.column((spec.build_column)());
        }

        if let Some(scroll_request) = pending_tree_scroll {
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
            .body(|mut body| {
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

fn normalize_single_filter_value(value: &str) -> String {
    value.trim().to_string()
}

fn record_paragraph_id_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.paragraph_id)
}

fn record_document_id_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.document_id)
}

fn record_municipality_name_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.municipality_name)
}

fn record_ordinance_or_rule_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.ordinance_or_rule)
}

fn record_doc_type_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.doc_type)
}

fn record_sentence_count_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.sentence_count)
}

fn record_matched_categories_values(record: &CsvRecord) -> Vec<String> {
    category_values(&record.matched_categories_text)
}

fn record_matched_condition_ids_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.matched_condition_ids_text)
}

fn record_match_group_ids_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.match_group_ids_text)
}

fn record_match_group_count_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.match_group_count)
}

fn record_annotated_token_count_value(record: &CsvRecord) -> String {
    normalize_single_filter_value(&record.annotated_token_count)
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

fn build_filter_options(records: &[CsvRecord]) -> HashMap<FilterColumn, Vec<FilterOption>> {
    let mut options = HashMap::new();

    for spec in FILTER_COLUMN_SPECS {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for record in records {
            for value in spec.values(record) {
                *counts.entry(value).or_insert(0) += 1;
            }
        }

        let mut column_options: Vec<FilterOption> = counts
            .into_iter()
            .map(|(value, count)| FilterOption { value, count })
            .collect();
        column_options.sort_by(|a, b| spec.compare_values(&a.value, &b.value));
        options.insert(spec.column, column_options);
    }

    options
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
            let mut app = App::new(csv_path.clone());

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
