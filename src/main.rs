#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use egui::{Color32, FontData, FontDefinitions, FontFamily, RichText, ScrollArea, TextStyle, Ui};
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

// ─── 定数 ────────────────────────────────────────────────────────────────────

const DEFAULT_PAGE_SIZE: usize = 100;
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

    let header_names: Vec<&str> = headers.iter().collect();
    let missing: Vec<&str> = REQUIRED_COLUMNS
        .iter()
        .filter(|&&col| !header_names.contains(&col))
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
        header_names.iter().position(|&h| h == name).unwrap_or(0)
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
    records: Vec<CsvRecord>,
    page_index: usize,
    selected_row: Option<usize>, // current_page_records 内のインデックス
    error_message: Option<String>,
    // キャッシュ: 選択中レコードのセグメント
    cached_segments: Option<(usize, Vec<TextSegment>)>, // (row_no, segments)
}

impl App {
    fn new(csv_path: PathBuf, page_size: usize) -> Self {
        let mut app = Self {
            csv_path: csv_path.clone(),
            page_size,
            records: Vec::new(),
            page_index: 0,
            selected_row: None,
            error_message: None,
            cached_segments: None,
        };
        app.load_csv(csv_path);
        app
    }

    fn load_csv(&mut self, path: PathBuf) {
        match load_records(&path) {
            Ok(records) => {
                self.records = records;
                self.csv_path = path;
                self.page_index = 0;
                self.selected_row = if self.records.is_empty() { None } else { Some(0) };
                self.cached_segments = None;
                self.error_message = None;
            }
            Err(e) => {
                self.error_message = Some(e);
            }
        }
    }

    fn total_pages(&self) -> usize {
        if self.records.is_empty() {
            return 1;
        }
        (self.records.len() + self.page_size - 1) / self.page_size
    }

    fn current_page_records(&self) -> &[CsvRecord] {
        let start = self.page_index * self.page_size;
        let end = (start + self.page_size).min(self.records.len());
        &self.records[start..end]
    }

    fn selected_record(&self) -> Option<&CsvRecord> {
        let idx = self.selected_row?;
        self.current_page_records().get(idx)
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
                self.page_index -= 1;
                self.selected_row = Some(0);
                self.cached_segments = None;
            }

            let can_next = self.page_index + 1 < self.total_pages();
            if ui
                .add_enabled(can_next, egui::Button::new("次の100件 ▶"))
                .clicked()
            {
                self.page_index += 1;
                self.selected_row = Some(0);
                self.cached_segments = None;
            }

            let total = self.total_pages();
            ui.label(format!("{} / {}", self.page_index + 1, total));
            ui.separator();
            ui.label(format!(
                "総件数: {} 件  表示: {} 件",
                self.records.len(),
                self.current_page_records().len()
            ));
        });
    }

    fn draw_body(&mut self, ui: &mut Ui) {
        let available = ui.available_rect_before_wrap();
        let left_width = available.width() * 0.40;

        ui.horizontal(|ui| {
            // ─── 左ペイン: ツリービュー ───────────────────────────────────────
            ui.allocate_ui(egui::vec2(left_width, available.height()), |ui| {
                self.draw_tree(ui);
            });

            ui.separator();

            // ─── 右ペイン: 詳細表示 ──────────────────────────────────────────
            ui.vertical(|ui| {
                self.draw_detail(ui);
            });
        });
    }

    fn draw_tree(&mut self, ui: &mut Ui) {
        // ヘッダー行
        egui::Grid::new("tree_header")
            .num_columns(6)
            .min_col_width(0.0)
            .show(ui, |ui| {
                ui.label(RichText::new("No").strong());
                ui.label(RichText::new("paragraph_id").strong());
                ui.label(RichText::new("自治体").strong());
                ui.label(RichText::new("条例/規則").strong());
                ui.label(RichText::new("カテゴリ").strong());
                ui.label(RichText::new("強調token数").strong());
                ui.end_row();
            });

        ui.separator();

        let page_records: Vec<CsvRecord> = self.current_page_records().to_vec();
        let mut new_selected = self.selected_row;

        ScrollArea::vertical()
            .id_salt("tree_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("tree_body")
                    .num_columns(6)
                    .min_col_width(0.0)
                    .striped(true)
                    .show(ui, |ui| {
                        for (i, record) in page_records.iter().enumerate() {
                            let is_selected = self.selected_row == Some(i);

                            let row_no_str = record.row_no.to_string();
                            let para_id = truncate(&record.paragraph_id, 12);
                            let muni = truncate(&record.municipality_name, 14);
                            let ordi = truncate(&record.ordinance_or_rule, 8);
                            let cat = truncate(&record.matched_categories_text, 20);
                            let tok = truncate(&record.annotated_token_count, 8);

                            let response = if is_selected {
                                ui.label(RichText::new(&row_no_str).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                                    | ui.label(RichText::new(para_id).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                                    | ui.label(RichText::new(muni).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                                    | ui.label(RichText::new(ordi).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                                    | ui.label(RichText::new(cat).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                                    | ui.label(RichText::new(tok).color(Color32::WHITE).background_color(Color32::from_rgb(70, 130, 180)))
                            } else {
                                ui.label(&row_no_str)
                                    | ui.label(para_id)
                                    | ui.label(muni)
                                    | ui.label(ordi)
                                    | ui.label(cat)
                                    | ui.label(tok)
                            };

                            if response.interact(egui::Sense::click()).clicked() {
                                new_selected = Some(i);
                            }

                            ui.end_row();
                        }
                    });
            });

        if new_selected != self.selected_row {
            self.selected_row = new_selected;
            self.cached_segments = None;
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

            ScrollArea::vertical()
                .id_salt("detail_scroll")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Wrap);
                        ui.spacing_mut().item_spacing.x = 0.0;

                        for seg in &segments {
                            if seg.text.is_empty() {
                                continue;
                            }
                            if seg.is_hit {
                                ui.label(
                                    RichText::new(&seg.text)
                                        .background_color(Color32::from_rgb(255, 224, 138))
                                        .text_style(TextStyle::Body),
                                );
                            } else {
                                ui.label(
                                    RichText::new(&seg.text)
                                        .text_style(TextStyle::Body),
                                );
                            }
                        }
                    });
                });
        } else {
            ui.label(RichText::new("レコード未選択").italics());
        }
    }
}

// ─── ユーティリティ ───────────────────────────────────────────────────────────

fn truncate(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = chars[..max_chars].iter().collect();
        format!("{}…", truncated)
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
