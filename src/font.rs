use eframe::egui;
use egui::{FontData, FontDefinitions, FontFamily};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

const JAPANESE_FONT_NAME: &str = "jp_ui_font";

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

pub(crate) fn configure_japanese_font(ctx: &egui::Context) -> Result<Option<PathBuf>, String> {
    let Some(font_path) = japanese_font_candidates()
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(None);
    };

    let font_bytes = fs::read(&font_path).map_err(|e| {
        format!(
            "日本語フォントを読み込めませんでした ({}): {e}",
            font_path.display()
        )
    })?;

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
