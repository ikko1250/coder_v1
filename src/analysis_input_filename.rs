//! Analysis DB フォルダー入力時のファイル名規則。`docs/build_ordinance_analysis_db.py` の
//! `FILE_NAME_PATTERN` と同一の stem 解釈を保つ。

use regex::Regex;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::OnceLock;

fn stem_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(?:\d+_)?(?P<category1>[^_]+)_(?P<category2>[^_]+)$")
            .expect("analysis input stem regex")
    })
}

/// `.txt` / `.md` のファイル名（拡張子付き）から category1 / category2 を取り出す。
/// 非 UTF-8 ファイル名は `None`。
pub(crate) fn parse_categories_from_analysis_input_file_name(file_name: &OsStr) -> Option<(String, String)> {
    let name = file_name.to_str()?;
    let stem = Path::new(name).file_stem()?.to_str()?;
    parse_categories_from_stem(stem)
}

/// 拡張子を除いた stem に対するパース（ビルダースクリプトと同じ）。
pub(crate) fn parse_categories_from_stem(stem: &str) -> Option<(String, String)> {
    let caps = stem_pattern().captures(stem)?;
    let category1 = caps.name("category1")?.as_str().to_string();
    let category2 = caps.name("category2")?.as_str().to_string();
    if category1.is_empty() || category2.is_empty() {
        return None;
    }
    Some((category1, category2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_part_stem_parses() {
        assert_eq!(
            parse_categories_from_stem("札幌市_条例"),
            Some(("札幌市".into(), "条例".into()))
        );
    }

    #[test]
    fn numeric_prefix_stem_parses() {
        assert_eq!(
            parse_categories_from_stem("100_かすみがうら市_施行規則"),
            Some(("かすみがうら市".into(), "施行規則".into()))
        );
    }

    #[test]
    fn file_name_with_md_extension_parses() {
        assert_eq!(
            parse_categories_from_analysis_input_file_name(OsStr::new(
                "149_長野市_条例.md"
            )),
            Some(("長野市".into(), "条例".into()))
        );
    }

    #[test]
    fn invalid_stem_returns_none() {
        assert_eq!(parse_categories_from_stem("invalid-name"), None);
        assert_eq!(parse_categories_from_stem("a_b_c_d"), None);
    }
}
