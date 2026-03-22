use crate::model::TextSegment;
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;

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

pub(crate) fn parse_tagged_text(tagged: &str) -> Vec<TextSegment> {
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

#[cfg(test)]
mod tests {
    use super::parse_tagged_text;

    #[test]
    fn parse_tagged_text_restores_escaped_attributes() {
        let tagged = concat!(
            "前文",
            "[[HIT condition_ids=\"cond\\\"1\" categories=\"cat\\\\1\" groups=\"group\\\\\\\"1\"]]",
            "抑制区域",
            "[[/HIT]]",
            "後文"
        );

        let segments = parse_tagged_text(tagged);

        assert_eq!(segments.len(), 3);
        assert!(!segments[0].is_hit);
        assert_eq!(segments[0].text, "前文");
        assert!(segments[1].is_hit);
        assert_eq!(segments[1].text, "抑制区域");
        assert_eq!(
            segments[1].attributes.get("condition_ids"),
            Some(&"cond\"1".to_string())
        );
        assert_eq!(
            segments[1].attributes.get("categories"),
            Some(&"cat\\1".to_string())
        );
        assert_eq!(
            segments[1].attributes.get("groups"),
            Some(&"group\\\"1".to_string())
        );
        assert!(!segments[2].is_hit);
        assert_eq!(segments[2].text, "後文");
    }

    #[test]
    fn parse_tagged_text_keeps_plain_text_when_no_hit_marker_exists() {
        let segments = parse_tagged_text("通常テキスト");

        assert_eq!(segments.len(), 1);
        assert!(!segments[0].is_hit);
        assert_eq!(segments[0].text, "通常テキスト");
        assert!(segments[0].attributes.is_empty());
    }
}
