use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CsvRecord {
    pub(crate) row_no: usize,
    pub(crate) paragraph_id: String,
    pub(crate) document_id: String,
    pub(crate) municipality_name: String,
    pub(crate) ordinance_or_rule: String,
    pub(crate) doc_type: String,
    pub(crate) sentence_count: String,
    pub(crate) paragraph_text: String,
    pub(crate) paragraph_text_tagged: String,
    pub(crate) matched_condition_ids_text: String,
    pub(crate) matched_categories_text: String,
    pub(crate) match_group_ids_text: String,
    pub(crate) match_group_count: String,
    pub(crate) annotated_token_count: String,
}

#[derive(Clone, Debug)]
pub(crate) struct TextSegment {
    pub(crate) text: String,
    pub(crate) is_hit: bool,
    #[allow(dead_code)]
    pub(crate) attributes: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FilterOption {
    pub(crate) value: String,
    pub(crate) count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FilterColumn {
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

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DbParagraph {
    pub(crate) paragraph_id: i64,
    pub(crate) document_id: i64,
    pub(crate) paragraph_no: i64,
    pub(crate) paragraph_text: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DbParagraphContext {
    pub(crate) center: DbParagraph,
    pub(crate) paragraphs: Vec<DbParagraph>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DbViewerState {
    pub(crate) is_open: bool,
    pub(crate) db_path: PathBuf,
    pub(crate) source_paragraph_id: Option<i64>,
    pub(crate) csv_paragraph_text: Option<String>,
    pub(crate) context: Option<DbParagraphContext>,
    pub(crate) error_message: Option<String>,
}

impl DbViewerState {
    pub(crate) fn new(db_path: PathBuf) -> Self {
        Self {
            is_open: false,
            db_path,
            source_paragraph_id: None,
            csv_paragraph_text: None,
            context: None,
            error_message: None,
        }
    }

    pub(crate) fn reset_loaded_state(&mut self) {
        self.is_open = false;
        self.source_paragraph_id = None;
        self.csv_paragraph_text = None;
        self.context = None;
        self.error_message = None;
    }
}
