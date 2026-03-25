use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AnalysisUnit {
    #[default]
    Paragraph,
    Sentence,
}

impl AnalysisUnit {
    pub(crate) fn id_column_name(self) -> &'static str {
        match self {
            Self::Paragraph => "paragraph_id",
            Self::Sentence => "sentence_id",
        }
    }

    pub(crate) fn count_label(self) -> &'static str {
        match self {
            Self::Paragraph => "段落",
            Self::Sentence => "文",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AnalysisRecord {
    pub(crate) row_no: usize,
    pub(crate) analysis_unit: AnalysisUnit,
    pub(crate) paragraph_id: String,
    pub(crate) sentence_id: String,
    pub(crate) document_id: String,
    pub(crate) municipality_name: String,
    pub(crate) ordinance_or_rule: String,
    pub(crate) doc_type: String,
    pub(crate) sentence_count: String,
    pub(crate) sentence_no_in_paragraph: String,
    pub(crate) sentence_no_in_document: String,
    pub(crate) sentence_text: String,
    pub(crate) sentence_text_tagged: String,
    pub(crate) paragraph_text: String,
    pub(crate) paragraph_text_tagged: String,
    pub(crate) matched_condition_ids_text: String,
    pub(crate) matched_categories_text: String,
    pub(crate) matched_form_group_ids_text: String,
    pub(crate) matched_form_group_logics_text: String,
    pub(crate) form_group_explanations_text: String,
    pub(crate) text_groups_explanations_text: String,
    pub(crate) mixed_scope_warning_text: String,
    pub(crate) match_group_ids_text: String,
    pub(crate) match_group_count: String,
    pub(crate) annotated_token_count: String,
    pub(crate) manual_annotation_count: String,
    pub(crate) manual_annotation_pairs_text: String,
    pub(crate) manual_annotation_namespaces_text: String,
}

impl AnalysisRecord {
    pub(crate) fn unit_id(&self) -> &str {
        match self.analysis_unit {
            AnalysisUnit::Paragraph => &self.paragraph_id,
            AnalysisUnit::Sentence => &self.sentence_id,
        }
    }

    pub(crate) fn primary_text(&self) -> &str {
        match self.analysis_unit {
            AnalysisUnit::Paragraph => &self.paragraph_text,
            AnalysisUnit::Sentence => &self.sentence_text,
        }
    }

    pub(crate) fn primary_text_tagged(&self) -> &str {
        match self.analysis_unit {
            AnalysisUnit::Paragraph => &self.paragraph_text_tagged,
            AnalysisUnit::Sentence => &self.sentence_text_tagged,
        }
    }

    pub(crate) fn supports_db_viewer(&self) -> bool {
        self.analysis_unit == AnalysisUnit::Paragraph
    }

    pub(crate) fn supports_manual_annotation(&self) -> bool {
        self.analysis_unit == AnalysisUnit::Paragraph
    }
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
    pub(crate) source_paragraph_text: Option<String>,
    pub(crate) context: Option<DbParagraphContext>,
    pub(crate) error_message: Option<String>,
    /// P2-09: `prepare_db_viewer_state` 時点の `ViewerCoreState::data_source_generation`（整合確認用）。
    pub(crate) data_source_generation_when_prepared: Option<u64>,
}

impl DbViewerState {
    pub(crate) fn new(db_path: PathBuf) -> Self {
        Self {
            is_open: false,
            db_path,
            source_paragraph_id: None,
            source_paragraph_text: None,
            context: None,
            error_message: None,
            data_source_generation_when_prepared: None,
        }
    }

    pub(crate) fn reset_loaded_state(&mut self) {
        self.is_open = false;
        self.source_paragraph_id = None;
        self.source_paragraph_text = None;
        self.context = None;
        self.error_message = None;
        self.data_source_generation_when_prepared = None;
    }
}
