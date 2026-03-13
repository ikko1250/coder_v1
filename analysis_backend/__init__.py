from __future__ import annotations

from importlib import import_module

__all__ = [
    "ConfigIssue",
    "ConditionHitResult",
    "DataAccessIssue",
    "DataAccessResult",
    "DistanceMatchingMode",
    "FilterConfig",
    "LoadFilterConfigResult",
    "MatchingWarning",
    "NormalizeConditionsResult",
    "NormalizedCondition",
    "TargetSelectionResult",
    "build_condition_hit_result",
    "build_condition_hit_tokens_df",
    "build_reconstructed_paragraphs_export_df",
    "build_rendered_paragraphs_df",
    "build_token_annotations_df",
    "build_tokens_with_position_df",
    "enrich_reconstructed_paragraphs_df",
    "load_filter_config",
    "load_filter_config_result",
    "read_analysis_sentences_result",
    "read_analysis_sentences",
    "read_analysis_tokens_result",
    "read_analysis_tokens",
    "read_paragraph_document_metadata_result",
    "read_paragraph_document_metadata",
    "reconstruct_paragraphs_by_ids",
    "reconstruct_sentences_by_ids",
    "select_target_ids_by_cooccurrence_conditions",
]


def __getattr__(name: str):
    if name in __all__:
        module_name = ".condition_model" if name in {
            "ConfigIssue",
            "ConditionHitResult",
            "DataAccessIssue",
            "DataAccessResult",
            "DistanceMatchingMode",
            "FilterConfig",
            "LoadFilterConfigResult",
            "MatchingWarning",
            "NormalizeConditionsResult",
            "NormalizedCondition",
            "TargetSelectionResult",
        } else ".filter_config" if name in {"load_filter_config", "load_filter_config_result"} else ".data_access" if name in {
            "read_analysis_sentences_result",
            "read_analysis_sentences",
            "read_analysis_tokens_result",
            "read_analysis_tokens",
            "read_paragraph_document_metadata_result",
            "read_paragraph_document_metadata",
        } else ".token_position" if name == "build_tokens_with_position_df" else ".rendering" if name in {
            "build_rendered_paragraphs_df",
            "build_token_annotations_df",
        } else ".export_formatter" if name in {
            "build_reconstructed_paragraphs_export_df",
            "enrich_reconstructed_paragraphs_df",
        } else ".analysis_core"
        module = import_module(module_name, __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
