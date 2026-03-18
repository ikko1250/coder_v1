from __future__ import annotations

from html import escape

import polars as pl

from .frame_schema import POSITIONED_TOKEN_SCHEMA
from .frame_schema import empty_df

PARAGRAPH_ID_COL = "paragraph_id"
SENTENCE_ID_COL = "sentence_id"
SENTENCE_NO_COL = "sentence_no_in_paragraph"
TOKEN_NO_COL = "token_no"
SURFACE_COL = "surface"
TOKEN_ANNOTATION_SCHEMA = {
    **POSITIONED_TOKEN_SCHEMA,
    "condition_ids": pl.List(pl.String),
    "category_texts": pl.List(pl.String),
    "categories": pl.List(pl.String),
    "match_group_ids": pl.List(pl.String),
    "match_roles": pl.List(pl.String),
    "annotation_count": pl.UInt32,
}
RENDERED_PARAGRAPH_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "sentence_count": pl.UInt32,
    "paragraph_text": pl.String,
    "paragraph_text_tagged": pl.String,
    "paragraph_text_highlight_html": pl.String,
    "matched_condition_ids": pl.List(pl.String),
    "matched_condition_ids_text": pl.String,
    "matched_categories": pl.List(pl.String),
    "matched_categories_text": pl.String,
    "match_group_ids": pl.List(pl.String),
    "match_group_count": pl.UInt32,
    "annotated_token_count": pl.UInt32,
}


def _empty_token_annotations_df() -> pl.DataFrame:
    return empty_df(TOKEN_ANNOTATION_SCHEMA)


def _empty_rendered_paragraphs_df() -> pl.DataFrame:
    return empty_df(RENDERED_PARAGRAPH_SCHEMA)


def _unique_in_order(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value and value not in seen_values:
            seen_values.add(value)
            unique_values.append(value)
    return unique_values


def _escape_tag_attribute(value: str) -> str:
    # This is for the custom [[HIT ...]] tag format, not HTML.
    # Keep it aligned with src/tagged_text.rs::unescape_attribute.
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def _build_annotation_lookup(
    token_annotations_df: pl.DataFrame,
) -> dict[tuple[int, int, int], dict[str, object]]:
    annotation_lookup: dict[tuple[int, int, int], dict[str, object]] = {}
    if token_annotations_df.is_empty():
        return annotation_lookup

    for annotation_row in token_annotations_df.iter_rows(named=True):
        annotation_lookup[
            (
                int(annotation_row[PARAGRAPH_ID_COL]),
                int(annotation_row[SENTENCE_ID_COL]),
                int(annotation_row[TOKEN_NO_COL]),
            )
        ] = dict(annotation_row)
    return annotation_lookup


def _paragraph_join_separator(paragraph_df: pl.DataFrame) -> str:
    if "is_table_paragraph" not in paragraph_df.columns or paragraph_df.is_empty():
        return ""
    return "\n" if int(paragraph_df.get_column("is_table_paragraph")[0]) == 1 else ""


def _merge_paragraph_match_summary(
    rendered_paragraphs_df: pl.DataFrame,
    paragraph_match_summary_df: pl.DataFrame | None,
) -> pl.DataFrame:
    if paragraph_match_summary_df is None or paragraph_match_summary_df.is_empty():
        return rendered_paragraphs_df

    summary_columns = [
        "paragraph_id",
        "matched_condition_ids",
        "matched_condition_ids_text",
        "matched_categories",
        "matched_categories_text",
    ]
    available_summary_columns = [
        column_name
        for column_name in summary_columns
        if column_name in paragraph_match_summary_df.columns
    ]
    if available_summary_columns == ["paragraph_id"] or "paragraph_id" not in available_summary_columns:
        return rendered_paragraphs_df

    merged_df = rendered_paragraphs_df.join(
        paragraph_match_summary_df.select(available_summary_columns),
        on="paragraph_id",
        how="left",
        suffix="_summary",
    )
    return (
        merged_df
        .with_columns([
            pl.when(pl.col("matched_condition_ids_summary").is_not_null())
            .then(pl.col("matched_condition_ids_summary"))
            .otherwise(pl.col("matched_condition_ids"))
            .alias("matched_condition_ids"),
            pl.when(pl.col("matched_condition_ids_text_summary").is_not_null())
            .then(pl.col("matched_condition_ids_text_summary"))
            .otherwise(pl.col("matched_condition_ids_text"))
            .alias("matched_condition_ids_text"),
            pl.when(pl.col("matched_categories_summary").is_not_null())
            .then(pl.col("matched_categories_summary"))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories"),
            pl.when(pl.col("matched_categories_text_summary").is_not_null())
            .then(pl.col("matched_categories_text_summary"))
            .otherwise(pl.col("matched_categories_text"))
            .alias("matched_categories_text"),
        ])
        .select(list(RENDERED_PARAGRAPH_SCHEMA.keys()))
    )


def render_tagged_token(
    surface: str,
    annotation: dict[str, object] | None,
) -> tuple[str, str, list[str], list[str], list[str], int]:
    if annotation is None:
        return surface, escape(surface), [], [], [], 0

    condition_ids = list(annotation["condition_ids"])
    categories = list(annotation["categories"])
    category_texts = list(annotation["category_texts"])
    match_group_ids = list(annotation["match_group_ids"])

    tagged_fragment = (
        "[[HIT "
        f"condition_ids=\"{_escape_tag_attribute(','.join(condition_ids))}\" "
        f"categories=\"{_escape_tag_attribute(','.join(categories))}\" "
        f"groups=\"{_escape_tag_attribute(','.join(match_group_ids))}\""
        f"]]{surface}[[/HIT]]"
    )
    title_text = " / ".join(_unique_in_order(category_texts + condition_ids))
    html_fragment = (
        "<mark "
        "class=\"co-hit\" "
        f"data-condition-ids=\"{escape(' '.join(condition_ids))}\" "
        f"data-categories=\"{escape(' | '.join(categories))}\" "
        f"title=\"{escape(title_text)}\""
        f">{escape(surface)}</mark>"
    )
    return tagged_fragment, html_fragment, condition_ids, categories, match_group_ids, 1


def build_token_annotations_df(condition_hit_tokens_df: pl.DataFrame) -> pl.DataFrame:
    if condition_hit_tokens_df.is_empty():
        return _empty_token_annotations_df()

    annotation_rows: list[dict[str, object]] = []
    grouped_rows: dict[tuple[int, int, int], dict[str, object]] = {}
    sorted_hit_rows = condition_hit_tokens_df.sort([
        "paragraph_id",
        "sentence_id",
        "sentence_token_position",
        "condition_id",
        "match_group_id",
    ]).iter_rows(named=True)

    for hit_row in sorted_hit_rows:
        key = (
            int(hit_row["paragraph_id"]),
            int(hit_row["sentence_id"]),
            int(hit_row["token_no"]),
        )
        if key not in grouped_rows:
            grouped_rows[key] = {
                "paragraph_id": int(hit_row["paragraph_id"]),
                "sentence_id": int(hit_row["sentence_id"]),
                "sentence_no_in_paragraph": int(hit_row["sentence_no_in_paragraph"]),
                "is_table_paragraph": int(hit_row.get("is_table_paragraph", 0) or 0),
                "token_no": int(hit_row["token_no"]),
                "sentence_token_position": int(hit_row["sentence_token_position"]),
                "paragraph_token_position": int(hit_row["paragraph_token_position"]),
                "normalized_form": str(hit_row["normalized_form"]) if hit_row["normalized_form"] is not None else "",
                "surface": str(hit_row["surface"]) if hit_row["surface"] is not None else "",
                "condition_ids": [],
                "category_texts": [],
                "categories": [],
                "match_group_ids": [],
                "match_roles": [],
            }

        grouped_row = grouped_rows[key]
        condition_id = str(hit_row["condition_id"])
        category_text = str(hit_row["category_text"])
        match_group_id = str(hit_row["match_group_id"])
        match_role = str(hit_row["match_role"])
        if condition_id not in grouped_row["condition_ids"]:
            grouped_row["condition_ids"].append(condition_id)
        if category_text and category_text not in grouped_row["category_texts"]:
            grouped_row["category_texts"].append(category_text)
        for category in hit_row["categories"]:
            category_value = str(category)
            if category_value and category_value not in grouped_row["categories"]:
                grouped_row["categories"].append(category_value)
        if match_group_id not in grouped_row["match_group_ids"]:
            grouped_row["match_group_ids"].append(match_group_id)
        if match_role not in grouped_row["match_roles"]:
            grouped_row["match_roles"].append(match_role)

    for grouped_row in grouped_rows.values():
        annotation_rows.append({**grouped_row, "annotation_count": len(grouped_row["condition_ids"])})

    return (
        pl.DataFrame(annotation_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            pl.col("sentence_token_position").cast(pl.Int64),
            pl.col("paragraph_token_position").cast(pl.Int64),
            pl.col("annotation_count").cast(pl.UInt32),
        ])
        .sort(["paragraph_id", "sentence_id", "sentence_token_position"])
    )


def _render_sentence_fragment(
    sentence_df: pl.DataFrame,
    annotation_lookup: dict[tuple[int, int, int], dict[str, object]],
) -> dict[str, object]:
    sentence_no = int(sentence_df.get_column(SENTENCE_NO_COL)[0])
    plain_parts: list[str] = []
    tagged_parts: list[str] = []
    html_parts: list[str] = []
    matched_condition_ids: list[str] = []
    matched_categories: list[str] = []
    match_group_ids: list[str] = []
    annotated_token_count = 0

    for token_row in sentence_df.sort(TOKEN_NO_COL).iter_rows(named=True):
        key = (
            int(token_row[PARAGRAPH_ID_COL]),
            int(token_row[SENTENCE_ID_COL]),
            int(token_row[TOKEN_NO_COL]),
        )
        surface = str(token_row[SURFACE_COL]) if token_row[SURFACE_COL] is not None else ""
        plain_parts.append(surface)

        tagged_fragment, html_fragment, condition_ids, categories, grouped_match_ids, annotated_increment = (
            render_tagged_token(surface=surface, annotation=annotation_lookup.get(key))
        )
        tagged_parts.append(tagged_fragment)
        html_parts.append(html_fragment)
        matched_condition_ids.extend(condition_ids)
        matched_categories.extend(categories)
        match_group_ids.extend(grouped_match_ids)
        annotated_token_count += annotated_increment

    return {
        "sentence_no": sentence_no,
        "plain_text": "".join(plain_parts),
        "tagged_text": "".join(tagged_parts),
        "html_text": "".join(html_parts),
        "matched_condition_ids": matched_condition_ids,
        "matched_categories": matched_categories,
        "match_group_ids": match_group_ids,
        "annotated_token_count": annotated_token_count,
    }


def build_rendered_paragraphs_df(
    tokens_with_position_df: pl.DataFrame,
    token_annotations_df: pl.DataFrame,
    paragraph_match_summary_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_rendered_paragraphs_df()

    annotation_lookup = _build_annotation_lookup(token_annotations_df)
    paragraph_rows: list[dict[str, object]] = []
    for paragraph_df in tokens_with_position_df.sort([
        PARAGRAPH_ID_COL,
        SENTENCE_NO_COL,
        TOKEN_NO_COL,
    ]).partition_by(PARAGRAPH_ID_COL):
        paragraph_id = int(paragraph_df.get_column(PARAGRAPH_ID_COL)[0])
        paragraph_separator = _paragraph_join_separator(paragraph_df)
        sentence_fragments: list[dict[str, object]] = []
        matched_condition_ids: list[str] = []
        matched_categories: list[str] = []
        match_group_ids: list[str] = []
        annotated_token_count = 0

        for sentence_df in paragraph_df.partition_by(SENTENCE_ID_COL):
            sentence_fragment = _render_sentence_fragment(
                sentence_df=sentence_df,
                annotation_lookup=annotation_lookup,
            )
            sentence_fragments.append(sentence_fragment)
            matched_condition_ids.extend(sentence_fragment["matched_condition_ids"])
            matched_categories.extend(sentence_fragment["matched_categories"])
            match_group_ids.extend(sentence_fragment["match_group_ids"])
            annotated_token_count += int(sentence_fragment["annotated_token_count"])

        sorted_sentence_fragments = sorted(sentence_fragments, key=lambda fragment: int(fragment["sentence_no"]))
        paragraph_rows.append(
            {
                PARAGRAPH_ID_COL: paragraph_id,
                "sentence_count": len(sorted_sentence_fragments),
                "paragraph_text": paragraph_separator.join(fragment["plain_text"] for fragment in sorted_sentence_fragments),
                "paragraph_text_tagged": paragraph_separator.join(
                    fragment["tagged_text"] for fragment in sorted_sentence_fragments
                ),
                "paragraph_text_highlight_html": paragraph_separator.join(
                    fragment["html_text"] for fragment in sorted_sentence_fragments
                ),
                "matched_condition_ids": _unique_in_order(matched_condition_ids),
                "matched_condition_ids_text": ", ".join(_unique_in_order(matched_condition_ids)),
                "matched_categories": _unique_in_order(matched_categories),
                "matched_categories_text": ", ".join(_unique_in_order(matched_categories)),
                "match_group_ids": _unique_in_order(match_group_ids),
                "match_group_count": len(_unique_in_order(match_group_ids)),
                "annotated_token_count": annotated_token_count,
            }
        )

    rendered_paragraphs_df = (
        pl.DataFrame(paragraph_rows)
        .with_columns([
            pl.col(PARAGRAPH_ID_COL).cast(pl.Int64),
            pl.col("sentence_count").cast(pl.UInt32),
            pl.col("match_group_count").cast(pl.UInt32),
            pl.col("annotated_token_count").cast(pl.UInt32),
        ])
        .sort(PARAGRAPH_ID_COL)
    )
    return _merge_paragraph_match_summary(
        rendered_paragraphs_df=rendered_paragraphs_df,
        paragraph_match_summary_df=paragraph_match_summary_df,
    )
