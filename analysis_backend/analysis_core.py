from __future__ import annotations

import heapq
from html import escape
from itertools import product
import json
from pathlib import Path
import sqlite3

import polars as pl

from .condition_model import FilterConfig

PARAGRAPH_ID_COL = "paragraph_id"
SENTENCE_ID_COL = "sentence_id"
SENTENCE_NO_COL = "sentence_no_in_paragraph"
TOKEN_NO_COL = "token_no"
NORMALIZED_FORM_COL = "normalized_form"
SURFACE_COL = "surface"
SENTENCE_TOKEN_POSITION_COL = "sentence_token_position"
PARAGRAPH_TOKEN_POSITION_COL = "paragraph_token_position"
PARAGRAPH_METADATA_CHUNK_SIZE = 900
MAX_DISTANCE_MATCH_COMBINATIONS = 10000

PARAGRAPH_METADATA_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "document_id": pl.Int64,
    "municipality_name": pl.String,
    "doc_type": pl.String,
}
CONDITION_EVAL_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "condition_id": pl.String,
    "category_text": pl.String,
    "search_scope": pl.String,
    "form_match_logic": pl.String,
    "condition_forms": pl.String,
    "required_form_count": pl.UInt32,
    "matched_form_count": pl.UInt32,
    "evaluated_unit_count": pl.UInt32,
    "matched_unit_count": pl.UInt32,
    "requested_max_token_distance": pl.Int64,
    "effective_max_token_distance": pl.Int64,
    "distance_check_applied": pl.Boolean,
    "distance_is_match": pl.Boolean,
    "is_match": pl.Boolean,
}
PARAGRAPH_SUMMARY_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "condition_count": pl.UInt32,
    "matched_condition_count": pl.UInt32,
    "is_selected": pl.Boolean,
    "matched_condition_ids": pl.List(pl.String),
}
POSITIONED_TOKEN_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    SENTENCE_ID_COL: pl.Int64,
    SENTENCE_NO_COL: pl.Int64,
    TOKEN_NO_COL: pl.Int64,
    SENTENCE_TOKEN_POSITION_COL: pl.Int64,
    PARAGRAPH_TOKEN_POSITION_COL: pl.Int64,
    NORMALIZED_FORM_COL: pl.String,
    SURFACE_COL: pl.String,
}
CONDITION_HIT_SCHEMA = {
    **POSITIONED_TOKEN_SCHEMA,
    "condition_id": pl.String,
    "category_text": pl.String,
    "categories": pl.List(pl.String),
    "match_group_id": pl.String,
    "match_role": pl.String,
}
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

def load_filter_config(filter_config_path: Path) -> FilterConfig:
    if not filter_config_path.exists():
        raise FileNotFoundError(f"Filter config JSON not found: {filter_config_path}")

    try:
        raw_config = json.loads(filter_config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON format: {filter_config_path} ({exc})") from exc

    if not isinstance(raw_config, dict):
        raise ValueError(f"JSON root must be object: {filter_config_path}")

    raw_conditions = raw_config.get("cooccurrence_conditions", [])
    if not isinstance(raw_conditions, list):
        raise ValueError(f"'cooccurrence_conditions' must be list: {filter_config_path}")

    raw_match_logic = str(raw_config.get("condition_match_logic", "any")).strip().lower()
    condition_match_logic = raw_match_logic if raw_match_logic in {"any", "all"} else "any"

    raw_max_reconstructed = raw_config.get("max_reconstructed_paragraphs", 10000)
    try:
        max_reconstructed_paragraphs = int(raw_max_reconstructed)
    except (TypeError, ValueError):
        max_reconstructed_paragraphs = 10000
    if max_reconstructed_paragraphs < 1:
        max_reconstructed_paragraphs = 10000

    return FilterConfig(
        condition_match_logic=condition_match_logic,
        cooccurrence_conditions=raw_conditions,
        loaded_condition_count=len(raw_conditions),
        max_reconstructed_paragraphs=max_reconstructed_paragraphs,
    )


def _empty_df(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _read_database_df(db_path: Path, query: str) -> pl.DataFrame:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            return pl.read_database(query=query, connection=conn)
    except sqlite3.Error as exc:
        raise RuntimeError(f"SQLite read failed: {db_path} ({exc})") from exc


def read_analysis_tokens(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    query = "SELECT * FROM analysis_tokens"
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df(db_path=db_path, query=query)


def read_analysis_sentences(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    query = """
        SELECT sentence_id, paragraph_id, sentence_no_in_paragraph
        FROM analysis_sentences
    """
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df(db_path=db_path, query=query)


def read_paragraph_document_metadata(db_path: Path, paragraph_ids: list[int]) -> pl.DataFrame:
    if not paragraph_ids:
        return _empty_df(PARAGRAPH_METADATA_SCHEMA)

    rows: list[tuple[int, int, str | None, str | None]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            for start_idx in range(0, len(paragraph_ids), PARAGRAPH_METADATA_CHUNK_SIZE):
                chunk_ids = paragraph_ids[start_idx:start_idx + PARAGRAPH_METADATA_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk_ids)
                query = f"""
                    SELECT
                        p.paragraph_id,
                        p.document_id,
                        d.municipality_name,
                        d.doc_type
                    FROM analysis_paragraphs AS p
                    JOIN analysis_documents AS d
                      ON d.document_id = p.document_id
                    WHERE p.paragraph_id IN ({placeholders})
                """
                rows.extend(conn.execute(query, tuple(chunk_ids)).fetchall())
    except sqlite3.Error as exc:
        raise RuntimeError(f"SQLite metadata read failed: {db_path} ({exc})") from exc

    if not rows:
        return _empty_df(PARAGRAPH_METADATA_SCHEMA)

    return (
        pl.DataFrame(rows, schema=list(PARAGRAPH_METADATA_SCHEMA.keys()), orient="row")
        .with_columns([
            pl.col(PARAGRAPH_ID_COL).cast(pl.Int64),
            pl.col("document_id").cast(pl.Int64),
        ])
        .sort(PARAGRAPH_ID_COL)
    )


def _empty_condition_eval_df() -> pl.DataFrame:
    return _empty_df(CONDITION_EVAL_SCHEMA)


def _empty_paragraph_summary_df() -> pl.DataFrame:
    return _empty_df(PARAGRAPH_SUMMARY_SCHEMA)


def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return _empty_df(CONDITION_HIT_SCHEMA)


def _empty_token_annotations_df() -> pl.DataFrame:
    return _empty_df(TOKEN_ANNOTATION_SCHEMA)


def _empty_rendered_paragraphs_df() -> pl.DataFrame:
    return _empty_df(RENDERED_PARAGRAPH_SCHEMA)


def _normalize_condition_categories(raw_categories: object) -> list[str]:
    raw_category_values = raw_categories if isinstance(raw_categories, list) else [raw_categories]
    categories: list[str] = []
    for raw_category in raw_category_values:
        category = str(raw_category).strip() if raw_category is not None else ""
        if category and category not in categories:
            categories.append(category)
    if not categories:
        categories = ["未分類"]
    return categories


def _unique_in_order(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value and value not in seen_values:
            seen_values.add(value)
            unique_values.append(value)
    return unique_values


def _escape_tag_attribute(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def _clean_cooccurrence_conditions(
    cooccurrence_conditions: list[dict[str, object]],
) -> list[dict[str, object]]:
    cleaned_conditions: list[dict[str, object]] = []
    used_condition_ids: set[str] = set()

    for idx, raw_condition in enumerate(cooccurrence_conditions, start=1):
        if not isinstance(raw_condition, dict):
            continue

        raw_forms = raw_condition.get("forms", [])
        if not isinstance(raw_forms, list):
            continue

        forms: list[str] = []
        for raw_form in raw_forms:
            form = str(raw_form).strip()
            if form:
                forms.append(form)
        unique_forms = list(dict.fromkeys(forms))
        if not unique_forms:
            continue

        raw_condition_id_val = raw_condition.get("condition_id")
        raw_condition_id = (
            str(raw_condition_id_val).strip() if raw_condition_id_val is not None else ""
        )
        base_condition_id = raw_condition_id or f"condition_{idx}"
        condition_id = base_condition_id
        suffix = 2
        while condition_id in used_condition_ids:
            condition_id = f"{base_condition_id}_{suffix}"
            suffix += 1
        used_condition_ids.add(condition_id)

        raw_form_match_logic = str(raw_condition.get("form_match_logic", "all")).strip().lower()
        form_match_logic = raw_form_match_logic if raw_form_match_logic in {"all", "any"} else "all"
        raw_search_scope = str(raw_condition.get("search_scope", "paragraph")).strip().lower()
        search_scope = raw_search_scope if raw_search_scope in {"paragraph", "sentence"} else "paragraph"

        requested_max_token_distance: int | None = None
        raw_distance = raw_condition.get("max_token_distance")
        if raw_distance is not None:
            try:
                parsed_distance = int(raw_distance)
                if parsed_distance >= 0:
                    requested_max_token_distance = parsed_distance
            except (TypeError, ValueError):
                requested_max_token_distance = None

        effective_max_token_distance = (
            requested_max_token_distance if form_match_logic == "all" else None
        )
        categories = _normalize_condition_categories(raw_condition.get("categories"))

        cleaned_conditions.append(
            {
                "condition_id": condition_id,
                "categories": categories,
                "category_text": ", ".join(categories),
                "forms": unique_forms,
                "search_scope": search_scope,
                "form_match_logic": form_match_logic,
                "requested_max_token_distance": requested_max_token_distance,
                "effective_max_token_distance": effective_max_token_distance,
            }
        )

    return cleaned_conditions


def build_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int] | None = None,
    target_forms: list[str] | None = None,
) -> pl.DataFrame:
    if paragraph_ids is not None and not paragraph_ids:
        return _empty_df(POSITIONED_TOKEN_SCHEMA)

    sentence_order_df = sentences_df.select(["sentence_id", "paragraph_id", "sentence_no_in_paragraph"])
    base_tokens_df = tokens_df
    if paragraph_ids is not None:
        sentence_order_df = sentence_order_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))
        base_tokens_df = base_tokens_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))

    sentence_token_counts_df = (
        base_tokens_df
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
        .group_by(["paragraph_id", "sentence_no_in_paragraph"])
        .agg(pl.len().alias("sentence_token_count"))
        .sort(["paragraph_id", "sentence_no_in_paragraph"])
        .with_columns(
            (
                pl.col("sentence_token_count")
                .cum_sum()
                .over("paragraph_id", order_by="sentence_no_in_paragraph")
                - pl.col("sentence_token_count")
            )
            .alias("sentence_offset")
        )
        .select(["paragraph_id", "sentence_no_in_paragraph", "sentence_offset"])
    )

    selected_tokens_df = base_tokens_df
    if target_forms is not None:
        selected_tokens_df = selected_tokens_df.filter(pl.col("normalized_form").is_in(target_forms))

    return (
        selected_tokens_df
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
        .join(sentence_token_counts_df, on=["paragraph_id", "sentence_no_in_paragraph"], how="inner")
        .with_columns([
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64).alias("sentence_token_position"),
            (pl.col("sentence_offset") + pl.col("token_no")).cast(pl.Int64).alias("paragraph_token_position"),
        ])
        .select([
            "paragraph_id",
            "sentence_id",
            "sentence_no_in_paragraph",
            "token_no",
            "sentence_token_position",
            "paragraph_token_position",
            "normalized_form",
            "surface",
        ])
    )


def _build_candidate_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    target_forms: list[str],
) -> pl.DataFrame:
    return build_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        paragraph_ids=None,
        target_forms=target_forms,
    )


def _has_forms_within_distance(form_positions_list: list[list[int]], max_token_distance: int) -> bool:
    if max_token_distance < 0:
        return False
    if not form_positions_list or any(not positions for positions in form_positions_list):
        return False

    pointer_list = [0] * len(form_positions_list)
    length_list = [len(positions) for positions in form_positions_list]

    while True:
        current_values = [
            form_positions_list[idx][pointer_list[idx]]
            for idx in range(len(form_positions_list))
        ]
        if max(current_values) - min(current_values) <= max_token_distance:
            return True

        min_value_index = min(range(len(current_values)), key=lambda idx: current_values[idx])
        pointer_list[min_value_index] += 1
        if pointer_list[min_value_index] >= length_list[min_value_index]:
            return False


def _evaluate_distance_matches_by_unit(
    tokens_with_position_df: pl.DataFrame,
    forms: list[str],
    unit_column: str,
    position_column: str,
    max_token_distance: int,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_df({"unit_id": pl.Int64, "distance_is_match": pl.Boolean})

    rows: list[dict[str, object]] = []
    form_tokens_df = (
        tokens_with_position_df
        .filter(pl.col("normalized_form").is_in(forms))
        .select([unit_column, "normalized_form", position_column])
    )
    if form_tokens_df.is_empty():
        return _empty_df({"unit_id": pl.Int64, "distance_is_match": pl.Boolean})

    for unit_df in form_tokens_df.partition_by(unit_column):
        unit_id = int(unit_df.get_column(unit_column)[0])
        form_positions_list: list[list[int]] = []
        for form in forms:
            positions = (
                unit_df
                .filter(pl.col("normalized_form") == form)
                .sort(position_column)
                .get_column(position_column)
                .to_list()
            )
            form_positions_list.append(positions)

        rows.append(
            {
                "unit_id": unit_id,
                "distance_is_match": _has_forms_within_distance(
                    form_positions_list=form_positions_list,
                    max_token_distance=max_token_distance,
                ),
            }
        )

    return pl.DataFrame(rows, schema={"unit_id": pl.Int64, "distance_is_match": pl.Boolean})


def _build_condition_hit_row(
    row: dict[str, object],
    condition_id: str,
    categories: list[str],
    match_group_id: str,
    match_role: str,
) -> dict[str, object]:
    return {
        "paragraph_id": int(row["paragraph_id"]),
        "sentence_id": int(row["sentence_id"]),
        "sentence_no_in_paragraph": int(row["sentence_no_in_paragraph"]),
        "token_no": int(row["token_no"]),
        "sentence_token_position": int(row["sentence_token_position"]),
        "paragraph_token_position": int(row["paragraph_token_position"]),
        "normalized_form": str(row["normalized_form"]) if row["normalized_form"] is not None else "",
        "surface": str(row["surface"]) if row["surface"] is not None else "",
        "condition_id": condition_id,
        "category_text": ", ".join(categories),
        "categories": categories,
        "match_group_id": match_group_id,
        "match_role": match_role,
    }


def _row_position_key(row: dict[str, object], position_column: str) -> tuple[int, int, int]:
    return (
        int(row[position_column]),
        int(row["sentence_id"]),
        int(row["token_no"]),
    )


def _find_distance_match_groups_by_product(
    form_row_options: list[list[dict[str, object]]],
    position_column: str,
    max_token_distance: int,
) -> list[list[dict[str, object]]]:
    candidate_groups: list[dict[str, object]] = []
    for candidate_rows in product(*form_row_options):
        sorted_rows = sorted(candidate_rows, key=lambda row: _row_position_key(row, position_column))
        token_keys = [
            (int(row["sentence_id"]), int(row["token_no"]))
            for row in sorted_rows
        ]
        if len(set(token_keys)) != len(token_keys):
            continue

        positions = [int(row[position_column]) for row in sorted_rows]
        span_width = max(positions) - min(positions)
        if span_width > max_token_distance:
            continue

        candidate_groups.append(
            {
                "rows": sorted_rows,
                "span_width": span_width,
                "start_position": min(positions),
                "end_position": max(positions),
            }
        )

    candidate_groups.sort(
        key=lambda group: (
            int(group["span_width"]),
            int(group["start_position"]),
            int(group["end_position"]),
        )
    )
    return [list(group["rows"]) for group in candidate_groups]


def _find_best_distance_group(
    form_row_options: list[list[dict[str, object]]],
    position_column: str,
) -> tuple[list[dict[str, object]], list[int], int] | None:
    current_indices = [0] * len(form_row_options)
    heap: list[tuple[int, int, int, int]] = []
    current_max_position = -1

    for form_index, option_rows in enumerate(form_row_options):
        row = option_rows[0]
        position, sentence_id, token_no = _row_position_key(row, position_column)
        heapq.heappush(heap, (position, sentence_id, token_no, form_index))
        current_max_position = max(current_max_position, position)

    best_rows: list[dict[str, object]] | None = None
    best_indices: list[int] | None = None
    best_span_width: int | None = None
    best_start_position: int | None = None
    best_end_position: int | None = None

    while True:
        min_position, _sentence_id, _token_no, form_index = heapq.heappop(heap)
        current_rows = [
            form_row_options[idx][current_indices[idx]]
            for idx in range(len(form_row_options))
        ]
        span_width = current_max_position - min_position
        if best_rows is None or (
            span_width,
            min_position,
            current_max_position,
        ) < (
            best_span_width,
            best_start_position,
            best_end_position,
        ):
            best_rows = [dict(row) for row in current_rows]
            best_indices = current_indices.copy()
            best_span_width = span_width
            best_start_position = min_position
            best_end_position = current_max_position

        current_indices[form_index] += 1
        if current_indices[form_index] >= len(form_row_options[form_index]):
            break

        next_row = form_row_options[form_index][current_indices[form_index]]
        next_position, next_sentence_id, next_token_no = _row_position_key(next_row, position_column)
        heapq.heappush(heap, (next_position, next_sentence_id, next_token_no, form_index))
        current_max_position = max(current_max_position, next_position)

    if best_rows is None or best_indices is None or best_span_width is None:
        return None

    return best_rows, best_indices, best_span_width


def _find_distance_match_groups_greedily(
    form_row_options: list[list[dict[str, object]]],
    position_column: str,
    max_token_distance: int,
) -> list[list[dict[str, object]]]:
    remaining_row_options = [list(option_rows) for option_rows in form_row_options]
    matched_groups: list[list[dict[str, object]]] = []

    while all(remaining_row_options):
        best_group = _find_best_distance_group(
            form_row_options=remaining_row_options,
            position_column=position_column,
        )
        if best_group is None:
            break

        matched_rows, matched_indices, span_width = best_group
        if span_width > max_token_distance:
            break

        matched_groups.append(sorted(matched_rows, key=lambda row: _row_position_key(row, position_column)))
        for form_index in reversed(range(len(matched_indices))):
            del remaining_row_options[form_index][matched_indices[form_index]]

    return matched_groups


def _find_distance_match_groups_by_unit(
    tokens_with_position_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    forms: list[str],
    unit_column: str,
    position_column: str,
    max_token_distance: int,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_condition_hit_tokens_df()

    hit_rows: list[dict[str, object]] = []
    for unit_df in tokens_with_position_df.partition_by(unit_column):
        form_row_options: list[list[dict[str, object]]] = []
        for form in forms:
            form_rows = (
                unit_df
                .filter(pl.col("normalized_form") == form)
                .sort(position_column)
                .iter_rows(named=True)
            )
            form_row_list = [dict(form_row) for form_row in form_rows]
            if not form_row_list:
                form_row_options = []
                break
            form_row_options.append(form_row_list)
        if not form_row_options:
            continue

        unit_id = int(unit_df.get_column(unit_column)[0])
        group_index = 1
        total_combinations = 1
        for option_rows in form_row_options:
            total_combinations *= len(option_rows)
            if total_combinations > MAX_DISTANCE_MATCH_COMBINATIONS:
                break

        if total_combinations > MAX_DISTANCE_MATCH_COMBINATIONS:
            candidate_groups = _find_distance_match_groups_greedily(
                form_row_options=form_row_options,
                position_column=position_column,
                max_token_distance=max_token_distance,
            )
        else:
            candidate_groups = _find_distance_match_groups_by_product(
                form_row_options=form_row_options,
                position_column=position_column,
                max_token_distance=max_token_distance,
            )

        for candidate_group in candidate_groups:
            match_group_id = f"{condition_id}:{unit_id}:{group_index}"
            for matched_row in candidate_group:
                hit_rows.append(
                    _build_condition_hit_row(
                        row=matched_row,
                        condition_id=condition_id,
                        categories=categories,
                        match_group_id=match_group_id,
                        match_role=str(matched_row["normalized_form"]),
                    )
                )
            group_index += 1

    if not hit_rows:
        return _empty_condition_hit_tokens_df()

    return (
        pl.DataFrame(hit_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            pl.col("sentence_token_position").cast(pl.Int64),
            pl.col("paragraph_token_position").cast(pl.Int64),
        ])
        .sort([
            "paragraph_id",
            "sentence_id",
            "sentence_token_position",
            "condition_id",
            "match_group_id",
        ])
    )


def _find_token_hits_by_unit(
    tokens_with_position_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    forms: list[str],
    unit_column: str,
    position_column: str,
    form_match_logic: str,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_condition_hit_tokens_df()

    hit_rows: list[dict[str, object]] = []
    for unit_df in tokens_with_position_df.partition_by(unit_column):
        matched_rows = (
            unit_df
            .filter(pl.col("normalized_form").is_in(forms))
            .sort(position_column)
            .iter_rows(named=True)
        )
        matched_row_list = [dict(matched_row) for matched_row in matched_rows]
        if not matched_row_list:
            continue

        matched_forms = _unique_in_order([
            str(matched_row["normalized_form"])
            for matched_row in matched_row_list
            if matched_row["normalized_form"] is not None
        ])
        if form_match_logic == "all" and len(matched_forms) < len(forms):
            continue

        unit_id = int(unit_df.get_column(unit_column)[0])
        for group_index, matched_row in enumerate(matched_row_list, start=1):
            match_group_id = f"{condition_id}:{unit_id}:token:{group_index}"
            hit_rows.append(
                _build_condition_hit_row(
                    row=matched_row,
                    condition_id=condition_id,
                    categories=categories,
                    match_group_id=match_group_id,
                    match_role=str(matched_row["normalized_form"]),
                )
            )

    if not hit_rows:
        return _empty_condition_hit_tokens_df()

    return (
        pl.DataFrame(hit_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            pl.col("sentence_token_position").cast(pl.Int64),
            pl.col("paragraph_token_position").cast(pl.Int64),
        ])
        .sort([
            "paragraph_id",
            "sentence_id",
            "sentence_token_position",
            "condition_id",
            "match_group_id",
        ])
    )


def build_condition_hit_tokens_df(
    tokens_with_position_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_condition_hit_tokens_df()

    cleaned_conditions = _clean_cooccurrence_conditions(cooccurrence_conditions)
    if not cleaned_conditions:
        return _empty_condition_hit_tokens_df()

    condition_hit_frames: list[pl.DataFrame] = []
    for condition in cleaned_conditions:
        condition_id = str(condition["condition_id"])
        categories = list(condition["categories"])
        forms = list(condition["forms"])
        search_scope = str(condition["search_scope"])
        form_match_logic = str(condition["form_match_logic"])
        effective_max_token_distance = condition["effective_max_token_distance"]

        relevant_tokens_df = tokens_with_position_df.filter(pl.col("normalized_form").is_in(forms))
        if relevant_tokens_df.is_empty():
            continue

        if search_scope == "sentence":
            unit_column = "sentence_id"
            position_column = "sentence_token_position"
        else:
            unit_column = "paragraph_id"
            position_column = "paragraph_token_position"

        if effective_max_token_distance is not None:
            hit_df = _find_distance_match_groups_by_unit(
                tokens_with_position_df=relevant_tokens_df,
                condition_id=condition_id,
                categories=categories,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                max_token_distance=int(effective_max_token_distance),
            )
        else:
            hit_df = _find_token_hits_by_unit(
                tokens_with_position_df=relevant_tokens_df,
                condition_id=condition_id,
                categories=categories,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                form_match_logic=form_match_logic,
            )

        if not hit_df.is_empty():
            condition_hit_frames.append(hit_df)

    if not condition_hit_frames:
        return _empty_condition_hit_tokens_df()

    return pl.concat(condition_hit_frames, how="vertical")


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
            pl.col("token_no").cast(pl.Int64),
            pl.col("sentence_token_position").cast(pl.Int64),
            pl.col("paragraph_token_position").cast(pl.Int64),
            pl.col("annotation_count").cast(pl.UInt32),
        ])
        .sort(["paragraph_id", "sentence_id", "sentence_token_position"])
    )


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
                "paragraph_text": "".join(fragment["plain_text"] for fragment in sorted_sentence_fragments),
                "paragraph_text_tagged": "".join(fragment["tagged_text"] for fragment in sorted_sentence_fragments),
                "paragraph_text_highlight_html": "".join(
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

    return (
        pl.DataFrame(paragraph_rows)
        .with_columns([
            pl.col(PARAGRAPH_ID_COL).cast(pl.Int64),
            pl.col("sentence_count").cast(pl.UInt32),
            pl.col("match_group_count").cast(pl.UInt32),
            pl.col("annotated_token_count").cast(pl.UInt32),
        ])
        .sort(PARAGRAPH_ID_COL)
    )


def select_target_ids_by_cooccurrence_conditions(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    condition_match_logic: str = "any",
    max_paragraph_ids: int = 100,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list[int], list[int]]:
    cleaned_conditions = _clean_cooccurrence_conditions(cooccurrence_conditions)
    if not cleaned_conditions:
        return tokens_df.clear(), _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    all_forms = sorted({form for condition in cleaned_conditions for form in condition["forms"]})
    candidate_tokens_df = _build_candidate_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        target_forms=all_forms,
    )
    if candidate_tokens_df.is_empty():
        return candidate_tokens_df, _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    condition_eval_frames: list[pl.DataFrame] = []
    for condition in cleaned_conditions:
        condition_id = str(condition["condition_id"])
        category_text = str(condition["category_text"])
        forms = list(condition["forms"])
        search_scope = str(condition["search_scope"])
        form_match_logic = str(condition["form_match_logic"])
        requested_max_token_distance = condition["requested_max_token_distance"]
        effective_max_token_distance = condition["effective_max_token_distance"]
        distance_check_applied = effective_max_token_distance is not None
        required_form_count = len(forms)

        if search_scope == "sentence":
            unit_column = "sentence_id"
            position_column = "sentence_token_position"
            unit_df = (
                candidate_tokens_df
                .select([pl.col("sentence_id").alias("unit_id"), "paragraph_id"])
                .unique()
            )
            unit_form_df = (
                candidate_tokens_df
                .select([pl.col("sentence_id").alias("unit_id"), "normalized_form"])
                .unique()
            )
        else:
            unit_column = "paragraph_id"
            position_column = "paragraph_token_position"
            unit_df = (
                candidate_tokens_df
                .select(pl.col("paragraph_id").alias("unit_id"))
                .unique()
                .with_columns(pl.col("unit_id").alias("paragraph_id"))
            )
            unit_form_df = (
                candidate_tokens_df
                .select([pl.col("paragraph_id").alias("unit_id"), "normalized_form"])
                .unique()
            )

        matched_counts_df = (
            unit_form_df
            .filter(pl.col("normalized_form").is_in(forms))
            .group_by("unit_id")
            .agg(pl.col("normalized_form").n_unique().alias("matched_form_count"))
        )
        base_match_expr = (
            pl.col("matched_form_count") >= 1
            if form_match_logic == "any"
            else pl.col("matched_form_count") >= required_form_count
        )

        unit_eval_df = (
            unit_df
            .join(matched_counts_df, on="unit_id", how="left")
            .with_columns(pl.col("matched_form_count").fill_null(0).cast(pl.UInt32))
        )
        if distance_check_applied:
            distance_match_df = _evaluate_distance_matches_by_unit(
                tokens_with_position_df=candidate_tokens_df,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                max_token_distance=int(effective_max_token_distance),
            )
            unit_eval_df = (
                unit_eval_df
                .join(distance_match_df, on="unit_id", how="left")
                .with_columns(pl.col("distance_is_match").fill_null(False))
            )
            condition_match_expr = base_match_expr & pl.col("distance_is_match")
        else:
            unit_eval_df = unit_eval_df.with_columns(pl.lit(True).alias("distance_is_match"))
            condition_match_expr = base_match_expr

        paragraph_eval_df = (
            unit_eval_df
            .with_columns(condition_match_expr.alias("is_match"))
            .group_by("paragraph_id")
            .agg([
                pl.col("matched_form_count").max().cast(pl.UInt32).alias("matched_form_count"),
                pl.len().cast(pl.UInt32).alias("evaluated_unit_count"),
                pl.col("is_match").sum().cast(pl.UInt32).alias("matched_unit_count"),
                pl.col("distance_is_match").any().alias("distance_is_match"),
                pl.col("is_match").any().alias("is_match"),
            ])
        )

        condition_eval_frames.append(
            paragraph_eval_df
            .with_columns([
                pl.lit(condition_id).alias("condition_id"),
                pl.lit(category_text).alias("category_text"),
                pl.lit(search_scope).alias("search_scope"),
                pl.lit(form_match_logic).alias("form_match_logic"),
                pl.lit(", ".join(forms)).alias("condition_forms"),
                pl.lit(required_form_count).cast(pl.UInt32).alias("required_form_count"),
                pl.lit(requested_max_token_distance, dtype=pl.Int64).alias("requested_max_token_distance"),
                pl.lit(effective_max_token_distance, dtype=pl.Int64).alias("effective_max_token_distance"),
                pl.lit(distance_check_applied).alias("distance_check_applied"),
            ])
            .select([
                "paragraph_id",
                "condition_id",
                "category_text",
                "search_scope",
                "form_match_logic",
                "condition_forms",
                "required_form_count",
                "matched_form_count",
                "evaluated_unit_count",
                "matched_unit_count",
                "requested_max_token_distance",
                "effective_max_token_distance",
                "distance_check_applied",
                "distance_is_match",
                "is_match",
            ])
        )

    if not condition_eval_frames:
        return candidate_tokens_df, _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    condition_eval_df = pl.concat(condition_eval_frames, how="vertical")
    match_logic = condition_match_logic.strip().lower()
    selected_expr = pl.col("is_match").all() if match_logic == "all" else pl.col("is_match").any()
    paragraph_match_summary_df = (
        condition_eval_df
        .group_by("paragraph_id")
        .agg([
            pl.len().alias("condition_count"),
            pl.col("is_match").sum().alias("matched_condition_count"),
            selected_expr.alias("is_selected"),
            pl.col("condition_id").filter(pl.col("is_match")).alias("matched_condition_ids"),
        ])
        .sort("paragraph_id")
    )

    target_paragraph_ids = (
        paragraph_match_summary_df
        .filter(pl.col("is_selected"))
        .sort(["matched_condition_count", "paragraph_id"], descending=[True, False])
        .head(max_paragraph_ids)
        .sort("paragraph_id")
        .get_column("paragraph_id")
        .to_list()
    )
    target_sentence_ids = (
        tokens_df
        .filter(pl.col("paragraph_id").is_in(target_paragraph_ids))
        .select("sentence_id")
        .unique()
        .sort("sentence_id")
        .get_column("sentence_id")
        .to_list()
    )
    return (
        candidate_tokens_df,
        condition_eval_df,
        paragraph_match_summary_df,
        target_paragraph_ids,
        target_sentence_ids,
    )


def reconstruct_sentences_by_ids(tokens_df: pl.DataFrame, sentence_ids: list[int]) -> pl.DataFrame:
    if not sentence_ids:
        return pl.DataFrame(
            schema={
                "sentence_id": pl.Int64,
                "paragraph_id": pl.Int64,
                "token_count": pl.UInt32,
                "sentence_text": pl.String,
            }
        )

    return (
        tokens_df
        .filter(pl.col("sentence_id").is_in(sentence_ids))
        .group_by(["sentence_id", "paragraph_id"])
        .agg([
            pl.len().alias("token_count"),
            pl.col("surface").sort_by("token_no").list.join("").alias("sentence_text"),
        ])
        .sort("sentence_id")
    )


def reconstruct_paragraphs_by_ids(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int],
) -> pl.DataFrame:
    if not paragraph_ids:
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "sentence_count": pl.UInt32,
                "paragraph_text": pl.String,
            }
        )

    sentence_order_df = (
        sentences_df
        .filter(pl.col("paragraph_id").is_in(paragraph_ids))
        .select(["sentence_id", "paragraph_id", "sentence_no_in_paragraph"])
    )
    joined_df = (
        tokens_df
        .filter(pl.col("paragraph_id").is_in(paragraph_ids))
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
    )
    if joined_df.is_empty():
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "sentence_count": pl.UInt32,
                "paragraph_text": pl.String,
            }
        )

    sentence_text_df = (
        joined_df
        .group_by(["paragraph_id", "sentence_id", "sentence_no_in_paragraph"])
        .agg(pl.col("surface").sort_by("token_no").list.join("").alias("sentence_text"))
    )
    return (
        sentence_text_df
        .group_by("paragraph_id")
        .agg([
            pl.len().alias("sentence_count"),
            pl.col("sentence_text").sort_by("sentence_no_in_paragraph").list.join("").alias("paragraph_text"),
        ])
        .sort("paragraph_id")
    )


def enrich_reconstructed_paragraphs_df(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
) -> pl.DataFrame:
    paragraph_ids = (
        reconstructed_paragraphs_base_df.get_column("paragraph_id").to_list()
        if not reconstructed_paragraphs_base_df.is_empty()
        else []
    )
    paragraph_metadata_df = read_paragraph_document_metadata(
        db_path=db_path,
        paragraph_ids=paragraph_ids,
    )
    return (
        reconstructed_paragraphs_base_df
        .join(paragraph_metadata_df, on="paragraph_id", how="left")
        .with_columns(
            pl.when(pl.col("doc_type").fill_null("").str.contains("施行規則", literal=True))
            .then(pl.lit("施行規則"))
            .when(pl.col("doc_type").fill_null("").str.contains("条例", literal=True))
            .then(pl.lit("条例"))
            .otherwise(pl.lit("不明"))
            .alias("ordinance_or_rule")
        )
        .select([
            "paragraph_id",
            "document_id",
            "municipality_name",
            "ordinance_or_rule",
            "doc_type",
            "sentence_count",
            "paragraph_text",
            "paragraph_text_tagged",
            "paragraph_text_highlight_html",
            "matched_condition_ids",
            "matched_condition_ids_text",
            "matched_categories",
            "matched_categories_text",
            "match_group_ids",
            "match_group_count",
            "annotated_token_count",
        ])
    )


def build_reconstructed_paragraphs_export_df(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    return (
        reconstructed_paragraphs_df
        .with_columns(pl.col("match_group_ids").list.join(", ").alias("match_group_ids_text"))
        .select([
            "paragraph_id",
            "document_id",
            "municipality_name",
            "ordinance_or_rule",
            "doc_type",
            "sentence_count",
            "paragraph_text",
            "paragraph_text_tagged",
            "paragraph_text_highlight_html",
            "matched_condition_ids_text",
            "matched_categories_text",
            "match_group_ids_text",
            "match_group_count",
            "annotated_token_count",
        ])
    )
