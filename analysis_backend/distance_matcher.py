from __future__ import annotations

import heapq
from itertools import product

import polars as pl

from .condition_model import ConditionHitResult
from .condition_model import DistanceMatchingMode
from .condition_model import MatchingWarning
from .frame_schema import CONDITION_HIT_SCHEMA
from .frame_schema import empty_df


class DistanceMatchLimitExceededError(RuntimeError):
    pass


def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return empty_df(CONDITION_HIT_SCHEMA)


def _unique_in_order(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value and value not in seen_values:
            seen_values.add(value)
            unique_values.append(value)
    return unique_values


def _default_used_mode(requested_mode: DistanceMatchingMode) -> DistanceMatchingMode:
    return "approx" if requested_mode == "approx" else "strict"


def _normalize_distance_matching_mode(raw_mode: object) -> DistanceMatchingMode:
    normalized_mode = str(raw_mode).strip().lower()
    if normalized_mode in {"strict", "auto-approx", "approx"}:
        return normalized_mode
    return "auto-approx"


def _normalize_positive_int(raw_value: object, default_value: int) -> int:
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return default_value
    if parsed_value < 1:
        return default_value
    return parsed_value


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


def evaluate_distance_matches_by_unit(
    tokens_with_position_df: pl.DataFrame,
    forms: list[str],
    unit_column: str,
    position_column: str,
    max_token_distance: int,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return empty_df({"unit_id": pl.Int64, "distance_is_match": pl.Boolean})

    rows: list[dict[str, object]] = []
    form_tokens_df = (
        tokens_with_position_df
        .filter(pl.col("normalized_form").is_in(forms))
        .select([unit_column, "normalized_form", position_column])
    )
    if form_tokens_df.is_empty():
        return empty_df({"unit_id": pl.Int64, "distance_is_match": pl.Boolean})

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
        "is_table_paragraph": int(row.get("is_table_paragraph", 0) or 0),
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


def _build_hit_df(hit_rows: list[dict[str, object]]) -> pl.DataFrame:
    if not hit_rows:
        return _empty_condition_hit_tokens_df()

    return (
        pl.DataFrame(hit_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64),
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


def _build_distance_match_warning(
    *,
    condition_id: str,
    unit_id: int,
    combination_count: int,
    combination_cap: int,
) -> MatchingWarning:
    return MatchingWarning(
        code="distance_match_fallback",
        message="distance matching switched from strict to approx because combination cap was exceeded",
        condition_id=condition_id,
        unit_id=unit_id,
        requested_mode="auto-approx",
        used_mode="approx",
        combination_count=combination_count,
        combination_cap=combination_cap,
    )


def _raise_strict_safety_limit_exceeded(
    *,
    condition_id: str,
    unit_id: int,
    combination_count: int,
    safety_limit: int,
) -> None:
    raise DistanceMatchLimitExceededError(
        "distance_match_strict_limit_exceeded: "
        f"condition_id={condition_id} "
        f"unit_id={unit_id} "
        f"combination_count={combination_count} "
        f"safety_limit={safety_limit}"
    )


def _build_distance_condition_hits(
    *,
    tokens_with_position_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    forms: list[str],
    unit_column: str,
    position_column: str,
    max_token_distance: int,
    requested_mode: DistanceMatchingMode,
    combination_cap: int,
    strict_safety_limit: int,
) -> ConditionHitResult:
    if tokens_with_position_df.is_empty():
        return ConditionHitResult(
            condition_hit_tokens_df=_empty_condition_hit_tokens_df(),
            requested_mode=requested_mode,
            used_mode=_default_used_mode(requested_mode),
        )

    hit_rows: list[dict[str, object]] = []
    warning_messages: list[MatchingWarning] = []
    used_mode = _default_used_mode(requested_mode)

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
        total_combinations = 1
        for option_rows in form_row_options:
            total_combinations *= len(option_rows)

        selected_mode: DistanceMatchingMode
        if requested_mode == "strict":
            if total_combinations > strict_safety_limit:
                _raise_strict_safety_limit_exceeded(
                    condition_id=condition_id,
                    unit_id=unit_id,
                    combination_count=total_combinations,
                    safety_limit=strict_safety_limit,
                )
            selected_mode = "strict"
        elif requested_mode == "approx":
            selected_mode = "approx"
        else:
            if total_combinations > combination_cap:
                selected_mode = "approx"
                warning_messages.append(
                    _build_distance_match_warning(
                        condition_id=condition_id,
                        unit_id=unit_id,
                        combination_count=total_combinations,
                        combination_cap=combination_cap,
                    )
                )
            else:
                selected_mode = "strict"

        if selected_mode == "approx":
            used_mode = "approx"
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

        for group_index, candidate_group in enumerate(candidate_groups, start=1):
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

    return ConditionHitResult(
        condition_hit_tokens_df=_build_hit_df(hit_rows),
        requested_mode=requested_mode,
        used_mode=used_mode,
        warning_messages=warning_messages,
    )


def _build_token_hits_by_unit(
    *,
    tokens_with_position_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    forms: list[str],
    unit_column: str,
    position_column: str,
    form_match_logic: str,
    requested_mode: DistanceMatchingMode,
) -> ConditionHitResult:
    if tokens_with_position_df.is_empty():
        return ConditionHitResult(
            condition_hit_tokens_df=_empty_condition_hit_tokens_df(),
            requested_mode=requested_mode,
            used_mode=_default_used_mode(requested_mode),
        )

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

    return ConditionHitResult(
        condition_hit_tokens_df=_build_hit_df(hit_rows),
        requested_mode=requested_mode,
        used_mode=_default_used_mode(requested_mode),
    )


def _dedupe_hit_rows(hit_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped_rows: list[dict[str, object]] = []
    seen_keys: set[tuple[object, ...]] = set()
    for hit_row in hit_rows:
        row_key = (
            int(hit_row["paragraph_id"]),
            int(hit_row["sentence_id"]),
            int(hit_row["token_no"]),
            str(hit_row["condition_id"]),
            str(hit_row["match_group_id"]),
            str(hit_row["match_role"]),
        )
        if row_key in seen_keys:
            continue
        seen_keys.add(row_key)
        deduped_rows.append(hit_row)
    return deduped_rows


def _candidate_sort_key(candidate: dict[str, object]) -> tuple[int, int, int, int]:
    hit_rows = list(candidate.get("hit_rows", []))
    if not hit_rows:
        return (1, 10**9, 10**9, int(candidate["unit_id"]))
    return (
        0,
        int(candidate["start_position"]),
        int(candidate["end_position"]),
        int(candidate["unit_id"]),
    )


def _choose_better_candidate(
    left_candidate: dict[str, object] | None,
    right_candidate: dict[str, object] | None,
) -> dict[str, object] | None:
    if left_candidate is None:
        return right_candidate
    if right_candidate is None:
        return left_candidate
    return min([left_candidate, right_candidate], key=_candidate_sort_key)


def _build_candidate(
    *,
    paragraph_id: int,
    unit_scope: str,
    unit_id: int,
    hit_rows: list[dict[str, object]],
    position_column: str,
) -> dict[str, object]:
    deduped_rows = _dedupe_hit_rows(hit_rows)
    if deduped_rows:
        positions = [int(row[position_column]) for row in deduped_rows]
        start_position = min(positions)
        end_position = max(positions)
    else:
        start_position = 10**9
        end_position = 10**9
    return {
        "paragraph_id": paragraph_id,
        "unit_scope": unit_scope,
        "unit_id": unit_id,
        "hit_rows": deduped_rows,
        "start_position": start_position,
        "end_position": end_position,
    }


def _combine_candidate_hit_rows(
    left_rows: list[dict[str, object]],
    right_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return _dedupe_hit_rows([*left_rows, *right_rows])


def _build_positive_group_candidates(
    *,
    unit_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    form_group: dict[str, object],
    group_index: int,
    unit_column: str,
    position_column: str,
) -> list[dict[str, object]]:
    paragraph_id = int(unit_df.get_column("paragraph_id")[0])
    unit_id = int(unit_df.get_column(unit_column)[0])
    unit_scope = "sentence" if unit_column == "sentence_id" else "paragraph"
    forms = [str(value) for value in form_group.get("forms", [])]
    match_logic = str(form_group.get("match_logic", "and")).strip().lower()
    exclude_forms_any = [str(value) for value in form_group.get("exclude_forms_any", [])]
    effective_max_token_distance = form_group.get("effective_max_token_distance")
    anchor_form = (
        str(form_group.get("anchor_form", "")).strip()
        if form_group.get("anchor_form") is not None
        else ""
    )
    if match_logic == "not":
        relevant_rows = [
            dict(row)
            for row in unit_df
            .filter(pl.col("normalized_form").is_in(forms))
            .sort(position_column)
            .iter_rows(named=True)
        ]
        if relevant_rows:
            return []
        return [
            _build_candidate(
                paragraph_id=paragraph_id,
                unit_scope=unit_scope,
                unit_id=unit_id,
                hit_rows=[],
                position_column=position_column,
            )
        ]

    target_forms = list(dict.fromkeys(forms + exclude_forms_any))
    relevant_rows = [
        dict(row)
        for row in unit_df
        .filter(pl.col("normalized_form").is_in(target_forms))
        .sort(position_column)
        .iter_rows(named=True)
    ]
    positions_by_form: dict[str, list[dict[str, object]]] = {}
    for form in target_forms:
        positions_by_form[form] = [
            row for row in relevant_rows if str(row["normalized_form"]) == form
        ]

    match_group_id = f"{condition_id}:g{group_index + 1}:{unit_scope}:{unit_id}"

    def build_group_hit_rows(selected_rows: list[dict[str, object]]) -> list[dict[str, object]]:
        return [
            _build_condition_hit_row(
                row=row,
                condition_id=condition_id,
                categories=categories,
                match_group_id=match_group_id,
                match_role=str(row["normalized_form"]),
            )
            for row in selected_rows
        ]

<<<<<<< HEAD
=======
    if match_logic == "not":
        matched_form_count = sum(1 for form in forms if positions_by_form.get(form))
        if matched_form_count == 0:
            return [
                _build_candidate(
                    paragraph_id=paragraph_id,
                    unit_scope=unit_scope,
                    unit_id=unit_id,
                    hit_rows=[],
                    position_column=position_column,
                )
            ]
        return []

    if not relevant_rows:
        return []

>>>>>>> 8160fac91fa818d5c84b858c052e6aa59c21ff5c
    if effective_max_token_distance is not None:
        max_token_distance = int(effective_max_token_distance)
        if match_logic == "and":
            anchor_rows = positions_by_form.get(anchor_form, [])
            for anchor_row in anchor_rows:
                anchor_position = int(anchor_row[position_column])
                window_start = max(0, anchor_position - max_token_distance)
                window_end = anchor_position + max_token_distance
                selected_rows = [anchor_row]
                is_valid = True
                for form in forms:
                    if form == anchor_form:
                        continue
                    matching_rows = [
                        row
                        for row in positions_by_form.get(form, [])
                        if window_start <= int(row[position_column]) <= window_end
                    ]
                    if not matching_rows:
                        is_valid = False
                        break
                    selected_rows.append(matching_rows[0])
                if not is_valid:
                    continue
                if exclude_forms_any:
                    has_excluded = any(
                        window_start <= int(excluded_row[position_column]) <= window_end
                        for exclude_form in exclude_forms_any
                        for excluded_row in positions_by_form.get(exclude_form, [])
                    )
                    if has_excluded:
                        continue
                window_rows = [
                    row
                    for row in relevant_rows
                    if str(row["normalized_form"]) in forms
                    and window_start <= int(row[position_column]) <= window_end
                ]
                return [
                    _build_candidate(
                        paragraph_id=paragraph_id,
                        unit_scope=unit_scope,
                        unit_id=unit_id,
                        hit_rows=build_group_hit_rows(window_rows or selected_rows),
                        position_column=position_column,
                    )
                ]
            return []

        for form in forms:
            for anchor_row in positions_by_form.get(form, []):
                anchor_position = int(anchor_row[position_column])
                window_start = max(0, anchor_position - max_token_distance)
                window_end = anchor_position + max_token_distance
                if exclude_forms_any:
                    has_excluded = any(
                        window_start <= int(excluded_row[position_column]) <= window_end
                        for exclude_form in exclude_forms_any
                        for excluded_row in positions_by_form.get(exclude_form, [])
                    )
                    if has_excluded:
                        continue
                window_rows = [
                    row
                    for row in relevant_rows
                    if str(row["normalized_form"]) in forms
                    and window_start <= int(row[position_column]) <= window_end
                ]
                if not window_rows:
                    continue
                return [
                    _build_candidate(
                        paragraph_id=paragraph_id,
                        unit_scope=unit_scope,
                        unit_id=unit_id,
                        hit_rows=build_group_hit_rows(window_rows),
                        position_column=position_column,
                    )
                ]
        return []

    positive_rows = [row for row in relevant_rows if str(row["normalized_form"]) in forms]
    matched_forms = _unique_in_order([str(row["normalized_form"]) for row in positive_rows])
    if match_logic == "and" and len(matched_forms) < len(forms):
        return []
    if match_logic == "or" and not matched_forms:
        return []
    if exclude_forms_any:
        excluded_rows = [
            row
            for exclude_form in exclude_forms_any
            for row in positions_by_form.get(exclude_form, [])
        ]
        if excluded_rows:
            return []
    return [
        _build_candidate(
            paragraph_id=paragraph_id,
            unit_scope=unit_scope,
            unit_id=unit_id,
            hit_rows=build_group_hit_rows(positive_rows),
            position_column=position_column,
        )
    ]


def _build_group_candidate_map(
    *,
    paragraph_df: pl.DataFrame,
    condition_id: str,
    categories: list[str],
    form_group: dict[str, object],
    group_index: int,
) -> tuple[dict[tuple[int, int], dict[str, object]], str]:
    search_scope = str(form_group.get("search_scope", "paragraph")).strip().lower()
    unit_column = "sentence_id" if search_scope == "sentence" else "paragraph_id"
    candidate_map: dict[tuple[int, int], dict[str, object]] = {}
    for unit_df in paragraph_df.partition_by(unit_column, maintain_order=True):
        candidates = _build_positive_group_candidates(
            unit_df=unit_df,
            condition_id=condition_id,
            categories=categories,
            form_group=form_group,
            group_index=group_index,
            unit_column=unit_column,
            position_column=(
                "sentence_token_position" if search_scope == "sentence" else "paragraph_token_position"
            ),
        )
        for candidate in candidates:
            candidate_map[(candidate["paragraph_id"], candidate["unit_id"])] = candidate
    return candidate_map, search_scope


def _promote_candidate_map_to_paragraph(
    candidate_map: dict[tuple[int, int], dict[str, object]],
) -> dict[tuple[int, int], dict[str, object]]:
    promoted_map: dict[tuple[int, int], dict[str, object]] = {}
    for candidate in candidate_map.values():
        paragraph_key = (int(candidate["paragraph_id"]), int(candidate["paragraph_id"]))
        paragraph_candidate = {
            "paragraph_id": int(candidate["paragraph_id"]),
            "unit_scope": "paragraph",
            "unit_id": int(candidate["paragraph_id"]),
            "hit_rows": list(candidate["hit_rows"]),
            "start_position": int(candidate["start_position"]),
            "end_position": int(candidate["end_position"]),
        }
        promoted_map[paragraph_key] = _choose_better_candidate(
            promoted_map.get(paragraph_key),
            paragraph_candidate,
        )
    return promoted_map


def _combine_candidate_maps(
    left_map: dict[tuple[int, int], dict[str, object]],
    left_scope: str,
    right_map: dict[tuple[int, int], dict[str, object]],
    right_scope: str,
    combine_logic: str,
) -> tuple[dict[tuple[int, int], dict[str, object]], str]:
    effective_left_map = left_map
    effective_right_map = right_map
    result_scope = left_scope
    if left_scope != right_scope:
        effective_left_map = _promote_candidate_map_to_paragraph(left_map)
        effective_right_map = _promote_candidate_map_to_paragraph(right_map)
        result_scope = "paragraph"

    result_map: dict[tuple[int, int], dict[str, object]] = {}
    all_keys = set(effective_left_map) | set(effective_right_map)
    for key in all_keys:
        left_candidate = effective_left_map.get(key)
        right_candidate = effective_right_map.get(key)
        if combine_logic == "and":
            if left_candidate is None or right_candidate is None:
                continue
            result_map[key] = _build_candidate(
                paragraph_id=int(left_candidate["paragraph_id"]),
                unit_scope=result_scope,
                unit_id=int(left_candidate["unit_id"]),
                hit_rows=_combine_candidate_hit_rows(
                    list(left_candidate["hit_rows"]),
                    list(right_candidate["hit_rows"]),
                ),
                position_column="paragraph_token_position",
            )
        else:
            chosen_candidate = _choose_better_candidate(left_candidate, right_candidate)
            if chosen_candidate is not None:
                result_map[key] = chosen_candidate
    return result_map, result_scope


def _build_advanced_condition_hit_result(
    *,
    paragraph_df: pl.DataFrame,
    condition: dict[str, object],
    requested_mode: DistanceMatchingMode,
) -> ConditionHitResult:
    condition_id = str(condition["condition_id"])
    categories = list(condition["categories"])
    form_groups = [
        form_group
        for form_group in condition.get("form_groups", [])
        if isinstance(form_group, dict)
    ]
    if not form_groups:
        return ConditionHitResult(
            condition_hit_tokens_df=_empty_condition_hit_tokens_df(),
            requested_mode=requested_mode,
            used_mode=_default_used_mode(requested_mode),
        )

    current_candidate_map, current_scope = _build_group_candidate_map(
        paragraph_df=paragraph_df,
        condition_id=condition_id,
        categories=categories,
        form_group=form_groups[0],
        group_index=0,
    )
    for group_index, form_group in enumerate(form_groups[1:], start=1):
        next_candidate_map, next_scope = _build_group_candidate_map(
            paragraph_df=paragraph_df,
            condition_id=condition_id,
            categories=categories,
            form_group=form_group,
            group_index=group_index,
        )
        current_candidate_map, current_scope = _combine_candidate_maps(
            left_map=current_candidate_map,
            left_scope=current_scope,
            right_map=next_candidate_map,
            right_scope=next_scope,
            combine_logic=str(form_group.get("combine_logic", "and")).strip().lower() or "and",
        )

    hit_rows: list[dict[str, object]] = []
    for candidate in current_candidate_map.values():
        hit_rows.extend(list(candidate["hit_rows"]))

    return ConditionHitResult(
        condition_hit_tokens_df=_build_hit_df(_dedupe_hit_rows(hit_rows)),
        requested_mode=requested_mode,
        used_mode=_default_used_mode(requested_mode),
    )


def build_condition_hit_result(
    *,
    tokens_with_position_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    distance_matching_mode: DistanceMatchingMode = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> ConditionHitResult:
    normalized_distance_matching_mode = _normalize_distance_matching_mode(distance_matching_mode)
    normalized_distance_match_combination_cap = _normalize_positive_int(
        distance_match_combination_cap,
        10000,
    )
    normalized_distance_match_strict_safety_limit = _normalize_positive_int(
        distance_match_strict_safety_limit,
        1000000,
    )
    if tokens_with_position_df.is_empty():
        return ConditionHitResult(
            condition_hit_tokens_df=_empty_condition_hit_tokens_df(),
            requested_mode=normalized_distance_matching_mode,
            used_mode=_default_used_mode(normalized_distance_matching_mode),
        )

    if not cooccurrence_conditions:
        return ConditionHitResult(
            condition_hit_tokens_df=_empty_condition_hit_tokens_df(),
            requested_mode=normalized_distance_matching_mode,
            used_mode=_default_used_mode(normalized_distance_matching_mode),
        )

    condition_hit_frames: list[pl.DataFrame] = []
    warning_messages: list[MatchingWarning] = []
    used_mode = _default_used_mode(normalized_distance_matching_mode)

    for condition in cooccurrence_conditions:
        form_groups = condition.get("form_groups")
        if isinstance(form_groups, list) and (
            len(form_groups) >= 2
            or any(
                str(form_group.get("match_logic", "")).strip().lower() == "not"
                or bool(
                    str(form_group.get("anchor_form", "")).strip()
                    if form_group.get("anchor_form") is not None
                    else ""
                )
                or bool(form_group.get("exclude_forms_any"))
                or (
                    str(form_group.get("match_logic", "")).strip().lower() == "or"
                    and form_group.get("effective_max_token_distance") is not None
                )
                for form_group in form_groups
                if isinstance(form_group, dict)
            )
        ):
            advanced_hit_frames: list[pl.DataFrame] = []
            for paragraph_df in tokens_with_position_df.partition_by("paragraph_id", maintain_order=True):
                advanced_result = _build_advanced_condition_hit_result(
                    paragraph_df=paragraph_df,
                    condition=condition,
                    requested_mode=normalized_distance_matching_mode,
                )
                if advanced_result.used_mode == "approx":
                    used_mode = "approx"
                warning_messages.extend(advanced_result.warning_messages)
                if not advanced_result.condition_hit_tokens_df.is_empty():
                    advanced_hit_frames.append(advanced_result.condition_hit_tokens_df)
            if advanced_hit_frames:
                condition_hit_frames.append(pl.concat(advanced_hit_frames, how="vertical"))
            continue
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
            hit_result = _build_distance_condition_hits(
                tokens_with_position_df=relevant_tokens_df,
                condition_id=condition_id,
                categories=categories,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                max_token_distance=int(effective_max_token_distance),
                requested_mode=normalized_distance_matching_mode,
                combination_cap=normalized_distance_match_combination_cap,
                strict_safety_limit=normalized_distance_match_strict_safety_limit,
            )
        else:
            hit_result = _build_token_hits_by_unit(
                tokens_with_position_df=relevant_tokens_df,
                condition_id=condition_id,
                categories=categories,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                form_match_logic=form_match_logic,
                requested_mode=normalized_distance_matching_mode,
            )

        if hit_result.used_mode == "approx":
            used_mode = "approx"
        warning_messages.extend(hit_result.warning_messages)
        if not hit_result.condition_hit_tokens_df.is_empty():
            condition_hit_frames.append(hit_result.condition_hit_tokens_df)

    condition_hit_tokens_df = (
        pl.concat(condition_hit_frames, how="vertical")
        if condition_hit_frames
        else _empty_condition_hit_tokens_df()
    )
    return ConditionHitResult(
        condition_hit_tokens_df=condition_hit_tokens_df,
        requested_mode=normalized_distance_matching_mode,
        used_mode=used_mode,
        warning_messages=warning_messages,
    )
