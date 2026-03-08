import marimo

__generated_with = "0.20.1"
app = marimo.App(width="medium", app_title="analysis_tokens reader")


@app.cell
def _():
    from html import escape
    import marimo as mo
    import polars as pl
    from pathlib import Path

    return Path, escape, mo, pl


@app.cell
def _(mo):
    mo.md("""
    # analysis_tokens Reader

    `data/ordinance_analysis.db` の `analysis_tokens` テーブルを
    Polarsで読み込むためのmarimoノートブックです。
    `normalized_form` の段落内共起・距離条件で絞り込み、
    指定IDのみ文・段落を再構成できます。
    """)
    return


@app.cell
def _(Path):
    db_path = Path(__file__).resolve().parent.parent / "data" / "ordinance_analysis.db"
    table_name = "analysis_tokens"
    sentences_table_name = "analysis_sentences"

    # 全件読み込みしたい場合は None に変更
    limit_rows = None
    return db_path, limit_rows, sentences_table_name, table_name


@app.cell
def _(Path):
    filter_config_path = (
        Path(__file__).resolve().parent.parent / "asset" / "cooccurrence-conditions.json"
    )
    return (filter_config_path,)


@app.cell
def _(filter_config_path, mo):
    from analysis_backend.analysis_core import load_filter_config

    try:
        filter_config = load_filter_config(filter_config_path)
    except (FileNotFoundError, ValueError) as exc:
        mo.stop(True, str(exc))

    return (
        filter_config.condition_match_logic,
        filter_config.cooccurrence_conditions,
        filter_config_path,
        filter_config.loaded_condition_count,
        filter_config.max_reconstructed_paragraphs,
    )


@app.function(hide_code=True)
def build_condition_filter_entries(
    raw_conditions: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[str]]:
    """Normalize raw config entries for the category and condition selectors."""
    entries: list[dict[str, object]] = []
    category_options: list[str] = []
    seen_condition_ids: set[str] = set()
    seen_categories: set[str] = set()

    for idx, raw_condition in enumerate(raw_conditions, start=1):
        if not isinstance(raw_condition, dict):
            continue

        raw_condition_id_val = raw_condition.get("condition_id")
        raw_condition_id = str(raw_condition_id_val).strip() if raw_condition_id_val is not None else ""
        base_condition_id = raw_condition_id or f"condition_{idx}"
        condition_id = base_condition_id
        suffix = 2
        while condition_id in seen_condition_ids:
            condition_id = f"{base_condition_id}_{suffix}"
            suffix += 1
        seen_condition_ids.add(condition_id)

        raw_categories = raw_condition.get("categories")
        raw_category_values = raw_categories if isinstance(raw_categories, list) else [raw_categories]
        categories: list[str] = []
        for raw_category in raw_category_values:
            category = str(raw_category).strip() if raw_category is not None else ""
            if category and category not in categories:
                categories.append(category)
        if not categories:
            categories = ["未分類"]

        normalized_condition = dict(raw_condition)
        normalized_condition["condition_id"] = condition_id
        normalized_condition["categories"] = categories
        entries.append(
            {
                "condition_id": condition_id,
                "categories": categories,
                "condition": normalized_condition,
            }
        )

        for category in categories:
            if category not in seen_categories:
                seen_categories.add(category)
                category_options.append(category)

    return entries, category_options


@app.cell(hide_code=True)
def _(cooccurrence_conditions):
    condition_filter_entries, category_options = build_condition_filter_entries(
        raw_conditions=cooccurrence_conditions
    )
    return category_options, condition_filter_entries


@app.cell
def _(category_options, mo):
    category_selector = mo.ui.multiselect(
        options=category_options,
        value=category_options,
        label="適用カテゴリ",
    )
    category_selector
    return (category_selector,)


@app.cell(hide_code=True)
def _(category_selector, condition_filter_entries):
    selected_categories = list(category_selector.value)
    available_condition_entries = [
        entry
        for entry in condition_filter_entries
        if any(category in selected_categories for category in entry["categories"])
    ]
    available_condition_ids = [
        str(entry["condition_id"])
        for entry in available_condition_entries
    ]
    return available_condition_ids, selected_categories


@app.cell
def _(available_condition_ids, mo):
    condition_selector = mo.ui.multiselect(
        options=available_condition_ids,
        value=available_condition_ids,
        label="適用 condition_id",
    )
    condition_selector
    return (condition_selector,)


@app.cell(hide_code=True)
def _(condition_filter_entries, condition_selector):
    selected_condition_ids = list(condition_selector.value)
    selected_condition_id_set = set(selected_condition_ids)
    filtered_cooccurrence_conditions = [
        entry["condition"]
        for entry in condition_filter_entries
        if entry["condition_id"] in selected_condition_id_set
    ]
    filtered_condition_count = len(filtered_cooccurrence_conditions)
    return (
        filtered_condition_count,
        filtered_cooccurrence_conditions,
        selected_condition_ids,
    )


@app.cell
def _(db_path, mo, sentences_table_name, table_name):
    exists = db_path.exists()
    mo.md(
        f"""
        - dbPath: `{db_path}`
        - tokensTable: `{table_name}`
        - sentencesTable: `{sentences_table_name}`
        - exists: `{exists}`
        """
    )
    return (exists,)


@app.cell(hide_code=True)
def _():
    from analysis_backend.analysis_core import (
        build_condition_hit_tokens_df,
        build_reconstructed_paragraphs_export_df,
        build_rendered_paragraphs_df,
        build_token_annotations_df,
        build_tokens_with_position_df,
        enrich_reconstructed_paragraphs_df,
        read_analysis_sentences,
        read_analysis_tokens,
        read_paragraph_document_metadata,
        reconstruct_paragraphs_by_ids,
        reconstruct_sentences_by_ids,
        select_target_ids_by_cooccurrence_conditions,
    )

    return (
        build_condition_hit_tokens_df,
        build_reconstructed_paragraphs_export_df,
        build_rendered_paragraphs_df,
        build_token_annotations_df,
        build_tokens_with_position_df,
        enrich_reconstructed_paragraphs_df,
        read_analysis_sentences,
        read_analysis_tokens,
        read_paragraph_document_metadata,
        reconstruct_paragraphs_by_ids,
        reconstruct_sentences_by_ids,
        select_target_ids_by_cooccurrence_conditions,
    )


@app.cell
def _(
    db_path,
    exists,
    limit_rows,
    mo,
    read_analysis_sentences,
    read_analysis_tokens,
):
    if not exists:
        mo.stop(True, f"DB file not found: {db_path}")

    analysis_tokens_df = read_analysis_tokens(db_path=db_path, limit_rows=limit_rows)
    analysis_sentences_df = read_analysis_sentences(db_path=db_path, limit_rows=None)
    return analysis_sentences_df, analysis_tokens_df


@app.cell
def _(analysis_sentences_df, analysis_tokens_df, limit_rows, mo):
    mo.md(f"""
    読み込み完了:
    - tokensRows: `{analysis_tokens_df.height}`
    - tokensCols: `{analysis_tokens_df.width}`
    - sentencesRows: `{analysis_sentences_df.height}`
    - limitRows: `{limit_rows}`
    """)
    return


@app.cell
def _(analysis_tokens_df):
    analysis_tokens_df.head(30)
    return


@app.cell
def _(
    analysis_sentences_df,
    analysis_tokens_df,
    condition_match_logic,
    filtered_cooccurrence_conditions,
    max_reconstructed_paragraphs,
    pl,
    select_target_ids_by_cooccurrence_conditions,
):
    (
        candidate_tokens_df,
        condition_eval_df,
        paragraph_match_summary_df,
        target_paragraph_ids,
        target_sentence_ids,
    ) = select_target_ids_by_cooccurrence_conditions(
        tokens_df=analysis_tokens_df,
        sentences_df=analysis_sentences_df,
        cooccurrence_conditions=filtered_cooccurrence_conditions,
        condition_match_logic=condition_match_logic,
        max_paragraph_ids=max_reconstructed_paragraphs,
    )
    # candidate_tokens_df はマッチ対象 form のトークンのみ含む（全トークンではない）
    selected_candidate_tokens_df = candidate_tokens_df.filter(pl.col("paragraph_id").is_in(target_paragraph_ids))
    return (
        condition_eval_df,
        paragraph_match_summary_df,
        selected_candidate_tokens_df,
        target_paragraph_ids,
        target_sentence_ids,
    )


@app.cell
def _(
    analysis_sentences_df,
    analysis_tokens_df,
    build_tokens_with_position_df,
    build_condition_hit_tokens_df,
    build_token_annotations_df,
    filtered_cooccurrence_conditions,
    target_paragraph_ids,
):
    selected_tokens_with_position_df = build_tokens_with_position_df(
        tokens_df=analysis_tokens_df,
        sentences_df=analysis_sentences_df,
        paragraph_ids=target_paragraph_ids,
        target_forms=None,
    )
    condition_hit_tokens_df = build_condition_hit_tokens_df(
        tokens_with_position_df=selected_tokens_with_position_df,
        cooccurrence_conditions=filtered_cooccurrence_conditions,
    )
    token_annotations_df = build_token_annotations_df(
        condition_hit_tokens_df=condition_hit_tokens_df
    )
    return (
        condition_hit_tokens_df,
        selected_tokens_with_position_df,
        token_annotations_df,
    )


@app.cell
def _(
    available_condition_ids,
    category_options,
    condition_hit_tokens_df,
    condition_eval_df,
    condition_filter_entries,
    condition_match_logic,
    cooccurrence_conditions,
    filter_config_path,
    filtered_condition_count,
    loaded_condition_count,
    max_reconstructed_paragraphs,
    mo,
    paragraph_match_summary_df,
    selected_candidate_tokens_df,
    selected_categories,
    selected_condition_ids,
    token_annotations_df,
    target_paragraph_ids,
    target_sentence_ids,
):
    mo.md(f"""
    フィルター設定:
    - filterConfigPath: `{filter_config_path}`
    - loadedConditionCount: `{loaded_condition_count}`
    - normalizedConditionCount: `{len(condition_filter_entries)}`
    - categoryOptions: `{category_options}`
    - selectedCategories: `{selected_categories}`
    - availableConditionIds: `{available_condition_ids}`
    - selectedConditionIds: `{selected_condition_ids}`
    - filteredConditionCount: `{filtered_condition_count}`
    - cooccurrenceConditions: `{cooccurrence_conditions}`
    - conditionMatchLogic: `{condition_match_logic}`
    - maxReconstructedParagraphs: `{max_reconstructed_paragraphs}`

    抽出結果:
    - conditionEvalRows: `{condition_eval_df.height}`
    - evaluatedParagraphs: `{paragraph_match_summary_df.height}`
    - matchedCandidateTokenRowsInSelectedParagraphs: `{selected_candidate_tokens_df.height}`
    - conditionHitTokenRows: `{condition_hit_tokens_df.height}`
    - annotatedTokenRows: `{token_annotations_df.height}`
    - sentenceIds: `{len(target_sentence_ids)}`
    - paragraphIds: `{len(target_paragraph_ids)}`
    """)
    return


@app.cell
def _(condition_eval_df, pl):
    condition_eval_summary_df = (
        condition_eval_df
        .group_by("condition_id")
        .agg([
            pl.first("category_text").alias("category_text"),
            pl.first("search_scope").alias("search_scope"),
            pl.first("form_match_logic").alias("form_match_logic"),
            pl.first("condition_forms").alias("condition_forms"),
            pl.first("requested_max_token_distance").alias("requested_max_token_distance"),
            pl.first("effective_max_token_distance").alias("effective_max_token_distance"),
            pl.first("distance_check_applied").alias("distance_check_applied"),
            pl.col("evaluated_unit_count").sum().alias("evaluated_units"),
            pl.col("matched_unit_count").sum().alias("matched_units"),
            pl.col("is_match").sum().alias("matched_paragraphs"),
        ])
        .sort("condition_id")
    )
    return (condition_eval_summary_df,)


@app.cell
def _(condition_eval_summary_df):
    condition_eval_summary_df
    return


@app.cell
def _(paragraph_match_summary_df):
    paragraph_match_summary_df.head(30)
    return


@app.cell
def _(condition_hit_tokens_df):
    condition_hit_tokens_df.head(50)
    return


@app.cell
def _(token_annotations_df):
    token_annotations_df.head(50)
    return


@app.cell
def _(analysis_tokens_df, reconstruct_sentences_by_ids, target_sentence_ids):
    reconstructed_sentences_df = reconstruct_sentences_by_ids(
        tokens_df=analysis_tokens_df,
        sentence_ids=target_sentence_ids,
    )
    return (reconstructed_sentences_df,)


@app.cell
def _(reconstructed_sentences_df):
    reconstructed_sentences_df.head(30)
    return


@app.cell
def _(
    build_rendered_paragraphs_df,
    selected_tokens_with_position_df,
    token_annotations_df,
):
    reconstructed_paragraphs_base_df = build_rendered_paragraphs_df(
        tokens_with_position_df=selected_tokens_with_position_df,
        token_annotations_df=token_annotations_df,
    )
    return (reconstructed_paragraphs_base_df,)


@app.cell
def _(
    db_path,
    enrich_reconstructed_paragraphs_df,
    reconstructed_paragraphs_base_df,
):
    reconstructed_paragraphs_df = enrich_reconstructed_paragraphs_df(
        db_path=db_path,
        reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
    )
    return (reconstructed_paragraphs_df,)


@app.cell
def _(build_reconstructed_paragraphs_export_df, reconstructed_paragraphs_df):
    reconstructed_paragraphs_export_df = build_reconstructed_paragraphs_export_df(
        reconstructed_paragraphs_df=reconstructed_paragraphs_df
    )
    reconstructed_paragraphs_export_df.write_csv("抑制区域_段落_1.0.csv")
    reconstructed_paragraphs_export_df.head(100)
    return (reconstructed_paragraphs_export_df,)


@app.cell
def _(reconstructed_paragraphs_df):
    reconstructed_paragraphs_df.head(30)
    return


@app.cell
def _(escape, mo, reconstructed_paragraphs_df):
    preview_limit = 20
    preview_rows = reconstructed_paragraphs_df.head(preview_limit).iter_rows(named=True)
    preview_sections: list[str] = []
    for preview_row in preview_rows:
        municipality_name = escape(str(preview_row["municipality_name"] or ""))
        ordinance_or_rule = escape(str(preview_row["ordinance_or_rule"] or ""))
        doc_type = escape(str(preview_row["doc_type"] or ""))
        paragraph_id = int(preview_row["paragraph_id"])
        matched_categories_text = escape(str(preview_row["matched_categories_text"] or ""))
        matched_condition_ids_text = escape(str(preview_row["matched_condition_ids_text"] or ""))
        highlight_html = str(preview_row["paragraph_text_highlight_html"] or "")
        preview_sections.append(
            f"""
            <section class="co-preview-card">
              <div class="co-preview-meta">
                <strong>{municipality_name}</strong>
                <span>{ordinance_or_rule}</span>
                <span>{doc_type}</span>
                <span>paragraph_id={paragraph_id}</span>
              </div>
              <div class="co-preview-tags">
                <span>categories: {matched_categories_text}</span>
                <span>conditions: {matched_condition_ids_text}</span>
              </div>
              <div class="co-preview-body">{highlight_html}</div>
            </section>
            """
        )

    mo.Html(
        """
        <style>
          .co-preview-list {
            display: grid;
            gap: 12px;
          }
          .co-preview-card {
            border: 1px solid #d7d7d7;
            border-radius: 10px;
            padding: 12px 14px;
            background: #fffdfa;
          }
          .co-preview-meta,
          .co-preview-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 8px 12px;
            font-size: 0.9rem;
            margin-bottom: 8px;
          }
          .co-preview-tags {
            color: #6a4a00;
          }
          .co-preview-body {
            line-height: 1.8;
            font-size: 0.98rem;
          }
          .co-preview-body mark.co-hit {
            background: #ffe08a;
            padding: 0 0.05em;
            border-radius: 0.2em;
          }
        </style>
        <div class="co-preview-list">
        """
        + "".join(preview_sections)
        + "</div>"
    )
    return


@app.cell
def _(pl, reconstructed_paragraphs_df):
    target_column = "document_id"
    search_text = 630

    # 完全一致
    # analysis_tokens_df.filter(pl.col(target_column) == search_text).head(5)

    reconstructed_paragraphs_df.filter(pl.col(target_column) == search_text).head(5)
    return


if __name__ == "__main__":
    app.run()
