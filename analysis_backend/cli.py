from __future__ import annotations

from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import polars as pl


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="run-analysis",
        description="Run the ordinance token analysis pipeline and export CSV/metadata JSON.",
    )
    parser.add_argument("--job-id", required=True, help="Stable identifier for this analysis run.")
    parser.add_argument("--db-path", required=True, help="SQLite database path.")
    parser.add_argument("--filter-config-path", required=True, help="Cooccurrence condition JSON path.")
    parser.add_argument("--output-dir", required=True, help="Directory for this job's outputs.")
    parser.add_argument(
        "--output-csv-path",
        help="Explicit output CSV path. Defaults to <output-dir>/result.csv.",
    )
    parser.add_argument(
        "--output-meta-json-path",
        help="Explicit metadata JSON path. Defaults to <output-dir>/meta.json.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=None,
        help="Optional LIMIT for analysis_tokens reads.",
    )
    return parser


def _resolve_output_paths(args: Namespace) -> tuple[Path, Path, Path]:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_csv_path = (
        Path(args.output_csv_path).expanduser().resolve()
        if args.output_csv_path
        else output_dir / "result.csv"
    )
    output_meta_json_path = (
        Path(args.output_meta_json_path).expanduser().resolve()
        if args.output_meta_json_path
        else output_dir / "meta.json"
    )
    return output_dir, output_csv_path, output_meta_json_path


def _write_meta_json(output_meta_json_path: Path, payload: dict[str, object]) -> None:
    output_meta_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_meta_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_failure_payload(
    *,
    job_id: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    db_path: Path,
    filter_config_path: Path,
    output_csv_path: Path,
    warning_messages: list[dict[str, object]],
    error_summary: str,
) -> dict[str, object]:
    return {
        "jobId": job_id,
        "status": "failed",
        "startedAt": started_at,
        "finishedAt": finished_at,
        "durationSeconds": duration_seconds,
        "dbPath": str(db_path),
        "filterConfigPath": str(filter_config_path),
        "outputCsvPath": str(output_csv_path),
        "targetParagraphCount": 0,
        "selectedParagraphCount": 0,
        "warningMessages": warning_messages,
        "errorSummary": error_summary,
    }


def _build_success_payload(
    *,
    job_id: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    db_path: Path,
    filter_config_path: Path,
    output_csv_path: Path,
    target_paragraph_count: int,
    selected_paragraph_count: int,
    warning_messages: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "jobId": job_id,
        "status": "succeeded",
        "startedAt": started_at,
        "finishedAt": finished_at,
        "durationSeconds": duration_seconds,
        "dbPath": str(db_path),
        "filterConfigPath": str(filter_config_path),
        "outputCsvPath": str(output_csv_path),
        "targetParagraphCount": target_paragraph_count,
        "selectedParagraphCount": selected_paragraph_count,
        "warningMessages": warning_messages,
        "errorSummary": "",
    }


def _serialize_warning_messages(warning_messages: list[object]) -> list[dict[str, object]]:
    serialized_messages: list[dict[str, object]] = []
    for warning_message in warning_messages:
        serialized_messages.append(
            {
                "code": getattr(warning_message, "code", ""),
                "message": getattr(warning_message, "message", ""),
                "conditionId": getattr(warning_message, "condition_id", None),
                "unitId": getattr(warning_message, "unit_id", None),
                "requestedMode": getattr(warning_message, "requested_mode", None),
                "usedMode": getattr(warning_message, "used_mode", None),
                "combinationCount": getattr(warning_message, "combination_count", None),
                "combinationCap": getattr(warning_message, "combination_cap", None),
                "safetyLimit": getattr(warning_message, "safety_limit", None),
            }
        )
    return serialized_messages


def _filter_sentences_for_tokens(
    analysis_tokens_df: pl.DataFrame,
    analysis_sentences_df: pl.DataFrame,
) -> pl.DataFrame:
    if analysis_tokens_df.is_empty() or analysis_sentences_df.is_empty():
        return analysis_sentences_df.clear()

    token_sentence_keys_df = (
        analysis_tokens_df
        .select(["sentence_id", "paragraph_id"])
        .unique()
    )
    return analysis_sentences_df.join(
        token_sentence_keys_df,
        on=["sentence_id", "paragraph_id"],
        how="inner",
    )


def run_analysis_job(args: Namespace) -> int:
    started_at = _utc_now_iso()
    start_timestamp = datetime.now(timezone.utc)

    output_dir, output_csv_path, output_meta_json_path = _resolve_output_paths(args)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_meta_json_path.parent.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path).expanduser().resolve()
    filter_config_path = Path(args.filter_config_path).expanduser().resolve()

    try:
        from .analysis_core import (
            build_condition_hit_result,
            build_reconstructed_paragraphs_export_df,
            build_rendered_paragraphs_df,
            build_token_annotations_df,
            build_tokens_with_position_df,
            enrich_reconstructed_paragraphs_df,
            load_filter_config,
            read_analysis_sentences,
            read_analysis_tokens,
            select_target_ids_by_cooccurrence_conditions,
        )

        filter_config = load_filter_config(filter_config_path)
        analysis_tokens_df = read_analysis_tokens(db_path=db_path, limit_rows=args.limit_rows)
        analysis_sentences_df = read_analysis_sentences(db_path=db_path, limit_rows=None)
        if args.limit_rows is not None:
            analysis_sentences_df = _filter_sentences_for_tokens(
                analysis_tokens_df=analysis_tokens_df,
                analysis_sentences_df=analysis_sentences_df,
            )

        (
            _candidate_tokens_df,
            _condition_eval_df,
            paragraph_match_summary_df,
            target_paragraph_ids,
            _target_sentence_ids,
        ) = select_target_ids_by_cooccurrence_conditions(
            tokens_df=analysis_tokens_df,
            sentences_df=analysis_sentences_df,
            cooccurrence_conditions=filter_config.cooccurrence_conditions,
            condition_match_logic=filter_config.condition_match_logic,
            max_paragraph_ids=filter_config.max_reconstructed_paragraphs,
        )

        selected_tokens_with_position_df = build_tokens_with_position_df(
            tokens_df=analysis_tokens_df,
            sentences_df=analysis_sentences_df,
            paragraph_ids=target_paragraph_ids,
            target_forms=None,
        )
        condition_hit_result = build_condition_hit_result(
            tokens_with_position_df=selected_tokens_with_position_df,
            cooccurrence_conditions=filter_config.cooccurrence_conditions,
            distance_matching_mode=filter_config.distance_matching_mode,
            distance_match_combination_cap=filter_config.distance_match_combination_cap,
            distance_match_strict_safety_limit=filter_config.distance_match_strict_safety_limit,
        )
        condition_hit_tokens_df = condition_hit_result.condition_hit_tokens_df
        token_annotations_df = build_token_annotations_df(
            condition_hit_tokens_df=condition_hit_tokens_df,
        )
        reconstructed_paragraphs_base_df = build_rendered_paragraphs_df(
            tokens_with_position_df=selected_tokens_with_position_df,
            token_annotations_df=token_annotations_df,
        )
        reconstructed_paragraphs_df = enrich_reconstructed_paragraphs_df(
            db_path=db_path,
            reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
        )
        reconstructed_paragraphs_export_df = build_reconstructed_paragraphs_export_df(
            reconstructed_paragraphs_df=reconstructed_paragraphs_df,
        )
        reconstructed_paragraphs_export_df.write_csv(output_csv_path)

        finished_at = _utc_now_iso()
        duration_seconds = round(
            (datetime.now(timezone.utc) - start_timestamp).total_seconds(),
            6,
        )
        serialized_warning_messages = _serialize_warning_messages(
            condition_hit_result.warning_messages
        )
        success_payload = _build_success_payload(
            job_id=args.job_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            db_path=db_path,
            filter_config_path=filter_config_path,
            output_csv_path=output_csv_path,
            target_paragraph_count=paragraph_match_summary_df.height,
            selected_paragraph_count=len(target_paragraph_ids),
            warning_messages=serialized_warning_messages,
        )
        _write_meta_json(output_meta_json_path=output_meta_json_path, payload=success_payload)
        print(json.dumps(success_payload, ensure_ascii=False))
        return 0
    except Exception as exc:
        finished_at = _utc_now_iso()
        duration_seconds = round(
            (datetime.now(timezone.utc) - start_timestamp).total_seconds(),
            6,
        )
        failure_payload = _build_failure_payload(
            job_id=args.job_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            db_path=db_path,
            filter_config_path=filter_config_path,
            output_csv_path=output_csv_path,
            warning_messages=[],
            error_summary=str(exc),
        )
        try:
            _write_meta_json(output_meta_json_path=output_meta_json_path, payload=failure_payload)
        except Exception as meta_exc:
            print(
                f"failed to write meta json: {output_meta_json_path} ({meta_exc})",
                file=sys.stderr,
            )

        print(str(exc), file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return run_analysis_job(args)


if __name__ == "__main__":
    raise SystemExit(main())
