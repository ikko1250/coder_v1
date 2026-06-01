from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .condition_authoring import load_authoring_config_result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="condition-authoring-cli",
        description="Convert authoring config to runtime JSON.",
    )
    parser.add_argument("--input", required=True, help="Path to authoring config JSON.")
    parser.add_argument("--output", help="Path to write compiled runtime JSON.")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate only; do not require --output.",
    )
    parser.add_argument(
        "--issues-json",
        dest="issues_json",
        help="Path to write issues list as JSON.",
    )
    return parser


def _issue_to_dict(issue: object) -> dict[str, object]:
    return {
        "code": getattr(issue, "code", ""),
        "severity": getattr(issue, "severity", ""),
        "scope": getattr(issue, "scope", ""),
        "message": getattr(issue, "message", ""),
        "condition_index": getattr(issue, "condition_index", None),
        "condition_id": getattr(issue, "condition_id", None),
        "field_name": getattr(issue, "field_name", None),
    }


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.validate and not args.output:
        parser.error("--output is required unless --validate is set")

    result = load_authoring_config_result(args.input)

    issues = [_issue_to_dict(issue) for issue in result.issues]
    has_errors = any(
        issue.severity == "error" for issue in result.issues
    )

    if args.issues_json:
        issues_path = Path(args.issues_json)
        try:
            issues_path.write_text(
                json.dumps(issues, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            print(f"Failed to write issues JSON: {exc}", file=sys.stderr)
            return 1

    if has_errors:
        for issue in result.issues:
            if issue.severity == "error":
                print(issue.message, file=sys.stderr)
        return 1

    if not args.validate:
        output_path = Path(args.output)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(result.raw_config, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            print(f"Failed to write output JSON: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
