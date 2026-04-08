"""
Task 0-3: 成功系で GenerateContentResponse の形を観察し、extract_response_text の前提を固める。

- PDF + thinking（Task 0-2 と同条件）
- テキストのみ + thinking（現行 CLI と同条件）

blocked / SAFETY などの異常系は API を故意に壊さず、SDK ソース上の挙動をログ文書にまとめる。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from google import genai
from google.genai import types


def loadDotenv(dotenvPath: str) -> None:
    if not os.path.exists(dotenvPath):
        return

    with open(dotenvPath, encoding="utf-8") as dotenvFile:
        for rawLine in dotenvFile:
            line = rawLine.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            os.environ.setdefault(key, value)


DEFAULT_MODEL = "gemma-4-31b-it"
DEFAULT_API_KEY_ENV = "GEMINI_API_KEY"


def parseArgs() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Task 0-3: inspect GenerateContentResponse shape (success paths)."
    )
    parser.add_argument(
        "--pdf-path",
        default="",
        help="PDF path for scenario A (skip A if empty).",
    )
    parser.add_argument(
        "--api-key-env",
        default=DEFAULT_API_KEY_ENV,
        metavar="NAME",
        help=f"API key env name (default: {DEFAULT_API_KEY_ENV}).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model id (default: {DEFAULT_MODEL}).",
    )
    return parser.parse_args()


def safeJson(obj: Any) -> str:
    try:
        if hasattr(obj, "model_dump"):
            return json.dumps(obj.model_dump(mode="json"), ensure_ascii=False, default=str)
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(obj)


def describeResponse(label: str, response: types.GenerateContentResponse) -> None:
    print(f"\n========== {label} ==========")

    pf = response.prompt_feedback
    print(f"prompt_feedback: {safeJson(pf) if pf is not None else None}")

    cands = response.candidates
    print(f"candidates: {None if cands is None else len(cands)}")

    if not cands:
        print("first candidate: (none)")
        textProp: str | None
        try:
            textProp = response.text
        except Exception as exc:
            print(f"response.text raised: {type(exc).__name__}: {exc}")
            return
        print(f"response.text: {repr(textProp)}")
        return

    c0 = cands[0]
    print(f"finish_reason: {repr(c0.finish_reason)}")
    print(f"finish_message: {repr(c0.finish_message)}")
    print(f"safety_ratings: {safeJson(c0.safety_ratings) if c0.safety_ratings else None}")

    parts = c0.content.parts if c0.content and c0.content.parts else None
    if not parts:
        print("parts: (none or empty)")
    else:
        print(f"parts count: {len(parts)}")
        for i, part in enumerate(parts):
            thought = getattr(part, "thought", None)
            t = part.text
            tPreview = (t[:120] + "…") if isinstance(t, str) and len(t) > 120 else t
            print(
                f"  [{i}] thought={thought!r} text_is_none={t is None} "
                f"text_len={len(t) if isinstance(t, str) else 'n/a'} preview={tPreview!r}"
            )

    try:
        textProp = response.text
    except Exception as exc:
        print(f"response.text raised: {type(exc).__name__}: {exc}")
        return

    print(f"response.text type={type(textProp).__name__} len={len(textProp) if textProp else 0}")
    if textProp:
        prev = (textProp[:200] + "…") if len(textProp) > 200 else textProp
        print(f"response.text preview: {prev!r}")


def main() -> int:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        pass

    args = parseArgs()
    dotenvPath = os.path.join(os.path.dirname(__file__), ".env")
    loadDotenv(dotenvPath)

    apiKey = os.getenv(args.api_key_env, "").strip()
    if not apiKey:
        print(
            f"Error: {args.api_key_env} is not set. Add it to pdf_converter/.env.",
            file=sys.stderr,
        )
        return 1

    client = genai.Client(api_key=apiKey)
    thinkingCfg = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="high"),
    )

    if args.pdf_path.strip():
        pdfPath = os.path.abspath(args.pdf_path.strip())
        if not os.path.isfile(pdfPath):
            print(f"Error: PDF not found: {pdfPath}", file=sys.stderr)
            return 1
        with open(pdfPath, "rb") as f:
            pdfBytes = f.read()
        if len(pdfBytes) < 5 or pdfBytes[:5] != b"%PDF-":
            print("Error: not a PDF header", file=sys.stderr)
            return 1
        pdfPart = types.Part.from_bytes(data=pdfBytes, mime_type="application/pdf")
        prompt = "この PDF の内容を1文で要約してください。"
        rA = client.models.generate_content(
            model=args.model,
            contents=[pdfPart, prompt],
            config=thinkingCfg,
        )
        describeResponse("Scenario A: PDF inline + thinking", rA)

    promptB = "水の化学式は何ですか？簡潔に答えてください。"
    rB = client.models.generate_content(
        model=args.model,
        contents=promptB,
        config=thinkingCfg,
    )
    describeResponse("Scenario B: text only + thinking", rB)

    print("\nTask 0-3: inspection finished (see document/logs for interpretation).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
