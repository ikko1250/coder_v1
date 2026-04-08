"""
Task 0-1: gemma-4-31b-it へ小さな PDF を inline（Part.from_bytes）で渡し、
generate_content() が成功するかを確認する。

thinking_config は Task 0-2 の対象のため、ここでは付けない。
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback

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
        description="Task 0-1: verify PDF inline input to Gemma on Gemini API."
    )
    parser.add_argument(
        "--pdf-path",
        required=True,
        help="Path to a small PDF file (e.g. repo root temp.pdf).",
    )
    parser.add_argument(
        "--api-key-env",
        default=DEFAULT_API_KEY_ENV,
        metavar="NAME",
        help=f"Environment variable name for the API key (default: {DEFAULT_API_KEY_ENV}).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model id (default: {DEFAULT_MODEL}).",
    )
    return parser.parse_args()


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
            f"Error: {args.api_key_env} is not set. Add it to pdf_converter/.env or the environment.",
            file=sys.stderr,
        )
        return 1

    pdfPath = os.path.abspath(args.pdf_path)
    if not os.path.isfile(pdfPath):
        print(f"Error: PDF file not found: {pdfPath}", file=sys.stderr)
        return 1

    with open(pdfPath, "rb") as f:
        pdfBytes = f.read()

    if len(pdfBytes) < 5 or pdfBytes[:5] != b"%PDF-":
        print("Error: file does not start with %PDF- header", file=sys.stderr)
        return 1

    pdfPart = types.Part.from_bytes(data=pdfBytes, mime_type="application/pdf")
    prompt = "この PDF の内容を1文で要約してください。"

    client = genai.Client(api_key=apiKey)

    try:
        response = client.models.generate_content(
            model=args.model,
            contents=[pdfPart, prompt],
            config=types.GenerateContentConfig(),
        )
    except Exception as exc:
        print("Task 0-1: generate_content FAILED", file=sys.stderr)
        print(f"Exception type: {type(exc).__name__}", file=sys.stderr)
        print(f"Message: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    text = getattr(response, "text", None)
    if not text:
        print(
            "Task 0-1: API returned but response.text is empty or missing.",
            file=sys.stderr,
        )
        return 1

    print("Task 0-1: SUCCESS - generate_content completed with non-empty response.text")
    print("--- response.text ---")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
