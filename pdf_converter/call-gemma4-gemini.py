"""
Gemma 4 31B IT を Gemini API（generativelanguage.googleapis.com）経由で呼び出す最小例。

公式: https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api
"""

import argparse
import os
import sys

from google import genai
from google.genai import types


def load_dotenv(dotenv_path: str) -> None:
    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, encoding="utf-8") as dotenv_file:
        for raw_line in dotenv_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            os.environ.setdefault(key, value)


DEFAULT_MODEL = "gemma-4-31b-it"
DEFAULT_API_KEY_ENV = "GEMINI_API_KEY"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Call Gemma 4 31B IT on Gemini API with thinking (thinking_level=high)."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default="水の化学式は何ですか？簡潔に答えてください。",
        help="User prompt (default: short demo question).",
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
    args = parse_args()
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)

    api_key = os.getenv(args.api_key_env, "").strip()
    if not api_key:
        print(
            f"Error: {args.api_key_env} is not set. Add it to pdf_converter/.env or the environment.",
            file=sys.stderr,
        )
        return 1

    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model=args.model,
        contents=args.prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="high"),
        ),
    )

    print(response.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
