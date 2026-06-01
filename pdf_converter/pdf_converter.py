from __future__ import annotations

"""Legacy compatibility CLI for the original PDF layout parser.

This module intentionally remains import-side-effect free because older tests and
callers import ``pdf_converter.pdf_converter`` directly.  The newer Gemini CLI
lives in ``pdf_converter.call_gemma4_gemini``; this file preserves the previous
layout-parsing API surface (``main`` and ``convert_pdf``).
"""

import argparse
import base64
import os

import requests

from pdf_converter.project_paths import resolve_manual_root

API_URL = "https://sbhezbf9vda905l6.aistudio-app.com/layout-parsing"
DEFAULT_FILE_PATH = str(resolve_manual_root() / "pdf" / "根室市_条例.pdf")
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_DOTENV_PATH = os.path.join(os.path.dirname(__file__), ".env")


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


def get_token(dotenv_path: str = DEFAULT_DOTENV_PATH) -> str:
    load_dotenv(dotenv_path)
    return os.getenv("PDF_CONVERTER_TOKEN", "")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", nargs="?", default=DEFAULT_FILE_PATH)
    parser.add_argument("--save-page-jpg", action="store_true")
    return parser.parse_args(argv)


def build_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Content-Type": "application/json",
    }


def build_payload(file_data: str) -> dict[str, object]:
    required_payload = {
        "file": file_data,
        "fileType": 0,
    }
    optional_payload = {
        "useDocOrientationClassify": False,
        "useDocUnwarping": False,
        "useChartRecognition": False,
    }
    return {**required_payload, **optional_payload}


def convert_pdf(
    file_path: str,
    save_page_jpg: bool,
    token: str,
    output_dir: str | None = None,
) -> None:
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    with open(file_path, "rb") as file:
        file_bytes = file.read()
        file_data = base64.b64encode(file_bytes).decode("ascii")

    response = requests.post(
        API_URL,
        json=build_payload(file_data),
        headers=build_headers(token),
    )
    print(response.status_code)
    assert response.status_code == 200
    result = response.json()["result"]

    os.makedirs(output_dir, exist_ok=True)

    pdf_stem = os.path.splitext(os.path.basename(file_path))[0]
    md_filename = os.path.join(output_dir, f"{pdf_stem}.md")
    markdown_parts: list[str] = []

    for index, layout_result in enumerate(result["layoutParsingResults"]):
        markdown_parts.append(layout_result["markdown"]["text"])
        for image_path, image_url in layout_result["markdown"]["images"].items():
            full_image_path = os.path.join(output_dir, image_path)
            os.makedirs(os.path.dirname(full_image_path), exist_ok=True)
            image_bytes = requests.get(image_url).content
            with open(full_image_path, "wb") as image_file:
                image_file.write(image_bytes)
            print(f"Image saved to: {full_image_path}")
        if save_page_jpg:
            for image_name, image_url in layout_result["outputImages"].items():
                image_response = requests.get(image_url)
                if image_response.status_code == 200:
                    filename = os.path.join(output_dir, f"{image_name}_{index}.jpg")
                    with open(filename, "wb") as output_file:
                        output_file.write(image_response.content)
                    print(f"Image saved to: {filename}")
                else:
                    print(
                        f"Failed to download image, status code: {image_response.status_code}"
                    )

    combined_markdown = "\n\n".join(part.strip() for part in markdown_parts if part.strip())
    with open(md_filename, "w", encoding="utf-8") as markdown_file:
        markdown_file.write(combined_markdown)

    print(f"Markdown document saved at {md_filename}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = get_token()
    if not token:
        raise RuntimeError("PDF_CONVERTER_TOKEN is not set. Put it in pdf_converter/.env")

    convert_pdf(args.file_path, args.save_page_jpg, token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
