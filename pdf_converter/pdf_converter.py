import argparse
import base64
import os

import requests

API_URL = "https://sbhezbf9vda905l6.aistudio-app.com/layout-parsing"
DEFAULT_FILE_PATH = "asset/texts_2nd/manual/pdf/根室市_条例.pdf"
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

    for i, res in enumerate(result["layoutParsingResults"]):
        markdown_parts.append(res["markdown"]["text"])
        for img_path, img in res["markdown"]["images"].items():
            full_img_path = os.path.join(output_dir, img_path)
            os.makedirs(os.path.dirname(full_img_path), exist_ok=True)
            img_bytes = requests.get(img).content
            with open(full_img_path, "wb") as img_file:
                img_file.write(img_bytes)
            print(f"Image saved to: {full_img_path}")
        if save_page_jpg:
            for img_name, img in res["outputImages"].items():
                img_response = requests.get(img)
                if img_response.status_code == 200:
                    filename = os.path.join(output_dir, f"{img_name}_{i}.jpg")
                    with open(filename, "wb") as f:
                        f.write(img_response.content)
                    print(f"Image saved to: {filename}")
                else:
                    print(
                        f"Failed to download image, status code: {img_response.status_code}"
                    )

    combined_markdown = "\n\n".join(part.strip() for part in markdown_parts if part.strip())
    with open(md_filename, "w", encoding="utf-8") as md_file:
        md_file.write(combined_markdown)

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
