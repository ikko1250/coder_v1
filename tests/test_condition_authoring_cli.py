from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


MODULE = "analysis_backend.condition_authoring_cli"


class TestConditionAuthoringCli:
    def test_successful_conversion_writes_output_json_and_exit_0(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        output_path = tmp_path / "output.json"
        input_path.write_text(
            json.dumps({"format": "condition-authoring/v1", "rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-m", MODULE, "--input", str(input_path), "--output", str(output_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert output_path.exists()
        data = json.loads(output_path.read_text(encoding="utf-8"))
        assert data["condition_match_logic"] == "any"
        assert data["cooccurrence_conditions"] == []

    def test_validate_success_no_output_required(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        input_path.write_text(
            json.dumps({"format": "condition-authoring/v1", "rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-m", MODULE, "--input", str(input_path), "--validate"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

    def test_invalid_input_exits_non_zero_and_does_not_write_output(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        output_path = tmp_path / "output.json"
        input_path.write_text(
            json.dumps({"rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-m", MODULE, "--input", str(input_path), "--output", str(output_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert not output_path.exists()

    def test_issues_json_writes_issues_on_error(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        issues_path = tmp_path / "issues.json"
        input_path.write_text(
            json.dumps({"rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                MODULE,
                "--input",
                str(input_path),
                "--output",
                str(tmp_path / "output.json"),
                "--issues-json",
                str(issues_path),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert issues_path.exists()
        issues = json.loads(issues_path.read_text(encoding="utf-8"))
        assert len(issues) >= 1
        assert issues[0]["code"] == "authoring_format_missing"
        assert issues[0]["severity"] == "error"

    def test_issues_json_writes_empty_list_on_success(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        issues_path = tmp_path / "issues.json"
        output_path = tmp_path / "output.json"
        input_path.write_text(
            json.dumps({"format": "condition-authoring/v1", "rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                MODULE,
                "--input",
                str(input_path),
                "--output",
                str(output_path),
                "--issues-json",
                str(issues_path),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert issues_path.exists()
        issues = json.loads(issues_path.read_text(encoding="utf-8"))
        assert issues == []

    def test_missing_required_args_fails_with_non_zero(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", MODULE],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0

    def test_validate_with_invalid_input_exits_non_zero(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        input_path.write_text(
            json.dumps({"rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [sys.executable, "-m", MODULE, "--input", str(input_path), "--validate"],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0

    def test_issues_json_writes_issues_on_validate_error(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.json"
        issues_path = tmp_path / "issues.json"
        input_path.write_text(
            json.dumps({"rules": []}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                MODULE,
                "--input",
                str(input_path),
                "--validate",
                "--issues-json",
                str(issues_path),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert issues_path.exists()
        issues = json.loads(issues_path.read_text(encoding="utf-8"))
        assert len(issues) >= 1
