from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis_backend.condition_authoring import (
    CompileAuthoringResult,
    load_authoring_config_result,
)


class TestLoadAuthoringConfigResult:
    def test_valid_json_file_loads(self, tmp_path: Path) -> None:
        config_path = tmp_path / "config.json"
        config_path.write_text(
            json.dumps({"format": "condition-authoring/v1", "rules": []}),
            encoding="utf-8",
        )

        result = load_authoring_config_result(config_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is not None
        assert result.filter_config is not None
        assert result.issues == []
        assert result.raw_config["condition_match_logic"] == "any"

    def test_yaml_not_supported_in_v1(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text("format: condition-authoring/v1\nrules: []\n", encoding="utf-8")

        result = load_authoring_config_result(yaml_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "yaml_not_supported_in_v1"
        assert issue.severity == "error"
        assert "yaml" in issue.message.lower()

    def test_yml_not_supported_in_v1(self, tmp_path: Path) -> None:
        yml_path = tmp_path / "config.yml"
        yml_path.write_text("format: condition-authoring/v1\nrules: []\n", encoding="utf-8")

        result = load_authoring_config_result(yml_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "yaml_not_supported_in_v1"
        assert issue.severity == "error"

    def test_file_not_found(self, tmp_path: Path) -> None:
        missing_path = tmp_path / "missing.json"

        result = load_authoring_config_result(missing_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_file_not_found"
        assert issue.severity == "error"
        assert str(missing_path) in issue.message

    def test_invalid_json(self, tmp_path: Path) -> None:
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("{ not json", encoding="utf-8")

        result = load_authoring_config_result(bad_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_json_invalid"
        assert issue.severity == "error"
        assert str(bad_path) in issue.message
        assert "json" in issue.message.lower()

    def test_non_dict_json_root(self, tmp_path: Path) -> None:
        config_path = tmp_path / "list.json"
        config_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")

        result = load_authoring_config_result(config_path)

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_root_invalid"
        assert issue.severity == "error"

    def test_str_path_accepted(self, tmp_path: Path) -> None:
        config_path = tmp_path / "config.json"
        config_path.write_text(
            json.dumps({"format": "condition-authoring/v1", "rules": []}),
            encoding="utf-8",
        )

        result = load_authoring_config_result(str(config_path))

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is not None
        assert result.filter_config is not None


class TestInitLazyExport:
    def test_load_authoring_config_result_exported_from_init(self) -> None:
        from analysis_backend import load_authoring_config_result as fn

        assert callable(fn)
