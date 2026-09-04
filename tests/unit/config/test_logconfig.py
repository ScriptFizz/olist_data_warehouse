from pathlib import Path

import pytest

from olist_dw.config.logconfig import resolve_logs_directory


def test_logs_directory_defaults_to_working_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OLIST_LOG_DIR", raising=False)

    assert resolve_logs_directory() == tmp_path / "logs"


def test_logs_directory_can_be_configured_by_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configured_directory = tmp_path / "runtime-logs"
    monkeypatch.setenv("OLIST_LOG_DIR", str(configured_directory))

    assert resolve_logs_directory() == configured_directory


def test_explicit_logs_directory_overrides_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OLIST_LOG_DIR", str(tmp_path / "environment"))

    assert resolve_logs_directory(tmp_path / "explicit") == (
        tmp_path / "explicit"
    )
