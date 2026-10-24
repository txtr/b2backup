"""CLI tests for b2backup."""
import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, Mock

from b2backup.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def dummy_config(tmp_path):
    config_file = tmp_path / "accounts.yaml"
    config_file.write_text("""
accounts:
  - account_id: "abc123"
    account_key: "key1"
""")
    return config_file


def test_cli_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "b2backup - Personal backup to Backblaze B2" in result.output


def test_cli_missing_config(runner):
    result = runner.invoke(cli, ["backup", "/some/path"])
    assert result.exit_code == 2
    assert "Missing option" in result.output or "required" in result.output


def test_backup_command_help(runner, dummy_config):
    result = runner.invoke(cli, ["--config", str(dummy_config), "backup", "--help"])
    assert result.exit_code == 0
    assert "Backup a directory" in result.output


def test_restore_command_help(runner, dummy_config):
    result = runner.invoke(cli, ["--config", str(dummy_config), "restore", "--help"])
    assert result.exit_code == 0
    assert "Restore a file" in result.output


def test_delete_command_help(runner, dummy_config):
    result = runner.invoke(cli, ["--config", str(dummy_config), "delete", "--help"])
    assert result.exit_code == 0
    assert "Delete a file" in result.output


def test_ls_command_help(runner, dummy_config):
    result = runner.invoke(cli, ["--config", str(dummy_config), "ls", "--help"])
    assert result.exit_code == 0
    assert "List all backed up files" in result.output


def test_stats_command_help(runner, dummy_config):
    result = runner.invoke(cli, ["--config", str(dummy_config), "stats", "--help"])
    assert result.exit_code == 0
    assert "Show pool capacity and usage" in result.output


@patch("b2backup.cli.B2AccountPool")
def test_backup_no_files(mock_pool_class, tmp_path, runner, dummy_config):
    """Test backup with empty directory."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    mock_pool = Mock()
    mock_pool_class.return_value = mock_pool

    result = runner.invoke(cli, ["--config", str(dummy_config), "backup", str(empty_dir)])

    assert result.exit_code == 0
    assert "No files to backup" in result.output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])