"""
Tests for CLI commands
"""

from click.testing import CliRunner
from dbt_bq_sourcegen.cli import cli


class TestCLICommands:
    def setup_method(self):
        self.runner = CliRunner()

    def test_cli_help(self):
        """Test CLI help"""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "dbt-bq-sourcegen" in result.output
        assert "Create or update BigQuery source YAML" in result.output

    def test_apply_command_help(self):
        """Test apply command help"""
        result = self.runner.invoke(cli, ["apply", "--help"])
        assert result.exit_code == 0
        assert "Create or update source YAML" in result.output

    def test_apply_missing_args(self):
        """Test apply command with missing args"""
        result = self.runner.invoke(cli, ["apply"])
        assert result.exit_code != 0
        assert "Missing option" in result.output
