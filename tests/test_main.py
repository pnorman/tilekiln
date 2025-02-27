from unittest import TestCase
from click.testing import CliRunner

from tilekiln.main import cli as main_cli


class TestMainCLI(TestCase):
    """Tests for the tilekiln main CLI"""

    def setUp(self):
        self.runner = CliRunner()

    def test_cli_commands_exist(self):
        """Test that all expected commands exist in the CLI"""
        result = self.runner.invoke(main_cli, ["--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that all expected commands are listed in the help output
        self.assertIn("config", result.output)
        self.assertIn("generate", result.output)
        self.assertIn("serve", result.output)
        self.assertIn("storage", result.output)
        self.assertIn("prometheus", result.output)

    def test_prometheus_command_help(self):
        """Test the prometheus command help"""
        result = self.runner.invoke(main_cli, ["prometheus", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that the expected options are listed
        self.assertIn("--bind-host", result.output)
        self.assertIn("--bind-port", result.output)
        self.assertIn("--storage-dbname", result.output)
        self.assertIn("--storage-host", result.output)
        self.assertIn("--storage-port", result.output)
        self.assertIn("--storage-username", result.output)

    def test_config_command_group_exists(self):
        """Test that the config command group exists and has subcommands"""
        result = self.runner.invoke(main_cli, ["config", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that the expected subcommands are listed
        self.assertIn("test", result.output)
        self.assertIn("sql", result.output)

    def test_serve_command_group_exists(self):
        """Test that the serve command group exists and has subcommands"""
        result = self.runner.invoke(main_cli, ["serve", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that the expected subcommands are listed
        self.assertIn("static", result.output)
        self.assertIn("dev", result.output)
        self.assertIn("live", result.output)

    def test_storage_command_group_exists(self):
        """Test that the storage command group exists and has subcommands"""
        result = self.runner.invoke(main_cli, ["storage", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that the expected subcommands are listed
        self.assertIn("init", result.output)
        self.assertIn("destroy", result.output)

    def test_generate_command_help(self):
        """Test the generate command help"""
        result = self.runner.invoke(main_cli, ["generate", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that the subcommands exist
        self.assertIn("tiles", result.output)
        self.assertIn("zooms", result.output)
