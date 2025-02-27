from unittest import TestCase
from click.testing import CliRunner

from tilekiln.scripts.serve import serve as serve_cmd


class TestScriptsServe(TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_serve_commands_exist(self):
        """Test that the serve command has all expected subcommands"""
        result = self.runner.invoke(serve_cmd, ["--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that all expected commands are listed in the help output
        self.assertIn("static", result.output)
        self.assertIn("dev", result.output)
        self.assertIn("live", result.output)

    def test_serve_static_command_help(self):
        """Test the serve static command help"""
        result = self.runner.invoke(serve_cmd, ["static", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that all expected options are listed - no config parameter for static
        self.assertIn("--bind-host", result.output)
        self.assertIn("--bind-port", result.output)
        self.assertIn("--storage-dbname", result.output)

    def test_serve_dev_command_help(self):
        """Test the serve dev command help"""
        result = self.runner.invoke(serve_cmd, ["dev", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that all expected options are listed
        self.assertIn("--config", result.output)
        self.assertIn("--bind-host", result.output)
        self.assertIn("--bind-port", result.output)

    def test_serve_live_command_help(self):
        """Test the serve live command help"""
        result = self.runner.invoke(serve_cmd, ["live", "--help"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that all expected options are listed
        self.assertIn("--config", result.output)
        self.assertIn("--bind-host", result.output)
        self.assertIn("--bind-port", result.output)
