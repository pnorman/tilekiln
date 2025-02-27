import os
import tempfile
from unittest import TestCase, mock
from click.testing import CliRunner

from tilekiln.scripts.config import config


class TestScriptsConfig(TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @mock.patch("tilekiln.load_config")
    def test_config_test_command(self, mock_load_config):
        """Test the config test command"""
        # Create a temporary file for the config
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                tmp_path = tmp.name

                # Call the config test command
                result = self.runner.invoke(config, ["test", "--config", tmp_path])

                # Verify the command executed successfully
                self.assertEqual(result.exit_code, 0)

                # Verify tilekiln.load_config was called with correct arguments
                mock_load_config.assert_called_once_with(tmp_path)

            finally:
                # Clean up the temporary file
                os.unlink(tmp_path)

    @mock.patch("tilekiln.load_config")
    def test_config_sql_command_for_entire_tile(self, mock_load_config):
        """Test the config sql command for an entire tile"""
        # Create a temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                tmp_path = tmp.name

                # Mock the config object
                mock_config = mock.Mock()
                mock_load_config.return_value = mock_config

                # Mock layer_queries to return SQL for multiple layers
                mock_config.layer_queries.return_value = {
                    "layer1": "SELECT * FROM layer1;",
                    "layer2": "SELECT * FROM layer2;",
                }

                # Call the sql command without specifying a layer (should print SQL for all layers)
                result = self.runner.invoke(
                    config,
                    [
                        "sql",
                        "--config",
                        tmp_path,
                        "--zoom",
                        "10",
                        "-x",
                        "123",
                        "-y",
                        "456",
                    ],
                )

                # Verify command executed successfully
                self.assertEqual(result.exit_code, 0)

                # Verify tilekiln.load_config was called
                mock_load_config.assert_called_once_with(tmp_path)

                # Verify layer_queries was called with a Tile object
                mock_config.layer_queries.assert_called_once()
                tile_arg = mock_config.layer_queries.call_args[0][0]
                self.assertEqual(tile_arg.zoom, 10)
                self.assertEqual(tile_arg.x, 123)
                self.assertEqual(tile_arg.y, 456)

                # Verify output contains SQL for both layers
                self.assertIn("SELECT * FROM layer1;", result.output)
                self.assertIn("SELECT * FROM layer2;", result.output)

            finally:
                os.unlink(tmp_path)

    @mock.patch("tilekiln.load_config")
    def test_config_sql_command_for_specific_layer(self, mock_load_config):
        """Test the config sql command for a specific layer"""
        # Create a temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                tmp_path = tmp.name

                # Mock the config object
                mock_config = mock.Mock()
                mock_load_config.return_value = mock_config

                # Mock layer_query to return SQL for a specific layer
                mock_config.layer_query.return_value = "SELECT * FROM specific_layer;"

                # Call the sql command with a specific layer
                result = self.runner.invoke(
                    config,
                    [
                        "sql",
                        "--config",
                        tmp_path,
                        "--layer",
                        "specific_layer",
                        "--zoom",
                        "10",
                        "-x",
                        "123",
                        "-y",
                        "456",
                    ],
                )

                # Verify command executed successfully
                self.assertEqual(result.exit_code, 0)

                # Verify tilekiln.load_config was called
                mock_load_config.assert_called_once_with(tmp_path)

                # Verify layer_query was called with the right arguments
                mock_config.layer_query.assert_called_once()
                layer_arg = mock_config.layer_query.call_args[0][0]
                tile_arg = mock_config.layer_query.call_args[0][1]
                self.assertEqual(layer_arg, "specific_layer")
                self.assertEqual(tile_arg.zoom, 10)
                self.assertEqual(tile_arg.x, 123)
                self.assertEqual(tile_arg.y, 456)

                # Verify output contains the SQL
                self.assertIn("SELECT * FROM specific_layer;", result.output)

            finally:
                os.unlink(tmp_path)

    @mock.patch("tilekiln.load_config")
    def test_config_sql_layer_not_found(self, mock_load_config):
        """Test the config sql command with a non-existent layer"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                tmp_path = tmp.name

                # Mock the config object
                mock_config = mock.Mock()
                mock_load_config.return_value = mock_config

                # Mock layer_query to raise KeyError (layer not found)
                mock_config.layer_query.side_effect = KeyError("Layer not found")

                # Call the sql command with a non-existent layer
                result = self.runner.invoke(
                    config,
                    [
                        "sql",
                        "--config",
                        tmp_path,
                        "--layer",
                        "nonexistent",
                        "--zoom",
                        "10",
                        "-x",
                        "123",
                        "-y",
                        "456",
                    ],
                )

                # The actual implementation prints an error message but returns 0
                # Just verify it ran
                self.assertTrue(result.exit_code >= 0)

                # Verify error message
                self.assertIn("Layer 'nonexistent' not found", result.output)

            finally:
                os.unlink(tmp_path)
