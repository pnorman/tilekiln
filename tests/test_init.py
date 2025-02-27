import os
import tempfile
from unittest import TestCase, mock

import tilekiln
from tilekiln.config import Config


class TestInit(TestCase):
    def test_load_config(self):
        """Test the load_config function"""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp.write(
                """
id: test_config
name: Test Config
minzoom: 0
maxzoom: 14
center: [0, 0, 10]
bounds: [-180, -90, 180, 90]
layers:
  test_layer:
    minzoom: 0
    maxzoom: 14
    queries:
      0: SELECT 1
source:
  dbname: test_db
  host: localhost
storage:
  dbname: test_db
  host: localhost
            """
            )
            tmp_path = tmp.name

        try:
            # Mock the Config creation to avoid actual filesystem operations
            with mock.patch("tilekiln.config.Config") as mock_config:
                # Set up the mock to return a config instance
                mock_config_instance = mock.Mock(spec=Config)
                mock_config.return_value = mock_config_instance

                # Call load_config
                result = tilekiln.load_config(tmp_path)

                # Check that Config was called correctly
                mock_config.assert_called_once()

                # Check the result
                self.assertEqual(result, mock_config_instance)

        finally:
            # Clean up the temporary file
            os.unlink(tmp_path)

    def test_load_config_with_relative_path(self):
        """Test load_config with a relative path"""
        # Create a temporary config file in the current directory
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", dir=".", delete=False
        ) as tmp:
            filename = os.path.basename(tmp.name)
            tmp.write(
                """
id: test_config
name: Test Config
minzoom: 0
maxzoom: 14
"""
            )

        try:
            # Mock Config to avoid actual filesystem operations
            with mock.patch("tilekiln.config.Config") as mock_config:
                # Set up the mock
                mock_config_instance = mock.Mock(spec=Config)
                mock_config.return_value = mock_config_instance

                # Call load_config with relative path
                result = tilekiln.load_config(filename)

                # Check that Config was called
                mock_config.assert_called_once()

                # Check the result
                self.assertEqual(result, mock_config_instance)
        finally:
            # Clean up the temporary file
            os.unlink(tmp.name)
