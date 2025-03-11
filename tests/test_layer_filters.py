import unittest
from unittest.mock import patch, MagicMock

from fs.memoryfs import MemoryFS

from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.tile import Tile


class TestLayerFilters(unittest.TestCase):

    def setUp(self):
        # Setup a mock config with multiple layers
        self.fs = MemoryFS()
        self.fs.writetext("blank.sql.jinja2", "blank sql")
        config_str = '''
        {
            "metadata": {"id":"test"},
            "vector_layers": {
                "layer1": {
                    "sql": [{"minzoom": 0, "maxzoom": 14, "file": "blank.sql.jinja2"}]
                },
                "layer2": {
                    "sql": [{"minzoom": 0, "maxzoom": 14, "file": "blank.sql.jinja2"}]
                },
                "layer3": {
                    "sql": [{"minzoom": 0, "maxzoom": 14, "file": "blank.sql.jinja2"}]
                }
            }
        }
        '''
        self.config = Config(config_str, self.fs)
        self.test_tile = Tile(10, 0, 0)

    @patch('psycopg_pool.ConnectionPool')
    def test_no_layer_filters(self, mock_pool):
        # Test rendering all layers (no layer filters)
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__.return_value = [(b'mock_data',)]

        mock_connection = MagicMock()
        conn_cursor = mock_connection.__enter__.return_value.cursor
        conn_cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool.connection.return_value = mock_connection

        kiln = Kiln(self.config, mock_pool)
        result = kiln.render_all(self.test_tile)

        # Should have rendered all three layers
        self.assertEqual(set(result.keys()), {"layer1", "layer2", "layer3"})
        self.assertEqual(mock_cursor.execute.call_count, 3)

    @patch('psycopg_pool.ConnectionPool')
    def test_single_layer_filter(self, mock_pool):
        # Test rendering only a single layer
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__.return_value = [(b'mock_data',)]

        mock_connection = MagicMock()
        conn_cursor = mock_connection.__enter__.return_value.cursor
        conn_cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool.connection.return_value = mock_connection

        kiln = Kiln(self.config, mock_pool)
        result = kiln.render_all(self.test_tile, {"layer1"})

        # Should have rendered only the requested layer
        self.assertEqual(set(result.keys()), {"layer1"})
        self.assertEqual(mock_cursor.execute.call_count, 1)

    @patch('psycopg_pool.ConnectionPool')
    def test_multiple_layer_filters(self, mock_pool):
        # Test rendering multiple specific layers
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__.return_value = [(b'mock_data',)]

        mock_connection = MagicMock()
        conn_cursor = mock_connection.__enter__.return_value.cursor
        conn_cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool.connection.return_value = mock_connection

        kiln = Kiln(self.config, mock_pool)
        result = kiln.render_all(self.test_tile, {"layer1", "layer3"})

        # Should have rendered only the requested layers
        self.assertEqual(set(result.keys()), {"layer1", "layer3"})
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @patch('psycopg_pool.ConnectionPool')
    def test_nonexistent_layer_filter(self, mock_pool):
        # Test with a layer that doesn't exist
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__.return_value = [(b'mock_data',)]

        mock_connection = MagicMock()
        conn_cursor = mock_connection.__enter__.return_value.cursor
        conn_cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool.connection.return_value = mock_connection

        kiln = Kiln(self.config, mock_pool)
        result = kiln.render_all(self.test_tile, {"nonexistent", "layer1"})

        # Should only render the valid layer
        self.assertEqual(set(result.keys()), {"layer1"})
        self.assertEqual(mock_cursor.execute.call_count, 1)


if __name__ == '__main__':
    unittest.main()
