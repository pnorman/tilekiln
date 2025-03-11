from unittest import TestCase
from unittest.mock import Mock, MagicMock

import tilekiln.errors

from tilekiln.kiln import Kiln
from tilekiln.config import Config
from tilekiln.tile import Tile


class TestKiln(TestCase):
    def setUp(self):
        # Create a mock config
        self.config = Mock(spec=Config)
        self.config.minzoom = 0
        self.config.maxzoom = 14
        # Mock DB connection and cursor
        self.conn = MagicMock()
        self.cursor = MagicMock()
        # Mock connection pool with context manager behavior
        self.pool = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__.return_value = self.conn
        self.pool.connection.return_value = mock_conn_ctx
        # Mock cursor context manager
        mock_cursor_ctx = MagicMock()
        mock_cursor_ctx.__enter__.return_value = self.cursor
        self.conn.cursor.return_value = mock_cursor_ctx
        # Mock record return values
        self.cursor.execute.return_value = None
        self.cursor.__iter__.return_value = [(b'mock_tile_data',)]
        # Set up layer data
        self.layers = {
            'layer1': 'SQL1',
            'layer2': 'SQL2',
            'layer3': 'SQL3'
        }
        self.config.layer_queries.return_value = self.layers
        # Create valid tile
        self.tile = Tile(5, 10, 10)

    def test_render_all_default(self):
        """Test rendering all layers by default"""
        kiln = Kiln(self.config, self.pool)
        result = kiln.render_all(self.tile)
        # Should render all layers
        self.assertEqual(len(result), 3)
        self.assertEqual(set(result.keys()), {'layer1', 'layer2', 'layer3'})
        # Check SQL queries
        self.assertEqual(self.cursor.execute.call_count, 3)
        # Verify correct config method called
        self.config.layer_queries.assert_called_once_with(self.tile)

    def test_render_filtered_layers(self):
        """Test rendering only specified layers"""
        kiln = Kiln(self.config, self.pool, layers=['layer1', 'layer3'])
        result = kiln.render_all(self.tile)
        # Should render only specified layers
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result.keys()), {'layer1', 'layer3'})
        # Check SQL queries - should execute only 2 queries
        self.assertEqual(self.cursor.execute.call_count, 2)
        # Verify correct config method called
        self.config.layer_queries.assert_called_once_with(self.tile)

    def test_zoom_outside_range(self):
        """Test that rendering a tile outside zoom range raises an error"""
        kiln = Kiln(self.config, self.pool)
        # Create tile with zoom outside allowed range
        invalid_tile = Tile(20, 0, 0)
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            kiln.render_all(invalid_tile)
