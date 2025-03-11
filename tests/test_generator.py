from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch

from tilekiln.config import Config
from tilekiln.tile import Tile
import tilekiln.generator


class TestGenerator(TestCase):
    def setUp(self):
        # Create a mock config
        self.config = Mock(spec=Config)
        self.config.minzoom = 0
        self.config.maxzoom = 14
        # Mock layer names
        self.config.layer_names.return_value = ['layer1', 'layer2', 'layer3']
        # Create a set of test tiles
        self.tiles = {Tile(0, 0, 0)}
        # Mock source and storage connection parameters
        self.source_kwargs = {'dbname': 'source_db'}
        self.storage_kwargs = {'dbname': 'storage_db'}

    @patch('tilekiln.generator.mp.Pool')
    def test_generate_all_layers(self, mock_pool):
        """Test generating tiles with no layer filtering"""
        # Set up mock pool
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        # Call generate with no layers specified
        tilekiln.generator.generate(
            self.config,
            self.source_kwargs,
            self.storage_kwargs,
            self.tiles,
            1,
            None
        )

        # Verify setup called with the correct parameters
        mock_pool.assert_called_once_with(
            1,
            tilekiln.generator.setup,
            (self.config, self.source_kwargs, self.storage_kwargs, None)
        )

        # Verify no layer info was printed (we can't easily test this,
        # but the test ensures code paths without layer filtering run)

    @patch('tilekiln.generator.mp.Pool')
    def test_generate_single_layer(self, mock_pool):
        """Test generating tiles with single layer filtering"""
        # Set up mock pool
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        # Call generate with one layer specified
        tilekiln.generator.generate(
            self.config,
            self.source_kwargs,
            self.storage_kwargs,
            self.tiles,
            1,
            ['layer1']
        )

        # Verify setup called with the correct parameters
        mock_pool.assert_called_once_with(
            1,
            tilekiln.generator.setup,
            (self.config, self.source_kwargs, self.storage_kwargs, ['layer1'])
        )

        # Verify layer_names was called to validate layers
        self.config.layer_names.assert_called_once()

    @patch('tilekiln.generator.mp.Pool')
    def test_generate_multiple_layers(self, mock_pool):
        """Test generating tiles with multiple layer filtering"""
        # Set up mock pool
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        # Call generate with multiple layers specified
        tilekiln.generator.generate(
            self.config,
            self.source_kwargs,
            self.storage_kwargs,
            self.tiles,
            1,
            ['layer1', 'layer3']
        )

        # Verify setup called with the correct parameters
        mock_pool.assert_called_once_with(
            1,
            tilekiln.generator.setup,
            (self.config, self.source_kwargs, self.storage_kwargs, ['layer1', 'layer3'])
        )

        # Verify layer_names was called to validate layers
        self.config.layer_names.assert_called_once()

    @patch('tilekiln.generator.mp.Pool')
    def test_generate_unknown_layer(self, mock_pool):
        """Test generating tiles with unknown layer name raises error"""
        # Call generate with an unknown layer
        with self.assertRaises(ValueError):
            tilekiln.generator.generate(
                self.config,
                self.source_kwargs,
                self.storage_kwargs,
                self.tiles,
                1,
                ['layer1', 'unknown_layer']
            )

        # Verify pool was not called since validation failed
        mock_pool.assert_not_called()

        # Verify layer_names was called to validate layers
        self.config.layer_names.assert_called_once()
