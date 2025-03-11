import unittest
from unittest.mock import patch, MagicMock

from tilekiln.generator import generate
from tilekiln.tile import Tile


class TestGeneratorLayerFilters(unittest.TestCase):
    # We'll skip the worker tests since they require direct patching of module globals
    # which is harder to do in test isolation, and we're testing the functionality via
    # the generate test anyway

    @patch('multiprocessing.Pool')
    def test_generate_without_layer_filters(self, mock_pool):
        # Test generate function without layer filters
        mock_config = MagicMock()
        mock_tiles = [Tile(10, 0, 0), Tile(10, 0, 1)]
        mock_source_kwargs = {"dbname": "test"}
        mock_storage_kwargs = {"dbname": "test"}
        mock_num_processes = 2

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        generate(mock_config, mock_source_kwargs, mock_storage_kwargs,
                 mock_tiles, mock_num_processes)

        # Should call starmap with work items
        mock_pool_instance.starmap.assert_called_once()
        args, kwargs = mock_pool_instance.starmap.call_args
        # Work items should be tiles with None for layer_filters
        work_items = list(args[1])
        self.assertEqual(len(work_items), 2)
        for item, tile in zip(work_items, mock_tiles):
            self.assertIsInstance(item, tuple)
            self.assertEqual(len(item), 2)
            self.assertEqual(item[0], tile)
            self.assertIsNone(item[1])

    @patch('multiprocessing.Pool')
    def test_generate_with_layer_filters(self, mock_pool):
        # Test generate function with layer filters
        mock_config = MagicMock()
        mock_tiles = [Tile(10, 0, 0), Tile(10, 0, 1)]
        mock_source_kwargs = {"dbname": "test"}
        mock_storage_kwargs = {"dbname": "test"}
        mock_num_processes = 2
        layer_filters = {"layer1", "layer2"}

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        generate(mock_config, mock_source_kwargs, mock_storage_kwargs,
                 mock_tiles, mock_num_processes, layer_filters)

        # Should call starmap with (tile, layer_filters) tuples
        mock_pool_instance.starmap.assert_called_once()
        args, kwargs = mock_pool_instance.starmap.call_args

        # Check that work items are correctly formatted with tile and layer filters
        work_items = list(args[1])
        self.assertEqual(len(work_items), 2)
        for item in work_items:
            self.assertIsInstance(item, tuple)
            self.assertEqual(len(item), 2)
            self.assertIsInstance(item[0], Tile)
            self.assertEqual(item[1], layer_filters)


if __name__ == '__main__':
    unittest.main()
