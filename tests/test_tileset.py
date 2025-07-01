import queue
import datetime
from unittest import TestCase, mock
from typing import List, Dict, Any, Optional, Union, cast

from tilekiln.tileset import Tileset
from tilekiln.storage import Storage
from tilekiln.config import Config
from tilekiln.tile import Tile
import tilekiln.errors


class FakeStorage:
    """
    A fake implementation of Storage for testing purposes.
    """
    def __init__(self, calls: List[str], rets: queue.SimpleQueue) -> None:
        self.calls = calls
        self.rets = rets

    def get_tileset(self, id: str) -> Any:
        self.calls.append(f"get_tileset_{id}")
        return self.rets.get()

    def save_tile(
        self,
        id: str,
        tile: Tile,
        _layers: Dict[str, bytes],
        _render_time: int = 0
    ) -> Optional[Union[datetime.datetime, str]]:
        self.calls.append(f"save_tile_{id}_{tile}")
        return self.rets.get() if not self.rets.empty() else None


class TestTileset(TestCase):
    def test_creation(self):
        # Test basic creation
        storage = mock.Mock(spec=Storage)
        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

        self.assertEqual(tileset.id, "test_id")
        self.assertEqual(tileset.layers, ["layer1", "layer2"])
        self.assertEqual(tileset.minzoom, 0)
        self.assertEqual(tileset.maxzoom, 14)
        self.assertEqual(tileset.tilejson, '{"name": "test"}')
        self.assertEqual(tileset.storage, storage)

    def test_from_config(self):
        # Test creation from config
        storage = mock.Mock(spec=Storage)
        config = mock.Mock(spec=Config)
        config.id = "config_id"
        config.layer_names.return_value = ["layer1", "layer2"]
        config.minzoom = 0
        config.maxzoom = 14
        config.tilejson.return_value = '{"name": "test_config"}'

        tileset = Tileset.from_config(storage, config)

        self.assertEqual(tileset.id, "config_id")
        self.assertEqual(tileset.layers, ["layer1", "layer2"])
        self.assertEqual(tileset.minzoom, 0)
        self.assertEqual(tileset.maxzoom, 14)
        self.assertEqual(tileset.tilejson, '{"name": "test_config"}')
        self.assertEqual(tileset.storage, storage)
        config.tilejson.assert_called_once_with("REPLACED_BY_SERVER")

    def test_save_tile(self):
        # Test saving a tile
        calls: List[str] = []
        rets = queue.SimpleQueue()
        storage = cast(Storage, FakeStorage(calls, rets))  # Cast to Storage for type checking

        # Mock the datetime returned by save_tile
        rets.put("2023-01-01T12:00:00")

        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )
        tile = Tile(10, 123, 456)
        layers_data = {"layer1": b"data1", "layer2": b"data2"}

        result = tileset.save_tile(tile, layers_data)

        self.assertEqual(result, "2023-01-01T12:00:00")
        self.assertEqual(calls[0], f"save_tile_test_id_{tile}")

    def test_get_tile(self):
        # Test retrieving a tile
        storage = mock.Mock(spec=Storage)

        # Mock the return value of get_tile
        storage.get_tile.return_value = (
            {"layer1": b"data1", "layer2": b"data2"},
            "2023-01-01T12:00:00",
        )

        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )
        tile = Tile(10, 123, 456)

        layers_data, timestamp = tileset.get_tile(tile)

        self.assertEqual(layers_data, {"layer1": b"data1", "layer2": b"data2"})
        self.assertEqual(timestamp, "2023-01-01T12:00:00")
        storage.get_tile.assert_called_once_with("test_id", tile)

    def test_tilejson_modification(self):
        # Test tilejson property with modification
        storage = mock.Mock(spec=Storage)
        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

        # Test getting the tilejson
        self.assertEqual(tileset.tilejson, '{"name": "test"}')

        # Test setting the tilejson
        tileset.tilejson = '{"name": "modified"}'
        self.assertEqual(tileset.tilejson, '{"name": "modified"}')

    def test_prepare_storage(self):
        """Test prepare_storage method"""
        # Mock the storage
        storage = mock.Mock(spec=Storage)

        # Create a tileset
        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

        # Call prepare_storage
        tileset.prepare_storage()

        # Verify storage.create_tileset was called with the right arguments
        storage.create_tileset.assert_called_once_with(
            "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

    def test_update_storage_metadata(self):
        """Test update_storage_metadata method"""
        # Mock the storage
        storage = mock.Mock(spec=Storage)

        # Create a tileset
        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

        # Call update_storage_metadata
        tileset.update_storage_metadata()

        # Verify storage.set_metadata was called with the right arguments
        storage.set_metadata.assert_called_once_with(
            "test_id", ["layer1", "layer2"], 0, 14, '{"name": "test"}'
        )

    def test_get_tile_zoom_error(self):
        """Test get_tile method with zoom out of range"""
        # Mock the storage
        storage = mock.Mock(spec=Storage)

        # Create a tileset with zoom range 5-10
        tileset = Tileset(
            storage, "test_id", ["layer1", "layer2"], 5, 10, '{"name": "test"}'
        )

        # Test with zoom too low
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            tileset.get_tile(Tile(4, 10, 10))

        # Test with zoom too high
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            tileset.get_tile(Tile(11, 100, 100))

    def test_save_tile_zoom_error(self):
        """Test save_tile method with zoom out of range"""
        # Create mock storage
        storage = mock.Mock(spec=Storage)

        # Create a tileset with zoom range 5-10
        tileset = Tileset(
            storage=storage,
            id="test_id",
            layers=["layer1"],
            minzoom=5,
            maxzoom=10,
            tilejson='{"name":"test"}',
        )

        # Create test tiles with zoom levels outside the range
        tile_low = Tile(4, 10, 10)  # Below minzoom
        tile_high = Tile(11, 10, 10)  # Above maxzoom
        tile_valid = Tile(7, 10, 10)  # Within zoom range

        # Test with zoom too low
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            tileset.save_tile(tile_low, {"layer1": b"data"})

        # Test with zoom too high
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            tileset.save_tile(tile_high, {"layer1": b"data"})

        # Verify storage.save_tile was not called for invalid zooms
        storage.save_tile.assert_not_called()

        # Test with valid zoom
        layers_data = {"layer1": b"data"}
        tileset.save_tile(tile_valid, layers_data)

        # Verify storage.save_tile was called with correct parameters for valid zoom
        storage.save_tile.assert_called_once_with("test_id", tile_valid, layers_data)

    def test_from_id(self):
        """Test from_id class method"""
        # Mock the storage
        storage = mock.Mock(spec=Storage)

        # Set up return values for the mocked methods
        storage.get_layer_ids.return_value = ["layer1", "layer2"]
        storage.get_minzoom.return_value = 5
        storage.get_maxzoom.return_value = 14
        storage.get_tilejson.return_value = '{"name": "test_from_id"}'

        # Call from_id
        tileset = Tileset.from_id(storage, "test_id")

        # Verify storage methods were called with correct parameters
        storage.get_layer_ids.assert_called_once_with("test_id")
        storage.get_minzoom.assert_called_once_with("test_id")
        storage.get_maxzoom.assert_called_once_with("test_id")
        storage.get_tilejson.assert_called_once_with("test_id", "REPLACED_BY_SERVER")

        # Verify tileset is created with the right values
        self.assertEqual(tileset.id, "test_id")
        self.assertEqual(tileset.layers, ["layer1", "layer2"])
        self.assertEqual(tileset.minzoom, 5)
        self.assertEqual(tileset.maxzoom, 14)
        self.assertEqual(tileset.tilejson, '{"name": "test_from_id"}')
