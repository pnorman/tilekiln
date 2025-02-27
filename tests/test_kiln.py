import queue
from unittest import TestCase, mock

import tilekiln.errors
from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.tile import Tile


class FakeCursor:
    def __init__(self, calls, rets):
        self.calls = calls
        self.rets = rets
        self.binary = None

    def execute(self, query, binary=None):
        self.calls.append(query)
        self.binary = binary

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.rets.get_nowait()
        except queue.Empty:
            raise StopIteration


class FakeCursCM:
    def __init__(self, calls, rets):
        self.curs = FakeCursor(calls, rets)

    def __enter__(self):
        return self.curs

    def __exit__(*args):
        pass


class FakeConnection:
    def __init__(self, calls, rets):
        self.curs = FakeCursCM(calls, rets)

    def cursor(self):
        return self.curs


class FakeConnCM:
    def __init__(self, calls, rets):
        self.conn = FakeConnection(calls, rets)

    def __enter__(self):
        return self.conn

    def __exit__(*args):
        pass


class FakePool:
    def __init__(self, calls, rets):
        self.cm = FakeConnCM(calls, rets)

    def connection(self):
        return self.cm


class TestKiln(TestCase):
    def test_render_all(self):
        # Setup
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Mock the Config class
        config = mock.Mock(spec=Config)
        config.minzoom = 0
        config.maxzoom = 14
        config.layer_queries.return_value = {
            "layer1": "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt;",
            "layer2": "SELECT ST_AsMVT(mvtgeom, 'layer2') FROM layer2_mvt;",
        }

        # Add return values for the queries
        rets.put(["layer1_mvt_data".encode()])
        rets.put(["layer2_mvt_data".encode()])

        kiln = Kiln(config, pool)
        tile = Tile(10, 123, 456)

        # Execute
        result = kiln.render_all(tile)

        # Verify
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            calls[0], "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt;"
        )
        self.assertEqual(
            calls[1], "SELECT ST_AsMVT(mvtgeom, 'layer2') FROM layer2_mvt;"
        )
        self.assertEqual(
            result, {"layer1": b"layer1_mvt_data", "layer2": b"layer2_mvt_data"}
        )
        self.assertTrue(config.layer_queries.called)
        config.layer_queries.assert_called_once_with(tile)

    def test_render_layer(self):
        # Setup
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Mock the Config class
        config = mock.Mock(spec=Config)
        config.layer_query.return_value = (
            "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt;"
        )

        # Add return value for the query
        rets.put(["layer1_mvt_data".encode()])

        kiln = Kiln(config, pool)
        tile = Tile(10, 123, 456)

        # Execute
        result = kiln.render_layer("layer1", tile)

        # Verify
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0], "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt;"
        )
        self.assertEqual(result, b"layer1_mvt_data")
        self.assertTrue(config.layer_query.called)
        config.layer_query.assert_called_once_with("layer1", tile)

    def test_render_sql_empty_layer(self):
        # Setup
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Mock the Config class
        config = mock.Mock(spec=Config)
        config.minzoom = 0
        config.maxzoom = 14
        config.layer_queries.return_value = {
            "layer1": None  # None indicates layer not present at this zoom
        }

        kiln = Kiln(config, pool)
        tile = Tile(10, 123, 456)

        # Execute
        result = kiln.render_all(tile)

        # Verify
        self.assertEqual(len(calls), 0)  # No SQL should be executed
        self.assertEqual(result, {"layer1": b""})

    def test_zoom_outside_range(self):
        # Setup
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Mock the Config class
        config = mock.Mock(spec=Config)
        config.minzoom = 5
        config.maxzoom = 14

        kiln = Kiln(config, pool)

        # Try with zoom below minzoom
        # For zoom 4, valid x,y coordinates are 0-15 (2^4-1)
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            kiln.render_all(Tile(4, 10, 10))

        # Try with zoom above maxzoom
        # For zoom 15, valid x,y coordinates are 0-32767 (2^15-1)
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            kiln.render_all(Tile(15, 1000, 1000))

    def test_empty_result(self):
        # Setup
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Mock the Config class
        config = mock.Mock(spec=Config)
        config.minzoom = 0
        config.maxzoom = 14
        config.layer_queries.return_value = {
            "layer1": "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt WHERE 1=0;"  # Empty
        }

        kiln = Kiln(config, pool)
        tile = Tile(10, 123, 456)

        # Add empty result (cursor will return no rows)

        # This should raise a RuntimeError
        with self.assertRaises(RuntimeError):
            kiln.render_all(tile)

        # Verify the SQL was executed
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0], "SELECT ST_AsMVT(mvtgeom, 'layer1') FROM layer1_mvt WHERE 1=0;"
        )
