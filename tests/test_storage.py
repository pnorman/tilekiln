import json
import queue
from unittest import TestCase

from tilekiln.storage import Storage
from tilekiln.tile import Tile
from tilekiln.metric import Metric
import tilekiln.errors


class FakeCursor:
    def __init__(self, calls, rets):
        self.calls = calls
        self.rets = rets

    def execute(self, query, vars=None, binary=None):
        try:
            self.calls.append(query.as_string())
        except AttributeError:
            self.calls.append(query)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.rets.get_nowait()
        except queue.Empty:
            raise StopIteration

    def fetchone(self):
        return next(self)


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

    def cursor(self, row_factory=None):
        return self.curs

    def commit(self):
        pass


class FakeConnCM:
    def __init__(self, calls, rets):
        self.conn = FakeConnection(calls, rets)

    def __enter__(self):
        return self.conn

    def __exit__(*args):
        pass


class FakePool:
    def __init__(self, calls: list[str], rets: queue.Queue[dict[str, str]]):
        self.cm = FakeConnCM(calls, rets)

    def connection(self):
        return self.cm


class TestStorageUtilityFunctions(TestCase):
    """Tests for the utility functions in the storage module"""

    def test_data_columns(self):
        from tilekiln.storage import data_columns

        # Test with a single layer
        result = data_columns(["layer1"])
        self.assertEqual(result.as_string(), '"layer1_data"')

        # Test with multiple layers
        result = data_columns(["layer1", "layer2", "layer3"])
        self.assertEqual(
            result.as_string(), '"layer1_data", "layer2_data", "layer3_data"'
        )

        # Test with empty list
        result = data_columns([])
        self.assertEqual(result.as_string(), "")

    def test_generated_columns(self):
        from tilekiln.storage import generated_columns

        # Test with a single layer
        result = generated_columns(["layer1"])
        self.assertEqual(result.as_string(), '"layer1_generated"')

        # Test with multiple layers
        result = generated_columns(["layer1", "layer2", "layer3"])
        self.assertEqual(
            result.as_string(),
            '"layer1_generated", "layer2_generated", "layer3_generated"',
        )

        # Test with empty list
        result = generated_columns([])
        self.assertEqual(result.as_string(), "")


class TestStorage(TestCase):
    def test_schema(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        storage.create_schema()

        self.assertRegex(calls[0], r"(?ims)CREATE SCHEMA.*tilekiln")
        self.assertRegex(calls[1], r"(?ims)CREATE.*TABLE.*generate_stats.*id.*zoom.*")
        self.assertRegex(calls[2], r"(?ims)CREATE.*TABLE.*tile_stats.*id.*zoom.*")
        self.assertRegex(calls[3], r"(?ims)CREATE.*TABLE.*metadata.*")
        self.assertRegex(calls[3], r"id text")
        self.assertRegex(calls[3], r"active boolean")
        self.assertRegex(calls[3], r"layers text\[\]")
        self.assertRegex(calls[3], r"minzoom smallint")
        self.assertRegex(calls[3], r"maxzoom smallint")
        calls.clear()

    def test_tileset(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        storage.create_tileset("foo", ["lyr1", "lyr2"], 0, 2, "{}")

        self.assertRegex(calls[0], r"(?ims)INSERT INTO.*metadata.*VALUES.*ON CONFLICT")
        self.assertRegex(calls[1], r"(?ims)CREATE TABLE.*foo.*")
        self.assertRegex(calls[1], r"""(?ims)"lyr1_generated" timestamptz""")
        self.assertRegex(calls[1], r"""(?ims)"lyr1_data" bytea""")
        # Check timestamps are before tile data for storage reasons
        self.assertRegex(calls[1], r"""(?ims)timestamptz.*bytea""")
        self.assertNotRegex(calls[1], r"""(?ims)bytea.*timestamptz""")

        self.assertRegex(calls[2], r"(?ims)CREATE TABLE.*foo_z0")
        self.assertRegex(calls[2], r"(?ims)PARTITION OF.*foo")
        self.assertRegex(calls[2], r"(?ims)FOR VALUES IN \(0\)")
        self.assertRegex(calls[3], r"(?ims)CREATE TABLE.*foo_z1")
        self.assertRegex(calls[3], r"(?ims)PARTITION OF.*foo")
        self.assertRegex(calls[3], r"(?ims)FOR VALUES IN \(1\)")
        self.assertRegex(calls[4], r"(?ims)CREATE TABLE.*foo_z2")
        self.assertRegex(calls[4], r"(?ims)PARTITION OF.*foo")
        self.assertRegex(calls[4], r"(?ims)FOR VALUES IN \(2\)")
        calls.clear()

        storage.remove_tileset("foo")

        self.assertRegex(calls[0], r"(?ims)DELETE FROM.*metadata.*id")
        self.assertRegex(calls[1], r"(?ims)DROP TABLE.*foo")
        self.assertRegex(calls[2], r"(?ims)DELETE FROM.*tile_stats.*id")
        calls.clear()

        rets.put({"id": "foo"})
        rets.put({"id": "bar"})

        ids = storage.get_tileset_ids()

        self.assertEqual(next(ids), "foo")
        self.assertEqual(next(ids), "bar")
        self.assertRegex(calls[0], r"(?ims)SELECT id.*metadata")

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 2,
                "tilejson": json.loads("{}"),
            }
        )

        tilesets = storage.get_tilesets()
        tileset = next(tilesets)
        self.assertRegex(calls[0], r"(?ims)SELECT id.*metadata")
        self.assertEqual(tileset.id, "foo")
        self.assertEqual(tileset.layers, ["lyr1", "lyr2"])
        self.assertEqual(tileset.minzoom, 0)
        self.assertEqual(tileset.maxzoom, 2)
        self.assertEqual(tileset.tilejson, "{}")

    def test_metadata(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)
        storage.set_metadata("foo", ["lyr1", "lyr2"], 0, 3, "{}")

        self.assertRegex(calls[0], r"(?ims)INSERT INTO.*metadata.*VALUES.*ON CONFLICT")
        calls.clear()

    def test_tiles(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 2,
                "tilejson": json.loads("{}"),
            }
        )
        rets.put({"lyr1_data": b"bar", "lyr2_data": b"baz", "generated": "datetime"})
        storage = Storage(pool)
        result, generated = storage.get_tile("foo", Tile(0, 0, 0))
        self.assertEqual(result["lyr1"], b"bar")
        self.assertEqual(result["lyr2"], b"baz")
        self.assertEqual(generated, "datetime")

        # calls[0] is get_tileset call tested above. TODO: test it above
        self.assertRegex(
            calls[1],
            r"(?ims)SELECT.*lyr1_generated.*.*lyr1_data.*FROM.*foo.*WHERE.*zoom",
        )

        calls.clear()
        while not rets.empty():
            queue.get()

        # Test no tile found
        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 2,
                "tilejson": json.loads("{}"),
            }
        )
        rets.put(None)
        self.assertEqual(storage.get_tile("foo", Tile(0, 0, 0)), (None, None))

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 2,
                "tilejson": json.loads("{}"),
            }
        )
        rets.put({"generated": "datetime"})

        self.assertEqual(
            storage.save_tile("foo", Tile(2, 1, 0), {"lyr1": b"bar", "lyr2": b"baz"}),
            "datetime",
        )
        self.assertRegex(calls[0], r"(?ims)minzoom.*maxzoom")
        self.assertRegex(calls[1], r"(?ims)INSERT INTO.*foo_z2")
        # Test colums are right
        self.assertRegex(
            calls[1], r"(?ms)\(zoom[^\)]+x[^\)]+y[^\)]+lyr1_data[^\)]+lyr2_data[^\)]*\)"
        )
        self.assertRegex(
            calls[1],
            r"""(?ims)VALUES\s+\(\s*2,\s+1,\s+0,\s+"""
            r"""\%\([^\)]*\)s,\s+\%\([^\)]*\)s\s*\)""",
        )
        self.assertRegex(calls[1], r"(?ims)ON CONFLICT\s+\(zoom,\s+x,\s+y\s*\)")
        # Test that the upsert sets data to something based on excluded
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr1_data[^,]+=[^,]*EXCLUDED[^,]+lyr1_data"
        )
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr2_data[^,]+=[^,]*EXCLUDED[^,]+lyr2_data"
        )

        # test that upserts sets generated to something based on stored and new lyr1_generated,
        # statement_timestamp, and that old generated is referenced
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr1_generated[^,]*=[^,]*STORE\.[^,]*lyr1_data"
        )
        self.assertRegex(
            calls[1],
            r"(?ims)DO UPDATE.*lyr1_generated[^,]*=[^,]*EXCLUDED\.[^,]*lyr1_data",
        )
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr1_generated[^,]+=[^,]*statement_timestamp"
        )
        self.assertRegex(
            calls[1],
            r"(?ims)DO UPDATE.*lyr1_generated[^,]+=[^,]*STORE\.[^,]*lyr1_generated",
        )
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr2_generated[^,]*=[^,]*STORE\.[^,]*lyr2_data"
        )
        self.assertRegex(
            calls[1],
            r"(?ims)DO UPDATE.*lyr2_generated[^,]*=[^,]*EXCLUDED\.[^,]*lyr2_data",
        )
        self.assertRegex(
            calls[1], r"(?ims)DO UPDATE.*lyr2_generated[^,]+=[^,]*statement_timestamp"
        )
        self.assertRegex(
            calls[1],
            r"(?ims)DO UPDATE.*lyr2_generated[^,]+=[^,]*STORE\.[^,]*lyr2_generated",
        )

        self.assertRegex(calls[1], r"(?ims)RETURNING.*lyr1_generated")
        self.assertRegex(calls[1], r"(?ims)RETURNING.*lyr2_generated")

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 2,
                "tilejson": json.loads("{}"),
            }
        )
        rets.put({"generated": "datetime"})

        # Check that an exception is raised if trying to save a tile with missing layers
        self.assertRaises(
            tilekiln.errors.Error,
            storage.save_tile,
            "foo",
            Tile(2, 1, 0),
            {"lyr1": b"bar"},
            "datetime",
        )

    def test_metrics(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        rets.put(
            {
                "id": "foo",
                "zoom": 0,
                "num_tiles": 1,
                "size": 1024,
                "percentiles": [0, 1, 2],
            }
        )
        rets.put(
            {
                "id": "foo",
                "zoom": 1,
                "num_tiles": 4,
                "size": 4096,
                "percentiles": [0, 1, 2],
            }
        )
        metrics = storage.metrics()
        self.assertEqual(
            metrics[0],
            Metric(id="foo", zoom=0, num_tiles=1, size=1024, percentiles=[0, 1, 2]),
        )
        self.assertEqual(
            metrics[1],
            Metric(id="foo", zoom=1, num_tiles=4, size=4096, percentiles=[0, 1, 2]),
        )

        self.assertRegex(
            calls[0], r"(?ims)SELECT.*id.*zoom.*num_tiles.*size.*percentiles"
        )
        self.assertRegex(calls[0], r"(?ims)FROM.*tile_stats")
        # update_metrics

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 1,
                "tilejson": json.loads("{}"),
            }
        )

        storage.update_metrics()

        # calls 0 and 1 are get_tileset and JIT statements
        self.assertRegex(calls[2], r"(?i)INSERT INTO.*tile_stats")
        self.assertRegex(
            calls[2],
            r"""(?ims)SUM\(length\("?lyr1_data"?\)"""
            r"""\+length\("?lyr2_data"?\)\)""",
        )
        self.assertRegex(
            calls[2], r"""(?ims)ARRAY\[.*COALESCE\(PERCENTILE_CONT\(.*\).*\).*\]"""
        )
        self.assertRegex(calls[2], r"(?i)FROM.*foo_z0")
        # call 3 is JIT
        self.assertRegex(calls[4], r"(?i)FROM.*foo_z1")

    def test_delete_tiles(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)
        tiles = {Tile(5, 10, 20), Tile(5, 11, 21)}

        storage.delete_tiles("foo", tiles)

        self.assertEqual(len(calls), 2)
        self.assertRegex(
            calls[0],
            r'DELETE FROM "tilekiln"."foo"\s+WHERE zoom = %s AND x = %s AND y = %s',
        )
        self.assertRegex(
            calls[1],
            r'DELETE FROM "tilekiln"."foo"\s+WHERE zoom = %s AND x = %s AND y = %s',
        )

    def test_truncate_tables(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        # Setup return values for get_minzoom and get_maxzoom
        rets.put({"minzoom": 5})
        rets.put({"maxzoom": 7})

        storage = Storage(pool)

        # Test with default zoom range (uses minzoom and maxzoom)
        storage.truncate_tables("foo")

        # First two calls are for getting minzoom and maxzoom
        self.assertEqual(len(calls), 5)  # 2 for min/max zoom + 3 for truncate calls
        self.assertRegex(calls[0], r"SELECT minzoom")
        self.assertRegex(calls[1], r"SELECT maxzoom")
        self.assertRegex(calls[2], r'TRUNCATE TABLE "tilekiln"."foo_z5"')
        self.assertRegex(calls[3], r'TRUNCATE TABLE "tilekiln"."foo_z6"')
        self.assertRegex(calls[4], r'TRUNCATE TABLE "tilekiln"."foo_z7"')

        # Clear calls and setup for specific zoom test
        calls.clear()

        # Test with specific zooms
        storage.truncate_tables("foo", [6, 7])

        self.assertEqual(len(calls), 2)
        self.assertRegex(calls[0], r'TRUNCATE TABLE "tilekiln"."foo_z6"')
        self.assertRegex(calls[1], r'TRUNCATE TABLE "tilekiln"."foo_z7"')

    def test_get_tilejson(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        # Setup return value for get_tilejson
        rets.put({"tilejson": {"version": "2.0", "name": "test"}})

        # Test successful retrieval
        result = storage.get_tilejson("foo", "https://example.com/tiles")

        self.assertEqual(len(calls), 1)
        self.assertRegex(
            calls[0], r'SELECT tilejson\s+FROM "tilekiln"."metadata"\s+WHERE id = %s'
        )

        # Check that URL was added to tilejson
        expected_json = (
            '{"version": "2.0", "name": "test"'
            ', "tiles": ["https://example.com/tiles/{z}/{x}/{y}.mvt"]}'
        )
        self.assertEqual(result, expected_json)

        # Test missing tileset (should use click.echo and sys.exit)
        calls.clear()
        rets.put(None)  # No result returned

        # We can't easily test sys.exit, so we'll just check the query was made
        with self.assertRaises(SystemExit):
            storage.get_tilejson("missing", "https://example.com/tiles")

        self.assertEqual(len(calls), 1)
        self.assertRegex(
            calls[0], r'SELECT tilejson\s+FROM "tilekiln"."metadata"\s+WHERE id = %s'
        )

    def test_save_tile_sql_construction(self):
        """Test the SQL construction in save_tile method"""
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 0,
                "maxzoom": 14,
                "tilejson": json.loads("{}"),
            }
        )
        rets.put({"generated": "datetime"})

        storage = Storage(pool)
        tile = Tile(5, 10, 20)

        # Test with multiple layers
        result = storage.save_tile("foo", tile, {"lyr1": b"data1", "lyr2": b"data2"})

        self.assertEqual(result, "datetime")

        # First call is to get_tileset, second is the INSERT with ON CONFLICT
        self.assertEqual(len(calls), 2)

        # Detailed checks on the SQL construction
        self.assertRegex(calls[1], r"INSERT INTO .* AS store")
        self.assertRegex(calls[1], r'\(zoom, x, y, "lyr1_data", "lyr2_data"\)')
        self.assertRegex(calls[1], r"VALUES \(5, 10, 20, %\(lyr1\)s, %\(lyr2\)s\)")
        self.assertRegex(calls[1], r"ON CONFLICT \(zoom, x, y\)")

        # Check the UPDATE part for timestamp update logic
        self.assertRegex(
            calls[1],
            r'"lyr1_generated" = CASE WHEN store."lyr1_data" != EXCLUDED."lyr1_data"',
        )
        self.assertRegex(
            calls[1], r'THEN statement_timestamp\(\) ELSE store."lyr1_generated" END'
        )
        self.assertRegex(
            calls[1],
            r'"lyr2_generated" = CASE WHEN store."lyr2_data" != EXCLUDED."lyr2_data"',
        )
        self.assertRegex(
            calls[1], r'THEN statement_timestamp\(\) ELSE store."lyr2_generated" END'
        )

    def test_save_tile_validation(self):
        """Test validation in save_tile method"""
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        # Test zoom outside range
        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 5,
                "maxzoom": 10,
                "tilejson": json.loads("{}"),
            }
        )

        # Test zoom too low (ensure x,y coordinates are valid for the zoom)
        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            # For zoom 4, valid coordinates are 0-15
            storage.save_tile(
                "foo", Tile(4, 10, 10), {"lyr1": b"data1", "lyr2": b"data2"}
            )

        # Clear and test zoom too high
        calls.clear()
        while not rets.empty():
            rets.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 5,
                "maxzoom": 10,
                "tilejson": json.loads("{}"),
            }
        )

        with self.assertRaises(tilekiln.errors.ZoomNotDefined):
            # For zoom 11, valid coordinates are 0-2047
            storage.save_tile(
                "foo", Tile(11, 100, 100), {"lyr1": b"data1", "lyr2": b"data2"}
            )

        # Test layers mismatch
        calls.clear()
        while not rets.empty():
            rets.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 5,
                "maxzoom": 10,
                "tilejson": json.loads("{}"),
            }
        )

        with self.assertRaises(tilekiln.errors.Error):
            # For zoom 5, valid coordinates are 0-31
            storage.save_tile(
                "foo", Tile(5, 10, 10), {"lyr1": b"data1"}
            )  # Missing lyr2

        # Test extra layer
        calls.clear()
        while not rets.empty():
            rets.get()

        rets.put(
            {
                "id": "foo",
                "layers": ["lyr1", "lyr2"],
                "minzoom": 5,
                "maxzoom": 10,
                "tilejson": json.loads("{}"),
            }
        )

        with self.assertRaises(tilekiln.errors.Error):
            # For zoom 5, valid coordinates are 0-31
            storage.save_tile(
                "foo",
                Tile(5, 10, 10),
                {"lyr1": b"data1", "lyr2": b"data2", "lyr3": b"data3"},
            )
