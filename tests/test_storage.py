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
        self.assertRegex(calls[1], r'''(?ims)"lyr1_generated" timestamptz''')
        self.assertRegex(calls[1], r'''(?ims)"lyr1_data" bytea''')
        # Check timestamps are before tile data for storage reasons
        self.assertRegex(calls[1], r'''(?ims)timestamptz.*bytea''')
        self.assertNotRegex(calls[1], r'''(?ims)bytea.*timestamptz''')

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

        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 2,
                  "tilejson": json.loads("{}")
                  })

        tilesets = storage.get_tilesets()
        tileset = next(tilesets)
        self.assertRegex(calls[0], r"(?ims)SELECT id.*metadata")
        self.assertEqual(tileset.id, "foo")
        self.assertEqual(tileset.layers, ["lyr1", "lyr2"])
        self.assertEqual(tileset.minzoom, 0)
        self.assertEqual(tileset.maxzoom, 2)
        self.assertEqual(tileset.tilejson, '{}')

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

        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 2,
                  "tilejson": json.loads("{}")
                  })
        rets.put({"lyr1_data": b"bar", "lyr2_data": b"baz", "generated": "datetime"})
        storage = Storage(pool)
        result, generated = storage.get_tile("foo", Tile(0, 0, 0))
        self.assertEqual(result["lyr1"], b"bar")
        self.assertEqual(result["lyr2"], b"baz")
        self.assertEqual(generated, "datetime")

        # calls[0] is get_tileset call tested above. TODO: test it above
        self.assertRegex(calls[1],
                         r"(?ims)SELECT.*lyr1_generated.*.*lyr1_data.*FROM.*foo.*WHERE.*zoom")

        calls.clear()
        while not rets.empty():
            queue.get()

        # Test no tile found
        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 2,
                  "tilejson": json.loads("{}")
                  })
        rets.put(None)
        self.assertEqual(storage.get_tile("foo", Tile(0, 0, 0)), (None, None))

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 2,
                  "tilejson": json.loads("{}")
                  })
        rets.put({"generated": "datetime"})

        self.assertEqual(storage.save_tile("foo", Tile(2, 1, 0),
                                           {"lyr1": b"bar", "lyr2": b"baz"}), "datetime")
        self.assertRegex(calls[0], r"(?ims)minzoom.*maxzoom")
        self.assertRegex(calls[1], r"(?ims)INSERT INTO.*foo_z2")
        # Test colums are right
        self.assertRegex(calls[1],
                         r"(?ms)\(zoom[^\)]+x[^\)]+y[^\)]+lyr1_data[^\)]+lyr2_data[^\)]*\)")
        self.assertRegex(calls[1],
                         r'''(?ims)VALUES\s+\(\s*2,\s+1,\s+0,\s+'''
                         r'''\%\([^\)]*\)s,\s+\%\([^\)]*\)s\s*\)''')
        self.assertRegex(calls[1],
                         r"(?ims)ON CONFLICT\s+\(zoom,\s+x,\s+y\s*\)")
        # Test that the upsert sets data to something based on excluded
        self.assertRegex(calls[1], r"(?ims)DO UPDATE.*lyr1_data[^,]+=[^,]*EXCLUDED[^,]+lyr1_data")
        self.assertRegex(calls[1], r"(?ims)DO UPDATE.*lyr2_data[^,]+=[^,]*EXCLUDED[^,]+lyr2_data")

        # test that upserts sets generated to something based on stored and new lyr1_generated,
        # statement_timestamp, and that old generated is referenced
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr1_generated[^,]*=[^,]*STORE\.[^,]*lyr1_data")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr1_generated[^,]*=[^,]*EXCLUDED\.[^,]*lyr1_data")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr1_generated[^,]+=[^,]*statement_timestamp")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr1_generated[^,]+=[^,]*STORE\.[^,]*lyr1_generated")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr2_generated[^,]*=[^,]*STORE\.[^,]*lyr2_data")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr2_generated[^,]*=[^,]*EXCLUDED\.[^,]*lyr2_data")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr2_generated[^,]+=[^,]*statement_timestamp")
        self.assertRegex(calls[1],
                         r"(?ims)DO UPDATE.*lyr2_generated[^,]+=[^,]*STORE\.[^,]*lyr2_generated")

        self.assertRegex(calls[1],
                         r"(?ims)RETURNING.*lyr1_generated")
        self.assertRegex(calls[1],
                         r"(?ims)RETURNING.*lyr2_generated")

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 2,
                  "tilejson": json.loads("{}")
                  })
        rets.put({"generated": "datetime"})

        # Check that an exception is raised if trying to save a tile with missing layers
        self.assertRaises(tilekiln.errors.Error, storage.save_tile, "foo", Tile(2, 1, 0),
                          {"lyr1": b"bar"}, "datetime")

    def test_metrics(self):
        calls = []
        rets = queue.SimpleQueue()
        pool = FakePool(calls, rets)

        storage = Storage(pool)

        rets.put({"id": "foo",
                  "zoom": 0,
                  "num_tiles": 1,
                  "size": 1024,
                  "percentiles": [0, 1, 2]})
        rets.put({"id": "foo",
                  "zoom": 1,
                  "num_tiles": 4,
                  "size": 4096,
                  "percentiles": [0, 1, 2]})
        metrics = storage.metrics()
        self.assertEqual(metrics[0], Metric(id="foo", zoom=0, num_tiles=1,
                         size=1024, percentiles=[0, 1, 2]))
        self.assertEqual(metrics[1], Metric(id="foo", zoom=1, num_tiles=4,
                         size=4096, percentiles=[0, 1, 2]))

        self.assertRegex(calls[0], r"(?ims)SELECT.*id.*zoom.*num_tiles.*size.*percentiles")
        self.assertRegex(calls[0], r"(?ims)FROM.*tile_stats")
        # update_metrics

        calls.clear()
        while not rets.empty():
            queue.get()

        rets.put({"id": "foo",
                  "layers": ["lyr1", "lyr2"],
                  "minzoom": 0,
                  "maxzoom": 1,
                  "tilejson": json.loads("{}")
                  })

        storage.update_metrics()

        # calls 0 and 1 are get_tileset and JIT statements
        self.assertRegex(calls[2], r"(?i)INSERT INTO.*tile_stats")
        self.assertRegex(calls[2], r'''(?ims)SUM\(length\("?lyr1_data"?\)'''
                                   r'''\+length\("?lyr2_data"?\)\)''')
        self.assertRegex(calls[2], r'''(?ims)ARRAY\[.*COALESCE\(PERCENTILE_CONT\(.*\).*\).*\]''')
        self.assertRegex(calls[2], r"(?i)FROM.*foo_z0")
        # call 3 is JIT
        self.assertRegex(calls[4], r"(?i)FROM.*foo_z1")
