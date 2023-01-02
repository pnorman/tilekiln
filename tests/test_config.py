from unittest import TestCase
from fs.memoryfs import MemoryFS
from tilekiln.config import Config, LayerConfig
from tilekiln.tile import Tile
import yaml


class TestConfig(TestCase):
    maxDiff = None

    def test_properties(self):
        with MemoryFS() as fs:
            c = Config('''{"metadata": {"id":"foo"}}''', fs)
            self.assertEqual(c.id, "foo")
            self.assertEqual(c.name, None)
            self.assertEqual(c.description, None)
            self.assertEqual(c.attribution, None)
            self.assertEqual(c.version, None)
            self.assertEqual(c.bounds, None)
            self.assertEqual(c.center, None)
            self.assertEqual(c.minzoom, None)
            self.assertEqual(c.maxzoom, None)

            self.assertEqual(c.tilejson("bar"), '''{
    "scheme": "xyz",
    "tilejson": "3.0.0",
    "tiles": [
        "bar/{z}/{x}/{y}.mvt"
    ],
    "vector_layers": []
}''')
        with MemoryFS() as fs:
            fs.writetext("blank.sql.jinja2", "")
            c_str = ('''{"metadata": {"id":"id", '''
                     '''"name": "name", '''
                     '''"description":"description", '''
                     '''"attribution":"attribution", "version": "1.0.0",'''
                     '''"bounds": [-180, -85, 180, 85], "center": [0, 0]},'''
                     '''"vector_layers": {"building":{'''
                     '''"description": "buildings",'''
                     '''"fields":{"foo": "bar"},'''
                     '''"sql": [{"minzoom":13, "maxzoom":14, "file": "blank.sql.jinja2"}]}}}''')

            # Check the test is valid yaml to save debugging
            yaml.safe_load(c_str)
            c = Config(c_str, fs)
            self.assertEqual(c.id, "id")
            self.assertEqual(c.name, "name")
            self.assertEqual(c.description, "description")
            self.assertEqual(c.attribution, "attribution")
            self.assertEqual(c.version, "1.0.0")
            self.assertEqual(c.bounds, [-180, -85, 180, 85])
            self.assertEqual(c.center, [0, 0])
            self.assertEqual(c.attribution, "attribution")
            self.assertEqual(c.minzoom, 13)
            self.assertEqual(c.maxzoom, 14)

            self.assertEqual(c.tilejson("foo"), '''{
    "attribution": "attribution",
    "bounds": [
        -180,
        -85,
        180,
        85
    ],
    "center": [
        0,
        0
    ],
    "description": "description",
    "maxzoom": 14,
    "minzoom": 13,
    "name": "name",
    "scheme": "xyz",
    "tilejson": "3.0.0",
    "tiles": [
        "foo/{z}/{x}/{y}.mvt"
    ],
    "vector_layers": [
        {
            "description": "buildings",
            "fields": {
                "foo": "bar"
            },
            "id": "building",
            "maxzoom": 14,
            "minzoom": 13
        }
    ]
}''')

            # Test without fields for the layer
            fs.writetext("blank.sql.jinja2", "")
            c_str = ('''{"metadata": {"id":"id", '''
                     '''"name": "name", '''
                     '''"description":"description", '''
                     '''"attribution":"attribution", "version": "1.0.0",'''
                     '''"bounds": [-180, -85, 180, 85], "center": [0, 0]},'''
                     '''"vector_layers": {"building":{'''
                     '''"sql": [{"minzoom":13, "maxzoom":14, "file": "blank.sql.jinja2"}]}}}''')

            # Check the test is valid yaml to save debugging
            yaml.safe_load(c_str)
            c = Config(c_str, fs)

            self.assertEqual(c.tilejson("foo"), '''{
    "attribution": "attribution",
    "bounds": [
        -180,
        -85,
        180,
        85
    ],
    "center": [
        0,
        0
    ],
    "description": "description",
    "maxzoom": 14,
    "minzoom": 13,
    "name": "name",
    "scheme": "xyz",
    "tilejson": "3.0.0",
    "tiles": [
        "foo/{z}/{x}/{y}.mvt"
    ],
    "vector_layers": [
        {
            "fields": {},
            "id": "building",
            "maxzoom": 14,
            "minzoom": 13
        }
    ]
}''')


class TestLayerConfig(TestCase):
    def test_render(self):
        with MemoryFS() as fs:
            fs.writetext("one.sql.jinja2", "one")
            fs.writetext("two.sql.jinja2", "two")
            layer = LayerConfig("foo", {"sql": [{"minzoom": 4, "maxzoom": 8,
                                                 "file": "one.sql.jinja2"}]}, fs)

            self.assertIsNone(layer.render_sql(Tile(2, 0, 0)))
            self.assertIsNotNone(layer.render_sql(Tile(6, 0, 0)))
            self.assertIsNone(layer.render_sql(Tile(10, 0, 0)))

            layer = LayerConfig("foo",
                                {"sql": [{"minzoom": 4, "maxzoom": 4, "file": "one.sql.jinja2"},
                                         {"minzoom": 6, "maxzoom": 6, "file": "two.sql.jinja2"}]},
                                fs)
            self.assertIsNone(layer.render_sql(Tile(3, 0, 0)))
            self.assertIsNone(layer.render_sql(Tile(5, 0, 0)))
            self.assertIsNone(layer.render_sql(Tile(7, 0, 0)))

            self.assertEqual(layer.render_sql(Tile(4, 0, 0)), '''WITH mvtgeom AS
(
one
)
SELECT ST_AsMVT(mvtgeom.*, 'foo', 4096, 'way', NULL)
FROM mvtgeom;''')
            self.assertEqual(layer.render_sql(Tile(6, 0, 0)), '''WITH mvtgeom AS
(
two
)
SELECT ST_AsMVT(mvtgeom.*, 'foo', 4096, 'way', NULL)
FROM mvtgeom;''')
