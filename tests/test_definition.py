from unittest import TestCase
from fs.memoryfs import MemoryFS
from tilekiln.definition import Definition
from tilekiln.tile import Tile


class TestDefinition(TestCase):
    def test_attributes(self):
        with MemoryFS() as fs:
            fs.writetext("blank.sql.jinja2", "")
            d = Definition("foo", {"minzoom": 1, "maxzoom": 3, "extent": 1024, "buffer": 8,
                                   "file": "blank.sql.jinja2"}, fs)
            self.assertEqual(d.id, "foo")
            self.assertEqual(d.minzoom, 1)
            self.assertEqual(d.maxzoom, 3)
            self.assertEqual(d.extent, 1024)
            self.assertEqual(d.buffer, 8)

            d = Definition("bar", {"minzoom": 2, "maxzoom": 4,
                                   "file": "blank.sql.jinja2"}, fs)
            self.assertEqual(d.id, "bar")
            self.assertEqual(d.minzoom, 2)
            self.assertEqual(d.maxzoom, 4)
            self.assertEqual(d.extent, 4096)
            self.assertEqual(d.buffer, 0)

    def test_render(self):
        with MemoryFS() as fs:
            fs.writetext("one.sql.jinja2", "SELECT 1")
            d = Definition("one", {"minzoom": 1, "maxzoom": 3, "extent": 1024, "buffer": 8,
                                   "file": "one.sql.jinja2"}, fs)
            expected = '''WITH mvtgeom AS
(
SELECT 1
)
SELECT ST_AsMVT(mvtgeom.*, 'one', 1024, 'way', NULL)
FROM mvtgeom;'''
            self.assertEqual(d.render_sql(Tile(2, 0, 0)), expected)

            fs.writetext("two.sql.jinja2", "SELECT {{zoom}}/{{x}}/{{y}}\n{{bbox}}\n" +
                                           "{{unbuffered_bbox}}\n{{extent}}\n{{buffer}}")
            d = Definition("two", {"minzoom": 1, "maxzoom": 3, "extent": 1024, "buffer": 256,
                                   "file": "two.sql.jinja2"}, fs)
            expected = '''WITH mvtgeom AS
(
SELECT 2/0/1
ST_TileEnvelope(2, 0, 1, margin=>0.25)
ST_TileEnvelope(2, 0, 1, margin=>0)
1024
256
)
SELECT ST_AsMVT(mvtgeom.*, 'two', 1024, 'way', NULL)
FROM mvtgeom;'''
            self.assertEqual(d.render_sql(Tile(2, 0, 1)), expected)

            fs.writetext("units.sql.jinja2", "{{tile_length}}\n{{tile_area}}\n" +
                                             "{{coordinate_length}}\n{{coordinate_area}}")
            d = Definition("units", {"minzoom": 1, "maxzoom": 3, "extent": 1024, "buffer": 256,
                                     "file": "units.sql.jinja2"}, fs)
            # Crudely slice up the string to turn it into numbers
            expected = '''WITH mvtgeom AS
(
10018754.17
100375435118892.39
9783.939619140625
95725474.4709896
)
SELECT ST_AsMVT(mvtgeom.*, 'units', 1024, 'way', NULL)
FROM mvtgeom;'''
            self.assertEqual(d.render_sql(Tile(2, 0, 1)), expected)
