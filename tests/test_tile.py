from unittest import TestCase
from tilekiln.tile import Tile


class TestConfig(TestCase):
    def test_properties(self):
        t = Tile(1, 2, 3)
        self.assertEqual(t.zoom, 1)
        self.assertEqual(t.x, 2)
        self.assertEqual(t.y, 3)

    def test_bounds(self):
        t = Tile(1, 2, 3)
        self.assertEqual(t.bbox(0), 'ST_TileEnvelope(1, 2, 3, margin=>0)')
        self.assertEqual(t.bbox(8/4096), 'ST_TileEnvelope(1, 2, 3, margin=>0.001953125)')
