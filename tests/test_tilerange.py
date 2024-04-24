from unittest import TestCase
from tilekiln.tile import Tile
from tilekiln.tilerange import Tilerange


class TestTilerange(TestCase):
    def test_length(self):
        self.assertEqual(len(Tilerange(0, 0)), 1)
        self.assertEqual(len(Tilerange(0, 1)), 5)
        # If this were not evaluated lazily it would be slow
        self.assertEqual(len(Tilerange(30, 30)), 4**30)
        self.assertEqual(len(Tilerange(0, 1)), 5)

    def test_items(self):
        # Only one tile
        for tile in Tilerange(0, 0):
            self.assertEqual(tile, Tile(0, 0, 0))

        it1 = iter(Tilerange(0, 1))
        self.assertEqual(next(it1), Tile(0, 0, 0))
        self.assertEqual(next(it1), Tile(1, 0, 0))
        self.assertEqual(next(it1), Tile(1, 0, 1))
        self.assertEqual(next(it1), Tile(1, 1, 1))
        self.assertEqual(next(it1), Tile(1, 1, 0))
        self.assertRaises(StopIteration, next, it1)

        # If this were not evaluated lazily it would be slow
        it2 = iter(Tilerange(0, 30))
        self.assertEqual(next(it2), Tile(0, 0, 0))
