from unittest import TestCase, mock

from tilekiln.prometheus import TilekilnCollector
from tilekiln.metric import Metric
from tilekiln.storage import Storage


class TestTilekilnCollector(TestCase):
    def setUp(self):
        # Create a mock storage
        self.storage = mock.Mock(spec=Storage)

        # Create test metrics
        self.metrics = [
            Metric(
                id="tileset1",
                zoom=0,
                num_tiles=1,
                size=1024,
                percentiles=[[0, 0.5, 0.9, 1], [100, 500, 900, 1000]],
            ),
            Metric(
                id="tileset1",
                zoom=1,
                num_tiles=4,
                size=4096,
                percentiles=[[0, 0.5, 0.9, 1], [200, 600, 1000, 1200]],
            ),
            Metric(
                id="tileset2",
                zoom=0,
                num_tiles=1,
                size=2048,
                percentiles=[[0, 0.5, 0.9, 1], [150, 550, 950, 1050]],
            ),
        ]

        # Mock storage.metrics to return our test metrics
        self.storage.metrics.return_value = self.metrics

        # Create the TilekilnCollector instance
        self.collector = TilekilnCollector(self.storage)

    def test_collect(self):
        """Test collecting metrics"""
        # Call collect method
        metrics = list(self.collector.collect())

        # Should have 3 metrics (count, size, percentiles)
        self.assertEqual(len(metrics), 3)

        # Check metric names
        self.assertEqual(metrics[0].name, "tilekiln_stored_count")
        self.assertEqual(metrics[1].name, "tilekiln_stored_bytes_sum")
        self.assertEqual(metrics[2].name, "tilekiln_stored_bytes")

        # Check that storage.metrics was called
        self.storage.metrics.assert_called_once()
