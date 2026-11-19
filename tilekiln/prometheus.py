import time

import prometheus_client
from prometheus_client.registry import Collector
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from tilekiln.storage import Storage

# Disable default metrics since we're not monitoring this process, we're monitoring
# the DB sizes
REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

# Don't auto-create metrics for when the metric was created
prometheus_client.disable_created_metrics()


class TilekilnCollector(Collector):
    def __init__(self, storage: Storage):
        self.__storage = storage
        super().__init__()

        self.__i = 0

    def collect(self):
        # This is manually producing the metrics described in
        # https://prometheus.io/docs/concepts/metric_types/#summary
        # Native histograms would be nice here, but are still only experimental
        size = GaugeMetricFamily(
            "tilekiln_stored_bytes_sum",
            "Total size of tiles",
            labels=["tileset", "zoom"],
        )
        quantiles = GaugeMetricFamily(
            "tilekiln_stored_bytes",
            "Tile percentiles",
            labels=["tileset", "zoom", "quantile"],
        )
        total = GaugeMetricFamily(
            "tilekiln_stored_count",
            "Tiles in tilekiln storage",
            labels=["tileset", "zoom"],
        )
        for metric in self.__storage.metrics():
            size.add_metric([metric.id, str(metric.zoom)], metric.size)
            total.add_metric([metric.id, str(metric.zoom)], metric.num_tiles)
            for i in range(0, len(metric.percentiles[0])):
                quantiles.add_metric(
                    [metric.id, str(metric.zoom), str(metric.percentiles[0][i])],
                    metric.percentiles[1][i],
                )
        yield total
        yield size
        yield quantiles

    def update(self):
        self.__i = self.__i + 1
        pass


def serve_prometheus(storage: Storage, addr, port, interval: int):
    """Start a prometheus server for storage info."""
    collector = TilekilnCollector(storage)
    REGISTRY.register(collector)
    # start http
    prometheus_client.start_http_server(port=port, addr=addr)
    while True:
        started_at = time.monotonic()
        storage.update_metrics()
        delay = interval - (time.monotonic() - started_at)
        if delay > 0:
            time.sleep(delay)
