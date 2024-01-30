from dataclasses import dataclass


@dataclass(kw_only=True, frozen=True)
class Metric:
    """ Class for a metric about a tileset in storage """
    id: str
    zoom: int
    num_tiles: int
    size: int
    percentiles: dict[float, float]
