import psycopg
import psycopg_pool

import tilekiln.errors
from tilekiln.config import Config
from tilekiln.tile import Tile


class Kiln:
    '''
    The kiln is what actually generates the tiles, using the config to compute SQL,
    and a DB connection to execute it
    '''
    def __init__(self, config: Config, pool: psycopg_pool.ConnectionPool):
        self.__config = config
        self.__pool = pool

    def render_all(self, tile: Tile) -> dict[str, bytes]:
        if tile.zoom < self.__config.minzoom or tile.zoom > self.__config.maxzoom:
            raise tilekiln.errors.ZoomNotDefined

        with self.__pool.connection() as conn:
            with conn.cursor() as curs:
                return {name: self.__render_sql(curs, sql)
                        for name, sql in self.__config.layer_queries(tile).items()}

    def render_layer(self, layer: str, tile: Tile) -> bytes:
        with self.__pool.connection() as conn:
            with conn.cursor() as curs:
                return self.__render_sql(curs, self.__config.layer_query(layer, tile))

    def __render_sql(self, curs: psycopg.Cursor, sql: str | None) -> bytes:
        if sql is None:
            # None is a query for a layer not present in this zoom
            return b''

        curs.execute(sql, binary=True)
        for record in curs:
            return record[0]
        raise RuntimeError("No rows in tile query result, should never reach here")
