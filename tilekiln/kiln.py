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

    def render(self, tile: Tile) -> bytes:
        if tile.zoom < self.__config.minzoom or tile.zoom > self.__config.maxzoom:
            raise tilekiln.errors.ZoomNotDefined

        with self.__pool.connection() as conn:
            with conn.cursor() as curs:
                result = b''
                for sql in self.__config.layer_queries(tile):
                    result += self.__render_layer(curs, sql)

        return result

    def __render_layer(self, curs: psycopg.Cursor, sql: str) -> bytes:
        curs.execute(sql, binary=True)
        for record in curs:
            return record[0]
        raise RuntimeError("No rows in tile query result, should never reach here")
