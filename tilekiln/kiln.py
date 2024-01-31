import psycopg

from tilekiln.config import Config
from tilekiln.tile import Tile


class Kiln:
    '''
    The kiln is what actually generates the tiles, using the config to compute SQL,
    and a DB connection to execute it
    '''
    def __init__(self, config: Config, connection: psycopg.Connection):
        self.__config = config
        self.__connection = connection

        # New connection setup
        self.__connection.autocommit = True
        self.__connection.prepare_threshold = None
        self.__connection.execute('''SET default_transaction_read_only = true;''')

    def render(self, tile: Tile) -> bytes:
        with self.__connection.cursor() as curs:
            result = b''
            for sql in self.__config.layer_queries(tile):
                result += self.__render_layer(curs, sql)

        return result

    def __render_layer(self, curs: psycopg.Cursor, sql: str) -> bytes:
        curs.execute(sql, binary=True)
        for record in curs:
            return record[0]
        raise RuntimeError("No rows in tile query result, should never reach here")
