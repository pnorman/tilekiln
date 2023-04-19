
from tilekiln.config import Config
from tilekiln.tile import Tile
from psycopg_pool import ConnectionPool
import gzip

DEFAULT_SCHEMA = "tilekiln"


class Storage:
    def __init__(self, config: Config, dbpool: ConnectionPool):
        self.__config = config
        self.__pool = dbpool

    def create_tables(self):
        with self.__pool.connection() as conn:
            minzoom = self.__config.minzoom
            maxzoom = self.__config.maxzoom
            id = self.__config.id
            schema = DEFAULT_SCHEMA
            with conn.cursor() as cur:
                cur.execute(f'''CREATE SCHEMA IF NOT EXISTS "{schema}"''')
                cur.execute(f'''CREATE TABLE "{schema}"."{id}" (
                    z smallint CHECK (z >= {minzoom} AND z <= {maxzoom}),
                    x int CHECK (x >= 0 AND x < 1 << z),
                    y int CHECK (x >= 0 AND x < 1 << z),
                    tile bytea NOT NULL,
                    primary key (z, x, y)
                    ) PARTITION BY LIST (z)''')
                for z in range(minzoom, maxzoom+1):
                    tablename = f"{id}_z{z}"
                    cur.execute(f'''CREATE TABLE "{schema}"."{tablename}"
                                    PARTITION OF "{schema}"."{id}" FOR VALUES IN ({z})''')
                    # tile is already compressed, so tell postgres to not compress it again
                    cur.execute(f'''ALTER TABLE "{schema}"."{tablename}"
                                    ALTER COLUMN tile SET STORAGE EXTERNAL''')

                conn.commit()

    def remove_tables(self):
        with self.__pool.connection() as conn:
            id = self.__config.id
            schema = DEFAULT_SCHEMA
            with conn.cursor() as cur:
                cur.execute(f'''DROP TABLE "{schema}"."{id}" CASCADE''')
                conn.commit()

    def truncate_tables(self, zooms=None):
        with self.__pool.connection() as conn:
            minzoom = self.__config.minzoom
            maxzoom = self.__config.maxzoom
            with conn.cursor() as cur:
                if zooms is None:
                    for z in range(minzoom, maxzoom+1):
                        self.__truncate_table(z, cur)
                else:
                    for z in zooms:
                        self.__truncate_table(z, cur)
                conn.commit()

    def __truncate_table(self, zoom, cur):
        tablename = f"{self.__config.id}_z{zoom}"
        schema = DEFAULT_SCHEMA
        cur.execute(f'''TRUNCATE TABLE "{schema}"."{tablename}"''')

    def __delete_tile(self, tile, cur):
        schema = DEFAULT_SCHEMA
        cur.execute(f'''DELETE FROM "{schema}"."{self.__config.id}"
                        WHERE z = %s AND x = %s AND y = %s''',
                    (tile.zoom, tile.x, tile.y))

    def delete_tiles(self, tiles):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for tile in tiles:
                    self.__delete_tile(tile, cur)
            conn.commit()

    def get_tile(self, tile):
        with self.__pool.connection() as conn:
            schema = DEFAULT_SCHEMA
            with conn.cursor() as cur:
                cur.execute(f'''SELECT tile FROM "{schema}"."{self.__config.id}"
                                WHERE z = %s AND x = %s AND y = %s''',
                            (tile.zoom, tile.x, tile.y), binary=True)
                result = cur.fetchone()
                if result is None:
                    return None
                return gzip.decompress(result[0])

    def save_tile(self, tile: Tile, tiledata):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                self.__write_to_storage(tile, gzip.compress(tiledata, mtime=0), cur)

    def __write_to_storage(self, tile: Tile, tiledata, cur):
        tablename = f"{self.__config.id}_z{tile.zoom}"
        schema = DEFAULT_SCHEMA
        cur.execute(f'''INSERT INTO "{schema}"."{tablename}" (z, x, y, tile)
VALUES (%s, %s, %s, %s)
ON CONFLICT (z, x, y)
DO UPDATE SET tile = EXCLUDED.tile''',
                    (tile.zoom, tile.x, tile.y, tiledata))
