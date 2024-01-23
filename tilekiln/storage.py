
from tilekiln.config import Config
from tilekiln.tile import Tile
from psycopg_pool import ConnectionPool
import psycopg.rows
import click
import sys
import gzip
import json

DEFAULT_SCHEMA = "tilekiln"
METADATA_TABLE = "metadata"


class Storage:
    def __init__(self, config: Config, dbpool: ConnectionPool, id=None):
        self.__config = config
        self.__pool = dbpool

        # Both config id and id could be set, but prefer the explicit one
        self.id = id or self.__config.id

        # If there's no config, load what's needed from metadata
        if self.__config is None:
            self.__load_metadata()
        else:
            # TODO: Refactor tile json vs string stuff
            self.__rawtilejson = json.loads(self.__config.tilejson('REPLACED_BY_SERVER'))
            self.minzoom = self.__config.minzoom
            self.maxzoom = self.__config.maxzoom

    def create_tables(self):
        if self.__config is None:
            raise RuntimeError("Config is not defined when trying to create tables")
        with self.__pool.connection() as conn:
            tilejson = self.__rawtilejson
            schema = DEFAULT_SCHEMA
            with conn.cursor() as cur:
                cur.execute(f'''CREATE SCHEMA IF NOT EXISTS "{schema}"''')
                cur.execute(f'''CREATE TABLE IF NOT EXISTS "{schema}"."{METADATA_TABLE}" (
                    id text,
                    active boolean NOT NULL DEFAULT TRUE,
                    minzoom smallint NOT NULL,
                    maxzoom smallint NOT NULL,
                    tilejson jsonb NOT NULL)''')
                cur.execute(f'''INSERT INTO "{schema}".metadata
                    (id, minzoom, maxzoom, tilejson)
                    VALUES (%s, %s, %s, %s)''', (self.id, self.minzoom, self.maxzoom, tilejson))

                cur.execute(f'''CREATE TABLE "{schema}"."{self.id}" (
                    z smallint CHECK (z >= {self.minzoom} AND z <= {self.maxzoom}),
                    x int CHECK (x >= 0 AND x < 1 << z),
                    y int CHECK (x >= 0 AND x < 1 << z),
                    tile bytea NOT NULL,
                    primary key (z, x, y)
                    ) PARTITION BY LIST (z)''')
                for z in range(self.minzoom, self.maxzoom+1):
                    tablename = f"{self.id}_z{z}"
                    cur.execute(f'''CREATE TABLE "{schema}"."{tablename}"
                                    PARTITION OF "{schema}"."{self.id}" FOR VALUES IN ({z})''')
                    # tile is already compressed, so tell postgres to not compress it again
                    cur.execute(f'''ALTER TABLE "{schema}"."{tablename}"
                                    ALTER COLUMN tile SET STORAGE EXTERNAL''')

                conn.commit()

    def update_tables(self):
        '''Update metadata tables with new zoom level/etc'''
        # TODO: Implement
        pass

    def __load_metadata(self):
        with self.__pool.connection() as conn:
            schema = DEFAULT_SCHEMA
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(f'''SELECT minzoom, maxzoom, tilejson
                                FROM "{schema}"."{METADATA_TABLE}"
                                WHERE id = %s''', (self.id,))
                result = cur.fetchone()
                if result is None:
                    click.echo(f"Failed to retrieve metadata for id {self.id}, "
                               f"does it exist in storage DB?",
                               err=True)
                    sys.exit(1)
                self.minzoom = result["minzoom"]
                self.maxzoom = result["maxzoom"]
                self.__rawtilejson = result["tilejson"]

    def tilejson(self, url):
        tilejson = self.__rawtilejson
        tilejson["tiles"] = [f"{url}/{self.id}" + "/{z}/{x}/{y}.mvt"],
        return json.dumps(tilejson, sort_keys=True, indent=4)

    def remove_tables(self):
        with self.__pool.connection() as conn:
            id = self.id
            schema = DEFAULT_SCHEMA
            with conn.cursor() as cur:
                cur.execute(f'''DELETE FROM "{schema}"."{METADATA_TABLE}" WHERE id = %s''',
                            (self.id,))
                cur.execute(f'''DROP TABLE "{schema}"."{id}" CASCADE''')
                conn.commit()

    def truncate_tables(self, zooms=None):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                if zooms is None:
                    for z in range(self.minzoom, self.maxzoom+1):
                        self.__truncate_table(z, cur)
                else:
                    for z in zooms:
                        self.__truncate_table(z, cur)
                conn.commit()

    def __truncate_table(self, zoom, cur):
        tablename = f"{self.id}_z{zoom}"
        schema = DEFAULT_SCHEMA
        cur.execute(f'''TRUNCATE TABLE "{schema}"."{tablename}"''')

    def __delete_tile(self, tile, cur):
        schema = DEFAULT_SCHEMA
        cur.execute(f'''DELETE FROM "{schema}"."{self.id}"
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
                cur.execute(f'''SELECT tile FROM "{schema}"."{self.id}"
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
        tablename = f"{self.id}_z{tile.zoom}"
        schema = DEFAULT_SCHEMA
        cur.execute(f'''INSERT INTO "{schema}"."{tablename}" (z, x, y, tile)
VALUES (%s, %s, %s, %s)
ON CONFLICT (z, x, y)
DO UPDATE SET tile = EXCLUDED.tile''',
                    (tile.zoom, tile.x, tile.y, tiledata))
