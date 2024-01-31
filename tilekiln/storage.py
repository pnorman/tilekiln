import sys
import gzip
import json
from collections.abc import Iterator

import click
import psycopg.rows
from psycopg_pool import ConnectionPool

from tilekiln.config import Config
from tilekiln.metric import Metric
from tilekiln.tile import Tile

METADATA_TABLE = "metadata"
GENERATE_STATS_TABLE = "generate_stats"
TILE_STATS_TABLE = "tile_stats"

# Lower percentiles are typically not interesting, because generally the
# smallest 50% of tiles are identical water tiles or something similarly
# sparse. Where the data gets interesting is p95 and above.
PERCENTILES = [0.0, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.0]


class Storage:
    def __init__(self, config: Config, dbpool: ConnectionPool, id=None, schema="tilekiln"):
        self.__config = config
        self.__pool = dbpool

        self.__schema = schema

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

    '''
    Methods that manipulate schema-related stuff and don't involve any tiles
    '''
    # TODO: Split off tileset specific stuff into Tileset class
    # Most change from Storage.foo(self) to Storage.foo(self, id) and Tileset.bar(self)
    def create_tables(self):
        if self.__config is None:
            raise RuntimeError("Config is not defined when trying to create tables")
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                # Perform one-time setup using CREATE ... IF NOT EXISTS
                # This is safe to rerun multiple times
                cur.execute(f'''CREATE SCHEMA IF NOT EXISTS "{self.__schema}"''')
                self.__setup_stats(cur)
                self.__setup_metadata(cur)

                self.__update_metadata(cur)

                self.__setup_tables(cur)

                conn.commit()

    def update_tables(self):
        ''' Update metadata tables with new zoom level/etc'''
        # TODO: Implement
        pass

    def remove_tables(self):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''DELETE FROM "{self.__schema}"."{METADATA_TABLE}" WHERE id = %s''',
                            (self.id,))
                cur.execute(f'''DROP TABLE "{self.__schema}"."{self.id}" CASCADE''')
                cur.execute(f'''DELETE FROM "{self.__schema}"."{TILE_STATS_TABLE}" WHERE id = %s''',
                            (self.id,))
                conn.commit()

    ''' Methods that return information about the tileset '''
    def metrics(self) -> Iterator[Metric]:
        with self.__pool.connection() as conn:
            conn.read_only = True
            with conn.cursor(row_factory=psycopg.rows.class_row(Metric)) as cur:
                cur.execute(f'''SELECT id, zoom, num_tiles, size, percentiles
                             FROM "{self.__schema}"."{TILE_STATS_TABLE}"''')
                for record in cur:
                    yield record

    def update_metrics(self):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for zoom in range(self.minzoom, self.maxzoom+1):
                    tablename = f"{self.id}_z{zoom}"
                    # This SQL statement needs to handle the case of an empty table.
                    # Except for COUNT(*) the aggregate functions return NULL for
                    # no rows, which is a problem. One option would be to save
                    # {{}, {}} as the array but 2-d empty arrays don't really work
                    # in PostgreSQL. Instead, we return 0 for all metrics.
                    #
                    # We set jit to ON as it is faster when the tables are large, but
                    # jit is commonly disabled on tile rendering servers because it
                    # slows down rendering queries.
                    # TODO: Consider if it would be better to completely skip the row
                    #       and emit no metric.
                    # TODO: Reformat this statement to be better with line breaks
                    cur.execute('SET LOCAL jit TO ON;')
                    cur.execute(f'''INSERT INTO "{self.__schema}"."{TILE_STATS_TABLE}"
                                SELECT
                                    %(id)s AS id,
                                    %(zoom)s AS zoom,
                                    COUNT(*) AS num_tiles,
                                    COALESCE(SUM(length(tile)),0) AS size,
                                    ARRAY[%(percentile)s,
                                        COALESCE(PERCENTILE_CONT(%(percentile)s::double precision[])
                                            WITHIN GROUP (ORDER BY length(tile)),
                                            array_fill(0,
                                            ARRAY[array_length(%(percentile)s, 1)]))] AS percentiles
                                    FROM "{self.__schema}"."{tablename}"
                                    ON CONFLICT (id, zoom)
                                DO UPDATE SET num_tiles = EXCLUDED.num_tiles,
                                    size = EXCLUDED.size,
                                    percentiles = EXCLUDED.percentiles;
                                ''', {'id': self.id, 'zoom': zoom, 'percentile': PERCENTILES})

    def tilejson(self, url):
        tilejson = self.__rawtilejson
        tilejson["tiles"] = [f"{url}/{self.id}" + "/{z}/{x}/{y}.mvt"],
        return json.dumps(tilejson, sort_keys=True, indent=4)

    '''
    Methods that involve saving, fetching, and deleting tiles
    '''
    # TODO: These will be moved to the new Tileset class
    def delete_tiles(self, tiles):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for tile in tiles:
                    self.__delete_tile(tile, cur)
            conn.commit()

    def truncate_tables(self, zooms=None):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                if zooms is None:
                    for zoom in range(self.minzoom, self.maxzoom+1):
                        self.__truncate_table(zoom, cur)
                else:
                    for zoom in zooms:
                        self.__truncate_table(zoom, cur)
                conn.commit()

    def get_tile(self, tile):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT tile FROM "{self.__schema}"."{self.id}"
                                WHERE zoom = %s AND x = %s AND y = %s''',
                            (tile.zoom, tile.x, tile.y), binary=True)
                result = cur.fetchone()
                if result is None:
                    return None
                return gzip.decompress(result[0])

    def save_tile(self, tile: Tile, tiledata, render_time=0):
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                self.__write_to_storage(tile, gzip.compress(tiledata, mtime=0), cur)

    def __setup_metadata(self, cur):
        ''' Create the metadata table in storage. This is safe to rerun
        '''
        # TODO: Updating metadata table schema??
        # Probably can only be done on a major version upgrade

        cur.execute(f'''CREATE TABLE IF NOT EXISTS "{self.__schema}"."{METADATA_TABLE}" (
            id text PRIMARY KEY,
            active boolean NOT NULL DEFAULT TRUE,
            minzoom smallint NOT NULL,
            maxzoom smallint NOT NULL,
            tilejson jsonb NOT NULL)''')

    def __update_metadata(self, cur):
        '''Insert the metadata for the current tileset into the storage

        If the data is already there, update it.
        '''
        cur.execute(f'''INSERT INTO "{self.__schema}"."{METADATA_TABLE}"
        (id, minzoom, maxzoom, tilejson)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET minzoom = EXCLUDED.minzoom,
        maxzoom = EXCLUDED.maxzoom,
        tilejson = EXCLUDED.tilejson
        ''', (self.id, self.minzoom, self.maxzoom, json.dumps(self.__rawtilejson)))

    def __setup_stats(self, cur):
        '''Create the stats tables.

        One table has tile generation stats, the other has tile storage stats.
        '''
        # Because we're just storing counters for prometheus here and unlogged table is fine.
        # Periodic resets are okay.
        # It's necessary to store this in-db since we might call tilerender more than once
        # in a polling interval.
        # TODO: Use this table
        cur.execute(f'''CREATE UNLOGGED TABLE IF NOT EXISTS
            "{self.__schema}"."{GENERATE_STATS_TABLE}" (
            id text,
            zoom smallint,
            num_rendered integer DEFAULT 0,
            time_rendered interval DEFAULT '0',
            PRIMARY KEY (id, zoom)
        )
        ''')

        # This caches information on the number of tiles. Prometheus can be called every 15 seconds
        # and doing a sequential scan that often is a bad idea
        cur.execute(f'''CREATE TABLE IF NOT EXISTS "{self.__schema}"."{TILE_STATS_TABLE}" (
            id text,
            zoom smallint,
            num_tiles integer NOT NULL,
            size bigint NOT NULL,
            percentiles double precision[][] NOT NULL,
            PRIMARY KEY (id, zoom),
            CHECK (array_length(percentiles, 1) = 2)
        )
        ''')

    def __setup_tables(self, cur):
        '''Create the tile storage tables

        This creates the tile storage tables. It intentionally
        does not try to overwrite existing tables.
        '''
        cur.execute(f'''CREATE TABLE "{self.__schema}"."{self.id}" (
                    zoom smallint CHECK (zoom >= {self.minzoom} AND zoom <= {self.maxzoom}),
                    x int CHECK (x >= 0 AND x < 1 << zoom),
                    y int CHECK (x >= 0 AND x < 1 << zoom),
                    tile bytea NOT NULL,
                    primary key (zoom, x, y)
                    ) PARTITION BY LIST (zoom)''')
        for zoom in range(self.minzoom, self.maxzoom+1):
            tablename = f"{self.id}_z{zoom}"
            cur.execute(f'''CREATE TABLE "{self.__schema}"."{tablename}"
                            PARTITION OF "{self.__schema}"."{self.id}"
                            FOR VALUES IN ({zoom})''')
            # tile is already compressed, so tell postgres to not compress it again
            cur.execute(f'''ALTER TABLE "{self.__schema}"."{tablename}"
                            ALTER COLUMN tile SET STORAGE EXTERNAL''')

    def __load_metadata(self):
        '''Load the stored metadata.

        This allows serving a TileJSON without having access to the config
        '''
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(f'''SELECT minzoom, maxzoom, tilejson
                                FROM "{self.__schema}"."{METADATA_TABLE}"
                                WHERE id = %s''', (self.id,))
                result = cur.fetchone()
                if result is None:
                    # TODO: raise exception and handle it at the calling level
                    click.echo(f"Failed to retrieve metadata for id {self.id}, "
                               f"does it exist in storage DB?",
                               err=True)
                    sys.exit(1)
                self.minzoom = result["minzoom"]
                self.maxzoom = result["maxzoom"]
                self.__rawtilejson = result["tilejson"]

    def __truncate_table(self, zoom, cur):
        '''Remove every tile from a particular tileset and zoom'''
        tablename = f"{self.id}_z{zoom}"
        cur.execute(f'''TRUNCATE TABLE "{self.__schema}"."{tablename}"''')

    def __delete_tile(self, tile, cur):
        '''Delete an individual tile

        How this is implemented is not ideal for long lists of tiles
        to delete, but generally a long list is an entire zoom or a box.

        In the former case it is implemented as __truncate_table, and
        the latter case is not implemented but would take min/max x/y.
        '''
        cur.execute(f'''DELETE FROM "{self.__schema}"."{self.id}"
                        WHERE zoom = %s AND x = %s AND y = %s''',
                    (tile.zoom, tile.x, tile.y))

    def __write_to_storage(self, tile: Tile, tiledata, cur):
        tablename = f"{self.id}_z{tile.zoom}"
        cur.execute(f'''INSERT INTO "{self.__schema}"."{tablename}" (zoom, x, y, tile)
VALUES (%s, %s, %s, %s)
ON CONFLICT (zoom, x, y)
DO UPDATE SET tile = EXCLUDED.tile''',
                    (tile.zoom, tile.x, tile.y, tiledata))
