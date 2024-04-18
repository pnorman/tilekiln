import datetime
import gzip
import json
import sys
from collections.abc import Iterator

import click
import psycopg.rows
from psycopg import Connection

from tilekiln.metric import Metric
from tilekiln.tile import Tile
from tilekiln.tileset import Tileset

METADATA_TABLE = "metadata"
GENERATE_STATS_TABLE = "generate_stats"
TILE_STATS_TABLE = "tile_stats"

# Lower percentiles are typically not interesting, because generally the
# smallest 50% of tiles are identical water tiles or something similarly
# sparse. Where the data gets interesting is p95 and above.
PERCENTILES = [0.0, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.0]


class Storage:
    '''
    Storage is an object representing a tile storage, backed by a PostgreSQL database

    A Storage contains tiles and metadata about tilesets, and has functions to update
    tiles and metadata based on the ID
    '''
    def __init__(self, conn: Connection, schema="tilekiln"):
        self.__conn = conn
        self.__conn.execute("SET TIMEZONE TO 'GMT'")
        self.__conn.commit()
        self.__schema = schema

    '''
    Methods that manipulate schema-related stuff and don't involve any tiles
    '''
    def create_schema(self) -> None:
        with self.__conn.cursor() as cur:
            # Perform one-time setup using CREATE ... IF NOT EXISTS
            # This is safe to rerun multiple times
            cur.execute(f'''CREATE SCHEMA IF NOT EXISTS "{self.__schema}"''')
            self.__setup_stats(cur)
            self.__setup_metadata(cur)
            self.__conn.commit()

    '''
    Methods for tilesets
    '''
    def create_tileset(self, id: str, minzoom: int, maxzoom: int, tilejson: str) -> None:
        with self.__conn.cursor() as cur:
            self.__set_metadata(cur, id, minzoom, maxzoom, tilejson)

            self.__setup_tables(cur, id, minzoom, maxzoom)

            self.__conn.commit()

    def remove_tileset(self, id: str) -> None:
        with self.__conn.cursor() as cur:
            cur.execute(f'''DELETE FROM "{self.__schema}"."{METADATA_TABLE}" WHERE id = %s''',
                        (id,))
            cur.execute(f'''DROP TABLE "{self.__schema}"."{id}" CASCADE''')
            cur.execute(f'''DELETE FROM "{self.__schema}"."{TILE_STATS_TABLE}" WHERE id = %s''',
                        (id,))
            self.__conn.commit()

    def get_tilesets(self) -> Iterator[Tileset]:
        '''
        Gets all tilesets in the storage
        '''

        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT id, minzoom, maxzoom, tilejson
                            FROM "{self.__schema}"."{METADATA_TABLE}"''')
            for record in cur:
                yield Tileset(self, record["id"], record["minzoom"], record["maxzoom"],
                              json.dumps(record["tilejson"]))

    def get_tileset_ids(self) -> Iterator[str]:
        '''
        Get only the tileset IDs
        '''

        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT id
                            FROM "{self.__schema}"."{METADATA_TABLE}"''')
            for record in cur:
                yield record["id"]

    ''' Methods for metrics'''
    def metrics(self) -> Iterator[Metric]:
        with self.__conn.cursor(row_factory=psycopg.rows.class_row(Metric)) as cur:
            cur.execute(f'''SELECT id, zoom, num_tiles, size, percentiles
                            FROM "{self.__schema}"."{TILE_STATS_TABLE}"''')
            for record in cur:
                yield record

    def update_metrics(self) -> None:
        with self.__conn.cursor() as cur:
            for id in self.get_tileset_ids():
                minzoom = self.get_minzoom(id)
                maxzoom = self.get_maxzoom(id)
                self.__update_tileset_metrics(cur, id, minzoom, maxzoom)
            self.__conn.commit()

    '''Methods that set/get metadata'''
    def set_metadata(self, id, minzoom, maxzoom, tilejson):
        '''
        Saves metadata into storage

        This just wraps __set_metadata, which requires a cursor
        '''
        with self.__conn.cursor() as cur:
            self.__set_metadata(cur, id, minzoom, maxzoom, tilejson)
            self.__conn.commit()

    # TODO: Should the various get_* functions be separate? The query has to fetch from the
    # DB each time, but only tilejson needs URL. Not an urgent issue.
    def get_tilejson(self, id, url) -> str:
        '''Gets the tilejson for a layer from storage.'''
        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT tilejson
                            FROM "{self.__schema}"."{METADATA_TABLE}"
                            WHERE id = %s''', (id,))
            result = cur.fetchone()
            if result is None:
                # TODO: raise exception and handle it at the calling level
                click.echo(f"Failed to retrieve tilejson for id {id}, "
                           f"does it exist in storage DB?",
                           err=True)
                sys.exit(1)
            tilejson = result["tilejson"]
            tilejson["tiles"] = [f"{url}" + "/{z}/{x}/{y}.mvt"]
            return json.dumps(tilejson)

    def get_minzoom(self, id):
        '''Gets the minzoom for a layer from storage.'''
        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT minzoom
                            FROM "{self.__schema}"."{METADATA_TABLE}"
                            WHERE id = %s''', (id,))
            result = cur.fetchone()
            if result is None:
                # TODO: raise exception and handle it at the calling level
                click.echo(f"Failed to retrieve minzoom for id {id}, "
                           f"does it exist in storage DB?",
                           err=True)
                sys.exit(1)
            return result["minzoom"]

    def get_maxzoom(self, id):
        '''Gets the minzoom for a layer from storage.'''
        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT maxzoom
                            FROM "{self.__schema}"."{METADATA_TABLE}"
                            WHERE id = %s''', (id,))
            result = cur.fetchone()
            if result is None:
                # TODO: raise exception and handle it at the calling level
                click.echo(f"Failed to retrieve minzoom for id {id}, "
                           "does it exist in storage DB?", err=True)
                sys.exit(1)
            return result["maxzoom"]

    '''
    Methods that involve saving, fetching, and deleting tiles
    '''
    def delete_tiles(self, id: str, tiles: set[Tile]):
        with self.__conn.cursor() as cur:
            for tile in tiles:
                self.__delete_tile(cur, id, tile)
        self.__conn.commit()

    def truncate_tables(self, id: str, zooms=None):
        with self.__conn.cursor() as cur:
            if zooms is None:
                zooms = range(self.get_minzoom(id), self.get_maxzoom(id)+1)
            for zoom in zooms:
                self.__truncate_table(cur, id, zoom)
            self.__conn.commit()

    def get_tile(self, id: str, tile: Tile) -> tuple[bytes | None, datetime.datetime | None]:

        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT generated, tile FROM "{self.__schema}"."{id}"
                            WHERE zoom = %s AND x = %s AND y = %s''',
                        (tile.zoom, tile.x, tile.y), binary=True)
            result = cur.fetchone()
            if result is None:
                return None, None
            return gzip.decompress(result["tile"]), result["generated"]

    # TODO: Needs to return timestamp written to the DB
    def save_tile(self, id: str, tile: Tile,
                  tiledata: bytes, render_time=0) -> datetime.datetime | None:
        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # TODO: This statement unconditionally writes the row even if it's unchanged. It
            # shouldn't. Adding WHERE tile != EXCLUDED.tile would help, but then it would
            # return zero rows if the contents are the same. The method here instead results
            # in extra writes but does preserve the datetime.
            tablename = f"{id}_z{tile.zoom}"
            cur.execute(f'''INSERT INTO "{self.__schema}"."{tablename}" AS store\n'''
                        '''(zoom, x, y, tile)\n'''
                        '''VALUES (%s, %s, %s, %s)\n'''
                        '''ON CONFLICT (zoom, x, y)\n'''
                        '''DO UPDATE SET tile = EXCLUDED.tile,\n'''
                        '''generated = CASE WHEN store.tile != EXCLUDED.tile\n'''
                        '''    THEN statement_timestamp()\n'''
                        '''    ELSE store.generated END\n'''
                        '''RETURNING generated''',
                        (tile.zoom, tile.x, tile.y, gzip.compress(tiledata, mtime=0)))
            result = cur.fetchone()
            self.__conn.commit()
            if result is None:
                return None
            return result["generated"]

    def __setup_metadata(self, cur):
        ''' Create the metadata table in storage. This is safe to rerun
        '''
        # TODO: Updating metadata table schema??
        # Probably can only be done on a major version upgrade

        cur.execute(f'''CREATE TABLE IF NOT EXISTS "{self.__schema}"."{METADATA_TABLE}" (\n'''
                    '''id text PRIMARY KEY,\n'''
                    '''active boolean NOT NULL DEFAULT TRUE,\n'''
                    '''minzoom smallint NOT NULL,\n'''
                    '''maxzoom smallint NOT NULL,\n'''
                    '''tilejson jsonb NOT NULL)''')

    def __set_metadata(self, cur, id, minzoom, maxzoom, tilejson):
        '''
        Sets metadata using a cursor

        This is separate from set_metadata because sometimes it needs
        calling within a transaction
        '''
        cur.execute(f'''INSERT INTO "{self.__schema}"."{METADATA_TABLE}"
        (id, minzoom, maxzoom, tilejson)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET minzoom = EXCLUDED.minzoom,
        maxzoom = EXCLUDED.maxzoom,
        tilejson = EXCLUDED.tilejson
        ''', (id, minzoom, maxzoom, tilejson))

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

    def __update_tileset_metrics(self, cur, id, minzoom, maxzoom) -> None:
        for zoom in range(minzoom, maxzoom+1):
            tablename = f"{id}_z{zoom}"
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
                        ''', {'id': id, 'zoom': zoom, 'percentile': PERCENTILES})

    def __setup_tables(self, cur, id, minzoom, maxzoom):
        '''Create the tile storage tables

        This creates the tile storage tables. It intentionally
        does not try to overwrite existing tables.
        '''
        # These columns are ordered to minimize wasted space between columns
        cur.execute(f'''CREATE TABLE "{self.__schema}"."{id}" (
                    zoom smallint CHECK (zoom >= {minzoom} AND zoom <= {maxzoom}),
                    x int CHECK (x >= 0 AND x < 1 << zoom),
                    y int CHECK (y >= 0 AND y < 1 << zoom),
                    generated timestamptz DEFAULT statement_timestamp(),
                    tile bytea NOT NULL,
                    primary key (zoom, x, y)
                    ) PARTITION BY LIST (zoom)''')
        for zoom in range(minzoom, maxzoom+1):
            tablename = f"{id}_z{zoom}"
            cur.execute(f'''CREATE TABLE "{self.__schema}"."{tablename}"
                            PARTITION OF "{self.__schema}"."{id}"
                            FOR VALUES IN ({zoom})''')
            # tile is already compressed, so tell postgres to not compress it again
            cur.execute(f'''ALTER TABLE "{self.__schema}"."{tablename}"
                            ALTER COLUMN tile SET STORAGE EXTERNAL''')

    def __load_metadata(self):
        '''Load the stored metadata.

        This allows serving a TileJSON without having access to the config
        '''
        with self.__conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f'''SELECT minzoom, maxzoom, tilejson
                            FROM "{self.__schema}"."{METADATA_TABLE}"
                            WHERE id = %s''', (self.id,))
            result = cur.fetchone()
            if result is None:
                # TODO: raise exception and handle it at the calling level
                click.echo(f"Failed to retrieve metadata for id {self.id}, "
                           "does it exist in storage DB?", err=True)
                sys.exit(1)
            self.minzoom = result["minzoom"]
            self.maxzoom = result["maxzoom"]
            self.__rawtilejson = result["tilejson"]

    def __truncate_table(self, cur, id: str, zoom: int) -> None:
        '''Remove every tile from a particular tileset and zoom'''
        tablename = f"{id}_z{zoom}"
        cur.execute(f'''TRUNCATE TABLE "{self.__schema}"."{tablename}"''')

    def __delete_tile(self, cur, id: str, tile: Tile):
        '''Delete an individual tile

        How this is implemented is not ideal for long lists of tiles
        to delete, but generally a long list is an entire zoom or a box.

        In the former case it is implemented as __truncate_table, and
        the latter case is not implemented but would take min/max x/y.
        '''
        cur.execute(f'''DELETE FROM "{self.__schema}"."{id}"
                        WHERE zoom = %s AND x = %s AND y = %s''',
                    (tile.zoom, tile.x, tile.y))
