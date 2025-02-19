import datetime
import json
import sys
from collections.abc import Collection, Iterator, Sequence
from typing import Optional

import click
import psycopg.rows
import psycopg_pool
from psycopg import sql

import tilekiln.errors

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
    def __init__(self, pool: psycopg_pool.ConnectionPool, schema: str = "tilekiln"):
        self.__pool = pool
        self.__schema = schema

    '''
    Methods that manipulate schema-related stuff and don't involve any tiles
    '''
    def create_schema(self) -> None:
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                # Perform one-time setup using CREATE ... IF NOT EXISTS
                # This is safe to rerun multiple times
                cur.execute(f'''CREATE SCHEMA IF NOT EXISTS "{self.__schema}"''')
                self.__setup_stats(cur)
                self.__setup_metadata(cur)
            conn.commit()

    '''
    Methods for tilesets
    '''
    def create_tileset(self, id: str, layers: list[str],
                       minzoom: int, maxzoom: int, tilejson: str) -> None:
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                self.__set_metadata(cur, id, layers, minzoom, maxzoom, tilejson)

                self.__setup_tables(cur, id, layers, minzoom, maxzoom)
            conn.commit()

    def remove_tileset(self, id: str) -> None:
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''DELETE FROM "{self.__schema}"."{METADATA_TABLE}" WHERE id = %s''',
                            (id,))
                cur.execute(f'''DROP TABLE "{self.__schema}"."{id}" CASCADE''')
                cur.execute(f'''DELETE FROM "{self.__schema}"."{TILE_STATS_TABLE}" WHERE id = %s''',
                            (id,))
            conn.commit()

    def get_tilesets(self) -> Iterator[Tileset]:
        '''
        Gets all tilesets in the storage
        '''

        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(f'''SELECT id, layers, minzoom, maxzoom, tilejson
                                FROM "{self.__schema}"."{METADATA_TABLE}"''')
                for record in cur:
                    yield Tileset(self, record["id"], record["layers"],
                                  record["minzoom"], record["maxzoom"],
                                  json.dumps(record["tilejson"]))

    def get_tileset(self, id: str) -> Tileset:
        '''
        Gets all tilesets in the storage
        '''

        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql.SQL('''SELECT id, layers, minzoom, maxzoom, tilejson FROM {}.{}''')
                            .format(sql.Identifier(self.__schema), sql.Identifier(METADATA_TABLE)) +
                            sql.SQL('''WHERE id = %s'''), (id, ))
                result = cur.fetchone()
                if result is None:
                    raise tilekiln.errors.TilesetMissing

                return Tileset(self, result["id"], result["layers"],
                               result["minzoom"], result["maxzoom"],
                               json.dumps(result["tilejson"]))

    def get_tileset_ids(self) -> Iterator[str]:
        '''
        Get only the tileset IDs
        '''

        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(f'''SELECT id
                                FROM "{self.__schema}"."{METADATA_TABLE}"''')
                for record in cur:
                    yield record["id"]

    ''' Methods for metrics'''
    def metrics(self) -> Collection[Metric]:
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(f'''SELECT id, zoom, num_tiles, size, percentiles
                                FROM "{self.__schema}"."{TILE_STATS_TABLE}"''')
                return [Metric(**record) for record in cur]

    def update_metrics(self) -> None:
        tilesets = self.get_tilesets()
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for tileset in tilesets:
                    self.__update_tileset_metrics(cur, tileset)
                conn.commit()

    '''Methods that set/get metadata'''
    def set_metadata(self, id: str, layers: list[str],
                     minzoom: int, maxzoom: int, tilejson: str) -> None:
        '''
        Saves metadata into storage

        This just wraps __set_metadata, which requires a cursor
        '''
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                self.__set_metadata(cur, id, layers, minzoom, maxzoom, tilejson)
            conn.commit()

    def get_layer_ids(self, id: str) -> list[str]:
        ''''''
        raise NotImplementedError()

    # TODO: Should the various get_* functions be separate? The query has to fetch from the
    # DB each time, but only tilejson needs URL. Not an urgent issue.
    def get_tilejson(self, id: str, url: str) -> str:
        '''Gets the tilejson for a layer from storage.'''
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
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

    # TODO: Get rid of get_minzoom/maxzoom functions and use get_tileset
    def get_minzoom(self, id: str):
        '''Gets the minzoom for a layer from storage.'''
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
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
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
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
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for tile in tiles:
                    self.__delete_tile(cur, id, tile)
            conn.commit()

    def truncate_tables(self, id: str, zooms: Optional[Sequence[int]] = None):
        if zooms is None:
            zooms = range(self.get_minzoom(id), self.get_maxzoom(id)+1)
        with self.__pool.connection() as conn:
            with conn.cursor() as cur:
                for zoom in zooms:
                    self.__truncate_table(cur, id, zoom)
            conn.commit()

    def get_tile(self, id: str, tile: Tile) -> tuple[dict[str, bytes] | None,
                                                     datetime.datetime | None]:
        tileset = self.get_tileset(id)
        if tile.zoom > tileset.maxzoom or tile.zoom < tileset.minzoom:
            raise tilekiln.errors.ZoomNotDefined
        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                query = (sql.SQL('SELECT GREATEST(')
                         + generated_columns(tileset.layers)
                         + sql.SQL(') AS generated,')
                         + data_columns(tileset.layers)
                         + sql.SQL('FROM {}.{}').format(sql.Identifier(self.__schema),
                                                        sql.Identifier(id))
                         + sql.SQL('WHERE zoom = %s AND x = %s AND y = %s'))
                cur.execute(query, (tile.zoom, tile.x, tile.y), binary=True)
                result = cur.fetchone()
                if result is None:
                    return None, None
                return {layer: result[f"{layer}_data"]
                        for layer in tileset.layers}, result["generated"]

    def save_tile(self, id: str, tile: Tile,
                  layers: dict[str, bytes], render_time=0) -> datetime.datetime | None:
        tileset = self.get_tileset(id)
        if tile.zoom > tileset.maxzoom or tile.zoom < tileset.minzoom:
            raise tilekiln.errors.ZoomNotDefined
        if tileset.layers != list(layers.keys()):
            raise tilekiln.errors.Error("Layers rendered do not match layers specified")
        tablename = f"{id}_z{tile.zoom}"

        with self.__pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # TODO: This statement unconditionally writes the row even if it's unchanged. It
                # shouldn't. Adding WHERE tile != EXCLUDED.tile would help, but then it would
                # return zero rows if the contents are the same. The method here instead results
                # in extra writes but does preserve the datetime.

                # These statements operate on the

                data_upsert = [sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(f"{layer}_data"),
                                                                  sql.Identifier(f"{layer}_data"))
                               for layer in tileset.layers]
                time_upsert = [sql.SQL("{generated} = CASE WHEN store.{data} != EXCLUDED.{data} "
                                       "THEN statement_timestamp() ELSE store.{generated} END")
                               .format(generated=sql.Identifier(f"{layer}_generated"),
                                       data=sql.Identifier(f"{layer}_data"))
                               for layer in layers]

                # arguments to the query
                data_idents = [sql.Placeholder(layer) for layer in layers]

                q = (sql.SQL("INSERT INTO {}.{} AS store\n").format(sql.Identifier(self.__schema),
                                                                    sql.Identifier(tablename))
                     + sql.SQL("(zoom, x, y, ") + data_columns(layers) + sql.SQL(")\n")
                     + sql.SQL("VALUES ({}, {}, {}, ").format(sql.Literal(tile.zoom),
                                                              sql.Literal(tile.x),
                                                              sql.Literal(tile.y))
                     + sql.SQL(", ").join(data_idents) + sql.SQL(")\n")
                     + sql.SQL("ON CONFLICT (zoom, x, y)\n")
                     + sql.SQL("DO UPDATE SET ")
                     + sql.SQL(", ").join([*data_upsert, *time_upsert])
                     + sql.SQL("\nRETURNING GREATEST(") + generated_columns(layers)
                     + sql.SQL(") AS generated"))

                cur.execute(q, layers)

                result = cur.fetchone()
                if result is None:
                    return None
                return result["generated"]

    def __setup_metadata(self, cur) -> None:
        ''' Create the metadata table in storage. This is safe to rerun
        '''
        # TODO: Updating metadata table schema??
        # Probably can only be done on a major version upgrade

        cur.execute(f'''CREATE TABLE IF NOT EXISTS "{self.__schema}"."{METADATA_TABLE}" (\n'''
                    '''id text PRIMARY KEY,\n'''
                    '''active boolean NOT NULL DEFAULT TRUE,\n'''
                    '''layers text[],\n'''
                    '''minzoom smallint NOT NULL,\n'''
                    '''maxzoom smallint NOT NULL,\n'''
                    '''tilejson jsonb NOT NULL)''')

    def __set_metadata(self, cur, id: str, layers: list[str], minzoom, maxzoom, tilejson):
        '''
        Sets metadata using a cursor

        This is separate from set_metadata because sometimes it needs
        calling within a transaction
        '''

        query = (sql.SQL("INSERT INTO {}.{}\n").format(sql.Identifier(self.__schema),
                                                       sql.Identifier(METADATA_TABLE))
                 + sql.SQL("(id, minzoom, maxzoom, layers, tilejson)\n")
                 + sql.SQL("VALUES (%s, %s, %s, %s, %s)\n")
                 + sql.SQL("ON CONFLICT (id)\n")
                 + sql.SQL("DO UPDATE SET minzoom = EXCLUDED.minzoom,\n")
                 + sql.SQL("maxzoom = EXCLUDED.maxzoom,\n")
                 + sql.SQL("tilejson = EXCLUDED.tilejson"))
        cur.execute(query, (id, minzoom, maxzoom, layers, tilejson))

    def __setup_stats(self, cur) -> None:
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

    def __update_tileset_metrics(self, cur, tileset: Tileset) -> None:
        id = tileset.id
        minzoom = tileset.minzoom
        maxzoom = tileset.maxzoom
        for zoom in range(minzoom, maxzoom+1):
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
            cur.execute('SET LOCAL jit TO ON;')

            length = sql.SQL("+").join([sql.SQL("length({})")
                                        .format(sql.Identifier(f"{layer}_data"))
                                        for layer in tileset.layers])

            query = (sql.SQL("INSERT INTO {}.{}\n").format(sql.Identifier(self.__schema),
                                                           sql.Identifier(TILE_STATS_TABLE))
                     + sql.SQL("SELECT %(id)s AS id, %(zoom)s AS zoom,")
                     + sql.SQL("COUNT(*) AS num_tiles,\n"
                               + "COALESCE (SUM({}), 0) AS size,\n").format(length)

                     + sql.SQL("ARRAY[%(percentile)s, "
                               + "COALESCE(PERCENTILE_CONT(%(percentile)s::double precision[])")
                     + sql.SQL("WITHIN GROUP (ORDER BY {}),\n").format(length)
                     + sql.SQL("array_fill(0, ARRAY[array_length(%(percentile)s, 1)]))] "
                               + "AS percentiles\n").format(length, length)
                     + sql.SQL("FROM {}.{}").format(sql.Identifier(self.__schema),
                                                    sql.Identifier(f"{id}_z{zoom}"))
                     + sql.SQL("ON CONFLICT (id, zoom)\n"
                               + "DO UPDATE SET num_tiles = EXCLUDED.num_tiles,\n"
                               + "size = EXCLUDED.size,\n"
                               + "percentiles = EXCLUDED.percentiles;"))

            cur.execute(query, {'id': id, 'zoom': zoom, 'percentile': PERCENTILES})

    def __setup_tables(self, cur, id: str, layers: list[str],
                       minzoom: int, maxzoom: int) -> None:
        '''Create the tile storage tables

        This creates the tile storage tables. It intentionally
        does not try to overwrite existing tables.
        '''

        columns = [sql.SQL('zoom smallint CHECK (zoom >= {} AND zoom <= {})')
                   .format(sql.Literal(minzoom), sql.Literal(maxzoom)),
                   sql.SQL('x int CHECK (x >= 0 AND x < 1 << zoom)'),
                   sql.SQL('y int CHECK (y >= 0 AND y < 1 << zoom)')]

        columns += [sql.SQL("{} timestamptz DEFAULT statement_timestamp()")
                    .format(sql.Identifier(f'{layer}_generated'))
                    for layer in layers]
        columns += [sql.SQL("{} bytea").format(sql.Identifier(f'{layer}_data'))
                    for layer in layers]
        columns += [sql.SQL("PRIMARY KEY (zoom, x, y)")]

        query = (sql.SQL('''CREATE TABLE {}.{} (\n''').format(sql.Identifier(self.__schema),
                                                              sql.Identifier(id))
                 + sql.SQL(',\n').join(columns)
                 + sql.SQL(''') PARTITION BY LIST (zoom)'''))

        cur.execute(query)
        for zoom in range(minzoom, maxzoom+1):
            tablename = f"{id}_z{zoom}"
            query = (sql.SQL('''CREATE TABLE {}.{}\n''').format(sql.Identifier(self.__schema),
                                                                sql.Identifier(tablename))
                     + sql.SQL('''PARTITION OF {}.{}\n''').format(sql.Identifier(self.__schema),
                                                                  sql.Identifier(id))
                     + sql.SQL('''FOR VALUES IN ({})''').format(sql.Literal(zoom)))

            cur.execute(query)

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


def data_columns(layers) -> sql.Composable:
    return sql.SQL(', ').join([sql.Identifier(f"{layer}_data")
                               for layer in layers])


def generated_columns(layers) -> sql.Composable:
    return sql.SQL(', ').join([sql.Identifier(f"{layer}_generated")
                               for layer in layers])
