from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import tilekiln
from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.tile import Tile
from tilekiln.storage import Storage
import os
import psycopg
import psycopg_pool


# Constants for environment variable names
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"
TILEKILN_THREADS = "TILEKILN_THREADS"

TILE_PREFIX = "/tiles"

STANDARD_HEADERS = {}

kiln: Kiln
config: Config
storage: Storage

# Two types of server are defined - one for static tiles, the other for live generated tiles.

server = FastAPI()
server.add_middleware(CORSMiddleware,
                      allow_origins=["*"],
                      allow_methods=["*"],
                      allow_headers=["*"])
live = FastAPI()
live.add_middleware(CORSMiddleware,
                      allow_origins=["*"],
                      allow_methods=["*"],
                      allow_headers=["*"])

@server.on_event("startup")
def load_server_config():
    global config
    global storage
    config = tilekiln.load_config(os.environ[TILEKILN_CONFIG])

    # Because the DB connection variables are passed as standard PG* vars,
    # a plain ConnectionPool() will connect to the right DB
    pool = psycopg_pool.NullConnectionPool()
    storage = Storage(config, pool)

@live.on_event("startup")
def load_live_config():
    global config
    global storage
    config = tilekiln.load_config(os.environ[TILEKILN_CONFIG])

    generate_args = {}
    if "GENERATE_PGDATABASE" in os.environ:
        generate_args["dbname"] = os.environ["GENERATE_PGDATABASE"]
    if "GENERATE_PGHOST" in os.environ:
        generate_args["host"] = os.environ["GENERATE_PGHOST"]
    if "GENERATE_PGPORT" in os.environ:
        generate_args["port"] = os.environ["GENERATE_PGPORT"]
    if "GENERATE_PGUSER" in os.environ:
        generate_args["username"] = os.environ["GENERATE_PGUSER"]

    storage_args = {}
    if "GENERATE_PGDATABASE" in os.environ:
        storage_args["dbname"] = os.environ["STORAGE_PGDATABASE"]
    if "GENERATE_PGHOST" in os.environ:
        storage_args["host"] = os.environ["STORAGE_PGHOST"]
    if "GENERATE_PGPORT" in os.environ:
        storage_args["port"] = os.environ["STORAGE_PGPORT"]
    if "GENERATE_PGUSER" in os.environ:
        storage_args["username"] = os.environ["STORAGE_PGUSER"]

    storage_pool = psycopg_pool.NullConnectionPool(kwargs=storage_args)
    storage = Storage(config, storage_pool)

    conn = psycopg.connect(**generate_args)
    global kiln
    kiln = Kiln(config, conn)


@server.head("/")
@server.get("/")
@live.head("/")
@live.get("/")
def root():
    raise HTTPException(status_code=404)


@server.head("/favicon.ico")
@server.get("/favicon.ico")
@live.head("/favicon.ico")
@live.get("/favicon.ico")
def favicon():
    return Response("")


@server.head("/tilejson.json")
@server.get("/tilejson.json")
@live.head("/tilejson.json")
@live.get("/tilejson.json")
def tilejson():
    global config
    return Response(content=config.tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@server.head(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
@server.get(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
def serve_tile(zoom: int, x: int, y: int):
    global storage
    return Response(storage.get_tile(Tile(zoom, x, y)),
                    media_type="application/vnd.mapbox-vector-tile",
                    headers=STANDARD_HEADERS)

@live.head(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
@live.get(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
def live_serve_tile(zoom: int, x: int, y:  int):
    tile = Tile(zoom, x, y)
    global storage
    existing = storage.get_tile(tile)

    # Handle storage hits
    if existing is not None:
        return Response(existing,
                        media_type="application/vnd.mapbox-vector-tile",
                        headers=STANDARD_HEADERS)

    # Storage miss, so generate a new tile
    global kiln
    generated = kiln.render(tile)
    # TODO: Make async
    storage.save_tile(tile, generated)
    return Response(generated,
                        media_type="application/vnd.mapbox-vector-tile",
                        headers=STANDARD_HEADERS)
