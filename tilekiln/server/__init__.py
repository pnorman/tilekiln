import os

import psycopg
import psycopg_pool
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import tilekiln
from tilekiln.config import Config
from tilekiln.kiln import Kiln
from tilekiln.tile import Tile
from tilekiln.storage import Storage


# Constants for environment variable names
# Passing around enviornment variables really is the best way to get this to fastapi
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"
TILEKILN_THREADS = "TILEKILN_THREADS"

STANDARD_HEADERS = {}

kiln: Kiln
config: Config
storage: dict[str, Storage]
storage = {}

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
    '''Load the config for the server with static pre-rendered tiles'''
    global config  # TODO: Refactor away config
    global storage

    # Because the DB connection variables are passed as standard PG* vars,
    # a plain ConnectionPool() will connect to the right DB
    pool = psycopg_pool.NullConnectionPool()

    storage = Storage(None, pool, None)


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
    if "STORAGE_PGDATABASE" in os.environ:
        storage_args["dbname"] = os.environ["STORAGE_PGDATABASE"]
    if "STORAGE_PGHOST" in os.environ:
        storage_args["host"] = os.environ["STORAGE_PGHOST"]
    if "STORAGE_PGPORT" in os.environ:
        storage_args["port"] = os.environ["STORAGE_PGPORT"]
    if "STORAGE_PGUSER" in os.environ:
        storage_args["username"] = os.environ["STORAGE_PGUSER"]

    storage_pool = psycopg_pool.NullConnectionPool(kwargs=storage_args)
    storage = Storage(config, storage_pool, None)

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


@server.head("/{prefix}/tilejson.json")
@server.get("/{prefix}/tilejson.json")
def tilejson(prefix: str):
    global storage
    if prefix != storage.id:
        raise HTTPException(status_code=404, detail=f'''Tileset {prefix} not found on server.''')
    return Response(content=storage[prefix].tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@live.head("/{prefix}/tilejson.json")
@live.get("/{prefix}/tilejson.json")
def live_tilejson(prefix: str):
    global config
    if prefix != storage.id:
        raise HTTPException(status_code=404, detail=f'''Tileset {prefix} not found on server.''')
    return Response(content=storage.tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@server.head("/{prefix}/{zoom}/{x}/{y}.mvt")
@server.get("/{prefix}/{zoom}/{x}/{y}.mvt")
def serve_tile(prefix: str, zoom: int, x: int, y: int):
    global storage
    if prefix != storage.id:
        raise HTTPException(status_code=404, detail=f"Tileset {prefix} not found on server.")

    return Response(storage[prefix].get_tile(Tile(zoom, x, y)),
                    media_type="application/vnd.mapbox-vector-tile",
                    headers=STANDARD_HEADERS)


@live.head("/{prefix}/{zoom}/{x}/{y}.mvt")
@live.get("/{prefix}/{zoom}/{x}/{y}.mvt")
def live_serve_tile(prefix: str, zoom: int, x: int, y:  int):
    global storage
    if prefix != storage.id:
        raise HTTPException(status_code=404, detail=f"Tileset {prefix} not found on server.")

    tile = Tile(zoom, x, y)
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
