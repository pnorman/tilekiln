from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import tilekiln
from tilekiln.kiln import Kiln
from tilekiln.config import Config
from tilekiln.tile import Tile
import os
import psycopg


# Constants for environment variable names
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"

TILE_PREFIX = "/tiles"

STANDARD_HEADERS = {"Cache-Control": "no-cache"}

kiln: Kiln
config: Config

dev = FastAPI()
dev.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])


@dev.on_event("startup")
def load_config():
    global config
    config = tilekiln.load_config(os.environ[TILEKILN_CONFIG])

    # Because the DB connection variables are passed as standard PG* vars,
    # a plain connect() will connect to the right DB

    conn = psycopg.connect()

    global kiln
    kiln = Kiln(config, conn)


@dev.head("/")
@dev.get("/")
def root():
    raise HTTPException(status_code=404)


@dev.head("/favicon.ico")
@dev.get("/favicon.ico")
def favicon():
    return Response("")


@dev.head("/tilejson.json")
@dev.get("/tilejson.json")
def tilejson():
    global config
    return Response(content=config.tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@dev.head(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
@dev.get(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
def serve_tile(zoom: int, x: int, y:  int):
    global kiln
    return Response(kiln.render(Tile(zoom, x, y)),
                    media_type="application/vnd.mapbox-vector-tile",
                    headers=STANDARD_HEADERS)
