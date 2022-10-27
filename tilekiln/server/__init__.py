from fastapi import FastAPI, Response, HTTPException
import tilekiln
from tilekiln.kiln import Kiln
import os
import psycopg


# Constants for environment variable names
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"

TILE_PREFIX = "/tiles"

STANDARD_HEADERS = {"Cache-Control": "no-cache"}

dev = FastAPI()


@dev.on_event("startup")
def load_config():
    global config
    config = tilekiln.load_config(os.environ[TILEKILN_CONFIG])

    # Because the DB connection variables are passed as standard PG* vars,
    # a plain connect() will connect to the right DB

    conn = psycopg.connect()

    global kiln
    kiln = Kiln(config, conn)


@dev.get("/")
def root():
    raise HTTPException(status_code=404)


@dev.get("/favicon.ico")
def favicon():
    return Response("")


@dev.get("/tilejson.json")
def tilejson():
    global config
    return Response(content=config.tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@dev.get(TILE_PREFIX + "/{zoom}/{x}/{y}.mvt")
def serve_tile(zoom, x, y):
    return Response(content=f"Tile for {zoom}/{x}/{y}",
                    headers=STANDARD_HEADERS)
