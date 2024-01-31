import os

import psycopg
from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

import tilekiln
from tilekiln.kiln import Kiln
from tilekiln.config import Config
from tilekiln.tile import Tile


# Constants for environment variable names
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"
TILEKILN_ID = "TILEKILN_ID"

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
    config.id = os.environ[TILEKILN_ID]
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
def redirect_tilejson():
    global config
    return RedirectResponse(f"/{config.id}/tilejson.json")


@dev.head("/{prefix}/tilejson.json")
@dev.get("/{prefix}/tilejson.json")
def tilejson(prefix):
    global config
    if prefix != config.id:
        raise HTTPException(status_code=404, detail=f"Tileset {prefix} not found on server.")
    return Response(content=config.tilejson(os.environ[TILEKILN_URL]),
                    media_type="application/json",
                    headers=STANDARD_HEADERS)


@dev.head("/{prefix}/{zoom}/{x}/{y}.mvt")
@dev.get("/{prefix}/{zoom}/{x}/{y}.mvt")
def serve_tile(prefix: str, zoom: int, x: int, y:  int):
    global config
    if prefix != config.id:
        raise HTTPException(status_code=404, detail=f"Tileset {prefix} not found on server.")
    global kiln
    return Response(kiln.render(Tile(zoom, x, y)),
                    media_type="application/vnd.mapbox-vector-tile",
                    headers=STANDARD_HEADERS)
