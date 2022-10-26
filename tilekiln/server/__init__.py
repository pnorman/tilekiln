from fastapi import FastAPI, Response, HTTPException
import tilekiln
import os

# Constants for environment variable names
TILEKILN_CONFIG = "TILEKILN_CONFIG"
TILEKILN_URL = "TILEKILN_URL"

STANDARD_HEADERS = {"Cache-Control": "no-cache"}
dev = FastAPI()


@dev.on_event("startup")
def load_config():
    global config
    config = tilekiln.load_config(os.environ[TILEKILN_CONFIG])


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
