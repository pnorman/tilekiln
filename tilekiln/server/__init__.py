from fastapi import FastAPI, Response
import tilekiln
import os


dev = FastAPI()

# This method of loading the config doesn't work for multiple workers. TODO: fix

config = None


@dev.on_event("startup")
def load_config():
    global config
    config = tilekiln.load_config(os.environ["TILEKILN_CONFIG"])


@dev.get("/tilejson.json")
def tilejson():
    global config
    return Response(content=config.tilejson())
