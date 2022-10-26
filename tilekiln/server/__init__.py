from fastapi import FastAPI, Response
import tilekiln


dev = FastAPI()

# This method of loading the config doesn't work for multiple workers. TODO: fix

config = None


def load_config(config_path):
    global config
    config = tilekiln.load_config(config_path)


@dev.get("/tilejson.json")
async def root():
    return Response(content=config.tilejson())
