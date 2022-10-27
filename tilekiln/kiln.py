'''
The kiln is what actually generates the tiles, using the config to compute SQL,
and a DB connection to execute it
'''
from tilekiln.config import Config


class Kiln:
    def __init__(self, config: Config, connection):
        self.__config = config
        self.__connection = connection
