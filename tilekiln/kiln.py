'''
The kiln is what actually generates the tiles, using the config to compute SQL,
and a DB connection to execute it
'''
from tilekiln.config import Config
import psycopg


class Kiln:
    def __init__(self, config: Config, connection: psycopg.Connection):
        self.__config = config
        self.__connection = connection

        # New connection setup
        self.__connection.execute('''SET default_transaction_read_only = true;''')
        self.__connection.commit()
