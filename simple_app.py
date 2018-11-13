import logging.config
import json 
import os.path
import pyodbc
import configparser
import argparse
    
class Database:
    def __init__(self, name):
        self._conn = pyodbc.connect(name)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()
        
    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor
        
    def commit(self):
        self.connection.commit()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()


def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):

    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def connect_db(con_str):

    cnxn = pyodbc.connect(con_str)
    return cnxn

def main(): 
    
    config = configparser.ConfigParser()
    
    #Check if config file path is specified. If yes, read config from there. Else, from default directory. 
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to config file")
    args = parser.parse_args()
    if args.config:
        config.read(args.config+'\config.ini')
    else: 
        config.read('config.ini')
        
    #Set up logging
    setup_logging()    
    logger = logging.getLogger(__name__)
    logger.info('Startlogging:')


    with Database(config.get("DATABASE_CONFIG",'connection_str')) as db:  
        db.query(config.get("SQL_CONFIG",'sel_q'))
        data=str(db.fetchall())

    logger.info('Saving table data to file ')
    
    #Wrire the info read to file
    save_path = config.get("FILE_CONFIG",'save_path')
    name_of_file = config.get("FILE_CONFIG",'name_of_file')
    completeName = os.path.join(save_path, name_of_file+'.'+config.get("FILE_CONFIG",'extension'))         
    file1 = open(completeName, "w")
    file1.write(data)
    file1.close()

if __name__ == "__main__": 
    main()









