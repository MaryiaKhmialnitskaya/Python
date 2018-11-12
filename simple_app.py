"""
-AdventureWorks2014. из query стоит удалить тк есть в connection_str и изменения имени бызв придется править в неск местах
-вызови конфиг из мэйна через аргумент (например помести конфиг в другую папку и задай там другой запрос чтоб протестить)
"""

import logging.config
import json 
import os.path
import pyodbc
import configparser

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
    config.read('config.ini')
    setup_logging()    
    logger = logging.getLogger(__name__)
    logger.info('Startlogging:')

    
    try:
        cnxn=connect_db(config.get("DATABASE_CONFIG",'connection_str'))
    
    except pyodbc.ProgrammingError as e:
        logger.logging.warning('There was a pyodbc warning.  This is the info we have about it: %s' %(e) )
           
    logger.info('Extracting table data  ')
    
    cursor = cnxn.cursor()
    sql=config.get("SQL_CONFIG",'sel_q')
    cursor.execute(sql)
    data=str(cursor.fetchall())
    
    
    cnxn.close()
    
    
    logger.info('Saving table data to file ')
    
    save_path = config.get("FILE_CONFIG",'save_path')
    name_of_file = config.get("FILE_CONFIG",'name_of_file')
    completeName = os.path.join(save_path, name_of_file+config.get("FILE_CONFIG",'extension'))         
    file1 = open(completeName, "w")
    file1.write(data)
    file1.close()

if __name__ == "__main__": 
    main()









