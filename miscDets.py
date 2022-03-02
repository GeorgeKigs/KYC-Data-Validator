import os
import json
import dotenv
import pyodbc
import mysql.connector as connector
import cx_Oracle

def chooseDB(configs:dict):
    server = configs['server'].lower()
    try:
        if server == 'odbc':
            connection = pyodbc.connect(
                f'DSN={configs["dsn"]};PWD={configs["password"]}'
            )

        elif server == 'oracle':
            connection = cx_Oracle.connect(
                user=configs['user'],
                password = configs['password'],
                dsn=configs['dsn'],
                encoding="UTF-8"
            )
            
        elif server == 'mysql':
            connection = connector.connect(**{
                'host':configs['host'],
                'database':configs['database'],
                'password':configs['password'],
                'user':configs['user']
                }
            )
        else:
            print("Database not supported")
    except Exception as e:
        print(f"Error occurred due to{e}")
        exit(0)
    

    return connection

def readFile(file):
    _,ext = os.path.splitext(file)
    if ext == '.json':
        with open(file,'r') as file:
            
            configData = json.loads(file.read())
            
            if type(configData) == list:
                configData = dict((key.lower(),value) for  data in configData for (key,value) in data.items())
            
            elif type(configData) == dict:
                configData = dict((key.lower(),value) for (key,value) in configData.items())

    elif ext == '.env':
        configData = dotenv.dotenv_values(file)
        configData = dict((key.lower(),value) for (key,value) in configData.items())
    else:
        exit(1)
    
    return configData
