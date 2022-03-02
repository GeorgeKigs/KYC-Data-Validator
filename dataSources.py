from flask.json import jsonify
import numpy
import pandas
import os
import pyodbc
from miscDets import chooseDB
import customLogger
import dotenv
import logging

logger = logging.getLogger("customLogger")

# * might be a bit tricky.......
def getSQLData(configs:dict):
    status = 200
    configKeys = list(configs.keys())

    if "query" in configKeys:
        sql_statement = configs['query']
    elif "table" in configKeys and "column" not in configKeys:
        sql_statement = f"select pin from {configs['table']}"
    elif "table" in configKeys and "column" in configKeys:
        sql_statement = f"select {configs['column']} from {configs['table']}"
    
    if 'server' in configKeys:
        connection = chooseDB(configs)

    data = pandas.read_sql(sql_statement,connection)

    pins = numpy.array(parseResults(data))
    uniquePins = list(numpy.unique(pins))
    return uniquePins,status




def getFileData(file ='ReminderPins.csv',column:str='pin'):

    if not os.path.isfile(file):
        raise ValueError("Error: File does not exist")

    else:
        status = 200
        _,ext = os.path.splitext(file)
        if ext == '.csv':
            data = pandas.read_csv(file,usecols=[column])
        elif ext == '.xls' or ext == '.xlsx':
            # todo: wait to accomodate more than one sheet
            data = pandas.read_excel(file,usecols=[column])
        else:
            raise ValueError("File extensions is not found")

        data = numpy.array(parseResults(data,[column]))
        data = list(numpy.unique(data))
        return data,status


def parseResults(data,column='pin'):
    if len(data.columns) == 1:
        column = data.columns.values[0]

    if column in data.columns.values:
        data.reset_index(inplace=True)
        reviewed = data[column].str.upper()
        data['first'] = data[column].astype(str).str[0]
        data['last'] = data[column].astype(str).str[-1]

        reviewed = data[column].loc[
            (data[column].str.len()==11) & 
            (data['first'].str.isalpha()) & 
            (data['last'].str.isalpha())
        ]
        
        return reviewed

    else:
        raise ValueError("Column could not be found")


def parseSimpleData(data:list):
    newData = []
    for pin in data:
        try:
            if not pin[0].isdigit() and len(pin) == 11 and not pin[-1].isdigit():
                newData.append(pin.upper())

        except Exception as e:
            logging.error(e.args)
            break

    return newData


def sampleData():
    return pandas.read_pickle('sample.pickle')



def convertString(data):
    if data.__contains__('VALID'):
        _,validDict = data.split('{')
        valid = validDict.replace('}','')
                
        splitData = valid.split(',')
        data = {}
        for obj in splitData:
            key,val = obj.split(':')
            data[key.replace("'",'').strip()]=val.replace("'",'').strip()
        return data


def readFile():
    # ! Change the name of the log file to the one specified on the configuration file
    try:
        logs = dotenv.dotenv_values("conf.env")['logs']

        with open(logs) as file:
            result = pandas.concat([
                pandas.DataFrame(
                    [convertString(line)],
                    columns=['PIN','Taxpayer Name','PIN Status','iTax Status']
                )
                for line in file.readlines() if line.__contains__('VALID')]
                ,ignore_index=True)
            result.drop_duplicates(keep='first',inplace=True)
        return result

    except Exception as e:
        logger.error(f'Could not read log files due to {e}')
        raise(e)

def queryLogs(pins):
    data = readFile()
    results = data.loc[data['PIN'].isin(pins)]
    return results