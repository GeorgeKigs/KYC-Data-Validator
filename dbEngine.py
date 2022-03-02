from os import name
import mysql.connector as connector
import numpy
from dataSources import queryLogs
from kraEngine import startMulti, startSingle
import pandas as pd
from testFiles.testDB import init_test_db
import sqlalchemy
import logging
import dotenv
import customLogger

logger = logging.getLogger('customLogger')

def init_prod_db():
# TODO: Should be able to handle different types of connections
    logger.info("Initiating the main DB")
    # e.g mysql, oracle and sql serevr. 
    # Could use sqlalchemy but requires the nessesary drivers. 
    # Thus we use the connectors the connectors themselves
    try:
        credentials = dict(dotenv.dotenv_values('conf.env'))
# TODO: DB TO WRITE TO IN THE DATABASE
        connection = connector.connect(**credentials)
        return connection
    except Exception as e:
        logger.error(f'Conncetion error due to {e}. Try again')
        exit(1)


def close_db(connection):
    logger.info("Closing the DB")
    connection.close()


def connectionDb(env='test'):
    connection = init_test_db()
    if env == 'prod':
        connection = init_prod_db()
    return connection


def queryDb(pins:list,env='test'):
    # // todo:  convert the list to a pandas dataframe
    query = f"""select * from pins where pin in ('{("','").join(pins)}');"""
    connection = connectionDb(env)

    # ! fetch the data using pandas to ensure the data is a pandas dataframe
    returnData = pd.read_sql(query,connection)
    returnData.rename(
        columns={
            'taxpayer_name':'Taxpayer Name',
            'pin':'PIN',
            'pin_status':'PIN Status',
            'itax_status':'iTax Status'
        },inplace=True)
    logger.info(f"The main db has returned the following data:{len(returnData)}")
    return returnData


def parseSingle(pins:list,env='test'):
    # todo: Convert the pandas Dataframe to a list using pandas.to_dict(record)
    logger.info(f"Parsing Single Pin: {pins}")

    returnData = queryDb(pins,env)
    if returnData.empty:
        KRA_data = startSingle(pins)
        if KRA_data:
            for data in KRA_data:
                InsertDbSingle(details=data)
        returnData = KRA_data
    else:
        returnData = returnData.to_dict('records')

    remaining_pins = numpy.append(pins,returnData[0]['PIN'] if returnData else [])
    pinsDf = pd.DataFrame(remaining_pins,columns=['pins']).drop_duplicates(keep=False)
    remaining_pins = list(pinsDf['pins'].array)

    return returnData,remaining_pins


def parseMulti(pins:list,env='test'):

    logger.info("Parsing Multiple Pins has started")
    pinSeries = numpy.array(pins)

    returnData = queryDb(pins,env)
    logger.info(f'length of the data is {len(returnData)}')

    pinSeriesEXD = numpy.append(pinSeries,returnData['PIN'])

    pinsDf = pd.DataFrame(pinSeriesEXD,columns=['pins']).drop_duplicates(keep=False)
    logger.info(f'length of the remained data is {len(pinsDf)}')

    if not pinsDf.empty:

        remaining_pins = pinsDf['pins'].array
        logger.info("Parsing Multiple Pins in the logs has started")

        query_data = queryLogs(remaining_pins)
        logger.info(f"Logs  have {len(query_data)} pins")

        if not query_data.empty:

            remaining_pins = numpy.append(remaining_pins,query_data['PIN'])
            pinsDf = pd.DataFrame(remaining_pins,columns=['pins']).drop_duplicates(keep=False)
            remaining_pins = pinsDf['pins'].array

            KRA_data = startMulti(remaining_pins)
            logger.info(f"KRA  has {len(KRA_data)} pins")

            KRA_data = pd.concat([query_data,KRA_data])

        else:
            KRA_data = startMulti(remaining_pins)
            logger.info(f"KRA  has {len(KRA_data)} pins")

        if not KRA_data.empty:
            insertDbMulti(KRA_data,env)

            KRA_data.rename(
            columns={
                'taxpayer_name':'Taxpayer Name',
                'pin':'PIN',
                'pin_status':'PIN Status',
                'itax_status':'iTax Status'
            },inplace=True)

        returnData=pd.concat([returnData,KRA_data],ignore_index=True)

    resultantPins = numpy.append(pinSeries,returnData['PIN'])
    missingPins = pd.DataFrame(resultantPins).drop_duplicates(keep=False)
        
    return returnData,missingPins



def InsertDbSingle(details:dict):
    try:
        connection = connectionDb()
        logger.info(f"Inserting {details['PIN']} into the main DB")
        queryDb = f'''
            INSERT INTO detailsmock.pins (taxpayer_name,pin,pin_status,itax_status)
            VALUES(
                '{details["Taxpayer Name"]}',
                '{details["PIN"]}',
                '{details["PIN Status"]}',
                '{details["iTax Status"]}'
                )
        '''
        with connection.cursor() as cursor:
            cursor.execute(queryDb)
        connection.commit()
        close_db(connection=connection)

    except Exception as e:
        logger.error(f"{e.args} while trying to enter the data into the DB")
        raise e

def insertDbMulti(details,env):
    try:
        # todo: Export the configurations to a .env file
        config = dotenv.dotenv_values("conf.env")

        connection = sqlalchemy.create_engine(f"mysql+pymysql://{config['USER']}:{config['PASSWORD']}@{config['HOST']}/{config['DATABASE']}")
        details.rename(
            columns={
                'Taxpayer Name':'taxpayer_name',
                'PIN':"pin",
                'PIN Status':'pin_status',
                'iTax Status':'itax_status'
            },inplace=True)

        logger.info(f"Inserting {len(details['pin'])} into the main DB")
        
        details.to_sql(name='pins',con=connection,if_exists = 'append',index=False)

    except Exception as e:
        logging.error(f"{e.args} while trying to enter the data into the DB")
        raise(e)