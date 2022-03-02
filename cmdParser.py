import argparse
import os
import numpy
import datetime
import dotenv
import json
import pandas
from dataSources import getFileData, getSQLData, parseSimpleData
from dbEngine import parseMulti
from miscDets import chooseDB, readFile

def parse():
    pins = numpy.array([])

    myParser = argparse.ArgumentParser(
        description="Tool that is used to scan for pins from the KRA website",
        allow_abbrev=False
    )
    
    
    myParser.add_argument(
        '--pins',
        type=str,
        action='store',
        nargs='+',
        required=False,
        help="Pins we want to check"
    )
    
    myParser.add_argument(
        '--file',
        nargs='+',
        help="""
        Usage: Enter a single param to indicate the file with the default column name 'pin'.
            a list [file,column] to indicate the file with the column.
            Enter any number of files.
        """,
        action='store',
        required=False
    )

    myParser.add_argument(
        '--defaultColumn',
        help="""Usage: Enter the default column to be used for one or more files.""",
        action='store',
        required=False
    )

    # the file to export the data to
    myParser.add_argument(
        '--export',
        required=False,
        default=None
    )

    # check a way of adding subcategories
    myParser.add_argument(
        '--importsql',
        required=False,
        default=None
    )

    myParser.add_argument(
        '--exportsql',
        required=False,
        default=None
    )



    # TODO: Checks for files that can be used to import the data
    # TODO all the dataSources to process the args that have been entered

    args = myParser.parse_args()
    if not args.pins and not args.file:
        print('Use a pin or a file that contains a column with pins')
        exit(1)


    if args.file:
        for file in args.file:
            try:
                if file.startswith('['):
                    # clean the input
                    file,column = file.removeprefix('[').removesuffix(']').split(',')
                    data = getFileData(file,column)
                else:
                    # vaidation
                    data = getFileData(file,args.defaultColumn) if args.defaultColumn else  getFileData(file)
            except Exception as e:
                print(f"Error: Please check on the file {file} due to {e}")
                continue

            if data[1] == 200:
                pins = numpy.append(pins,data[0])
            else:
                print("Error: Please check on the file")
                exit(1)

    if args.pins:
        # validation
        checkedPins = parseSimpleData(args.pins)
        pins = numpy.append(pins,checkedPins)

    if args.importsql:
        if os.path.isfile(args.importsql):
            file = args.importsql
            configData = readFile(file)
            data = getSQLData(configData)
            if data[1] == 200:
                pins = numpy.append(data,data[0])
            else:
                print("Error: Please check on the file")
                exit(1)
        else:
            exit(1)
    

    pins = numpy.unique(pins)
 
    # parse multi for all of the data
    resultData = parseMulti(pins=pins)

    if args.export:
        # write to the file path
        resultData.to_csv(f'/static/destFiles/KRA_Data_{datetime.datetime.now()}.csv')
    else:
        # print the results only and the result status
        print(resultData)
        # and the pins that have not been validated
        pass
    
    if args.exportsql:
        if os.path.isfile(args.exportsql):
            # import the configuration file to get the config data
            file = args.exportsql
            configData = readFile(file)
            connection = chooseDB(configData)
            resultData.to_sql(configData['table'],connection,if_exists="append",index=False)
        else:
            print(resultData)
            exit(1)
    return

if __name__ == '__main__':
    results = parse()
    print(len(results))

