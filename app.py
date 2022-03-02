'''
    Checks if the data is available in the database if not.
    scraps the data using the KRA Engine. Uses the data that the user uploads  
    from the API.
'''

import datetime
import os
from flask import Flask,request,send_from_directory
from flask.json import jsonify
from flask.templating import render_template
from flask.wrappers import Request
import pandas
from werkzeug.utils import secure_filename
from flask_cors import CORS,cross_origin
from dataSources import getSQLData, parseSimpleData
from dbEngine import parseMulti
from dbEngine import parseSingle
from dataSources import getFileData
import logging
import customLogger



app = Flask(__name__,template_folder='templates',static_folder='static')
app.config['MAX_CONTENT_LENGTH'] = 32*1000*1000
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config["UPLOAD_FILE"] = 'static\sourceFiles\\'

logger = logging.getLogger('customLogger')

logger.info('Server has been started')
@app.route('/',methods=['GET'])
@cross_origin()
def home():
    if request.method == 'GET':
        logger.info("Getting the log value")
        return render_template('sql.html')


@app.route('/single',methods=['GET','POST'])
@cross_origin()
def getSinglePin():
    '''
        Checks if the data is available in the database if not.
        scraps the data using the KRA Engine.
    '''
    error = None
    if request.method == 'POST':
        data = request.get_json(silent=True)
    elif request.method =='GET':
        data = {'pin':request.args.get('pin')}

    logger.info(f"ADDRESS:{request.remote_addr}:  has requested {data['pin']} from {__name__}.")

    try:
        if data['pin']:
            pins = parseSimpleData(data['pin'].split())
            if pins:

                results,missingPins = parseSingle(pins)
                logger.info(f'ADDRESS:{request.remote_addr}: MISSING: {missingPins}.')

                return jsonify({
                    'success':True,
                    'VALID':results,
                    "INVALID":missingPins
                    })

            raise ValueError('Pins are invalid')
    except Exception as e:

        logger.info(f"ADDRESS:{request.remote_addr}:  has the following error:{e.args}.")
        error = e.args
        return jsonify({
            'error Message':error,
            'success':False
        })



@app.route('/file',methods=['POST'])
@cross_origin()
def file():

    error = None
    try:
        configs = request.form
        column = configs['defaultColumn']
        export = True if configs['export'] else False
    except Exception as e:
        column = configs['defaultColumn']
        logger.info(column)
        export = False
    except Exception as e:
        export = False
        column = "pin"

    logger.info(f'The column of the file is {column}')
    data = request.files
    try:
        if 'file' not in data:
            raise ValueError('File Data Not Found')
            
        file = request.files['file']
        fileName = file.filename

        # TODO checks the extension of the file that has been uploaded
        # TODO IF THE FILE IS AN EXCEL FILE READ ASK FOR THE SHEET NAME OR DEFAULT TO THE INITIAL SHEET
        
        if fileName:
            fileName = secure_filename(fileName)
            logger.info(f"ADDRESS:{request.remote_addr}:  has requested {fileName} MODULE: {__name__}.")
            file.save(f'static\sourceFiles\{fileName}')
            
            if column:
                idData = getFileData(f'static\sourceFiles\{fileName}',column=column)
            else:
                idData = getFileData(f'static\sourceFiles\{fileName}')

            if idData[1] == 200:
                returndata , missingPins = parseMulti(idData[0])
            else:
                raise ValueError('Could not locate any pins in your file')

            if export:
                export_file = os.path.splitext(fileName)[0]
                with pandas.ExcelWriter(f'static/destFiles/{export_file}.xlsx') as writer:
                    returndata.to_excel(writer,sheet_name="VALID")
                    missingPins.to_excel(writer,sheet_name="INVALID")

                return send_from_directory(directory="static/destFiles/",path=f"{export_file}.xlsx")
            else:
                return jsonify({
                    'success':True,
                    'message':returndata.to_dict('records')
                })
        else:
            raise ValueError('File Not Found')

    except Exception as e:
        error = e.args
        logger.info(f"ADDRESS:{request.remote_addr}:  has the following error:{error}.")
        return jsonify({
            'error Message':error,
            'success':False
        })


@app.route('/sql',methods=['POST'])
@cross_origin()
def sqlFlask():
    if request.method == 'POST':
        configs = dict(request.form)
        fileName = f"sql_data_.xlsx"
        logger.info(f"The filename is {fileName}")

        try:
            copy = configs.copy()
            for i in configs.keys():
                if not configs[i]:
                    del copy[i]

            logger.info(f"These are the configs sent {copy}")

            pins = getSQLData(configs=copy)
            
            if pins[1] == 200:
                    returndata,missingPins = parseMulti(pins[0])

                    logger.info('The pins have been parsed')

                    with pandas.ExcelWriter(f'static/destFiles/{fileName}') as writer:
                        returndata.to_excel(writer,sheet_name='VALID PINS')
                        missingPins.to_excel(writer,sheet_name="INVALID PINS")

                    logger.info('the pins are written on the file')
                    
                    return send_from_directory(directory="static/destFiles/",path=f"{fileName}")
            else:
                raise ValueError('Could not locate any pins in your file')

        except Exception as e:
            error = e.args
            logger.info(f"ADDRESS:{request.remote_addr}:  has the following error:{error}.") 
            return jsonify({
                'error Message':error,
                'success':False
            })


if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=5000)