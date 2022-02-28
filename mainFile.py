from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from PIL import Image
import logging as log
import pytesseract
import concurrent.futures
import pandas
import csv
import io
import re
import logging as log



log.basicConfig(
    level=log.CRITICAL,
    filename='test.log',
    datefmt='%d-%b-%y %H:%M:%S',
    format='%(asctime)s -%(levelname)s:%(name)s - %(message)s')


def getData(pin,counter):
    option = Options()
    log.info(f'{pin} Started searching for the {counter} time')
    url = 'https://itax.kra.go.ke/KRA-Portal/pinChecker.htm'
    option.headless =True
    try:
        try:
            driver = webdriver.Chrome(executable_path=r'C:\Users\george.kiguta\.wdm\drivers\chromedriver\win32\92.0.4515.107\chromedriver.exe',options=option)
        except:
            log.warning(f'{pin} Driver not found. Installing the driver.')
            driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())

        driver.get(url)

        try:
            WebDriverWait(driver=driver,timeout=20).until(EC.presence_of_element_located((By.ID,'vo.pinNo')))
        except Exception as e:
            log.error(f'{pin} Page not loading. Exiting due to {e}')
            driver.close()
            return None

        # enter the pin to the required key.
        pinInputField = driver.find_element_by_id('vo.pinNo')
        log.info(f'{pin} : KRA page has been loaded successfully.')
        pinInputField.send_keys(pin)

        # ! source code for the html content
        # solve the capchta and enter the results
        
        # ! reload page if the image is empty
        try:
            resolveText = int(resolve(driver.find_element_by_id('captcha_img').screenshot_as_png,pin))

            if type(resolveText) == int:
                driver.find_element_by_id('captcahText').send_keys(resolveText)
                driver.find_element_by_id('consult').click()
                log.info(f'{pin} :POST Getting the data from the KRA page')
            else:
                log.error(f'{pin} . Arthmetic value is wrong.')
                raise ValueError('Value is empty or Wrong.')

        except Exception as e:
            log.error(f"{pin} : Number not identified. Due to {e}")
            raise e

        log.info(f'{pin} Has been identified. And page has loaded successfully.')
        page_source = driver.page_source
        driver.close()
        return page_source
    except:
        log.error('File failed to load.')
        driver.close()
        return None
    


def arithmetic(string:str):

    if len(string.strip())>20:
        log.error(f'{string}: That has been identified is invalid. The string is too long.')
        return None

    # checks if the OCR recognized the ? at the end.
    log.debug(f'{string} that has been identified using PyTesseract.')

    if not string.__contains__('?'):
        reversedStr = ''.join(reversed(string)).replace('7','',1)
        string = ''.join(reversed(reversedStr))

    # gets the arithmetic symbol
    symbol = re.split('\d*',string,1)[1].strip()[0]
    
    # gets the numbers of the captcha
    numbers = re.split('[-*+/?]',string)
    
    if type(numbers[-1]) != int:
        second = re.search('\d*',numbers[-1]).string
        try:
            second = int(second)
        except:
            numbers = re.split('[-*+/?]',string)[:-1]
    try:
        numbers[0] = int(numbers[0])
        numbers[1] = int(numbers[1])
    except:
        log.error(f'{string}: That has been identified is invalid. They are not integers.')
        return None

    # returns the data required for the flag according to the numeric captcha that is in place
    if symbol == '-':
        return int(numbers[0]) - int(numbers[1])
    elif symbol == '+':
        return int(numbers[0]) + int(numbers[1])
    else:
        return None


def resolve(file,pin):
    # todo: Look for a better OCR to escalate the image to.
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    string =  pytesseract.image_to_string(Image.open(io.BytesIO(file)))
    if type(string) != str or len(string) == 0:
        return None

    result = arithmetic(string.strip())
    
    if type(result) == int:
        log.info(f'{pin} : Captcha has been identified')
        return result  
    else:
        raise ValueError(f'{pin} : Value has not been found')  


def getPersonalData(html,pin):
    data = []
    soup = BeautifulSoup(html,'lxml')
    categories = soup.find_all('b')
    #  todo: Check for invalid messages.

    # todo: Identify the errors that might occur.
    try:
        for text in categories:
            if type(text.text) == int:
                log.critical(f'{pin}: might be invalid')
                return None

            if "System is not able to process your request. Please try again." in text.text:
                log.error(f'{pin} system error in the KRA Page.')
                return None

            if "Wrong result of the arithmetic operation." in text.text:
                log.error(f'{pin} Wrong Arithmetic Expressions')
                return None
            parent = text.find_parent().find_next_sibling()
            data.append('Valid')
            data.append(parent.text)

        log.debug(f'{pin} has the following data {data}')
        return data

    except Exception as e:
        log.error(f'{pin}: Personal data cannot be identified.')
        raise SystemError(f'{pin} error') from e
    

def procedure(pin,counter = 0):
    # //headers = ['PIN','Taxpayer Name','PIN Status','iTax Status'] 
    if len(pin) != 11:
        log.critical(f'{pin} : invalid length. It should be 11 characters.')
        return None
    try:
        file = getData(pin,counter)
        if file:    
            data = getPersonalData(file,pin)
            if data:
                with open('data.csv','a') as resultFiles:
                    dictWriter = csv.writer(resultFiles)
                    dictWriter.writerow(data)
                    return None
        log.error(f'{pin} entered cannot be identified correctly.')
        if counter < 3:
            return procedure(pin,counter=counter+1)
        return None
    except Exception as e:
        print(f'Error has occurred {counter} due to {e}')
        if counter < 3:
            return procedure(pin,counter=counter+1)
        log.critical(f'{pin} :Run time error for {pin}. Please check later {e}')
        return None


def sampleData():
    return pandas.read_pickle('sample.pickle')


def start():
    pins = sampleData()
    pins = pins[:30]
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        executor.map(procedure,pins)

if __name__ == '__main__':
    log.info('Scrapping has started')
    start()
