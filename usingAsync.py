from pandas.core import groupby
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
from dataClean import groupData
import asyncio
import csv
import io
import re
import logging as log



log.basicConfig(
    level=log.INFO,
    filename='test_3.log',
    datefmt='%d-%b-%y %H:%M:%S',
    format='%(asctime)s -%(levelname)s:%(name)s - %(message)s')


async def waitData(driver,url,pin):
    driver.get(url)
    try:
        WebDriverWait(driver=driver,timeout=20).until(EC.presence_of_element_located((By.ID,'vo.pinNo')))
    except Exception as e:
        log.error(f'{pin} Page not loading. Exiting due to {e}')
        driver.close()
        return False
    return driver


async def getDriver(url,pin,loop):
    option = Options()
    option.headless =True
    try:
        driver = webdriver.Chrome(executable_path=r'chromedriver.exe',options=option)
    except:
        log.warning(f'{pin} Driver not found. Installing the driver.')
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())
    driver = await loop.create_task(waitData(driver,url,pin))
    return driver 


async def pushData(driver,result,pin):
    try:
        if type(result) == int:
            driver.find_element_by_id('captcahText').send_keys(result)
            driver.find_element_by_id('consult').click()

            log.info(f'{pin} :POST Getting the data from the KRA page')

        else:
            log.error(f'{pin} . Arthmetic value is wrong.')
            raise ValueError('Value is empty or Wrong.')
        return driver
    except Exception as e:
        log.error(f'{pin} :{e} caused the driver to be corrupted')
        raise Exception(f'Error due to {e}') from e


async def getData(pin,counter,loop):
    log.info(f'{pin} Started searching for the {counter} time')
    url = 'https://itax.kra.go.ke/KRA-Portal/pinChecker.htm'
    try:
        driver = await getDriver(url,pin,loop)
        # enter the pin to the required key.
        pinInputField = driver.find_element_by_id('vo.pinNo')
        log.info(f'{pin} : KRA page has been loaded successfully.')
        pinInputField.send_keys(pin)

        # ! source code for the html content
        # solve the capchta and enter the results
        # ! reload page if the image is empty

        try:
            resolveText = await resolve(driver.find_element_by_id('captcha_img').screenshot_as_png,pin,loop)
            resolveText = int(resolveText)
            driver = await pushData(driver,resolveText,pin)
        except Exception as e:
            log.error(f"{pin} : Number not identified. Due to {e}")
            raise e

        log.info(f'{pin} Has been identified. And page has loaded successfully.')
        page_source = driver.page_source
        driver.close()
        log.info('getting the data to the other file')
        return page_source
    except Exception as e:
        # print(e)
        log.error(f'File failed to load. as {e}')
        driver.close()
        return False
    


async def arithmetic(string:str):

    if len(string.strip())>20:
        log.error(f'{string}: That has been identified is invalid. The string is too long.')
        return False

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
        log.error(f'{string}: That has been identified is invalid. They are nor integers.')
        return False

    # returns the data required for the flag according to the numeric captcha that is in place
    if symbol == '-':
        return int(numbers[0]) - int(numbers[1])
    elif symbol == '+':
        return int(numbers[0]) + int(numbers[1])
    else:
        return False


async def resolve(file,pin,loop):
    # todo: Look for a better OCR to escalate the image to.
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    
    try:
        string =  pytesseract.image_to_string(Image.open(io.BytesIO(file)),timeout=2)
    except RuntimeError:
        return False

    if type(string) != str or len(string) == 0:
        return False
    
    result = await loop.create_task(arithmetic(string.strip()))
    
    if type(result) == int:
        log.info(f'{pin} : Captcha has been identified')
        return result
    else:
        raise ValueError(f'{pin} : Value has not been found')  


async def getPersonalData(html,pin):
    data = []
    soup = BeautifulSoup(html,'lxml')
    categories = soup.find_all('b')
    #  todo: Check for invalid messages.
    log.info(f'{pin} : Getting the internal data')
    # todo: Identify the errors that might occur.
    try:
        for text in categories:
            if "System is not able to process your request. Please try again." in text.text:
                log.error(f'{pin} system error in the KRA Page.')
                return False

            if "Wrong result of the arithmetic operation." in text.text:
                log.error(f'{pin} Wrong Arithmetic Expressions')
                return False
            parent = text.find_parent().find_next_sibling()
            data.append(parent.text) 
        log.debug(f'{pin} has the following data {data}')
        return data
    except Exception as e:
        log.error(f'{pin}: Personal data cannot be identified.')
        raise SystemError(f'{pin} error') from e


async def writefile(data):
    with open('data.csv','a') as resultFiles:
        dictWriter = csv.writer(resultFiles)
        dictWriter.writerow(data)


async def procedure(pin,loop,counter = 0):
    #  //headers = ['PIN','Taxpayer Name','PIN Status','iTax Status'] 
    if len(pin) != 11:
        log.critical(f'{pin} invalid length. It should be 11 characters.')
        return False
    try:
        file = await loop.create_task(getData(pin,counter,loop))
        if file:    
            data = await loop.create_task(getPersonalData(file,pin))
            if data:
                log.info(f'{pin} has the pin content')
                await loop.create_task(writefile(data))
                return False

        log.error(f'{pin} entered cannot be identified correctly.')
        if counter < 3:
            return await loop.create_task(procedure(pin,loop,counter=counter+1))
        return False

    except Exception as e:

        print(f'Error has occurred {counter} due to {e}')
        if counter < 3:
            return await loop.create_task(procedure(pin,loop,counter=counter+1))
        log.critical(f'Run time error for {pin}. Please check later {e}')
        return False



# async def newProc(pins):
#     tasks = [loop.create_task(procedure(pin,loop))  for pin in pins]
#     await asyncio.wait([task for task in tasks])


async def asyncStart(group,loop):
    print(len(group))
    for indices in range(0,len(group)+7,7):
        try:
            tasks = [loop.create_task(procedure(pins,loop)) for pins in group[indices:indices+7]]
        except IndexError:
            tasks = [loop.create_task(procedure(pins,loop)) for pins in group[indices:len(group)]]
    # for i in pins:
    #     tasks.append(loop.create_task(procedure(i,loop)))
        await asyncio.wait(tasks)
    # await asyncio.wait(tasks)
    # with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    #     executor.map(procedure,pins)
  
def startLoops(group):
    loop =  asyncio.new_event_loop()
    loop.run_until_complete(asyncStart(group,loop))
    loop.close()

if __name__ == '__main__':
    allValidPins = groupData()
    print(len(allValidPins)) 
    # loop.set_debug(1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for group in allValidPins:
            executor.submit(startLoops,group)
            