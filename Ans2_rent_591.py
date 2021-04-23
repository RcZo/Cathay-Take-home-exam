#!/usr/bin/env python
# coding: utf-8

# Import
import re
import time

import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

import pymongo

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging


# Config
logger = logging.getLogger("airflow.task")
city_name="台北市" #"新北市" 
city_code="2" #"3"
update_param=[1,"小時"]

chrome_opt_main = Options()
chrome_opt_main.add_argument('--headless')
chrome_opt_main.add_argument('--no-sandbox')
chrome_opt_main.add_argument('--disable-dev-shm-usage')

chrome_opt_item = Options()
chrome_opt_item.add_argument('--headless')
chrome_opt_item.add_argument('--no-sandbox')
chrome_opt_item.add_argument('--disable-dev-shm-usage')
chrome_opt_item.add_argument('--user-agent=Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1')

mongo_cli = pymongo.MongoClient("mongodb://SERVER:27017/")
mongo_db = mongo_cli["HOUSE_RENT"]
mongo_coll = mongo_db["RENT_591_PAGES"]


# Crawler
def get_html_data(request_url,browser):    
    print(request_url)
    test1="未註明"
    test2="未註明"
    test3="未註明"
    test4="未註明"
    test5="未註明"
    test6="未註明"
    browser.get(request_url)
    try:
        _=WebDriverWait(browser, 5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='contact-name']")))       
        bs = BeautifulSoup(browser.page_source, 'html.parser')
        teststr=bs.find('div', {'class': 'contact'}).find('div', {'class': 'contact-name'})
        test1=teststr.em.text
        test2=teststr.p.text    
        try:            
            element = browser.find_element_by_xpath("//div[@class='contact-way']//a[@class='contact-squ contact-tel']")
            browser.execute_script("arguments[0].click();", element)
            _=WebDriverWait(browser, 5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='contact-way']//a[@class='contact-squ contact-tel']")))
            test3=element.get_attribute('href')[4:]
        except Exception as ex:
            logger.error("click phone error: %s" % ex)
            pass    
    
        teststrr=bs.find('div', {'class': 'info block'}).find_all('div', {'class': 'info-list-item'})    
        for item in teststrr:
            item_name=item.em.text.replace(' ','')
            item_text=item.text.replace(' ','')
            if item_name=="型態:":
                test4=item_text[3:]
            elif item_name=="現狀:":
                test5=item_text[3:]
            elif item_name=="性別要求:":
                test6=item_text[5:]
    
        return test1,test2,test3,test4,test5,test6
    except TimeoutException:
        logger.error("Timed out waiting for page to load: %s" % request_url)
        return "error", "error", "error", "error", "error", "error"

def main_task():
    browser_main = webdriver.Chrome('/usr/bin/chromedriver',options=chrome_opt_main)
    browser_detail = webdriver.Chrome('/usr/bin/chromedriver',options=chrome_opt_item)
    browser_main.get("https://rent.591.com.tw/?kind=0")    
    # print(browser.find_element_by_xpath("//div[@class='area-box-body']/*[1]/*[3]").text)    
    _=WebDriverWait(browser_main, 5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='area-box-body']/*[1]/*["+city_code+"]")))
    browser_main.find_element_by_xpath("//div[@class='area-box-body']/*[1]/*["+city_code+"]").click()
    # 輸入 ESC 關閉google 提示，否則無法點選
    try:
        _=WebDriverWait(browser_main, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@class='pageNext']")))
        time.sleep(3)
        browser_main.find_element_by_class_name('pageNext').send_keys(Keys.ESCAPE)  # ECS鍵
    except TimeoutException:
        logger.error("Timed out waiting for page to load")        
        
    bs = BeautifulSoup(browser_main.page_source, 'html.parser')
    totalpages = int(int(bs.find('span', {'class': 'TotalRecord'}).text.split(' ')[-2]) / 30 + 1)
    logger.info('Total pages: %d' % totalpages)
    time_interval='some parameter'
    # -------------loop main page ------------- #
    for page in range(totalpages):
        room_url_list = []  # 存放網址list
        bs = BeautifulSoup(browser_main.page_source, 'html.parser')
        list_house =bs.find_all('ul', {'class': 'listInfo clearfix j-house'})
        #titles = bs.findAll('h3')  # h3 放置物件的區塊
        page_id_list=[]
        for item in list_house:            
            update_time=item.find_all('em')[2].text            
            if int(re.findall(r'[0-9]+', update_time)[0])>update_param[0] and update_param[1] in update_time:
                break;
            else:
                url=item.find('h3').find('a').get('href')            
                page_id = re.findall(r'\-+\d+\.', str(url).strip())[0][1:-1]            
                page_id_list.append(page_id)                      
        
        # -------------loop detail ------------- #
        if len(page_id_list)==0:
            logger.info("All data has been updated to the latest.")
            break;
            
        for page_id in page_id_list:            
            lessor,lessor_type, phone_num,apt_type,apt_status, gender_req=get_html_data("https://m.591.com.tw/v2/rent/"+page_id, browser_detail)
            
            # ---------------------upsert mongo--------------------- #
            upsert_cond = {"page_id": page_id}
            upsert_data = {"page_id": page_id, "city": city_name, "lessor": lessor, "lessor_type": lessor_type,
                           "phone_num": phone_num, "apt_type": apt_type, "apt_status": apt_status, "gender_req": gender_req}

            try:
                mongo_coll.replace_one(upsert_cond, upsert_data, True)
            except Exception as ex:
                logger.error("mongo error: %s" % ex)
                break
           
        if bs.find('a', {'class': 'last'}):
            pass
        else:            
            browser_main.find_element_by_class_name('pageNext').send_keys(Keys.ESCAPE)
            logger.info("Page %s finished. Click next page..." % str(page+1))
            browser_main.find_element_by_class_name('pageNext').click()
            time.sleep(1)

    browser_detail.quit()
    browser_main.quit()
    return "End task."

  
# Airflow
dag = DAG(dag_id='591_rent_2', start_date=days_ago(1), catchup=False, schedule_interval="0 */1 * * *", tags=['591_rent'])

task1 = BashOperator(
    task_id='task1',
    dag=dag,
    bash_command="echo Start task.",
    # net and servers checking for future version...
    owner='Frank',
)




task2 =PythonOperator(
    task_id='main_task',
    python_callable=main_task,
    dag=dag,
    owner='Frank'
)



task1>>task2
