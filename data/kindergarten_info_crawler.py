# 全國教保資訊網-幼兒園查詢-基本資料查詢
# 2021/03/18 蘇彥庭
# 資料來源: 全國教保資訊網
# https://ap.ece.moe.edu.tw/webecems/pubSearch.aspx

import re
import requests
import pandas as pd
import time
import random
import json
from sqlalchemy import create_engine
from tqdm import tqdm
from bs4 import BeautifulSoup


# 建立連線函數
def CreateDBEngine():
    secretFile = json.load(open('dbSecret.json', 'r'))
    host = secretFile['host']
    username = secretFile['user']
    password = secretFile['password']
    port = secretFile['port']
    database = secretFile['dbName']
    return create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}', echo=False)


# 設定header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36'
}

# 連結網址
url = 'https://ap.ece.moe.edu.tw/webecems/pubSearch.aspx'

# 以session方式連入
ss = requests.session()
res = ss.get(url, headers=headers)
soup = BeautifulSoup(res.text, 'html.parser')

# 向網站post取得資訊(模擬按下搜尋按鈕)
# post參數
data = {
    'ScriptManager1': 'UpdatePanel1|btnSearch',
    '__EVENTTARGET': 'PageControl1$lbPageChg',
    '__VIEWSTATE': soup.find(id='__VIEWSTATE')['value'],
    '__VIEWSTATEGENERATOR': soup.find(id='__VIEWSTATEGENERATOR')['value'],
    '__EVENTVALIDATION': soup.find(id='__EVENTVALIDATION')['value'],
    '__ASYNCPOST': 'true',
    'btnSearch': '搜尋'
}

# 取的搜尋按鈕後之頁面結果 此頁面主要是獲得新的post參數及確認總查詢頁數
res = ss.post(url, data=data, headers=headers)
soup = BeautifulSoup(res.text, 'html.parser')

# 最大頁數
maxPage = int(soup.select('span#PageControl1_lblTotalPage')[0].text)

# 組合新的post參數
data = {
    'ScriptManager1': 'UpdatePanel1|PageControl1$lbPageChg',
    '__EVENTTARGET': 'PageControl1$lbPageChg',
    '__ASYNCPOST': 'true',
}

# post參數藏在網頁的html最下面 且無標籤 故要直接比對抓出
searchRange = res.text[res.text.find('__VIEWSTATE'):].split('|')
for i in range(len(searchRange)-1):
    if searchRange[i] in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
        data[searchRange[i]] = searchRange[i+1]

# 建立儲存表
kindergartenInfo = pd.DataFrame()

# 迴圈頁數
for page in tqdm(range(1, (maxPage+1))):

    # 修改本次post參數的目標頁數
    data['PageControl1$txtPages'] = str(page)

    # 向網站post取得資訊
    res = ss.post(url, data=data, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    # 幼稚園名稱
    name = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblSchName_'))]
    # 縣市
    county = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblCity_'))]
    # 設立別
    types = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblPub_'))]
    # 地址
    address = [elem.text for elem in soup.find_all(id=re.compile('GridView1_hlAddr_'))]
    # 電話
    tel = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblTel_'))]
    # 園所網址
    web_url = [elem.text for elem in soup.find_all(id=re.compile('GridView1_hlUrl_'))]
    # 核定人數
    quota = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblGenStd_'))]
    # 5歲免學費
    free_5 = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblCoopCom_'))]
    # 準公共幼兒園
    quasi_public = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblStdPub_'))]
    # 兼辦國小課後
    after_school = [elem.text for elem in soup.find_all(id=re.compile('GridView1_lblChildSvc_'))]

    # 彙整資料
    iKindergartenInfo = pd.DataFrame({'name': name,
                                      'county': county,
                                      'types': types,
                                      'address': address,
                                      'tel': tel,
                                      'web_url': web_url,
                                      'quota': quota,
                                      'free_5': free_5,
                                      'quasi_public': quasi_public,
                                      'after_school': after_school})

    # 儲存資料
    kindergartenInfo = pd.concat([kindergartenInfo, iKindergartenInfo])

    # 暫停隨機1-3秒
    time.sleep(random.randint(1, 3))

# 寫出csv檔案
kindergartenInfo.to_csv('kindergarten_info.csv', encoding='utf-8-sig', index=False)
kindergartenInfo = pd.read_csv('kindergarten_info.csv')

# 建立幼兒園所在鄉鎮市區資訊(利用郵遞區號)
# 取出地址中的郵遞區號資訊
kindergartenInfo['postal_code'] = kindergartenInfo['address'].str.extract(r'\[(\d+)\]').astype('int64')

# 至資料庫讀取郵遞區號資訊
sqlQuery = '''
select 
COUNTYNAME as county,  # 縣市名稱
TOWNNAME as town,  # 鄉鎮市區名稱
ZIPCODE as postal_code  # 郵遞區號
from project.postal_code;
'''
postalCodeData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())

# 依郵遞區號併入鄉鎮市區資訊
kindergartenInfo = pd.merge(kindergartenInfo,
                            postalCodeData[['town', 'postal_code']],
                            how='left',
                            on='postal_code')

# 確認資料
kindergartenInfo.to_csv('checkData.csv', encoding='utf-8-sig', index=False)

# 匯入資料庫
kindergartenInfo.to_sql('kindergarten_info', CreateDBEngine(), index=False, if_exists='replace')
