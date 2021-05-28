# 資料整理程式-幼兒(稚)園概況表
# 2021/04/27 蘇彥庭
# 教育部統計處
# 主要統計表-歷年
# 網址: https://depart.moe.edu.tw/ed4500/cp.aspx?n=1B58E0B736635285&s=D04C74553DB60CAD
# 幼兒(稚)園概況表(80～109 學年度)

import sys
import pandas as pd
import json
from sqlalchemy import create_engine

# 建立表格
kindergarten_data = pd.DataFrame()

# 要讀取的檔案名稱
fileName = './幼兒(稚)園概況表.xls'

# 要讀取的年份
yearLists = list(range(102, 110))

# 迴圈讀取頁籤資料
for year in yearLists:

    # 依據不同年度的報表調整讀取範圍
    if year == 102:
        skiprows = 5
        usecols = 'A,C:D,R:S'

    elif year == 109:
        skiprows = 6
        usecols = 'A,C:D,U:V'

    else:
        skiprows = 6
        usecols = 'A,C:D,S:T'

    # 讀取Excel檔案
    rawData = pd.read_excel(fileName, sheet_name=str(year)+'縣市', skiprows=skiprows, usecols=usecols)

    # 選取需要的資料欄位
    rawData = rawData.iloc[:25]

    # 重新命名欄位
    rawData.columns = ['county', 'total_school_nums', 'public_school_nums', 'total_children', 'public_children']

    # 調整空白值
    rawData['county'] = rawData['county'].str.replace('\\s+', '')

    # 檢查資料取值是否有問題
    if rawData['county'].tolist()[-1] != '連江縣':
        sys.exit('最後一列非連江縣資料!')

    # 新增年份欄位
    rawData.insert(0, 'year', year)

    # 儲存資料
    kindergarten_data = pd.concat([kindergarten_data, rawData])

# 將桃園縣資料改為桃園市
kindergarten_data['county'] = kindergarten_data['county'].str.replace('桃園縣', '桃園市')

# 檢查資料
# kindergarten_data.info()
# kindergarten_data.groupby('county').size()
# kindergarten_data[kindergarten_data['county'] == '總計']

# 輸出csv檔案
kindergarten_data.to_csv('kindergarten_data.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
kindergarten_data = pd.read_csv('kindergarten_data.csv')

# 匯入資料庫
secretFile = json.load(open('dbSecret.json', 'r', encoding='utf-8'))
dbHost = secretFile['host']
dbUser = secretFile['user']
dbPassword = secretFile['password']
dbPort = secretFile['port']
dbName = secretFile['dbName']
engine = create_engine('mysql+mysqlconnector://' + dbUser + ':' + dbPassword + '@' +
                       dbHost + ':' + dbPort + '/' + dbName)
conn = engine.connect()
kindergarten_data.to_sql('kindergarten_data', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
