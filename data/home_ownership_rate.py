# 資料整理程式-房屋自有率
# 2021/04/18 蘇彥庭
# 中華民國統計資訊網 -> 家庭收支調查 -> 統計表 -> 調查報告
# https://www.stat.gov.tw/ct.asp?xItem=27900&ctNode=511&mp=4
# 「第八表 - 家庭住宅及主要設備概況按區域別分」

import sys
import os
import pandas as pd
import json
from sqlalchemy import create_engine

# 要讀取的檔案名稱
readFileNames = [elem for elem in os.listdir() if '家庭住宅及主要設備概況按區域別分.xls' in elem]
# 讀取sheet的配對 因為同一個縣市的資料被拆成兩個sheet 所以此處先行對應
readSheetPair = [(0, 3), (1, 4), (2, 5)]

# 迴圈讀取檔案
output = pd.DataFrame()
for fileName in readFileNames:

    # 迴圈讀取各sheet資料
    iOutput = pd.DataFrame()
    for sheetPair in readSheetPair:

        # 讀取各縣市資料
        iData = pd.read_excel(fileName, sheet_name=sheetPair[0], skiprows=5, usecols='A:H')
        data = iData.iloc[[10]]

        # 因為不需要第二頁的資料 所以不需要讀取 但為避免未來有需要其他資料 故留下程式碼在此處
        # iData = pd.read_excel(fileName, sheet_name=sheetPair[1], skiprows=5, usecols='A:H')
        # data = pd.concat([data, iData.iloc[[]]])

        # 修改欄位名稱
        data = data.rename(columns={'Unnamed: 0': 'indicator'})

        # 確認資料欄位是否有抓錯
        if '自有(戶內經常居住成員所擁有)' not in data.iloc[0]['indicator']:
            sys.exit(fileName + ' 抓取位置的資料名稱有誤 請檢查!')

        # 合併資料
        if iOutput.empty:
            iOutput = data
        else:
            iOutput = pd.merge(iOutput, data, how='left', on='indicator')

    # 轉置欄位
    iOutput = iOutput.transpose()

    # 將index做為新欄位
    iOutput = iOutput.reset_index()

    # 修改欄位名稱
    iOutput.columns = iOutput.iloc[0]
    iOutput = iOutput.drop([0])

    # 添加年份資訊
    iOutput.insert(0, 'year', int(fileName[0:3]))

    # 儲存結果
    output = pd.concat([output, iOutput])

# 重新設定index
output = output.reset_index(drop=True)

# 重新命名欄位
output.columns = ['year', 'county', 'home_ownership_rate']

# 調整欄位格式與型別
output['county'] = output['county'].str.replace('\\s+', '')  # 移除空白
output['home_ownership_rate'] = output['home_ownership_rate'].astype('float64')  # 轉為float格式

# 例外處理: 將桃園縣改為桃園市
output['county'] = output['county'].replace(to_replace='桃園縣', value='桃園市')

# 輸出資料表
output.to_csv('home_ownership_rate.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
home_ownership_rate = pd.read_csv('home_ownership_rate.csv')

# 匯入資料庫
# 讀取資料庫帳密資訊
secretFile = json.load(open('dbSecret.json', 'r', encoding='utf-8'))
dbHost = secretFile['host']
dbUser = secretFile['user']
dbPassword = secretFile['password']
dbPort = secretFile['port']
dbName = secretFile['dbName']
engine = create_engine('mysql+mysqlconnector://' + dbUser + ':' + dbPassword + '@' +
                       dbHost + ':' + dbPort + '/' + dbName)
conn = engine.connect()
home_ownership_rate.to_sql('home_ownership_rate', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
