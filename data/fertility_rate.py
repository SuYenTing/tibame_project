# 資料整理程式-育齡婦女之年齡別生育率
# 2021/04/21 蘇彥庭
# 行政院性別平等會: 重要性別統計資料庫
# https://www.gender.ey.gov.tw/gecdb/Stat_Statistics_Field.aspx
# 首頁 -> 人口、婚姻與家庭 -> 生育 -> 育齡婦女之年齡別生育率
# 首頁 -> 人口、婚姻與家庭 -> 生育 -> 首次生產婦女之平均年齡(生第一胎平均年齡)

import sys
import os
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine

# 建立表格
fertility_rate = pd.DataFrame()

# ---------------------- 整理育齡婦女之年齡別生育率 ---------------------- #
# 要讀取的檔案名稱
fileName = './育齡婦女之年齡別生育率.xlsx'

# 讀取Excel檔案
rawData = pd.read_excel(fileName, skiprows=1, usecols='A:HJ')

# 整理縣市別
countyList = list(rawData.iloc[0].dropna())[1:]

# 整理年齡別
ageList = rawData.iloc[1, 0:8]
ageList = [elem.replace('\u3000', '') for elem in ageList][1:]
ageColumnsName = ['age' + elem.replace('歲', '').replace('-', '_') + '_childbearing' for elem in ageList]

# 迴圈建立表
output = pd.DataFrame()
for ix in range(len(countyList)):
    iOutput = rawData.iloc[2:, (ix * 7 + 1):(ix * 7 + 8)]
    iOutput.columns = ageColumnsName
    iOutput.insert(0, 'county', countyList[ix])
    iOutput.insert(0, 'year', rawData.iloc[2:, 0])
    output = pd.concat([output, iOutput])

# 處理缺失值
output = output.replace({'-': np.nan})

# 刪除整列為缺失值之資料列
output = output.dropna(how='all', subset=ageColumnsName)

# 將缺值轉為0
output = output.fillna(0)

# 處理年份欄位
output['year'] = output['year'].str.replace('年', '').astype('int64')

# 篩選民國100年以後資料(配合六都改制)
output = output[output['year'] >= 100]

# 將桃園縣替換為桃園市
output['county'] = output['county'].str.replace('桃園縣', '桃園市')

# # 驗證各縣市資料筆數是否有問題
# output[['year', 'county']].groupby(['county']).agg(['count'])

# 儲存資料
fertility_rate = output

# ---------------------- 整理各縣市生母生育平均年齡 ---------------------- #
# 要讀取的檔案名稱
fileName = './首次生產婦女之平均年齡(生第一胎平均年齡).xls'

# 讀取Excel檔案
rawData = pd.read_excel(fileName, sheet_name=2, skiprows=2, usecols='AL:AV')
rawData = rawData.iloc[0:23]

# 將資料寬轉長
rawData = pd.melt(rawData, id_vars=['區域別.1'])
rawData.columns = ['county', 'year', 'mean_age_childbearing']

# 調整縣市欄位: 刪除空白
rawData['county'] = rawData['county'].str.replace('\\s+', '')

# 調整年份資料: 刪除年及轉換型別
rawData['year'] = rawData['year'].str.replace('年', '').astype('int64')

# 調整生母生育平均年齡欄位的型別
rawData['mean_age_childbearing'] = rawData['mean_age_childbearing'].astype('float64')

# # 驗證各縣市資料筆數是否有問題
# rawData[['year', 'county']].groupby(['county']).agg(['count'])

# 合併資料
fertility_rate = pd.merge(fertility_rate, rawData, how='left', on=['county', 'year'])

# ---------------------- 整理各縣市生母生第1胎平均年齡 ---------------------- #
# 要讀取的檔案名稱
fileName = './首次生產婦女之平均年齡(生第一胎平均年齡).xls'

# 讀取Excel檔案
rawData = pd.read_excel(fileName, sheet_name=3, skiprows=2, usecols='AL:AV')
rawData = rawData.iloc[0:23]

# 將資料寬轉長
rawData = pd.melt(rawData, id_vars=['區域別.1'])
rawData.columns = ['county', 'year', 'mean_age_first_childbearing']

# 調整縣市欄位: 刪除空白
rawData['county'] = rawData['county'].str.replace('\\s+', '')

# 調整年份資料: 刪除年及轉換型別
rawData['year'] = rawData['year'].str.replace('年', '').astype('int64')

# 調整生母生育平均年齡欄位的型別
rawData['mean_age_first_childbearing'] = rawData['mean_age_first_childbearing'].astype('float64')

# # 驗證各縣市資料筆數是否有問題
# rawData[['year', 'county']].groupby(['county']).agg(['count'])

# 合併資料
fertility_rate = pd.merge(fertility_rate, rawData, how='left', on=['county', 'year'])

# ---------------------- 匯入資料庫 ---------------------- #
# # 檢查資料表
# fertility_rate[['year', 'county']].groupby(['county']).agg(['count'])

# 輸出資料表
fertility_rate.to_csv('fertility_rate.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
fertility_rate = pd.read_csv('fertility_rate.csv')

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
fertility_rate.to_sql('fertility_rate', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
