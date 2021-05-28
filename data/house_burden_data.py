# 資料整理程式-房價所得比與貸款負擔率
# 2021/05/12 蘇彥庭
# 內政部不動產資訊平台
# 首頁-> 住宅統計 -> 價格指標
# https://pip.moi.gov.tw/V3/E/SCRE0201.aspx

import sys
import pandas as pd
import json
from sqlalchemy import create_engine

# 建立表格
house_burden_data = pd.DataFrame()

# 整理房價所得比資料
fileName = '房價所得比.csv'
rawData = pd.read_csv(fileName, encoding='big5')
rawData = pd.melt(rawData, id_vars=['年度季別'])
rawData.columns = ['year', 'city', 'pir']
rawData[['year', 'season']] = rawData['year'].str.split('Q', expand=True)
rawData = rawData[['year', 'season', 'city', 'pir']]
rawData = rawData[rawData['pir'] != '---']

# 計算yoy變動率
comparePeriodData = rawData[['year', 'season']].drop_duplicates().sort_values(['year', 'season'])
comparePeriodData['lastYear'] = comparePeriodData['year'].shift(4)
comparePeriodData['lastSeason'] = comparePeriodData['season'].shift(4)
data = pd.merge(rawData, comparePeriodData, on=['year', 'season'], how='left')
data = pd.merge(data,
                rawData.rename({'year': 'lastYear',
                                'season': 'lastSeason',
                                'pir': 'lastPir'}, axis=1),
                left_on=['lastYear', 'lastSeason', 'city'],
                right_on=['lastYear', 'lastSeason', 'city'],
                how='left')
data['year_change_pir'] = data['pir']-data['lastPir']

# 調整欄位及資料內容
data = data[['year', 'season', 'city', 'pir', 'year_change_pir']]
data['city'] = data['city'].str.replace('台', '臺')

# 儲存資料
house_burden_data = data


# 整理貸款負擔率資料
fileName = '貸款負擔率.csv'
rawData = pd.read_csv(fileName, encoding='big5')
rawData = pd.melt(rawData, id_vars=['年度季別'])
rawData.columns = ['year', 'city', 'mbr']
rawData[['year', 'season']] = rawData['year'].str.split('Q', expand=True)
rawData = rawData[['year', 'season', 'city', 'mbr']]
rawData = rawData[rawData['mbr'] != '---']

# 計算yoy變動率
comparePeriodData = rawData[['year', 'season']].drop_duplicates().sort_values(['year', 'season'])
comparePeriodData['lastYear'] = comparePeriodData['year'].shift(4)
comparePeriodData['lastSeason'] = comparePeriodData['season'].shift(4)
data = pd.merge(rawData, comparePeriodData, on=['year', 'season'], how='left')
data = pd.merge(data,
                rawData.rename({'year': 'lastYear',
                                'season': 'lastSeason',
                                'mbr': 'lastMbr'}, axis=1),
                left_on=['lastYear', 'lastSeason', 'city'],
                right_on=['lastYear', 'lastSeason', 'city'],
                how='left')
data['year_change_mbr'] = data['mbr']-data['lastMbr']

# 調整欄位及資料內容
data = data[['year', 'season', 'city', 'mbr', 'year_change_mbr']]
data['city'] = data['city'].str.replace('台', '臺')

# 儲存資料
house_burden_data = pd.merge(house_burden_data, data, on=['year', 'season', 'city'])

# 調整小數位
house_burden_data = house_burden_data.round({'year_change_pir': 4, 'year_change_mbr': 4})

# 查看資料
house_burden_data.info()

# 輸出csv檔案
house_burden_data.to_csv('house_burden_data.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
house_burden_data = pd.read_csv('house_burden_data.csv')

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
house_burden_data.to_sql('house_burden_data', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
