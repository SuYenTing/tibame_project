# 整理郵遞區號資訊至資料庫
# 2021/04/01 蘇彥庭
# 資料來源: 政府資料開放平台-3碼郵遞區號與行政區地理座標對照KML
# https://data.gov.tw/dataset/37759

import pandas as pd
import json
from sqlalchemy import create_engine

# 讀取郵遞區號資訊
postalCodeData = pd.read_json('郵遞區號資料.json')

# 讀取資料庫帳密資訊
# secretFile = json.load(open('dbSecret.json', 'r', encoding='utf-8'))
secretFile = json.load(open('dbGcpSecret.json', 'r', encoding='utf-8'))
dbHost = secretFile['host']
dbUser = secretFile['user']
dbPassword = secretFile['password']
dbPort = secretFile['port']
dbName = secretFile['dbName']

# 建立連線
engine = create_engine('mysql+mysqlconnector://' + dbUser + ':' + dbPassword + '@' +
                       dbHost + ':' + dbPort + '/' + dbName)
conn = engine.connect()

# 匯入資料庫
postalCodeData.to_sql('postal_code', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()

