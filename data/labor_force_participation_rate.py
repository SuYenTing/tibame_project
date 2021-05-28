# 資料整理程式-勞動力參與率
# 2021/04/18 蘇彥庭
# 中華民國統計資訊網
# 首頁 -> 主計總處統計專區 -> 就業、失業統計 -> 電子書
# 資料來源: https://www.stat.gov.tw/ct.asp?xItem=45283&ctNode=518
# 婚姻狀況別勞動力參與率 / 年齡組別勞動力參與率 / 教育程度別勞動力參與率

import sys
import os
import pandas as pd
import json
from sqlalchemy import create_engine

# 建立儲存檔案
output = pd.DataFrame()

# 讀取年份(102年-109年)
readYearList = list(range(102, 110))

# 迴圈讀取檔案
for readYear in readYearList:

    # 建立儲存表
    iOutput = pd.DataFrame()


    # ---------------------------- 整理婚姻狀況別勞動力參與率 ---------------------------- #
    # 要讀取的檔案名稱
    fileName = './婚姻狀況別勞動力參與率/' + str(readYear) + '年婚姻狀況別勞動力參與率.xls'

    # 讀取Excel檔案
    rawData = pd.read_excel(fileName, skiprows=11, usecols='A:M')

    # 刪除NA資料
    rawData = rawData.dropna()

    # 確認列數是否有不一致狀況
    if len(rawData) != 25:
        sys.exit(fileName + ' 列數不一致 請檢查!')

    # 更換欄位名稱
    rawData.columns = ['area',  # 地區別
                       'total',  # 勞動力參與率(%)-總計
                       'male',  # 勞動力參與率(%)-男
                       'female',  # 勞動力參與率(%)-女
                       'never_married_total',  # 勞動力參與率(%)-未婚-合計
                       'never_married_male',  # 勞動力參與率(%)-未婚-男
                       'never_married_female',  # 勞動力參與率(%)-未婚-女
                       'married_total',  # 勞動力參與率(%)-有配偶或同居-合計
                       'married_male',  # 勞動力參與率(%)-有配偶或同居-男
                       'married_female',  # 勞動力參與率(%)-有配偶或同居-女
                       'divorced_total',  # 勞動力參與率(%)-離婚、分居或喪偶-合計
                       'divorced_male',  # 勞動力參與率(%)-離婚、分居或喪偶-男
                       'divorced_female']  # 勞動力參與率(%)-離婚、分居或喪偶-女

    # 修改地區欄位 將英文拿掉
    rawData['area'] = rawData['area'].str.split('\\s+', expand=True)[[1]]

    # 加入年份資訊
    rawData.insert(0, 'year', readYear)

    # 儲存結果
    iOutput = rawData


    # ---------------------------- 整理年齡組別勞動力參與率 ---------------------------- #
    # 要讀取的檔案名稱
    fileName = './年齡組別勞動力參與率/' + str(readYear) + '年年齡組別勞動力參與率.xls'

    # 讀取Excel檔案
    rawData = pd.read_excel(fileName, skiprows=11, usecols='B,J,K,N,O,T,U,W,X,AC,AD,AF,AG')

    # 刪除NA資料
    rawData = rawData.dropna()

    # 確認列數是否有不一致狀況
    if len(rawData) != 25:
        sys.exit(fileName + ' 列數不一致 請檢查!')

    # 更換欄位名稱
    rawData.columns = ['area',  # 地區別
                       'age_15_19_male',  # 勞動力參與率(%)-15-19歲男性
                       'age_15_19_female',  # 勞動力參與率(%)-15-19歲女性
                       'age_20_24_male',  # 勞動力參與率(%)-20-24歲男性
                       'age_20_24_female',  # 勞動力參與率(%)-20-24歲女性
                       'age_25_29_male',  # 勞動力參與率(%)-25-29歲男性
                       'age_25_29_female',  # 勞動力參與率(%)-25-29歲女性
                       'age_30_34_male',  # 勞動力參與率(%)-30-34歲男性
                       'age_30_34_female',  # 勞動力參與率(%)-30-34歲女性
                       'age_35_39_male',  # 勞動力參與率(%)-35-39歲男性
                       'age_35_39_female',  # 勞動力參與率(%)-35-39歲女性
                       'age_40_44_male',  # 勞動力參與率(%)-40-44歲男性
                       'age_40_44_female']  # 勞動力參與率(%)-40-44歲女性

    # 修改地區欄位 將英文拿掉
    rawData['area'] = rawData['area'].str.split('\\s+', expand=True)[[1]]

    # 加入年份資訊
    rawData.insert(0, 'year', readYear)

    # 儲存結果
    iOutput = iOutput.merge(rawData, how='left', on=['year', 'area'])

    # 儲存資料
    output = pd.concat([output, iOutput])


# 重新設定index
output = output.reset_index(drop=True)

# 例外處理: 將桃園縣改為桃園市
output['area'] = output['area'].replace(to_replace='桃園縣', value='桃園市')

# 輸出資料表
output.to_csv('labor_force_participation_rate.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
labor_force_participation_rate = pd.read_csv('labor_force_participation_rate.csv')

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
labor_force_participation_rate.to_sql('labor_force_participation_rate', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
