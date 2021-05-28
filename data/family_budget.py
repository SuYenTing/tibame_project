# 資料整理程式-平均每戶家庭收支按區域別分
# 2021/03/27 蘇彥庭
# 資料來源說明
# 中華民國統計資訊網 -> 家庭收支調查 -> 統計表 -> 調查報告
# https://www.stat.gov.tw/ct.asp?xItem=27900&ctNode=511&mp=4
# 「第二表 - 平均每戶家庭收支按區域別分」

import os
import pandas as pd
import json
from sqlalchemy import create_engine

# 要讀取的檔案名稱
readFileNames = [elem for elem in os.listdir() if '平均每戶家庭收支按區域別分.xls' in elem]
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
        data = iData.iloc[[3, 4, 5, 6, 7, 8, 23]]

        iData = pd.read_excel(fileName, sheet_name=sheetPair[1], skiprows=5, usecols='A:H')
        data = pd.concat([data, iData.iloc[[26, 27, 28, 29, 30, 31, 32]]])

        # 修改欄位名稱
        data = data.rename(columns={'Unnamed: 0': 'indicator'})

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
output = output.rename(columns={'year': 'year',
                                'indicator': 'county',
                                '家庭戶數': 'households',
                                '平均每戶人數': 'persons_per_household',
                                '平均每戶成年人數': 'adults_per_household',
                                '平均每戶就業人數': 'persons_employed_per_household',
                                '平均每戶所得收入者人數': 'income_recipients_per_household',
                                '一、所得收入總計': 'total_receipts',
                                '二、非消費支出': 'nonconsumption_expenditures',
                                '可支配所得(平均數)': 'disposable_income_mean',
                                '可支配所得(中位數)': 'disposable_income_median',
                                '消費支出': 'consumption_expenditures',
                                '儲蓄': 'saving',
                                '所得總額': 'current_receipts',
                                '樣本戶數': 'numbers_of_samples',
                                '可支配所得標準差': 'disposable_income_sd'})

# 調整欄位格式與型別
for colName in output.columns:
    if colName == 'county':
        output['county'] = output['county'].str.replace('\\s+', '')
    elif colName in ['year', 'households', 'numbers_of_samples']:
        output[colName] = output[colName].astype('int64')
    else:
        output[colName] = output[colName].astype('float64')

# 例外處理: 將桃園縣改為桃園市
output['county'] = output['county'].replace(to_replace='桃園縣', value='桃園市')

# 輸出資料表
output.to_csv('family_income_exp.csv', index=False, encoding='utf-8-sig')

# 讀取資料表
family_income_exp = pd.read_csv('family_income_exp.csv')

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
family_income_exp.to_sql('family_income_exp', conn, index=False, if_exists='replace')
conn.close()
engine.dispose()
