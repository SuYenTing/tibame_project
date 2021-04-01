# 整理Demo網站所需之數據
# 2021/04/01 蘇彥庭
import json
import pandas as pd
from sqlalchemy import create_engine


# 建立連線函數
def CreateDBEngine():
    secretFile = json.load(open('secretFile.json', 'r'))
    host = secretFile['host']
    username = secretFile['user']
    password = secretFile['password']
    port = secretFile['port']
    database = secretFile['dbName']
    return create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}', echo=False)


'''
整理內政部大數據鄉鎮市區資料
'''

# 查詢內政部大數據鄉鎮市區資料指令
sqlQuery = '''
select
convert(replace(INFO_TIME, 'Y', ''), unsigned) as year,  # 年度
COUNTY_ID as county_id,  # 縣市代碼
COUNTY as county,  # 縣市名稱
TOWN_ID as town_id,  # 鄉鎮市區代碼
TOWN as town,  # 鄉鎮市區名稱

# 人口結構
sum(COLUMN2) as population,  # 人口數
sum(COLUMN52) as births_nums,  # 出生人口數
sum(COLUMN59) as deaths_nums,  # 死亡人口數
sum(COLUMN66) as immigration_nums,  # 遷入人口數
sum(COLUMN67) as migration_nums,  # 遷出人口數
round(sum(COLUMN52)/sum(COLUMN2)*1000, 2) as birth_rate,  # 粗出生率=出生人口數/人口數
round(sum(COLUMN52-COLUMN59)/sum(COLUMN2)*1000, 2) as nature_increase_rate,  # 自然增加率=(出生人口數-死亡人口數)/人口數
round(sum(COLUMN66-COLUMN67)/sum(COLUMN2)*1000, 2) as social_increase_rate,  # 社會增加率=(遷入人口數-遷出人口數)/人口數

# 婚育概況
sum(COLUMN60) as married_pairs,  # 結婚對數
round(sum(COLUMN60)/sum(COLUMN2)*1000, 2) as married_rate,  # 粗結婚率 

# 托育教保
sum(COLUMN108+COLUMN109+COLUMN110) as age_0_2_population,  # 0-2歲學齡人口數
sum(COLUMN111+COLUMN112+COLUMN113+COLUMN114) as age_3_6_population  # 3-6學齡人口數

from project.demographic
group by info_time, county_id, county, town_id, town
order by COUNTY_ID, TOWN_ID, info_time;
'''

# 取得資料
segisData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())

# # 檢查資料
# segisData.info()

# 為能夠對應其他來源資料 鄉鎮市區代碼若只有7碼 則第一碼補0
# 此為108年內政大數據資料應用組競賽用資料集_村里資料問題
reviseIdx = segisData['town_id'].str.len() == 7
segisData.loc[reviseIdx, 'town_id'] = '0' + segisData.loc[reviseIdx, 'town_id']


'''
整理幼兒園資訊
'''

# 查詢各鄉鎮市區幼兒園數量(公立 非營利 私立 合計)
sqlQuery = '''
select * 
from (
    select
    county,
    town,
    types,
    count(*) as nums,
    sum(quota) as quota
    from project.kindergarten_info
    group by county, town, types
    union
    select 
    county,
    town,
    '合計' as types,
    count(*) as nums,
    sum(quota) as quota
    from project.kindergarten_info
    group by county, town
) as t
order by county, town, types
'''

# 取得資料
kindergartenInfoData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())

# 長資料轉寬資料
kindergartenInfoData = kindergartenInfoData.pivot(index=['county', 'town'], columns='types', values=['nums', 'quota']).\
    reset_index().\
    rename(columns={'公立': 'public',
                    '合計': 'total',
                    '私立': 'private',
                    '非營利': 'npo'})

# 重新命名欄位
kindergartenInfoData.columns = [elem[0] if elem[1] == ''
                                else elem[1] + '_kindergarten_' + elem[0]
                                for elem in kindergartenInfoData.columns]

# 併入完整索引表
kindergartenInfoData = pd.merge(segisData[['county', 'town']].drop_duplicates(),
                                kindergartenInfoData,
                                how='left',
                                on=['county', 'town'])

# 將缺值轉為0(此處欄位皆為名額資訊 轉為0是合理的)
kindergartenInfoData = kindergartenInfoData.fillna(0)

'''
整理房價所得比相關資訊
各鄉鎮市區房價所得比=鄉鎮市區房價中位數/所屬縣市之可支配所得
'''

# # 查詢內政部大數據鄉鎮市區資料所得資訊指令
# sqlQuery = '''
# select
# convert(replace(INFO_TIME, 'Y', ''), unsigned) as year,  # 年度
# COUNTY_ID as county_id,  # 縣市代碼
# COUNTY as county,  # 縣市名稱
# TOWN_ID as town_id,  # 鄉鎮市區代碼
# TOWN as town,  # 鄉鎮市區名稱
#
# # 所得資訊
# COLUMN137 as income_avg,  # 綜合所得平均數
# COLUMN138 as income_medium,  # 綜合所得中位數
# COLUMN139 as income_q1,  # 綜合所得第一分位數
# COLUMN140 as income_q3  # 綜合所得第三分位數
#
# from project.demographic
# order by COUNTY_ID, TOWN_ID, info_time;
# '''
#
# # 取得資料
# incomeData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())
#
# # 為能夠對應其他來源資料 鄉鎮市區代碼若只有7碼 則第一碼補0
# # 此為108年內政大數據資料應用組競賽用資料集_村里資料問題
# reviseIdx = incomeData['town_id'].str.len() == 7
# incomeData.loc[reviseIdx, 'town_id'] = '0' + incomeData.loc[reviseIdx, 'town_id']
#
# # 調整缺值欄位
# for col in ['income_avg', 'income_medium', 'income_q1', 'income_q3']:
#     incomeData[col] = incomeData[col].str.replace('-', 'NaN').astype('float64')
#
# # 檢查資料
# incomeData.info()


# 查詢可支配所得資料
sqlQuery = '''
select 
year,  # 年份
county,  # 縣市
disposable_income_median  # 可支配所得  
from project.family_income_exp
where county != '總平均';
'''

# 取得資料
disposableIncomeData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())

# # 檢查資料
# disposableIncomeData.info()


# 查詢不動產實價登錄資料
sqlQuery = '''
select 
city as county,  # 縣市
township as town,  # 鄉鎮市區
convert(substr(transaction_day, 1, 3), unsigned) as year,
total_price as price
from project.real_price_immovables
where transaction_sign in ('房地(土地+建物)', '房地(土地+建物)+車位');
'''

# 取得資料
housePriceData = pd.read_sql(sql=sqlQuery, con=CreateDBEngine())

# # 檢查資料
# housePriceData.info()

# 計算各年度鄉鎮市區房價中位數
housePriceMedianData = housePriceData.groupby(by=['county', 'town', 'year'], as_index=False)['price'].\
    agg(['median', 'count']).\
    reset_index().\
    rename(columns={'median': 'house_median_price', 'count': 'countNums'})

# 查看計算樣本數少的鄉鎮市區
housePriceMedianData[housePriceMedianData['countNums'] <= 30].sort_values(by='countNums', ascending=True)
# 各年度鄉鎮市區房價中位數全樣本筆數共有2671筆 其中有929筆樣本計算中位數的樣本低於30筆
# 低於30筆計算出來的中位數可能不具有代表性 將低於30筆的樣本移除
housePriceMedianData = housePriceMedianData[housePriceMedianData['countNums'] > 30]

# 處理不動產實價登錄 新竹市及嘉義市未分區狀況 將縣市資料套用至鄉鎮市區
for county in ['嘉義市', '新竹市']:
    # 將不動產實價登錄的縣市資料取出 並丟棄town欄位
    tempData = housePriceMedianData[housePriceMedianData['county'] == county].drop(columns=['town'])
    # 將segis資料的鄉鎮市區取出 以cell方式存為town欄位
    tempData['town'] = [segisData[segisData['county'] == county]['town'].unique().tolist()] * len(tempData)
    # 利用explod將縣市資料套用至鄉鎮市區
    tempData = tempData.explode('town')
    # 刪除舊資料
    housePriceMedianData = housePriceMedianData[housePriceMedianData['county'] != county]
    # 併入新資料
    housePriceMedianData = pd.concat([housePriceMedianData, tempData])

# 建立完整索引清單
housePriceMedianData = pd.merge(segisData[['year', 'county', 'town']],
                                housePriceMedianData,
                                how='left',
                                on=['year', 'county', 'town'])

# # 檢查資料
# housePriceMedianData.to_csv('checkData.csv', encoding='utf-8-sig')

# 併入可支配所得資料
housePriceToIncome = pd.merge(housePriceMedianData,
                              disposableIncomeData,
                              how='left',
                              on=['year', 'county'])

# 計算房價所得比
housePriceToIncome['house_price_to_income'] = housePriceToIncome['house_median_price']/\
                                              housePriceToIncome['disposable_income_median']

# # 檢查資料
# housePriceToIncome.to_csv('checkData.csv', encoding='utf-8-sig')

'''
彙整資料至資料庫
'''
# 建立匯入表
webOverViewData = segisData

# 將幼兒園資料併入segisData
webOverViewData = pd.merge(webOverViewData,
                           kindergartenInfoData,
                           how='left',
                           on=['county', 'town'])

# 計算公幼涵蓋比率(公幼核定名額總數/3-6歲學齡人口)
webOverViewData['public_cover_ratio'] = webOverViewData['public_kindergarten_quota']/\
                                        webOverViewData['age_3_6_population']

# 計算公幼+非營利涵蓋比率(公幼+非營利核定名額總數/3-6歲學齡人口)
webOverViewData['public_npo_cover_ratio'] = (webOverViewData['public_kindergarten_quota'] +
                                             webOverViewData['npo_kindergarten_quota'])/\
                                            webOverViewData['age_3_6_population']

# 將房價所得比資料併入segisData
webOverViewData = pd.merge(webOverViewData,
                           housePriceToIncome[['year', 'county', 'town',
                                               'house_median_price', 'disposable_income_median',
                                               'house_price_to_income']],
                           how='left',
                           on=['year', 'county', 'town'])

# 調整資料四捨五入小數點
webOverViewData = webOverViewData.round({'public_cover_ratio': 2,
                                         'public_npo_cover_ratio': 2,
                                         'house_price_to_income': 2})

# 輸出資料至資料庫
webOverViewData.to_sql('web_overview_data', CreateDBEngine(), index=False, if_exists='replace')

# # 檢查資料
# webOverViewData.to_csv('webOverViewData.csv', encoding='utf-8-sig')
