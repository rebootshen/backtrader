import pymongo
from datetime import datetime
import jqdatasdk
from file_dir import config_path, data_source_url
import pprint

import json
import time
import pandas as pd
import numpy as np
from file_dir import log_file
import logging

logging.basicConfig(filename=log_file,level=logging.DEBUG)
log = logging.getLogger(__name__)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
log.addHandler(ch)

# 建立连接
client = pymongo.MongoClient(host='localhost', port=27017)
log.info("connected mongodb")
db_name='jqdata'#数据库名
database=client[db_name]#建立数据库

def jq_login():
    with open(data_source_url, 'r') as f:
        r = json.load(f)
    
    success = False
    while not success:
        try:
            # 登录聚宽数据
            jqdatasdk.auth(r['JQ_Id'], r['JQ_passwd'])
            success = True
        except Exception as e_:
            print('链接聚宽数据失败，失败原因:\n %s \n5秒后重试...' % str(e_))
            time.sleep(5)

# 连接stock数据库，注意只有往数据库中插入了数据，数据库才会自动创建
#database = client.stock

# 创建一个daily集合，类似于MySQL中"表"的概念
#daily = database["daily"]

def MA(tsPrice,k):#MovingAverage计算
    Sma=pd.Series(0.0,index=tsPrice.index)
    for i in range(k-1,len(tsPrice)):
        Sma[i]=sum(tsPrice[(i-k+1):(i+1)])/k
    return(Sma)

def get_k_data_JQ(start_date=None, end_date=None, freq='daily'):
    """
    获得A股所有股票日数据

    Args:
        start_date (str): 起始日
        end_date (str): 结束日
    """

    #所有股票代码等
    stk = open('./codes-joinquan.csv', 'r', encoding='utf-8').readlines()

    stk_code = [jqdatasdk.normalize_code(x) for x in stk]
    log.info("stk_code {}".format(stk_code))
    #df = jqdatasdk.get_price(stk_code, frequency=freq,
    #                            end_date=end_date, start_date=start_date)
    #print(df.axes)
    #print(df.head)

    sleep_time=0.4
    # 遍历所有股票ID
    #for code in stock_list.ts_code:
    for code in stk_code:
        code = code.strip('\n')
        log.info(code)
        t0 = time.time()

        df = jqdatasdk.get_price(code, frequency=freq,
                                end_date=end_date, start_date=start_date, fq='pre')
        log.info(df.axes)
        log.info(df.head())
        log.info(df.tail())
        log.info(df.columns)
        log.info(df.index)

        df['date'] = df.index#.to_pydatetime()

        #log.info(df['date'])
        log.info(df.axes)
        log.info(df.head())
        log.info(df.columns)
        log.info(df.dtypes)
        #log.info(df.info())
        #查看缺失及每列数据类型
        
        data = json.loads(df.T.to_json()).values()
        #log.info(data)
        for row in data:
            #log.info(type(row['date']))
            row['date'] = datetime.fromtimestamp(row['date']/1000 - 60*60*8)# ns to s
            #row['date'] = datetime.utcfromtimestamp(row['date']/1000)# ns to s; same with up line


        elapsed = time.time() - t0
        log.info("get data {} rows for {} used {:.2f}s ".format(len(data), code, elapsed))


        t1 = time.time()

        # D5=MA(df.close,5)#5日均线
        # df['D5']=pd.DataFrame({'D5':D5})
        # D10=MA(df.close,10)#10日均线
        # df['D10']=pd.DataFrame({'D10':D10})
        # D20=MA(df.close,20)#20日均线
        # df['D20']=pd.DataFrame({'D20':D20})


        collection=database[code]    #建立以股票名命名的集合
        collection.insert_many(data)    #存储到MongoDB数据库中
        elapsed = time.time() - t1
        log.info("collection.insert_many used: {:.2f}s".format(elapsed))
        # 对股票每一天数据进行保存，注意唯一性
        # 这里也可以批量创建，速度更快，但批量创建容易丢失数据
        # 这里为了保证数据的绝对稳定性，选择一条条创建
        #for row in data:
        #    daily.update({"_id": f"{row['ts_code']}-{row['trade_date']}"}, row, upsert=True)

        time.sleep(sleep_time)     #暂停，tushare对积分不足用户限制每分钟200次访问，积分很重要
        if np.size(df,0)<60:
            time.sleep(sleep_time)
            continue
        #exit(0)

if __name__ == '__main__':
    start_date = '2010-01-01'
    end_date = '2020-07-25'
    #get_stock_daily(start_date, end_date)

    jq_login()
    #list1=jqdatasdk.get_industry("600519.XSHG",date="2018-06-01")
    #pprint.pprint(list1)

    get_k_data_JQ(start_date, end_date, freq='daily')

    # date_str="20100101"
    # print(pd.to_datetime(date_str, format="%Y%m%d"))
    # y = int(date_str[0:4])
    # m = int(date_str[4:6])
    # d = int(date_str[6:8])
    # dt = datetime(y, m, d)
    # print(dt)
