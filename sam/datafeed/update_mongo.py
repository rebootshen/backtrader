import pymongo
from datetime import datetime
import tushare as ts 
import json
import time
import pandas as pd
import numpy as np
import logging
logging.basicConfig(filename='example.log',level=logging.DEBUG)
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
db_name='tushare_storage'#数据库名
database=client[db_name]#建立数据库



# 连接stock数据库，注意只有往数据库中插入了数据，数据库才会自动创建
#database = client.stock

# 创建一个daily集合，类似于MySQL中"表"的概念
#daily = database["daily"]

def MA(tsPrice,k):#MovingAverage计算
    Sma=pd.Series(0.0,index=tsPrice.index)
    for i in range(k-1,len(tsPrice)):
        Sma[i]=sum(tsPrice[(i-k+1):(i+1)])/k
    return(Sma)

def get_stock_daily(start_date, end_date):
    """
    获得A股所有股票日数据

    Args:
        start_date (str): 起始日
        end_date (str): 结束日
    """
    print('=======================11')
    ts.set_token("1f5fcc75bfa0d0ddb8e3d7caab4c9623185529a5052e7671f3e9c7e2") #XXX为自己的token
    pro = ts.pro_api()
    log.info("connected tushare for data")

    #所有股票代码等
    stock_list = open('./codes.csv', 'r', encoding='utf-8').readlines()

    #stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
    #,symbol,name,area,industry,list_date')
    #print(stock_list)
    sleep_time=0.4
    # 遍历所有股票ID
    #for code in stock_list.ts_code:
    for code in stock_list:
        code = code.strip('\n')
        print(code)
        t0 = time.time()
        # 请求tushare数据，并转化为json格式
        df = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        #df['date'] = pd.to_datetime(df['trade_date'], format="%Y%m%d")
        #datetime.datetime(2010, 1, 1)

        data = json.loads(df.T.to_json()).values()
        print (data)
        for row in data:
            row['date'] = datetime.strptime(row['trade_date'], "%Y%m%d")


        elapsed = time.time() - t0
        print("pro.daily used: {:.2f}s".format(elapsed))
        log.info("load {} rows for {} used {} ".format(len(data), code, elapsed))


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
        print("collection.insert_many used: {:.2f}s".format(elapsed))
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
    get_stock_daily(start_date, end_date)
    # date_str="20100101"
    # print(pd.to_datetime(date_str, format="%Y%m%d"))
    # y = int(date_str[0:4])
    # m = int(date_str[4:6])
    # d = int(date_str[6:8])
    # dt = datetime(y, m, d)
    # print(dt)
