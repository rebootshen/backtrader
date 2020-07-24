import jqdatasdk as jq
from datetime import datetime

from pymongo import MongoClient, UpdateOne
import pandas as pd
from pandas import DataFrame

# -----------------主要接口---------------


class MongoDB:
    def __init__(self, dbname: str, address='127.0.0.1', port=27017):
        self.client = MongoClient(address, port)
        self.db = self.client[dbname]

    def insert_data(self, table_name: str, df: DataFrame):
        # 确保传入的DataFrame中有 'date', 'code'两项
        assert 'date' in df.columns
        assert 'code' in df.columns

        bulks = self._convert_df(df)
        col = self.db[table_name]
        for field, bulk in bulks.items():
            # 建立索引
            col[field].create_index('date')
            # 写入数据
            col[field].bulk_write(bulk, ordered=False)

    def query(self, table_name: str, codes: list, field: str, start: datetime, end: datetime):
        # 确定需要的数据表
        col = self.db[table_name][field]
        # 根据start end查询数据
        cur = col.find({'date': {'$gte': start, '$lte': end}},
                       [i.replace('.', '~') for i in codes])
        # 将数据读入DataFrame
        df = DataFrame(cur)
        # 重命名股票代码
        df.columns = [i.replace('~', '.') for i in df.columns]
        # 删除 _id
        del df['_id']
        return df

    def _convert_df(self, df: DataFrame) -> dict:
        '''将 DataFrame 转化为具体数据库的操作指令'''
        fields = set(df.columns) - {'code', 'date'}
        res = dict()
        for field in fields:
            # 通过pivot将表展开
            idf = df.pivot(index='date', columns='code',
                           values=field)
            # MongoDB 表头中不能有 '.'故此转换为'~'
            idf.columns = [i.replace('.', '~') for i in idf.columns]
            bulk = []
            # 将DataFrame转换为UpdateOne指令，
            # 这里手动update而不是直接insert_many
            # 主要是为了确保同一天的数据保存在同一行
            for index, value in idf.iterrows():
                q = {'date': index.to_pydatetime()}
                u = {'$set': value.dropna().to_dict()}
                bulk.append(UpdateOne(q, u, upsert=True))
            res[field] = bulk
        return res

# --------------例子---------------


if __name__ == "__main__":
    # 获取日交易数据
    print("Hello world!", flush=True)
    print('before connect jq', flush=True)
    # jq.auth('15814765423', 'Mast3rch@hk')

    # print('before get all from jq', flush=True)
    # #codes = list(jq.get_all_securities(types=[], date=None).index)
    # #print(codes)
    # codes = ['002065.XSHE', '600095.XSHG']
    # print('after get all', flush=True)
    # df = DataFrame()
    # print('after df', flush=True)
    # for code in codes:
    #     print('in for', flush=True)
    #     print(code)
    #     data = jq.get_price(code, start_date=datetime(2000, 1, 1), end_date=datetime(2020, 6, 26),
    #                         frequency='daily', fields=None, skip_paused=False, fq='pre', count=None)
        
    #     # 保证 'date' column 存在并且是 datetime 类型
    #     data.index.name = 'date'
    #     data = data.reset_index()
    #     data['date'] = pd.to_datetime(data['date'])
    #     # 保证 'code' column 存在
    #     data['code'] = code
    #     df = df.append(data)

    # print('before insert mongodb!', flush=True)
    # # 储存数据
    md = MongoDB('joinquant_test')
    # md.insert_data('daily_price', df)

    # 读取数据
    r = md.query('daily_price', ['600095.XSHG'], 'close',
                 datetime(2020, 6, 1), datetime(2020, 7, 1))
    print(r)