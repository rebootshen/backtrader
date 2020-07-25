import backtrader as bt
from backtrader import date2num
import pymongo
import datetime
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

class MongoData(bt.feed.DataBase):
    symbol: str


    # params = (
    #     ("dataname", None),
    #     ("fromdate", datetime.datetime(2000, 1, 1)),
    #     ("todate", datetime.datetime(2050, 1, 1)),
    #     )

    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        # name of the table is indicated by dataname
        # data is fetch between fromdate and todate
        assert(self.p.dataname is not None)
        assert(self.p.fromdate is not None)
        assert(self.p.todate is not None)

        # name of db
        self.db = db
        self.symbol = self.p.dataname

        # iterator 4 data in the list
        self.iter = None
        self.data = None

    def start(self):
        super().start()
        if self.data is None:
            # connect to mongo db local default config
            client = pymongo.MongoClient('mongodb://localhost:27017/')
            db = client[self.db]
            collection = db[self.p.dataname]
            log.info("date from  {} to {}".format(self.p.fromdate,self.p.todate))
            self.data = list(collection.find({'date':{'$gte':self.p.fromdate,'$lte':self.p.todate}}))#
            log.info("load {} rows for {}".format(len(self.data), self.symbol))
            client.close()

        # set the iterator anyway
        self.iter = iter(self.data)

        

    def stop(self):
        pass

    def _load(self):
        if self.iter is None:
            # if no data ... no parsing
            return False
        try: 
            row = next(self.iter)
        except StopIteration:
            # end of the list
            return False

        # fill the lines
        #log.info(row['trade_date'])
        #log.info(row['date'])
        # Format is YYYYMMDD
        y = int(row['trade_date'][0:4])
        m = int(row['trade_date'][4:6])
        d = int(row['trade_date'][6:8])
        dt = datetime.datetime(y, m, d)
        #log.info(dt)
        dtnum = date2num(dt)
        #log.info(dtnum)
        self.lines.datetime[0] = date2num(dt)
        #self.date2num(datetime.strptime(row['trade_date'], '%d/%m/%y'))
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['vol']
        self.lines.openinterest[0] = 0

        return True
