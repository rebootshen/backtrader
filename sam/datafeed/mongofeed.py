import backtrader as bt
import pymongo
import datetime
import logging
logging.basicConfig(filename='example.log',level=logging.DEBUG)
log = logging.getLogger(__name__)

class MongoData(bt.feed.DataBase):
    symbol: str


    params = (
        ("dataname", None),
        ("fromdate", datetime.datetime(2000, 1, 1)),
        ("todate", datetime.datetime(2050, 1, 1)),
        )

    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        # name of the table is indicated by dataname
        # data is fetch between fromdate and todate
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
            self.data = list(collection.find({'trade_date':{'$gte':self.p.fromdate,'$lte':self.p.todate}}))
            client.close()

        # set the iterator anyway
        self.iter = iter(self.data)

        log.info(
                        "load {} rows for {}".format(len(self.data), self.symbol)
                )

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
        self.lines.datetime[0] = self.date2num(row['date'])
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['volume']
        self.lines.openinterest[0] = 0

        return True
