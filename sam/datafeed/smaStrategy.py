import datetime
import backtrader as bt
from mongofeed import MongoData
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo


import logging
logging.basicConfig(filename='example.log',level=logging.DEBUG)
log = logging.getLogger(__name__)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

class SMACross(bt.Strategy):
    params = dict(
        sma_lower=10,  # period for lower SMA
        sma_higher=30,  # period for higher SMA
    )

    def __init__(self):
        # 10日SMA计算
        sma1 = bt.ind.SMA(period=self.p.sma_lower)
        # 50日SMA计算
        sma2 = bt.ind.SMA(period=self.p.sma_higher)

        # 均线交叉, 1是上穿，-1是下穿
        self.crossover = bt.ind.CrossOver(sma1, sma2)

    def next(self):
        close = self.data.close[0]
        date = self.data.datetime.date(0)
        
        #if self.crossover < 0:
        #    log.info("{} crossover down:{}".format(date,(self.crossover < 0)) )
        if not self.position:# not in the market
            if self.crossover > 0:# if fast crosses slow to the upside
                log.info("buy created at {} - {}".format(date, close))
                self.buy()# enter long
                #log.info("{} postion:{} ".format(date,self.position))

        elif self.crossover < 0:# in the market & cross to the downside
            log.info("sell created at {} - {}".format(date, close))
            self.close()# close long position
            #log.info("{} postion:{} ".format(date,self.position))



if __name__ == "__main__":
    cerebro = bt.Cerebro()

    #data = DBDataFeed(
        # 本地postgresql数据库
        # db_uri="postgresql://user:password@localhost:5432/dbname",
    data = MongoData( 
        db="tushare_storage",
        dataname="000875.SZ",
        fromdate=datetime.datetime(2019, 1, 1),
        todate=datetime.datetime(2019,12, 31)
    )

    log.info("data:".format(len(data)))
    #if len(data) == 0:
    #    exit(0)

    cerebro.adddata(data)

    cerebro.addstrategy(SMACross)
    cerebro.addsizer(bt.sizers.AllInSizerInt)
    cerebro.broker.set_cash(100000)

    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_returns")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="draw_down")
    cerebro.addanalyzer(bt.analyzers.Transactions, _name="transactions")

    results = cerebro.run()

    # 打印Analyzer结果到日志
    for result in results:

        annual_returns = result.analyzers.annual_returns.get_analysis()
        log.info("annual returns:")
        for year, ret in annual_returns.items():
            log.info("\t {} {}%, ".format(year, round(ret * 100, 2)))

        draw_down = result.analyzers.draw_down.get_analysis()
        log.info(
            "drawdown={drawdown}%, moneydown={moneydown}, drawdown len={len}, "
            "max.drawdown={max.drawdown}, max.moneydown={max.moneydown}, "
            "max.len={max.len}".format(**draw_down)
        )

        transactions = result.analyzers.transactions.get_analysis()
        log.info("transactions")

    # 运行结果绘图
    #cerebro.plot()
    b = Bokeh(style="bar", tabs="multi", scheme=Tradimo())
    cerebro.plot(b)