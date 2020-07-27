from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])

import backtrader as bt
from mongofeed import MongoData

from file_dir import log_file

import logging
logging.basicConfig(filename=log_file,level=logging.DEBUG)
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

class TestStrategy(bt.Strategy):
    params = (
        ('sma_lower', 10),
        ('sma_higher', 30),
        ('printlog', False),
    )
    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            log.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 10日SMA计算
        self.sma1 = bt.ind.SMA(period=self.p.sma_lower)
        # 50日SMA计算
        self.sma2 = bt.ind.SMA(period=self.p.sma_higher)
        # 均线交叉, 1是上穿，-1是下穿
        self.crossover = bt.ind.CrossOver(self.sma1, self.sma2)


    def next(self):
        # Simply log the closing price of the series from the reference
        #self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.crossover > 0:# if fast crosses slow to the upside
                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if self.crossover < 0:# in the market & cross to the downside
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()

    def stop(self):
        self.log('(MA1 Period %2d)(MA2 Period %2d) Ending Value %.2f' %
                 (self.params.sma_lower, self.params.sma_higher, self.broker.getvalue()), doprint=True)

if __name__ == "__main__":
    cerebro = bt.Cerebro(stdstats=False)

    data = MongoData( 
        db="jqdata",
        dataname="000001.XSHE",
        #db="tushare_storage",
        #dataname="000001.SZ",
        fromdate=datetime.datetime(2019, 1, 1),
        todate=datetime.datetime(2019,12, 31)
    )

    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(1000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.AllInSizer, percents=99.99)
    # Set the commission - 0.1% ... divide by 100 to remove the %
    cerebro.broker.setcommission(commission=0.0)
    cerebro.addanalyzer(bt.analyzers.SQN, _name="SQN")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name="Sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="DrawDown")

    strats = cerebro.optstrategy(
        TestStrategy,
        sma_lower=range(10, 20),
        sma_higher=range(20,30)
        )

    opt_runs = cerebro.run(optreturn=False)


    # Generate results list
    final_results_list = []
    for run in opt_runs:
        for strategy in run:
            value = round(strategy.broker.get_value(), 2)
            PnL = round(value - 10000, 2)
            n_sma1 = strategy.params.sma_lower
            n_sma2 = strategy.params.sma_higher
            sqn = strategy.analyzers.SQN.get_analysis()["sqn"]
            sharpe = strategy.analyzers.Sharpe.get_analysis()["sharperatio"]
            max_md = strategy.analyzers.DrawDown.\
                get_analysis()["max"]["moneydown"]
            final_results_list.append([n_sma1, n_sma2, PnL, sqn, sharpe, max_md])

    print(final_results_list)