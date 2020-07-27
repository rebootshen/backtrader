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

class firstStrategy(bt.Strategy):
    params = (
        ('period',21),
        ('printlog', False),
    )
    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            log.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.startcash = self.broker.getvalue()
        self.rsi = bt.indicators.RSI_SMA(self.data.close, period=self.params.period)

    def next(self):
        if not self.position:
            if self.rsi < 30:
                self.buy(size=100)
        else:
            if self.rsi > 70:
                self.sell(size=100)

    def stop(self):
        self.log('(rsi Period %2d) Ending Value %.2f' %
                 (self.params.period, self.broker.getvalue()), doprint=True)

if __name__ == "__main__":
    #Variable for our starting cash
    startcash = 10000

    #Create an instance of cerebro
    cerebro = bt.Cerebro(optreturn=False)


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
    cerebro.broker.setcash(startcash)

    # Add a FixedSize sizer according to the stake
    #cerebro.addsizer(bt.sizers.AllInSizer, percents=99.99)
    # Set the commission - 0.1% ... divide by 100 to remove the %
    #cerebro.broker.setcommission(commission=0.0)
    # cerebro.addanalyzer(bt.analyzers.SQN, _name="SQN")
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name="Sharpe")
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name="DrawDown")


    #Add our strategy
    cerebro.optstrategy(firstStrategy, period=range(14,21))

    opt_runs = cerebro.run()


    # Generate results list

    final_results_list = []
    for run in opt_runs:
        for strategy in run:
            value = round(strategy.broker.get_value(),2)
            PnL = round(value - startcash,2)
            period = strategy.params.period
            final_results_list.append([period,PnL])

    #Sort Results List
    by_period = sorted(final_results_list, key=lambda x: x[0])
    by_PnL = sorted(final_results_list, key=lambda x: x[1], reverse=True)

    #Print results
    print('Results: Ordered by period:')
    for result in by_period:
        print('Period: {}, PnL: {}'.format(result[0], result[1]))
    print('Results: Ordered by Profit:')
    for result in by_PnL:
        print('Period: {}, PnL: {}'.format(result[0], result[1]))