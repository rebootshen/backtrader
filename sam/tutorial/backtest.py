#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
"""计算定投某个标的的盈利情况"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects

# Import the backtrader platform
import backtrader as bt
import backtrader.analyzers as btanalyzers
import backtrader.feeds as btfeed
import click

# Create a Stratey
class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        # To keep track of pending orders
        self.month = -1

    def next(self):
        # Simply log the closing price of the series from the reference
        # 每月进行购买一次
        if self.month != self.datas[0].datetime.date(0).month:
            self.month = self.datas[0].datetime.date(0).month
            self.buy(size=10000/self.dataclose[0])


class TSCSVData(btfeed.GenericCSVData):
    params = (
        ("fromdate", datetime.datetime(2010, 1, 1)),
        ("todate", datetime.datetime(2019, 12, 31)),
        ('nullvalue', 0.0),
        ('dtformat', ('%Y-%m-%d')),
        ('openinterest', -1)
    )


def backtest():
    filename = "bs_sh.600000.csv"
    cash = 1200000.0
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Create a Data Feed
    data = TSCSVData(dataname="./datas/{0}".format(filename))

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(cash)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Analyzer
    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name='mysharpe')
    cerebro.addanalyzer(btanalyzers.AnnualReturn, _name='myannual')
    cerebro.addanalyzer(btanalyzers.DrawDown, _name='mydrawdown')

    # Run over everything
    thestrats = cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    thestrat = thestrats[0]
    print('Sharpe Ratio:', thestrat.analyzers.mysharpe.get_analysis())
    print('Annual Return:', thestrat.analyzers.myannual.get_analysis())
    print('Drawdown Info:', thestrat.analyzers.mydrawdown.get_analysis())

    cerebro.plot()


if __name__ == '__main__':
    backtest()
