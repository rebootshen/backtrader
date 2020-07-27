from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import time
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import argparse
from backtrader.utils.py3 import range

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

class OptimizeStrategy(bt.Strategy):
    params = (('smaperiod', 15),
              ('macdperiod1', 12),
              ('macdperiod2', 26),
              ('macdperiod3', 9),
              )
    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            log.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        # Add indicators to add load
        bt.ind.SMA(period=self.p.smaperiod)
        bt.ind.MACD(period_me1=self.p.macdperiod1,
                   period_me2=self.p.macdperiod2,
                   period_signal=self.p.macdperiod3)
                

    # def next(self):
    #     # Simply log the closing price of the series from the reference
    #     #self.log('Close, %.2f' % self.dataclose[0])

    #     # Check if an order is pending ... if yes, we cannot send a 2nd one
    #     if self.order:
    #         return

    #     # Check if we are in the market
    #     if not self.position:

    #         # Not yet ... we MIGHT BUY if ...
    #         if self.dataclose[0] > self.sma[0]:

    #             # BUY, BUY, BUY!!! (with all possible default parameters)
    #             self.log('BUY CREATE, %.2f' % self.dataclose[0])

    #             # Keep track of the created order to avoid a 2nd order
    #             self.buy()

    #     else:

    #         if self.dataclose[0] < self.sma[0]:
    #             # SELL, SELL, SELL!!! (with all possible default parameters)
    #             self.log('SELL CREATE, %.2f' % self.dataclose[0])

    #             # Keep track of the created order to avoid a 2nd order
    #             self.sell()

    # def stop(self):
    #     self.log('(MA Period %2d) Ending Value %.2f' %
    #              (self.params.maperiod, self.broker.getvalue()), doprint=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Optimization',
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        '--data', '-d',
        default='2006-day-001.txt',
        help='data to add to the system')

    parser.add_argument(
        '--fromdate', '-f',
        default='2006-01-01',
        help='Starting date in YYYY-MM-DD format')

    parser.add_argument(
        '--todate', '-t',
        default='2006-12-31',
        help='Starting date in YYYY-MM-DD format')

    parser.add_argument(
        '--maxcpus', '-m',
        type=int, required=False, default=0,
        help=('Number of CPUs to use in the optimization'
              '\n'
              '  - 0 (default): use all available CPUs\n'
              '  - 1 -> n: use as many as specified\n'))

    parser.add_argument(
        '--no-runonce', action='store_true', required=False,
        help='Run in next mode')

    parser.add_argument(
        '--exactbars', required=False, type=int, default=0,
        help=('Use the specified exactbars still compatible with preload\n'
              '  0 No memory savings\n'
              '  -1 Moderate memory savings\n'
              '  -2 Less moderate memory savings\n'))

    parser.add_argument(
        '--no-optdatas', action='store_true', required=False,
        help='Do not optimize data preloading in optimization')

    parser.add_argument(
        '--no-optreturn', action='store_true', required=False,
        help='Do not optimize the returned values to save time')

    parser.add_argument(
        '--ma_low', type=int,
        default=10, required=False,
        help='SMA range low to optimize')

    parser.add_argument(
        '--ma_high', type=int,
        default=30, required=False,
        help='SMA range high to optimize')

    parser.add_argument(
        '--m1_low', type=int,
        default=12, required=False,
        help='MACD Fast MA range low to optimize')

    parser.add_argument(
        '--m1_high', type=int,
        default=20, required=False,
        help='MACD Fast MA range high to optimize')

    parser.add_argument(
        '--m2_low', type=int,
        default=26, required=False,
        help='MACD Slow MA range low to optimize')

    parser.add_argument(
        '--m2_high', type=int,
        default=30, required=False,
        help='MACD Slow MA range high to optimize')

    parser.add_argument(
        '--m3_low', type=int,
        default=9, required=False,
        help='MACD Signal range low to optimize')

    parser.add_argument(
        '--m3_high', type=int,
        default=15, required=False,
        help='MACD Signal range high to optimize')

    return parser.parse_args()


def runstrat():
    args = parse_args()

    #cerebro = bt.Cerebro()
    #Create a cerebro entity
    cerebro = bt.Cerebro(maxcpus=args.maxcpus,
                         runonce=not args.no_runonce,
                         exactbars=args.exactbars,
                         optdatas=not args.no_optdatas,
                         optreturn=not args.no_optreturn)

    data = MongoData( 
        db="jqdata",
        dataname="000001.XSHE",
        #db="tushare_storage",
        #dataname="000001.SZ",
        fromdate=datetime.datetime(2019, 1, 1),
        todate=datetime.datetime(2019,12, 31)
    )

    cerebro.adddata(data)

    # Add a strategy
    cerebro.optstrategy(
        OptimizeStrategy,
        smaperiod=range(args.ma_low, args.ma_high),
        macdperiod1=range(args.m1_low, args.m1_high),
        macdperiod2=range(args.m2_low, args.m2_high),
        macdperiod3=range(args.m3_low, args.m3_high),
    )
    # Set our desired cash start
    cerebro.broker.setcash(10000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # Set the commission - 0.1% ... divide by 100 to remove the %
    cerebro.broker.setcommission(commission=0.0)


        # clock the start of the process
    tstart = time.clock()

    # Run over everything
    stratruns = cerebro.run()

    # clock the end of the process
    tend = time.clock()

    print('==================================================')
    for stratrun in stratruns:
        print('**************************************************')
        for strat in stratrun:
            print('--------------------------------------------------')
            print(strat.p._getkwargs())
    print('==================================================')

    # print out the result
    print('Time used:', str(tend - tstart))

if __name__ == '__main__':
    runstrat()