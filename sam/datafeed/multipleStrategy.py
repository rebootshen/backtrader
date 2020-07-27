# -*- coding: utf-8 -*-


import datetime
import backtrader as bt
from mongofeed_tushare import MongoData
#from backtrader_plotting import Bokeh
#from backtrader_plotting.schemes import Tradimo

import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Qt5Agg')
plt.switch_backend('Qt5Agg')

from file_dir import log_file, tmp_dir

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

class SMACross(bt.Strategy):
    params = dict(
        pfast=10,  # period for lower/fast SMA
        pslow=30,  # period for higher/slow SMA
        rsiperiod=21,
    )
    params['tr_strategy'] = None

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(period=30, devfactor=2)
        self.dataclose= self.datas[0].close    # Keep a reference to 
        self.sma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
        self.sma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        # 均线交叉, 1是上穿，-1是下穿
        self.crossover = bt.ind.CrossOver(self.sma1, self.sma2)  # crossover signal
        self.tr_strategy = self.params.tr_strategy

        self.rsi = bt.indicators.RSI_SMA(self.data.close, period=self.params.rsiperiod)


    def next(self, strategy_type=""):
        tr_str = self.tr_strategy

        close = self.data.close[0]
        date = self.data.datetime.date(0)
        #print ("{} strategy:{} type:{}".format(date, self.tr_strategy,strategy_type))

        # Log the closing prices of the series
        #self.log("Close, {0:8.2f} {0:8.2f} ".format(self.dataclose[0],close))
        #self.log('sma1, {0:8.2f}'.format(self.sma1[0]))

        if (tr_str == "cross"):
            if not self.position:  # not in the market
                if self.crossover > 0:  # if fast crosses slow to the upside
                    self.buy()  # enter long
            elif self.crossover < 0:# in the market & cross to the downside
                log.info("sell created at {} - {}".format(date, close))
                self.close()# close long position
                #log.info("{} postion:{} ".format(date,self.position))

        if (tr_str == "simple1"):
            if not self.position: # not in the market
                if self.dataclose[0] < self.dataclose[-1]:
                    if self.dataclose[-1] < self.dataclose[-2]:
                        self.log('BUY CREATE {0:8.2f}'.format(self.dataclose[0]))
                        self.order = self.buy()

        if (tr_str == "simple2"):
            if not self.position: # not in the market
                if (self.dataclose[0] - self.dataclose[-1]) < -0.05*self.dataclose[0] or (self.dataclose[0] - self.dataclose[-2]) < -0.05*self.dataclose[0] or (self.dataclose[0] - self.dataclose[-3]) < -0.05*self.dataclose[0] or (self.dataclose[0] - self.dataclose[-4]) < -0.05*self.dataclose[0]:
                    self.log('BUY CREATE {0:8.2f}'.format(self.dataclose[0]))
                    self.order = self.buy()  

        if (tr_str == "BB"):
            #if self.data.close > self.boll.lines.top:
            #self.sell(exectype=bt.Order.Stop, price=self.boll.lines.top[0], size=self.p.size)
            if self.data.close < self.boll.lines.bot:
                self.log('BUY CREATE {0:8.2f}'.format(self.dataclose[0]))
                self.order = self.buy()     

        if (tr_str == "rsi"):  
            if not self.position:
                if self.rsi < 30:
                    self.buy(size=100)
            else:
                if self.rsi > 70:
                    self.sell(size=100)

        #print('Current Portfolio Value: %.2f' % cerebro.broker.getvalue())            

    def log(self, txt, dt=None):
        # Logging function for the strategy.  'txt' is the statement and 'dt' can be used to specify a specific datetime
        dt = dt or self.datas[0].datetime.date(0)
        print('{0},{1}'.format(dt.isoformat(),txt))

    def notify_trade(self,trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS {0:8.2f}, NET {1:8.2f}'.format(
            trade.pnl, trade.pnlcomm))    



if __name__ == "__main__":
    strategy_final_values=[0,0,0,0,0]
    strategies = ["cross", "simple1", "simple2", "BB", "rsi"]
    
    
    for tr_strategy in strategies: 
        cerebro = bt.Cerebro()

        #data = DBDataFeed(
            # 本地postgresql数据库
            # db_uri="postgresql://user:password@localhost:5432/dbname",
        data = MongoData( 
            db="tushare_storage",
            dataname="000001.SZ",
            fromdate=datetime.datetime(2018, 1, 1),
            todate=datetime.datetime(2019,12, 31)
        )

        log.info("data:".format(len(data)))
        #if len(data) == 0:
        #    exit(0)

        cerebro.adddata(data)
        # Print out the starting conditions
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        

        cerebro.addstrategy(SMACross, tr_strategy=tr_strategy)
        # cerebro.addsizer(bt.sizers.AllInSizerInt)
        # cerebro.broker.set_cash(100000)

        # cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_returns")
        # cerebro.addanalyzer(bt.analyzers.DrawDown, _name="draw_down")
        # cerebro.addanalyzer(bt.analyzers.Transactions, _name="transactions")

        result = cerebro.run()

        # # 打印Analyzer结果到日志
        # for result in results:

        #     annual_returns = result.analyzers.annual_returns.get_analysis()
        #     log.info("annual returns:")
        #     for year, ret in annual_returns.items():
        #         log.info("\t {} {}%, ".format(year, round(ret * 100, 2)))

        #     draw_down = result.analyzers.draw_down.get_analysis()
        #     log.info(
        #         "drawdown={drawdown}%, moneydown={moneydown}, drawdown len={len}, "
        #         "max.drawdown={max.drawdown}, max.moneydown={max.moneydown}, "
        #         "max.len={max.len}".format(**draw_down)
        #     )

        #     transactions = result.analyzers.transactions.get_analysis()
        #     log.info("transactions")

        # 运行结果绘图
        #cerebro.plot()
        #b = Bokeh(style="bar", tabs="multi", scheme=Tradimo())
        #cerebro.plot(b)
        ind=strategies.index(tr_strategy)

        figure=cerebro.plot(style='candlestick',iplot=False)[0][0]  
        figure.savefig(tmp_dir + 'example_' + tr_strategy + '.png')
        
        # Print out the final result
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        strategy_final_values[ind] = cerebro.broker.getvalue()


    print ("Final Values for Strategies")
    for tr_strategy in strategies: 
        ind=strategies.index(tr_strategy)
        print ("{} {}  ". format(tr_strategy, strategy_final_values[ind]))     