import datetime
import backtrader as bt
from mongofeed import MongoData
#from mongofeed_tushare import MongoData
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo


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

class SMACross(bt.Strategy):
    params = dict(
        sma_lower=10,  # period for lower SMA
        sma_higher=30,  # period for higher SMA
    )
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        log.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        #self.dataclose = self.datas[0].close
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 10日SMA计算
        sma1 = bt.ind.SMA(period=self.p.sma_lower)
        # 50日SMA计算
        sma2 = bt.ind.SMA(period=self.p.sma_higher)
        # 均线交叉, 1是上穿，-1是下穿
        self.crossover = bt.ind.CrossOver(sma1, sma2)

                # Indicators for the plotting show
        bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                            subplot=True)
        bt.indicators.StochasticSlow(self.datas[0])
        bt.indicators.MACDHisto(self.datas[0])
        rsi = bt.indicators.RSI(self.datas[0])
        bt.indicators.SmoothedMovingAverage(rsi, period=10)
        bt.indicators.ATR(self.datas[0], plot=False)


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            order_type  = " buy" if( order.isbuy() ) else "sell"

            self.log(
                '{} EXECUTED [{}],[ORDER {}], Size: {}, Price: {:.2f}, Cost: {:.2f}, Comm {:.2f}'.format
                (
                order_type.upper(), 
                order.ref,
                len(self),
                order.executed.size,
                order.executed.price,
                order.executed.value,
                order.executed.comm))
            # if order.isbuy():
            #     self.log(
            #         'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
            #         (order.executed.price,
            #          order.executed.value,
            #          order.executed.comm))

            #     self.buyprice = order.executed.price
            #     self.buycomm = order.executed.comm
            # else:  # Sell
            #     self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
            #              (order.executed.price,
            #               order.executed.value,
            #               order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            print(order)

        self.order = None


    def notify_trade(self, trade):
        #self.order = None
        
        if not trade.isclosed:
            return

		# trade_info = {
		# 	"tradeid"  : trade.tradeid,
		# 	"traded_price" : trade.price,
		# 	"trade_value"   : trade.value,
		# 	"trade_gross" : trade.pnl,
		# 	"trade_net" : trade.pnlcomm,
		# 	"bars"  :len(self)
		# }

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))
                

    def next(self):
        close = self.data.close[0]
        date = self.data.datetime.date(0)
        # Simply log the closing price of the series from the reference
        #self.log('Close, %.2f %.2f ' % (self.dataclose[0],close))


        # if self.crossover > 0:
        #     self.log("{} crossover up:{}".format(date,(self.crossover > 0)) )
        #     #self.log("{} postion:{} ".format(date,self.position))      
        # if self.crossover < 0:
        #     self.log("{} crossover down:{}".format(date,(self.crossover < 0)) )
        #     #self.log("{} postion:{} ".format(date,self.position))


        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:# not in the market
            if self.crossover > 0:# if fast crosses slow to the upside
                self.log(" BUY CREATED at {} - {}".format(date, close))
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
                #self.buy()# enter long; same as up line
                #self.log("{} postion:{} ".format(date,self.position))
        elif self.crossover < 0:# in the market & cross to the downside
            self.log("SELL CREATED at {} - {}".format(date, close))
            #self.close()# close long position
            # Keep track of the created order to avoid a 2nd order
            self.order = self.sell()
            #self.sell()  # same as up line

            



if __name__ == "__main__":
    cerebro = bt.Cerebro()

    #data = DBDataFeed(
        # 本地postgresql数据库
        # db_uri="postgresql://user:password@localhost:5432/dbname",
    data = MongoData( 
        db="jqdata",
        dataname="000001.XSHE",
        #db="tushare_storage",
        #dataname="000001.SZ",
        fromdate=datetime.datetime(2019, 1, 1),
        todate=datetime.datetime(2019,12, 31)
    )

    log.info("data:{}".format(len(data)))
    #if len(data) == 0:
    #    exit(0)

    cerebro.adddata(data)

    cerebro.addstrategy(SMACross)
    #cerebro.addsizer(bt.sizers.AllInSizerInt)

    #cerebro.addsizer(bt.sizers.SizerFix, stake=10000)
    cerebro.addsizer(bt.sizers.PercentSizerInt, percents = 90)

    cerebro.broker.set_cash(1000000)
    # Set the commission - 0.1% ... divide by 100 to remove the %
    cerebro.broker.setcommission(commission=0.0012)

    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_returns")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="draw_down")
    cerebro.addanalyzer(bt.analyzers.Transactions, _name="transactions")

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='mysharpe')

    results = cerebro.run()

    # 打印Analyzer结果到日志
    for result in results:

        annual_returns = result.analyzers.annual_returns.get_analysis()
        log.info("annual returns:")
        for year, ret in annual_returns.items():
            log.info("\t {} {}%, ".format(year, round(ret * 100, 2)))

        draw_down = result.analyzers.draw_down.get_analysis()
        log.info("draw_down:")
        log.info(
            "drawdown={drawdown}%, moneydown={moneydown}, drawdown len={len}, "
            "max.drawdown={max.drawdown}, max.moneydown={max.moneydown}, "
            "max.len={max.len}".format(**draw_down)
        )

        transactions = result.analyzers.transactions.get_analysis()
        log.info("transactions:")
        log.info(transactions)
        log.info(result.analyzers.mysharpe.get_analysis())

    # 运行结果绘图
    cerebro.plot()
    #b = Bokeh(style="bar", tabs="multi", scheme=Tradimo())
    #cerebro.plot(b)