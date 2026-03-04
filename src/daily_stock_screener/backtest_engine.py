import backtrader as bt
import pandas as pd
from strategy_engine import StrategyEngine
from loguru import logger

class PandasDataPlus(bt.feeds.PandasData):
    """扩展 PandasData，映射 DataLayer 提前计算好的技术指标"""
    lines = ('sma_20', 'sma_50', 'sma_200', 'momentum_net', 'volatility_20d', 'rsi_14', 'vol_ratio', 'high_52w', 'market_bull',)
    params = (
        ('sma_20', -1), ('sma_50', -1), ('sma_200', -1),
        ('momentum_net', -1), ('volatility_20d', -1),
        ('rsi_14', -1), ('vol_ratio', -1), ('high_52w', -1),
        ('market_bull', -1),
    )

class MultiStrategyWrapper(bt.Strategy):
    """通用回测包装类 - 现已进化为纯量价引擎"""
    params = (
        ('top_n', 5), 
        ('rebalance_days', 20),
        ('strategy_name', 'Quality_Growth'), 
        ('stop_loss', 0.08), # 对应研报建议的 8% 止损
    )

    def __init__(self):
        self.engine = StrategyEngine()
        self.timer = 0
        self.entry_prices = {} # 记录入场价以实现止损

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.entry_prices[order.data._name] = order.executed.price
            else:
                self.entry_prices.pop(order.data._name, None)

    def next(self):
        self.timer += 1
        
        # 1. 每日硬止损检查 (确保回测收益真实性)
        for d in self.datas:
            pos = self.getposition(d).size
            if pos > 0:
                entry_price = self.entry_prices.get(d._name)
                if entry_price:
                    pcnt = (d.close[0] - entry_price) / entry_price
                    if pcnt <= -self.params.stop_loss:
                        self.close(d)

        # 2. 定期轮动逻辑
        if self.timer % self.params.rebalance_days != 1:
            return

        is_bull_market = True
        for d in self.datas:
            if len(d) > 0:
                is_bull_market = getattr(d, 'market_bull', [1])[0] == 1
                break

        target_ratio = 1.0 if is_bull_market else 0.2
        scores = []
        for d in self.datas:
            if len(d) < 252: continue
            
            row = {
                'close': d.close[0],
                'sma_20': d.sma_20[0],
                'sma_50': d.sma_50[0],
                'sma_200': d.sma_200[0],
                'momentum_net': d.momentum_net[0],
                'volatility_20d': d.volatility_20d[0],
                'rsi_14': d.rsi_14[0],
                'vol_ratio': d.vol_ratio[0],
                'high_52w': d.high_52w[0],
            }
            
            score = self.engine.get_score(self.params.strategy_name, row)
            scores.append((d, score))

        scores.sort(key=lambda x: x[1], reverse=True)
        top_stocks = [x[0] for x in scores[:self.params.top_n] if x[1] > 0]
        
        for d in self.datas:
            pos = self.getposition(d).size
            if pos > 0 and d not in top_stocks:
                self.close(d)
        
        if top_stocks:
            target_value = (self.broker.get_value() * target_ratio) / len(top_stocks)
            for d in top_stocks:
                self.order_target_value(d, target=target_value)

class AShareCommission(bt.CommInfoBase):
    """A 股佣金模型 (包含卖出印花税)"""
    params = (('stamp_duty', 0.001), ('commission', 0.0003), ('stocklike', True), ('commtype', bt.CommInfoBase.COMM_PERC),)
    def _getcommission(self, size, price, pseudo_cash):
        return abs(size) * price * (self.p.commission + (self.p.stamp_duty if size < 0 else 0))

class CompetitiveBacktest:
    """策略竞争回测器"""
    def __init__(self, data_dict, market='A', initial_cash=100000.0):
        self.data_dict = data_dict
        self.market = market
        self.initial_cash = initial_cash
        self.strategies_list = [
            'Quality_Growth', 'Value_Reversal', 'Low_Volatility_Trend',
            'Multi_Factor_Alpha', 'Momentum_Breakout'
        ]

    def test_strategy(self, strategy_name):
        cerebro = bt.Cerebro()
        cerebro.addstrategy(MultiStrategyWrapper, strategy_name=strategy_name)

        valid_count = 0
        for symbol, df in self.data_dict.items():
            if len(df) >= 252:
                data = PandasDataPlus(
                    dataname=df, name=symbol,
                    open='open', high='high', low='low', close='close', volume='volume',
                    sma_20='sma_20', sma_50='sma_50', sma_200='sma_200',
                    momentum_net='momentum_net', volatility_20d='volatility_20d',
                    rsi_14='rsi_14', vol_ratio='vol_ratio', high_52w='high_52w',
                    market_bull='market_bull'
                )
                cerebro.adddata(data)
                valid_count += 1
                
        if valid_count == 0: return 0.0, 0.0, 0.0

        cerebro.broker.setcash(self.initial_cash)
        if self.market == 'A':
            cerebro.broker.addcommissioninfo(AShareCommission())
        else:
            cerebro.broker.setcommission(commission=0.0003) # 美股万三

        cerebro.broker.set_slippage_perc(0.001) # 0.1% 滑点
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        
        results = cerebro.run()
        strat = results[0]
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - self.initial_cash) / self.initial_cash
        dd = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0.0) / 100.0
        sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0.0) or 0.0
        return total_return, dd, sharpe

    def find_best_strategy(self):
        results = {}
        for name in self.strategies_list:
            try:
                ret, dd, sharpe = self.test_strategy(name)
                calmar = ret / dd if dd > 0 else ret * 10
                results[name] = {'return': ret, 'drawdown': dd, 'sharpe': sharpe, 'calmar': calmar}
            except Exception as e:
                logger.error(f"策略 {name} 失败: {e}")
                results[name] = {'return': -1.0, 'drawdown': 1.0, 'sharpe': -1.0, 'calmar': -1.0}
        
        df_res = pd.DataFrame(results).T
        df_res['score'] = df_res['return'].rank(pct=True) * 0.4 + df_res['sharpe'].rank(pct=True) * 0.6
        return df_res['score'].idxmax(), results
