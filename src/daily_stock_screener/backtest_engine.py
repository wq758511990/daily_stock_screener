import backtrader as bt
import pandas as pd
from strategy_engine import StrategyEngine
from loguru import logger

class PandasDataPlus(bt.feeds.PandasData):
    """扩展 PandasData，映射 DataLayer 提前计算好的技术指标，防止回测卡顿和重计算"""
    lines = ('sma_20', 'sma_50', 'sma_200', 'momentum_net', 'volatility_20d', 'rsi_14', 'vol_ratio', 'high_52w',)
    params = (
        ('sma_20', -1), ('sma_50', -1), ('sma_200', -1),
        ('momentum_net', -1), ('volatility_20d', -1),
        ('rsi_14', -1), ('vol_ratio', -1), ('high_52w', -1),
    )

class MultiStrategyWrapper(bt.Strategy):
    """通用回测包装类 - 现已进化为纯量价引擎"""
    params = (
        ('top_n', 5), 
        ('rebalance_days', 20),
        ('strategy_name', 'Quality_Growth'), 
    )

    def __init__(self):
        self.engine = StrategyEngine()
        self.timer = 0

    def next(self):
        self.timer += 1
        if self.timer % self.params.rebalance_days != 1:
            return

        scores = []
        for d in self.datas:
            # 高级量化基准：必须确保该资产在当下截面已经彻底度过 252 天的指标计算预热期
            # 否则 200日均线、12月动量 均为 0 会导致错误的交易信号
            if len(d) < 252: continue
            
            # 读取当前这根K线附带的所有预计算技术指标
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
            target_value = self.broker.get_value() / len(top_stocks)
            for d in top_stocks:
                self.order_target_value(d, target=target_value)

class CompetitiveBacktest:
    """策略竞争回测器"""
    def __init__(self, data_dict, initial_cash=100000.0):
        self.data_dict = data_dict
        self.initial_cash = initial_cash
        self.strategies_list = [
            'Quality_Growth', 
            'Value_Reversal', 
            'Dividend_Yield',
            'Multi_Factor_Alpha',
            'Momentum_Breakout'
        ]

    def test_strategy(self, strategy_name):
        cerebro = bt.Cerebro()
        cerebro.addstrategy(MultiStrategyWrapper, strategy_name=strategy_name)

        valid_data_count = 0
        for symbol, df in self.data_dict.items():
            if len(df) >= 252:
                # 使用扩展的 PandasDataPlus 注入所有指标列
                data = PandasDataPlus(
                    dataname=df, 
                    name=symbol,
                    open='open', high='high', low='low', close='close', volume='volume',
                    sma_20='sma_20', sma_50='sma_50', sma_200='sma_200',
                    momentum_net='momentum_net', volatility_20d='volatility_20d',
                    rsi_14='rsi_14', vol_ratio='vol_ratio', high_52w='high_52w'
                )
                cerebro.adddata(data)
                valid_data_count += 1
                
        if valid_data_count == 0:
            return 0.0, 0.0, 0.0

        cerebro.broker.setcash(self.initial_cash)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.broker.set_slippage_perc(0.001)

        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.02)
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        
        results = cerebro.run()
        strat = results[0]
        
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - self.initial_cash) / self.initial_cash
        
        drawdown_analysis = strat.analyzers.drawdown.get_analysis()
        max_drawdown = drawdown_analysis['max']['drawdown'] / 100.0 if 'max' in drawdown_analysis else 0.0
        
        sharpe_analysis = strat.analyzers.sharpe.get_analysis()
        sharpe_ratio = sharpe_analysis.get('sharperatio', 0.0)
        if sharpe_ratio is None:
            sharpe_ratio = 0.0
            
        return total_return, max_drawdown, sharpe_ratio

    def find_best_strategy(self):
        """综合表现评估 (结合收益、回撤与夏普)"""
        results = {}
        for name in self.strategies_list:
            try:
                ret, dd, sharpe = self.test_strategy(name)
                calmar = ret / dd if dd > 0 else ret * 10
                results[name] = {'return': ret, 'drawdown': dd, 'sharpe': sharpe, 'calmar': calmar}
            except Exception as e:
                logger.error(f"策略 {name} 回测失败: {e}")
                results[name] = {'return': -1.0, 'drawdown': 1.0, 'sharpe': -1.0, 'calmar': -1.0}
        
        df_res = pd.DataFrame(results).T
        df_res['ret_rank'] = df_res['return'].rank(pct=True)
        df_res['sharpe_rank'] = df_res['sharpe'].rank(pct=True)
        df_res['calmar_rank'] = df_res['calmar'].rank(pct=True)
        
        df_res['composite_score'] = df_res['ret_rank'] * 0.4 + df_res['sharpe_rank'] * 0.3 + df_res['calmar_rank'] * 0.3
        best_name = df_res['composite_score'].idxmax()
        
        return best_name, results
