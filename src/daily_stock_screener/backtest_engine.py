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
    """实盘机构版回测核心：截面排位打分 + 闲置资金动态替补"""
    params = (
        ('top_n', 5), 
        ('rebalance_days', 20),
        ('strategy_name', 'Quality_Growth'), 
        ('stop_loss', 0.08), 
    )

    def __init__(self):
        self.engine = StrategyEngine()
        self.timer = 0
        self.entry_prices = {} 

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.entry_prices[order.data._name] = order.executed.price
            else:
                self.entry_prices.pop(order.data._name, None)

    def next(self):
        self.timer += 1
        
        # 1. 每日硬止损检查：跌破 8% 无条件出局
        stopped_out_symbols = set()
        for d in self.datas:
            pos = self.getposition(d).size
            if pos > 0:
                entry_price = self.entry_prices.get(d._name)
                if entry_price:
                    pcnt = (d.close[0] - entry_price) / entry_price
                    if pcnt <= -self.params.stop_loss:
                        self.close(d)
                        stopped_out_symbols.add(d._name)

        is_rebalance_day = (self.timer % self.params.rebalance_days == 1)
        
        # 计算当前仍在正常持仓的票数 (不含今天刚被止损的)
        current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0 and d._name not in stopped_out_symbols)
        
        # 优化资金利用率：如果既不是调仓日，坑位又是满的，直接跳过 (节省算力)
        if not is_rebalance_day and current_positions >= self.params.top_n:
            return

        # 获取当前大盘的牛熊状态 (防守逻辑)
        is_bull_market = True
        for d in self.datas:
            if len(d) > 0:
                is_bull_market = getattr(d, 'market_bull', [1])[0] == 1
                break

        target_ratio = 1.0 if is_bull_market else 0.2
        target_value_per_stock = (self.broker.get_value() * target_ratio) / self.params.top_n

        # 2. 提取当日全市场横截面数据 (用于策略因子打分)
        records = []
        symbol_to_data = {}
        for d in self.datas:
            if len(d) < 252: continue
            symbol_to_data[d._name] = d
            records.append({
                'symbol': d._name,
                'close': d.close[0],
                'sma_20': d.sma_20[0],
                'sma_50': d.sma_50[0],
                'sma_200': d.sma_200[0],
                'momentum_net': d.momentum_net[0],
                'volatility_20d': d.volatility_20d[0],
                'rsi_14': d.rsi_14[0],
                'vol_ratio': d.vol_ratio[0],
                'high_52w': d.high_52w[0],
            })

        if not records:
            return
            
        # 向量化截面打分
        df_cross = pd.DataFrame(records)
        scores = self.engine.get_score(self.params.strategy_name, df_cross)
        df_cross['score'] = scores.values
        df_cross.sort_values(by='score', ascending=False, inplace=True)
        
        if is_rebalance_day:
            # 【A】定期大调仓：强制优胜劣汰
            top_symbols = df_cross.head(self.params.top_n)['symbol'].tolist()
            
            # 卖出不再符合 Top N 的老票
            for d in self.datas:
                pos = self.getposition(d).size
                if pos > 0 and d._name not in top_symbols and d._name not in stopped_out_symbols:
                    self.close(d)
            
            # 买入新的 Top N
            for sym in top_symbols:
                self.order_target_value(symbol_to_data[sym], target=target_value_per_stock)
        else:
            # 【B】闲置资金再平衡 (替补机制)：发现空缺，立即找最高分的未持仓标的补上
            needed_slots = self.params.top_n - current_positions
            if needed_slots > 0:
                candidates = []
                for _, row in df_cross.iterrows():
                    sym = row['symbol']
                    d = symbol_to_data[sym]
                    pos = self.getposition(d).size
                    # 必须是：1.目前没持仓 2.不是今天刚被止损卖掉的
                    if pos == 0 and sym not in stopped_out_symbols:
                        candidates.append(d)
                    if len(candidates) == needed_slots:
                        break
                
                # 立即动用闲置资金买入替补股票
                for d in candidates:
                    self.order_target_value(d, target=target_value_per_stock)


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
            cerebro.broker.setcommission(commission=0.0003) 

        cerebro.broker.set_slippage_perc(0.001) 
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        
        results = cerebro.run()
        strat = results[0]
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - self.initial_cash) / self.initial_cash
        dd = strat.analyzers.drawdown.get_analysis()['max']['drawdown'] / 100.0
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
