class QualityGrowthStrategy:
    """长牛趋势 (原质量成长) - 纯技术面: 高动量、低波动"""
    def score(self, row):
        mom = row.get('momentum_net', 0)
        vol = row.get('volatility_20d', 1.0)
        rsi = row.get('rsi_14', 50)
        
        score_mom = max(0, min(100, mom)) # 0-100% 动量
        score_vol = max(0, 100 - (vol * 100)) # 波动率越小越高
        
        # RSI惩罚过热
        if rsi > 80: score_mom *= 0.5
        
        return score_mom * 0.7 + score_vol * 0.3

class ValueReversalStrategy:
    """超跌反转 (原深度价值) - 纯技术面: 均线大幅乖离、RSI超卖"""
    def score(self, row):
        close = row.get('close', 0)
        sma200 = row.get('sma_200', close)
        rsi = row.get('rsi_14', 50)
        
        score_rev = 0
        if close > 0 and sma200 > 0:
            bias = (sma200 - close) / sma200 * 100 # 低于200日线越多，正乖离越大
            if bias > 0:
                score_rev = min(100, bias * 2)
                
        score_rsi = 0
        if rsi < 30:
            score_rsi = 100 - (rsi * 2) # rsi越小分越高
            
        return score_rev * 0.6 + score_rsi * 0.4

class DividendStrategy:
    """低波稳健 (原红利贵族) - 纯技术面: 极低波动率、均线上方"""
    def score(self, row):
        vol = row.get('volatility_20d', 1.0)
        close = row.get('close', 0)
        sma50 = row.get('sma_50', 0)
        
        score_vol = max(0, 100 - (vol * 150)) # 对波动率要求极其严苛
        
        trend_bonus = 20 if close > sma50 else 0
        
        return min(100, score_vol * 0.8 + trend_bonus)

class MultiFactorAlphaStrategy:
    """综合量价 (原多因子) - 纯技术面: 动量适中、波动适中、均线多头"""
    def score(self, row):
        mom = row.get('momentum_net', 0)
        vol = row.get('volatility_20d', 1.0)
        close = row.get('close', 0)
        sma20 = row.get('sma_20', 0)
        sma50 = row.get('sma_50', 0)
        
        score_mom = max(0, min(100, mom * 1.5))
        score_vol = max(0, 100 - (vol * 100))
        
        trend = 100 if (close > sma20 and sma20 > sma50) else 0
        
        return score_mom * 0.4 + score_vol * 0.3 + trend * 0.3

class MomentumBreakoutStrategy:
    """动量突破 - 纯技术面: 逼近52周新高、近期放量"""
    def score(self, row):
        close = row.get('close', 0)
        high_52w = row.get('high_52w', close)
        vol_ratio = row.get('vol_ratio', 1.0)
        
        score_breakout = 0
        if high_52w > 0:
            dist_to_high = close / high_52w
            if dist_to_high >= 0.95:
                score_breakout = 100
            elif dist_to_high >= 0.85:
                score_breakout = (dist_to_high - 0.85) * 1000
                
        score_vol = min(100, (vol_ratio - 1) * 50) if vol_ratio > 1 else 0
        
        return score_breakout * 0.6 + score_vol * 0.4

class StrategyEngine:
    """纯量价多因子策略引擎工厂 (彻底剔除基本面和未来函数)"""
    def __init__(self):
        self.strategies = {
            'Quality_Growth': QualityGrowthStrategy(),
            'Value_Reversal': ValueReversalStrategy(),
            'Dividend_Yield': DividendStrategy(),
            'Multi_Factor_Alpha': MultiFactorAlphaStrategy(),
            'Momentum_Breakout': MomentumBreakoutStrategy()
        }

    def get_score(self, strategy_name, row):
        """传入单行指标截面数据即可计算绝对得分"""
        strat = self.strategies.get(strategy_name)
        if strat:
            return strat.score(row)
        return 0
