import pandas as pd

class QualityGrowthStrategy:
    """长牛趋势 - 纯技术面: 高动量、低波动"""
    def score(self, df: pd.DataFrame) -> pd.Series:
        # 动量越大排名越高 (接近1)
        mom_rank = df['momentum_net'].rank(pct=True, ascending=True)
        # 波动越小排名越高 (由于ascending=False，波动率越小rank值越接近1)
        vol_rank = df['volatility_20d'].rank(pct=True, ascending=False) 
        
        # RSI 惩罚：防止高位接盘
        rsi_penalty = df['rsi_14'].apply(lambda x: 0.5 if x > 80 else 1.0)
        
        return (mom_rank * 0.7 + vol_rank * 0.3) * rsi_penalty * 100

class ValueReversalStrategy:
    """超跌反转 - 纯技术面: 均线负乖离极大、RSI极端超卖"""
    def score(self, df: pd.DataFrame) -> pd.Series:
        # 乖离率 = (200日均线 - 当前价) / 200日均线
        bias = (df['sma_200'] - df['close']) / df['sma_200'].replace(0, 1)
        # 乖离率越大，说明跌得越惨，排名越高
        bias_rank = bias.rank(pct=True, ascending=True) 
        # RSI 越小，说明越超卖，排名越高
        rsi_rank = df['rsi_14'].rank(pct=True, ascending=False) 
        
        return (bias_rank * 0.6 + rsi_rank * 0.4) * 100

class LowVolStrategy:
    """低波稳健 - 纯技术面: 极低波动率、均线多头"""
    def score(self, df: pd.DataFrame) -> pd.Series:
        vol_rank = df['volatility_20d'].rank(pct=True, ascending=False)
        # 趋势加分：站上50日均线
        trend_bonus = (df['close'] > df['sma_50']).astype(int) * 0.2
        
        return (vol_rank * 0.8 + trend_bonus) * 100

class MultiFactorAlphaStrategy:
    """综合量价 - 纯技术面: 动量适中、波动适中、均线多头"""
    def score(self, df: pd.DataFrame) -> pd.Series:
        mom_rank = df['momentum_net'].rank(pct=True, ascending=True)
        vol_rank = df['volatility_20d'].rank(pct=True, ascending=False)
        trend = ((df['close'] > df['sma_20']) & (df['sma_20'] > df['sma_50'])).astype(int) * 0.3
        
        return (mom_rank * 0.4 + vol_rank * 0.3 + trend) * 100

class MomentumBreakoutStrategy:
    """动量突破 - 纯技术面: 逼近52周新高、近期放量"""
    def score(self, df: pd.DataFrame) -> pd.Series:
        # 距离52周新高越近越好
        dist_to_high = df['close'] / df['high_52w'].replace(0, 1)
        dist_rank = dist_to_high.rank(pct=True, ascending=True)
        # 放量比越高越好
        vol_ratio_rank = df['vol_ratio'].rank(pct=True, ascending=True)
        
        return (dist_rank * 0.6 + vol_ratio_rank * 0.4) * 100

class StrategyEngine:
    """纯量价多因子截面策略引擎 (实盘机构版)"""
    def __init__(self):
        self.strategies = {
            'Quality_Growth': QualityGrowthStrategy(),
            'Value_Reversal': ValueReversalStrategy(),
            'Low_Volatility_Trend': LowVolStrategy(),
            'Multi_Factor_Alpha': MultiFactorAlphaStrategy(),
            'Momentum_Breakout': MomentumBreakoutStrategy()
        }

    def get_score(self, strategy_name: str, df: pd.DataFrame) -> pd.Series:
        """
        传入当日全市场截面 DataFrame
        返回一个对齐的 Series，包含经过百分位排序标准化后的最终得分
        """
        strat = self.strategies.get(strategy_name)
        if strat and not df.empty:
            return strat.score(df).fillna(0)
        return pd.Series(0, index=df.index if not df.empty else [])
