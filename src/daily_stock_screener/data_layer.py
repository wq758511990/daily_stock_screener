import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from loguru import logger
import datetime
import os
import requests
from cache_manager import CacheManager

class DataLayer:
    """
    数据接入层：负责获取 A 股和美股的股票名单、历史数据及财务因子。
    优化更新：增加了技术指标的向量化预计算，极大提升回测速度。
    """
    def __init__(self):
        # 仅针对 A 股数据源域名禁用代理，确保国内流量直连
        a_share_domains = "eastmoney.com,sina.com.cn,163.com,akshare.xyz,ths123.com,iwencai.com,baidu.com,qq.com"
        os.environ['NO_PROXY'] = a_share_domains
        os.environ['no_proxy'] = a_share_domains
        self.cache = CacheManager()

    def get_hs300_list(self):
        """获取沪深300成分股列表"""
        try:
            df = ak.index_stock_cons(symbol="000300")
            return df[['品种代码', '品种名称']].rename(columns={'品种代码': 'symbol', '品种名称': 'name'})
        except Exception as e:
            logger.error(f"获取沪深300名单失败: {e}，请检查网络连接及代理设置。")
            return pd.DataFrame()

    def get_sp500_list(self):
        """获取标普500成分股列表"""
        try:
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            table = pd.read_html(response.text)
            df = table[0]
            df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
            return df[['Symbol', 'Security']].rename(columns={'Symbol': 'symbol', 'Security': 'name'})
        except Exception as e:
            logger.error(f"获取标普500名单失败: {e}")
            return pd.DataFrame()

    def _add_technical_indicators(self, df):
        """核心优化：在 DataFrame 层面进行向量化技术指标计算，摒弃单行计算瓶颈"""
        if df.empty or len(df) < 50:
            return df
        
        # 1. 均线系统
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['sma_200'] = df['close'].rolling(200).mean()
        
        # 2. 动量系统 (12个月减1个月)
        df['momentum_12m'] = df['close'].pct_change(periods=252)
        df['momentum_1m'] = df['close'].pct_change(periods=21)
        df['momentum_net'] = (df['momentum_12m'] - df['momentum_1m']) * 100
        
        # 3. 波动率 (20日年化波动)
        df['volatility_20d'] = df['close'].pct_change().rolling(20).std() * np.sqrt(252)
        
        # 4. RSI_14 (使用传统看盘软件标准的指数移动平均 RMA)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        # 5. 成交量放量比
        df['vol_50d_avg'] = df['volume'].rolling(50).mean()
        df['vol_ratio'] = df['volume'] / df['vol_50d_avg'].replace(0, np.nan)
        
        # 6. 52周新高
        df['high_52w'] = df['high'].rolling(252).max()
        
        # 极其重要：坚决不能使用 bfill，否则会引发极其严重的数据穿越(未来函数)
        # 将无法计算出指标的前置预热期NaN值填充为0即可，让策略在这些天打分为0避免交易
        df.fillna(0, inplace=True)
        
        return df

    def get_a_stock_history(self, symbol, days=500):
        """获取 A 股历史数据并附加技术指标"""
        cached_data = self.cache.load('A', symbol, 'history')
        if cached_data is not None:
            return cached_data

        import time
        import random
        for attempt in range(3):
            try:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime("%Y%m%d")
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, adjust="qfq")
                if df.empty: return pd.DataFrame()
                
                df = df.rename(columns={
                    '日期': 'date', '开盘': 'open', '收盘': 'close', 
                    '最高': 'high', '最低': 'low', '成交量': 'volume'
                })
                df.columns = [c.lower() for c in df.columns]
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                # 计算指标
                df = self._add_technical_indicators(df)
                
                df_final = df.tail(days)
                self.cache.save('A', symbol, 'history', df_final)
                return df_final
            except Exception as e:
                if attempt < 2:
                    time.sleep(random.uniform(1, 3))
                    continue
                logger.debug(f"获取 A 股 {symbol} 失败 (已重试{attempt+1}次): {e}")
        return pd.DataFrame()

    def get_us_stock_history(self, symbol, days=500):
        """获取美股历史数据并附加技术指标"""
        cached_data = self.cache.load('US', symbol, 'history')
        if cached_data is not None:
            return cached_data

        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=f"{days}d")
            df.columns = [c.lower() for c in df.columns]
            
            # 计算指标
            df = self._add_technical_indicators(df)
            
            self.cache.save('US', symbol, 'history', df)
            return df
        except Exception as e:
            logger.debug(f"获取美股 {symbol} 失败: {e}")
            return pd.DataFrame()

    def get_a_financial_factors(self, symbol):
        """获取 A 股财务因子（仅作报告展示用）"""
        cached_data = self.cache.load('A', symbol, 'financial')
        if cached_data is not None:
            return cached_data

        try:
            df = ak.stock_financial_abstract_ths(symbol=symbol)
            if df.empty: return {}
            latest = df.iloc[0]
            factors = {
                'pe': float(latest.get('市盈率(动态)', 50)),
                'pb': float(latest.get('市净率', 2)),
                'roe': float(latest.get('净资产收益率', 0)),
                'dividend_yield': float(latest.get('股息率', 0)),
                'sector': 'A股' 
            }
            self.cache.save('A', symbol, 'financial', factors)
            return factors
        except Exception:
            return {'pe': 'N/A', 'pb': 'N/A', 'roe': 'N/A', 'sector': 'A股'}

    def get_us_financial_factors(self, symbol):
        """获取美股财务因子（仅作报告展示用）"""
        cached_data = self.cache.load('US', symbol, 'financial')
        if cached_data is not None:
            return cached_data

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            factors = {
                'pe': info.get('forwardPE', 'N/A'),
                'pb': info.get('priceToBook', 'N/A'),
                'roe': info.get('returnOnEquity', 0) * 100 if info.get('returnOnEquity') else 'N/A',
                'dividend_yield': info.get('dividendYield', 0) * 100 if info.get('dividendYield') else 'N/A',
                'sector': info.get('sector', 'US')
            }
            self.cache.save('US', symbol, 'financial', factors)
            return factors
        except Exception:
            return {'pe': 'N/A', 'pb': 'N/A', 'roe': 'N/A', 'sector': 'US'}

    def get_index_history(self, market='A', days=1260):
        """获取市场基准指数（A股: 沪深300, US: 标普500），默认取5年以增强择时稳健性"""
        try:
            if market == 'A':
                df = ak.stock_zh_index_daily_em(symbol="sh000300")
                df = df.rename(columns={'date': 'date', 'open': 'open', 'close': 'close', 'high': 'high', 'low': 'low', 'volume': 'volume'})
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            else:
                ticker = yf.Ticker("^GSPC")
                # 使用 period="5y" 替代固定天数
                df = ticker.history(period="5y")
                df.columns = [c.lower() for c in df.columns]
            
            return df.tail(days)
        except Exception as e:
            logger.error(f"获取基准指数失败: {e}")
            return pd.DataFrame()

    def validate_data(self, df, financial):
        """实盘数据校验：包含流动性校验与基本面初步排雷"""
        if df.empty or len(df) < 252:
            return False, "历史数据不足1年"
        
        if df['volume'].tail(5).mean() <= 0:
            return False, "流动性异常(停牌或无成交)"

        # 核心排雷：剔除亏损公司 (PE <= 0)
        pe = financial.get('pe')
        if isinstance(pe, (int, float)) and pe <= 0:
            return False, f"基本面风险 (PE: {pe:.2f})"
            
        return True, "OK"
