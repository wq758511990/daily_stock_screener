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
            logger.error(f"获取沪深300名单失败: {e}")
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

    def get_russell_1000_list(self):
        """获取罗素1000成分股列表 (大中盘股)，极其健壮的解析器"""
        try:
            from io import StringIO
            url = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            tables = pd.read_html(StringIO(response.text))
            for df in tables:
                col_names = [str(c).lower() for c in df.columns]
                ticker_col = None
                company_col = None
                if 'ticker' in col_names:
                    ticker_col = df.columns[col_names.index('ticker')]
                elif 'symbol' in col_names:
                    ticker_col = df.columns[col_names.index('symbol')]
                if 'company' in col_names:
                    company_col = df.columns[col_names.index('company')]
                elif 'security' in col_names:
                    company_col = df.columns[col_names.index('security')]
                if ticker_col is not None and company_col is not None:
                    df = df.rename(columns={ticker_col: 'symbol', company_col: 'name'})
                    df['symbol'] = df['symbol'].astype(str).str.replace('.', '-', regex=False)
                    df = df[df['symbol'].str.strip() != '']
                    logger.info(f"成功从维基百科提取 {len(df)} 只罗素 1000 成分股")
                    return df[['symbol', 'name']]
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取罗素1000名单失败: {e}")
            return pd.DataFrame()

    def _add_technical_indicators(self, df):
        """向量化预计算技术指标"""
        if df.empty or len(df) < 50:
            return df
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['sma_200'] = df['close'].rolling(200).mean()
        df['momentum_12m'] = df['close'].pct_change(periods=252)
        df['momentum_1m'] = df['close'].pct_change(periods=21)
        df['momentum_net'] = (df['momentum_12m'] - df['momentum_1m']) * 100
        df['volatility_20d'] = df['close'].pct_change().rolling(20).std() * np.sqrt(252)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        df['vol_ratio'] = df['volume'] / df['volume'].rolling(50).mean().replace(0, np.nan)
        df['high_52w'] = df['high'].rolling(252).max()
        df.fillna(0, inplace=True)
        return df

    def get_a_stock_history(self, symbol, days=750):
        """获取 A 股历史数据并附加技术指标"""
        cached_data = self.cache.load('A', symbol, 'history')
        if cached_data is not None:
            return cached_data
        import time
        import random
        for attempt in range(3):
            try:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=1100)).strftime("%Y%m%d")
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, adjust="qfq")
                if df.empty: return pd.DataFrame()
                df = df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume'})
                df.columns = [c.lower() for c in df.columns]
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df = self._add_technical_indicators(df)
                df_final = df.tail(days)
                self.cache.save('A', symbol, 'history', df_final)
                return df_final
            except Exception as e:
                if attempt < 2: time.sleep(random.uniform(1, 3)); continue
        return pd.DataFrame()

    def get_us_stock_history(self, symbol, days=750):
        """获取美股历史数据并附加技术指标"""
        cached_data = self.cache.load('US', symbol, 'history')
        if cached_data is not None:
            return cached_data
        try:
            ticker = yf.Ticker(symbol)
            # 为了给 750 天留下足够计算空间，多取一点
            df = ticker.history(period="4y")
            df.columns = [c.lower() for c in df.columns]
            df = self._add_technical_indicators(df)
            df_final = df.tail(days)
            self.cache.save('US', symbol, 'history', df_final)
            return df_final
        except Exception:
            return pd.DataFrame()

    def get_a_financial_factors(self, symbol):
        """获取 A 股财务因子"""
        cached_data = self.cache.load('A', symbol, 'financial')
        if cached_data is not None: return cached_data
        try:
            df = ak.stock_financial_abstract_ths(symbol=symbol)
            if df.empty: return {}
            latest = df.iloc[0]
            factors = {'pe': float(latest.get('市盈率(动态)', 50)), 'pb': float(latest.get('市净率', 2)), 
                       'roe': float(latest.get('净资产收益率', 0)), 'dividend_yield': float(latest.get('股息率', 0)), 'sector': 'A股'}
            self.cache.save('A', symbol, 'financial', factors)
            return factors
        except Exception: return {'pe': 'N/A', 'pb': 'N/A', 'roe': 'N/A', 'sector': 'A股'}

    def get_us_financial_factors(self, symbol):
        """获取美股财务因子"""
        cached_data = self.cache.load('US', symbol, 'financial')
        if cached_data is not None: return cached_data
        try:
            info = yf.Ticker(symbol).info
            factors = {'pe': info.get('forwardPE', 'N/A'), 'pb': info.get('priceToBook', 'N/A'), 
                       'roe': info.get('returnOnEquity', 0) * 100 if info.get('returnOnEquity') else 'N/A', 
                       'dividend_yield': info.get('dividendYield', 0) * 100 if info.get('dividendYield') else 'N/A', 'sector': info.get('sector', 'US')}
            self.cache.save('US', symbol, 'financial', factors)
            return factors
        except Exception: return {'pe': 'N/A', 'pb': 'N/A', 'roe': 'N/A', 'sector': 'US'}

    def get_index_history(self, market='A', days=1260):
        """获取基准指数历史"""
        try:
            if market == 'A':
                df = ak.stock_zh_index_daily_em(symbol="sh000300")
                df = df.rename(columns={'date': 'date', 'open': 'open', 'close': 'close', 'high': 'high', 'low': 'low', 'volume': 'volume'})
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            else:
                df = yf.Ticker("^GSPC").history(period="5y")
                df.columns = [c.lower() for c in df.columns]
            return df.tail(days)
        except Exception: return pd.DataFrame()

    def validate_data(self, df, financial):
        """实盘数据校验"""
        if df.empty or len(df) < 500: # 提升准入门槛，必须有足够 IS 数据
            return False, "历史数据纵深不足"
        if df['volume'].tail(5).mean() <= 0:
            return False, "流动性异常"
        pe = financial.get('pe')
        if isinstance(pe, (int, float)) and pe <= 0:
            return False, f"亏损排雷(PE:{pe:.2f})"
        return True, "OK"
