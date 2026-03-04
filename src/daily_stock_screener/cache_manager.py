import os
import shutil
import pickle
import datetime
from loguru import logger

class CacheManager:
    """管理股票数据的本地缓存，支持按日期自动清理旧数据"""
    def __init__(self, base_path='cache'):
        self.base_path = base_path
        self.today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        self.today_dir = os.path.join(self.base_path, self.today_str)
        
        # 确保基础目录存在
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
        
        if not os.path.exists(self.today_dir):
            os.makedirs(self.today_dir)

    def get_path(self, market, symbol, data_type):
        """生成缓存文件路径"""
        market_dir = os.path.join(self.today_dir, market)
        if not os.path.exists(market_dir):
            os.makedirs(market_dir)
        return os.path.join(market_dir, f"{symbol}_{data_type}.pkl")

    def save(self, market, symbol, data_type, data):
        """保存数据到缓存"""
        path = self.get_path(market, symbol, data_type)
        try:
            with open(path, 'wb') as f:
                pickle.dump(data, f)
            # logger.debug(f"已缓存: {market} {symbol} {data_type}")
        except Exception as e:
            logger.warning(f"保存缓存失败 {symbol}: {e}")

    def load(self, market, symbol, data_type):
        """从缓存加载数据"""
        path = self.get_path(market, symbol, data_type)
        if os.path.exists(path):
            try:
                with open(path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"读取缓存失败 {symbol}: {e}")
        return None

    def clean_old_cache(self, days=5):
        """清理超过 N 天的旧缓存目录"""
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
            for dirname in os.listdir(self.base_path):
                dir_path = os.path.join(self.base_path, dirname)
                if not os.path.isdir(dir_path):
                    continue
                
                try:
                    # 尝试解析目录名作为日期
                    dir_date = datetime.datetime.strptime(dirname, "%Y-%m-%d")
                    if dir_date < cutoff_date:
                        shutil.rmtree(dir_path)
                        logger.info(f"已清理旧缓存目录: {dirname}")
                except ValueError:
                    # 忽略非日期格式的目录
                    continue
        except Exception as e:
            logger.error(f"清理缓存失败: {e}")
