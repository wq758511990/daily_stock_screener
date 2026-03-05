from data_layer import DataLayer
from strategy_engine import StrategyEngine
from backtest_engine import CompetitiveBacktest
from report_generator import ReportGenerator
from email_service import EmailService
from loguru import logger
import pandas as pd
from tqdm import tqdm
import argparse
import sys

class ScreenerApp:
    def __init__(self, market='A', real_trade=False):
        self.market = market
        self.real_trade = real_trade
        self.data_layer = DataLayer()
        self.strategy_engine = StrategyEngine()
        
        if self.real_trade:
            self.pool_size = 300 if self.market == 'A' else 500
            logger.info(f"--- 实盘模式启动: 分析 {self.pool_size} 支股票 ---")
        else:
            self.pool_size = 50 
            logger.info(f"--- 测试模式启动: 分析前 {self.pool_size} 支股票以确保回测有效性 ---")

    def run(self):
        # 0. 清理缓存，保留当日数据，清理更旧的
        logger.info("清理过往缓存数据...")
        self.data_layer.cache.clean_old_cache(days=1) 

        logger.info("="*60)
        logger.info(f"开启 {self.market} 股市场『纯量价实盘安全版』选股与研报生成系统")
        logger.info("="*60)

        # 1. 大盘择时判断与特征提取
        logger.info(f"正在分析 {self.market} 股大盘趋势 (择时系统)...")
        index_df = self.data_layer.get_index_history(self.market, days=1260) 
        market_status = "BULL" 
        market_trend_series = None
        if not index_df.empty and len(index_df) >= 200:
            index_df['sma_200'] = index_df['close'].rolling(200).mean()
            # 提取大盘每日牛熊信号
            index_df['market_bull'] = (index_df['close'] > index_df['sma_200']).astype(int)
            market_trend_series = index_df[['market_bull']]
            
            current_index = index_df['close'].iloc[-1]
            index_sma200 = index_df['sma_200'].iloc[-1]
            if current_index < index_sma200:
                market_status = "BEAR"
                logger.warning(f"⚠️ 大盘处于空头排列 (当前指数 {current_index:.2f} < 200日均线 {index_sma200:.2f})，系统将启动 20% 轻仓防守机制！")
            else:
                logger.info(f"✅ 大盘处于多头排列 (当前指数 {current_index:.2f} > 200日均线 {index_sma200:.2f})，系统保持满仓轮动。")
        
        # 2. 获取名单
        if self.market == 'A':
            full_pool = self.data_layer.get_hs300_list()
        else:
            full_pool = self.data_layer.get_sp500_list()
        
        if full_pool.empty:
            logger.error("无法获取股票名单")
            return

        # 3. 准备数据与精细校验
        test_pool = full_pool.head(self.pool_size)
        data_dict = {}
        financial_dict = {} 
        
        logger.info(f"正在深度抓取数据并执行向量化计算 (样本 {len(test_pool)} 支)...")
        for _, row in tqdm(test_pool.iterrows(), total=len(test_pool)):
            symbol = row['symbol']
            if self.market == 'A':
                history = self.data_layer.get_a_stock_history(symbol, days=500)
                financial = self.data_layer.get_a_financial_factors(symbol)
            else:
                history = self.data_layer.get_us_stock_history(symbol, days=500)
                financial = self.data_layer.get_us_financial_factors(symbol)
            
            is_valid, reason = self.data_layer.validate_data(history, financial)
            if is_valid:
                # 注入大盘择时信号
                if market_trend_series is not None:
                    history = history.join(market_trend_series, how='left')
                    history['market_bull'] = history['market_bull'].fillna(0)
                else:
                    history['market_bull'] = 1 
                
                data_dict[symbol] = history
                financial_dict[symbol] = financial

        logger.info(f"成功获取有效数据：{len(data_dict)} 支股票进入量化竞赛。")

        # 4. 初始化回测竞争器 (传入市场参数以适配佣金)
        competitor = CompetitiveBacktest(data_dict, market=self.market)

        # 5. 对所有策略生成评分与精选池
        all_recommendations = {}
        
        logger.info("正在执行纯量价多因子截面打分引擎 (已全面应用百分位排名)...")

        # 构建当日全市场截面 DataFrame
        cross_section_records = []
        for symbol, history in data_dict.items():
            last_row = history.iloc[-1].to_dict()
            last_row['symbol'] = symbol
            last_row['name'] = test_pool[test_pool['symbol'] == symbol]['name'].iloc[0]
            financial = financial_dict[symbol]
            last_row['pe'] = financial.get('pe', 'N/A')
            last_row['pb'] = financial.get('pb', 'N/A')
            last_row['roe'] = financial.get('roe', 'N/A')
            cross_section_records.append(last_row)
            
        if cross_section_records:
            df_cross = pd.DataFrame(cross_section_records)
            for strat_name in competitor.strategies_list:
                scores = self.strategy_engine.get_score(strat_name, df_cross)
                df_cross['score'] = scores.values
                
                # 提取精选池并排序
                df_rec = df_cross[['symbol', 'name', 'score', 'close', 'pe', 'pb', 'roe']].rename(columns={'close': 'price'})
                df_rec = df_rec.sort_values(by='score', ascending=False)
                all_recommendations[strat_name] = df_rec.to_dict('records')
        else:
            for strat_name in competitor.strategies_list:
                all_recommendations[strat_name] = []

        # 6. 执行策略回测竞赛
        logger.info("正在执行 5 大量化模型的回测竞赛...")
        best_name, results_dict = competitor.find_best_strategy()
        
        logger.info(f"竞赛结束，当前胜率最高策略为: 【{best_name}】")

        if not any(all_recommendations.values()):
            logger.warning("本次运行未获取到足够的有效数据，无法生成完整建议清单。")

        # 7. 生成专业级 Markdown 报告
        logger.info("正在生成选股研究报告...")
        report = ReportGenerator(self.market)
        report.add_header(market_status=market_status)
        report.add_competition_results(best_name, results_dict)
        report.add_top_recommendations(best_name, all_recommendations[best_name])
        
        other_recs = {k: v for k, v in all_recommendations.items() if k != best_name}
        report.add_other_strategies(other_recs)
        
        report.save_report()

        # 8. 发送邮件
        logger.info("正在发送研报邮件...")
        email_service = EmailService()
        subject = f"【实盘安全量化研报】{self.market}股市场 - {report.date_str}"
        content = "\n".join(report.content)
        email_service.send_report(subject, content)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="纯技术面量化选股与研报生成系统")
    parser.add_argument('--market', type=str, default='A', choices=['A', 'US'], help='选择分析市场: A 或 US')
    parser.add_argument('--real-trade', action='store_true', help='实盘模式：A股分析300支，美股分析500支全量数据')
    args = parser.parse_args()
    
    app = ScreenerApp(market=args.market, real_trade=args.real_trade)
    app.run()
