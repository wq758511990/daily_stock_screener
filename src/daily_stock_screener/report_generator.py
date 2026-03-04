import pandas as pd
from datetime import datetime
from loguru import logger
import os

class ReportGenerator:
    """生成 Markdown 格式的选股分析报告"""
    
    def __init__(self, market):
        self.market = market
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.filename = f"Quant_Report_{self.market}_{self.date_str}.md"
        self.content = []

    def add_header(self, market_status="BULL"):
        self.content.append(f"# 📈 实盘安全版每日量化研报 ({self.market}股市场)")
        self.content.append(f"**生成日期**: {self.date_str}")
        
        if market_status == "BEAR":
            self.content.append("## 🚩 [大盘择时信号]: ⚠️ **风险/空头排列 (空仓/轻仓避险)**")
            self.content.append("> **分析**: 当前基准指数低于 200 日均线，市场整体势能较弱。选股应更注重防御（如极低波动率策略），并严格控制总仓位。")
        else:
            self.content.append("## 🚩 [大盘择时信号]: ✅ **多头/趋势向上 (积极参与)**")
            self.content.append("> **分析**: 当前基准指数运行在 200 日均线上方，市场处于上升通道，动量策略胜率极高。")
            
        self.content.append("---\n")

    def add_competition_results(self, best_name, results_dict):
        self.content.append("## 🏆 一、 纯量价回测锦标赛 (过去1年实盘防穿越模拟)")
        self.content.append("> **架构说明**: 锦标赛完全基于**纯量价技术面**展开竞争，已加入 **0.1% 滑点**与真实佣金（A股含卖出印花税）。通过收益+夏普综合排名选出当前最强策略，用于驱动今日选股。")
        self.content.append("> ⚠️ **数据局限声明**: 本回测基于**当前指数成分股**历史数据，不含回测期内已被剔除或退市的个股，存在**幸存者偏差**，回测收益系统性偏高。此处数据仅用于五种策略逻辑的**横向相对比较**，不代表未来可获得的真实收益，请勿据此推算预期回报。")
        
        self.content.append("\n| 策略名称 | 核心逻辑 | 年化收益 | 最大回撤 | 夏普比率 | 综合排位 |")
        self.content.append("| :--- | :--- | :---: | :---: | :---: | :---: |")
        
        df_res = pd.DataFrame(results_dict).T
        df_res['ret_rank'] = df_res['return'].rank(pct=True)
        df_res['sharpe_rank'] = df_res['sharpe'].rank(pct=True)
        df_res['calmar_rank'] = df_res['calmar'].rank(pct=True)
        df_res['composite_score'] = df_res['ret_rank'] * 0.4 + df_res['sharpe_rank'] * 0.3 + df_res['calmar_rank'] * 0.3
        
        logic_map = {
            'Quality_Growth': '长牛趋势: 极高动量+极低波动',
            'Value_Reversal': '超跌反转: 均线负乖离+RSI超卖',
            'Low_Volatility_Trend': '低波稳健: 极低波动+均线多头',
            'Multi_Factor_Alpha': '均衡量价: 动量与波动均衡配置',
            'Momentum_Breakout': '动量突破: 逼近52周新高+放量'
        }
        
        for name, metrics in results_dict.items():
            ret = metrics['return']
            dd = metrics['drawdown']
            sharpe = metrics['sharpe']
            score = df_res.loc[name, 'composite_score']
            logic = logic_map.get(name, '量化策略')
            
            if name == best_name:
                self.content.append(f"| **👑 {name}** | **{logic}** | **{ret:.2%}** | **{dd:.2%}** | **{sharpe:.2f}** | **{score:.2f}** |")
            else:
                self.content.append(f"| {name} | {logic} | {ret:.2%} | {dd:.2%} | {sharpe:.2f} | {score:.2f} |")
        
        self.content.append(f"\n> **结论**: 盘面资金最认可 **【{best_name}】** 的技术形态，系统将以此作为今日实盘主推。\n")

    def add_top_recommendations(self, strategy_name, recommendations):
        self.content.append(f"## 🎯 二、 今日主推精选池 (基于 {strategy_name})")
        self.content.append("> **风险排雷提示**: 量化系统仅给出最佳技术形态！买入前请务必参考表格中的 **PE(市盈率) 和 ROE**，若 PE 为负或异乎寻常的高，说明存在**极大概率的暴雷风险**，请人工剔除。")
        
        if not recommendations:
            self.content.append("今日无符合该严苛策略的股票。")
            return

        self.content.append("\n| 股票代码 | 股票名称 | 技术面评分 | 当前价格 | ⚠️ PE (参考) | ⚠️ ROE (参考) | 建议买入 | 硬止损 |")
        self.content.append("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |")
        
        for item in recommendations[:10]:
            price = item['price']
            buy_zone = f"{price*0.99:.2f} - {price*1.01:.2f}"
            stop_loss = f"{price*0.92:.2f} (-8%)" 
            
            pe_str = f"{item['pe']:.2f}" if isinstance(item['pe'], (int, float)) else str(item['pe'])
            roe_str = f"{item['roe']:.2f}%" if isinstance(item['roe'], (int, float)) else str(item['roe'])
            
            # 高亮危险基本面
            if isinstance(item['pe'], (int, float)) and (item['pe'] < 0 or item['pe'] > 150):
                pe_str = f"**{pe_str}**"
                
            self.content.append(f"| `{item['symbol']}` | {item['name']} | {item['score']:.1f} | {price:.2f} | {pe_str} | {roe_str} | {buy_zone} | {stop_loss} |")
        
        self.content.append("\n---\n")

    def add_other_strategies(self, all_recommendations):
        self.content.append("## 💡 三、 其他形态备选池 (Top 3)")
        self.content.append("供偏好其他技术形态的投资者参考：\n")
        
        for name, items in all_recommendations.items():
            self.content.append(f"### 形态：{name}")
            self.content.append("| 股票代码 | 股票名称 | 技术评分 | 当前价格 | PE参考 |")
            self.content.append("| :--- | :--- | :---: | :---: | :---: |")
            for item in items[:3]:
                pe_str = f"{item['pe']:.2f}" if isinstance(item['pe'], (int, float)) else str(item['pe'])
                self.content.append(f"| `{item['symbol']}` | {item['name']} | {item['score']:.1f} | {item['price']:.2f} | {pe_str} |")
            self.content.append("\n")

    def save_report(self):
        full_text = "\n".join(self.content)
        with open(self.filename, 'w', encoding='utf-8') as f:
            f.write(full_text)
        logger.info(f"✅ 报告已生成: {os.path.abspath(self.filename)}")
        return self.filename
