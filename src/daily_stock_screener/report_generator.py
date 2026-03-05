import pandas as pd
from datetime import datetime
from loguru import logger
import os

class ReportGenerator:
    """生成 Markdown 格式的选股分析报告 - 样本外验证版"""
    
    def __init__(self, market, pool='sp500'):
        self.market = market
        self.pool = pool
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.filename = f"Quant_Report_{self.market}_{self.pool}_{self.date_str}.md"
        self.content = []

    def add_header(self, market_status="BULL"):
        self.content.append(f"# 📈 实盘安全版每日量化研报 ({self.market}股市场 | {self.pool.upper()})")
        self.content.append(f"**生成日期**: {self.date_str}")
        
        if market_status == "BEAR":
            self.content.append("## 🚩 [大盘择时信号]: ⚠️ **风险/空头排列 (空仓/轻仓避险)**")
            self.content.append("> **分析**: 当前基准指数低于 200 日均线，市场整体势能较弱。选股应更注重防御，并严格控制总仓位。")
        else:
            self.content.append("## 🚩 [大盘择时信号]: ✅ **多头/趋势向上 (积极参与)**")
            self.content.append("> **分析**: 当前基准指数运行在 200 日均线上方，市场处于上升通道，动量策略胜率极高。")
            
        self.content.append("---\n")

    def add_competition_results(self, best_name, results_dict):
        self.content.append("## 🏆 一、 纯量价回测锦标赛 (样本外盲测版)")
        self.content.append("> **WFA步进验证**: 为了防止过度拟合，系统将回测数据切分为两段：")
        self.content.append("> 1. **样本内 (IS)**: 前 ~180 个交易日，用于策略逻辑历史优选。")
        self.content.append("> 2. **样本外 (OOS)**: **最后 60 个交易日 (约3个月)**，模拟实盘盲测。")
        self.content.append("只有在两段表现都稳健的策略才会被选为今日主推。")
        
        self.content.append("\n| 策略名称 | 历史收益(IS) | **最近收益(OOS)** | 最大回撤 | 稳定性评分 |")
        self.content.append("| :--- | :---: | :---: | :---: | :---: |")
        
        # 按照综合评分排序展示
        sorted_results = sorted(results_dict.items(), key=lambda x: x[1]['composite_score'], reverse=True)
        
        for name, metrics in sorted_results:
            is_ret = metrics['is_return']
            oos_ret = metrics['oos_return']
            dd = metrics['drawdown']
            score = metrics['composite_score']
            
            # 高亮 OOS 收益，如果是正的用粗体
            oos_str = f"**{oos_ret:+.2%}**" if oos_ret > 0 else f"{oos_ret:+.2%}"
            
            if name == best_name:
                self.content.append(f"| **👑 {name}** | {is_ret:.2%} | {oos_str} | {dd:.2%} | **{score:.2f}** |")
            else:
                self.content.append(f"| {name} | {is_ret:.2%} | {oos_str} | {dd:.2%} | {score:.2f} |")
        
        self.content.append(f"\n> **结论**: 综合长期逻辑与最近 60 天的市场适应性，**【{best_name}】** 展现了最强的逻辑稳定性。\n")

    def add_top_recommendations(self, strategy_name, recommendations):
        self.content.append(f"## 🎯 二、 今日主推精选池 (基于 {strategy_name})")
        self.content.append("> **风险排雷提示**: 系统仅给出技术形态最佳标的！买入前请务必核对 **PE(市盈率)**，若 PE 为负说明公司亏损，请人工剔除。")
        
        if not recommendations:
            self.content.append("今日无符合该严苛策略的股票。")
            return

        self.content.append("\n| 股票代码 | 股票名称 | 技术评分 | 当前价格 | ⚠️ PE (参考) | 建议买入区间 | 硬止损位 |")
        self.content.append("| :--- | :--- | :---: | :---: | :---: | :---: | :---: |")
        
        for item in recommendations[:10]:
            price = item['price']
            buy_zone = f"{price*0.995:.2f} - {price*1.005:.2f}" # 缩窄买入区间，更实盘
            stop_loss = f"{price*0.92:.2f} (-8%)" 
            
            pe_val = item['pe']
            pe_str = f"{pe_val:.2f}" if isinstance(pe_val, (int, float)) else str(pe_val)
            
            self.content.append(f"| `{item['symbol']}` | {item['name']} | {item['score']:.1f} | {price:.2f} | {pe_str} | {buy_zone} | {stop_loss} |")
        
        self.content.append("\n---\n")

    def add_other_strategies(self, all_recommendations):
        self.content.append("## 💡 三、 其他形态备选池 (Top 3)")
        self.content.append("供偏好其他技术形态的投资者参考：\n")
        
        for name, items in all_recommendations.items():
            self.content.append(f"### 形态：{name}")
            self.content.append("| 股票代码 | 股票名称 | 技术评分 | 当前价格 | PE参考 |")
            self.content.append("| :--- | :--- | :---: | :---: | :---: |")
            for item in items[:3]:
                pe_val = item['pe']
                pe_str = f"{pe_val:.2f}" if isinstance(pe_val, (int, float)) else str(pe_val)
                self.content.append(f"| `{item['symbol']}` | {item['name']} | {item['score']:.1f} | {item['price']:.2f} | {pe_str} |")
            self.content.append("\n")

    def save_report(self):
        full_text = "\n".join(self.content)
        with open(self.filename, 'w', encoding='utf-8') as f:
            f.write(full_text)
        logger.info(f"✅ 样本外验证报告已生成: {os.path.abspath(self.filename)}")
        return self.filename
