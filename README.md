# 🚀 Daily Stock Screener (实盘安全量化选股系统)

这是一个专为 A 股（沪深300）和美股（标普500）设计的每日量化选股与研报生成系统。本系统的核心特色是 **“纯量价逻辑”** 与 **“实盘防穿越架构”**，确保回测结果与实盘表现高度一致。

## 🌟 核心特性

- **零未来函数 (Anti-Look-ahead Bias)**：彻底剔除基本面因子参与回测打分，使用 500 天滑动窗口进行特征预热，杜绝任何形式的数据穿越。
- **纯量价打分引擎**：内置 5 大国际成熟量化流派（长牛趋势、超跌反转、低波稳健、综合量价、动量突破），每日通过“回测锦标赛”自动选出最契合当前盘面的策略。
- **百倍级性能优化**：采用 Pandas 向量化特征工程，所有技术指标（均线、RSI、动量、波动率等）在数据接入层一次性计算完成。
- **实盘仿真回测**：内置 0.1% 滑点与双边万三佣金，采用“次日开盘价”成交假设，真实模拟实盘交易摩擦。
- **自动化研报**：每日分析结束后自动生成 Markdown 研报，并通过邮件发送至指定邮箱，包含“技术面冠军策略”及“基本面风险排雷”参考。

## 🛠️ 快速开始

### 1. 环境初始化
```bash
# 创建并激活虚拟环境
python3 -m venv venv
source venv/bin/activate  # Windows 使用 venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 运行分析
```bash
# 分析美股全盘 (标普500) 并生成实盘建议
PYTHONPATH=src python3 src/daily_stock_screener/main.py --market US --real-trade

# 分析 A 股 (沪深300)
PYTHONPATH=src python3 src/daily_stock_screener/main.py --market A --real-trade
```

## 📊 策略说明
系统通过锦标赛实时评估以下策略的近期表现：
1. **Quality_Growth**: 寻找长周期高动量且低波动的标的。
2. **Value_Reversal**: 寻找均线负乖离且 RSI 超卖的超跌标的。
3. **Dividend_Yield**: 寻找极低波动率且处于均线上方的稳健标的。
4. **Multi_Factor_Alpha**: 动量、波动与趋势均衡配置的综合方案。
5. **Momentum_Breakout**: 寻找放量逼近 52 周新高的突破标的。

## ⚠️ 免责声明
本系统仅供量化研究与选股形态辅助参考，不构成任何投资建议。股市有风险，入市需谨慎。
