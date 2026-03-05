#!/bin/bash
# 自动生成的量化选股运行脚本
PROJECT_DIR="/Users/wangzixin/coding/python-ai/daily_stock_screener"
LOG_FILE="$PROJECT_DIR/cron_us_run.log"

echo "--------------------------------------------------" >> $LOG_FILE
echo "执行时间: $(date)" >> $LOG_FILE

cd $PROJECT_DIR

# 使用虚拟环境执行美股实盘选股
./venv/bin/python src/daily_stock_screener/main.py --market US --real-trade >> $LOG_FILE 2>&1

echo "执行完毕: $(date)" >> $LOG_FILE
