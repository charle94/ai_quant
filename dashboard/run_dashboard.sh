#!/bin/bash

# 启动量化分析看板

echo "🚀 启动量化分析系统看板..."

# 检查是否安装了streamlit
if ! command -v streamlit &> /dev/null; then
    echo "⚠️  Streamlit未安装，正在安装..."
    pip install streamlit plotly pandas numpy
fi

# 设置环境变量
export STREAMLIT_SERVER_PORT=8501
export STREAMLIT_SERVER_ADDRESS=0.0.0.0

# 启动看板
echo "📊 看板将在 http://localhost:8501 启动"
echo "🔄 按 Ctrl+C 停止服务"

streamlit run dashboard_app.py --server.port 8501 --server.address 0.0.0.0