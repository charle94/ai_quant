#!/bin/bash

# Rill Data 启动脚本

echo "🚀 启动Rill Data看板服务..."

# 检查Rill是否已安装
if ! command -v rill &> /dev/null; then
    echo "⚠️  Rill未安装，正在安装..."
    
    # 安装Rill (根据系统选择合适的安装方法)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install rilldata/tap/rill
        else
            curl -s https://cdn.rilldata.com/install.sh | bash
        fi
    else
        # Linux
        curl -s https://cdn.rilldata.com/install.sh | bash
    fi
    
    # 添加到PATH
    export PATH="$HOME/.local/bin:$PATH"
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
fi

# 进入项目目录
cd /workspace/rill_project

# 启动Rill开发服务器
echo "📊 启动Rill看板，访问地址: http://localhost:9009"
echo "🔄 按 Ctrl+C 停止服务"
echo ""
echo "可用看板:"
echo "  • 量化策略绩效概览: http://localhost:9009/dashboard/quant-performance-overview"
echo "  • 交易分析看板: http://localhost:9009/dashboard/trading-analysis"
echo ""

rill start --verbose
