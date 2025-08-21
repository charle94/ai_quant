#!/usr/bin/env python3
"""
Rill Data集成模块 - 简化版本
"""
import json
import os
import csv
from datetime import datetime, timedelta
from pathlib import Path
import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RillDataIntegration:
    """Rill Data集成类"""
    
    def __init__(self, project_path: str = "/workspace/rill_project"):
        self.project_path = Path(project_path)
        self.data_path = self.project_path / "data"
        self.models_path = self.project_path / "models"
        self.dashboards_path = self.project_path / "dashboards"
        
        # 创建项目目录结构
        self._init_project_structure()
    
    def _init_project_structure(self):
        """初始化Rill项目结构"""
        logger.info("初始化Rill Data项目结构...")
        
        # 创建目录
        self.project_path.mkdir(exist_ok=True)
        self.data_path.mkdir(exist_ok=True)
        self.models_path.mkdir(exist_ok=True)
        self.dashboards_path.mkdir(exist_ok=True)
        
        # 创建rill.yaml配置文件
        self._create_rill_config()
        
        logger.info(f"Rill项目结构已创建: {self.project_path}")
    
    def _create_rill_config(self):
        """创建Rill配置文件"""
        config = {
            'compiler': {
                'duckdb_path': str(self.project_path / "stage.db")
            },
            'runtime': {
                'host': '0.0.0.0',
                'port': 9009,
                'instance_id': 'quant_analysis'
            },
            'connectors': [
                {
                    'name': 'performance_db',
                    'type': 'duckdb',
                    'config': {
                        'dsn': str(self.project_path / "performance.db")
                    }
                }
            ]
        }
        
        config_file = self.project_path / "rill.yaml"
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Rill配置文件已创建: {config_file}")
    
    def create_sample_data(self):
        """创建示例数据"""
        logger.info("创建示例数据...")
        
        # 1. 创建日收益率数据
        daily_returns_data = []
        base_date = datetime(2024, 1, 1)
        portfolio_value = 100000
        
        for i in range(60):
            date = base_date + timedelta(days=i)
            
            # 模拟日收益率
            import random
            daily_return = random.gauss(0.001, 0.02)
            portfolio_value *= (1 + daily_return)
            
            daily_returns_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'portfolio_value': round(portfolio_value, 2),
                'daily_return': round(daily_return, 6),
                'cumulative_return': round((portfolio_value - 100000) / 100000, 6),
                'benchmark_return': round(random.gauss(0.0003, 0.015), 6),
                'excess_return': round(daily_return - random.gauss(0.0003, 0.015), 6)
            })
        
        # 保存为CSV
        daily_returns_file = self.data_path / "daily_returns.csv"
        with open(daily_returns_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=daily_returns_data[0].keys())
            writer.writeheader()
            writer.writerows(daily_returns_data)
        
        logger.info(f"日收益率数据已创建: {daily_returns_file}")
        
        # 2. 创建交易记录数据
        trades_data = []
        for i in range(20):
            trade_date = base_date + timedelta(days=random.randint(0, 59))
            pnl = random.gauss(500, 1000)
            
            trades_data.append({
                'trade_id': f'trade_{i+1}',
                'timestamp': trade_date.strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': random.choice(['BTCUSDT', 'ETHUSDT']),
                'side': random.choice(['BUY', 'SELL']),
                'quantity': round(random.uniform(0.1, 1.0), 4),
                'price': round(random.uniform(40000, 50000), 2),
                'commission': round(random.uniform(10, 50), 2),
                'pnl': round(pnl, 2),
                'cumulative_pnl': round(sum(t['pnl'] for t in trades_data) + pnl, 2)
            })
        
        trades_file = self.data_path / "trades.csv"
        with open(trades_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=trades_data[0].keys())
            writer.writeheader()
            writer.writerows(trades_data)
        
        logger.info(f"交易记录已创建: {trades_file}")
        
        # 3. 创建绩效指标数据
        metrics_data = [
            {'metric_name': 'total_return', 'metric_value': 0.1738, 'category': 'returns'},
            {'metric_name': 'annualized_return', 'metric_value': 0.9599, 'category': 'returns'},
            {'metric_name': 'volatility', 'metric_value': 0.3585, 'category': 'risk'},
            {'metric_name': 'max_drawdown', 'metric_value': 0.2189, 'category': 'risk'},
            {'metric_name': 'sharpe_ratio', 'metric_value': 2.00, 'category': 'risk_adjusted'},
            {'metric_name': 'sortino_ratio', 'metric_value': 1.64, 'category': 'risk_adjusted'},
            {'metric_name': 'win_rate', 'metric_value': 0.45, 'category': 'trading'},
            {'metric_name': 'profit_factor', 'metric_value': 1.66, 'category': 'trading'},
            {'metric_name': 'total_trades', 'metric_value': 20, 'category': 'trading'},
            {'metric_name': 'var_95', 'metric_value': 0.05, 'category': 'risk'},
        ]
        
        for metric in metrics_data:
            metric['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        metrics_file = self.data_path / "performance_metrics.csv"
        with open(metrics_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=metrics_data[0].keys())
            writer.writeheader()
            writer.writerows(metrics_data)
        
        logger.info(f"绩效指标已创建: {metrics_file}")
        
        return {
            'daily_returns': str(daily_returns_file),
            'trades': str(trades_file),
            'metrics': str(metrics_file)
        }
    
    def create_rill_models(self):
        """创建Rill数据模型"""
        logger.info("创建Rill数据模型...")
        
        # 1. 日收益率模型
        daily_returns_model = """
-- Daily Returns Model
-- @materialize: true

SELECT 
    CAST(date AS DATE) as date,
    portfolio_value,
    daily_return,
    cumulative_return,
    benchmark_return,
    excess_return,
    -- 计算移动平均
    AVG(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as ma_7_return,
    AVG(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30_return,
    -- 计算滚动波动率
    STDDEV(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_volatility_30d,
    -- 计算滚动夏普比率
    AVG(excess_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) / NULLIF(STDDEV(excess_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 0) as rolling_sharpe_30d
FROM read_csv_auto('data/daily_returns.csv')
ORDER BY date
"""
        
        model_file = self.models_path / "daily_returns.sql"
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(daily_returns_model)
        
        # 2. 交易分析模型
        trades_model = """
-- Trades Analysis Model
-- @materialize: true

WITH trade_stats AS (
    SELECT 
        *,
        CAST(timestamp AS TIMESTAMP) as trade_timestamp,
        -- 按月分组
        DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) as trade_month,
        -- 按周分组
        DATE_TRUNC('week', CAST(timestamp AS TIMESTAMP)) as trade_week,
        -- 盈亏分类
        CASE 
            WHEN pnl > 0 THEN 'Win'
            WHEN pnl < 0 THEN 'Loss'
            ELSE 'Breakeven'
        END as trade_result,
        -- 交易规模分类
        CASE 
            WHEN ABS(pnl) > 1000 THEN 'Large'
            WHEN ABS(pnl) > 500 THEN 'Medium'
            ELSE 'Small'
        END as trade_size
    FROM read_csv_auto('data/trades.csv')
)

SELECT 
    *,
    -- 计算累计统计
    ROW_NUMBER() OVER (ORDER BY trade_timestamp) as trade_number,
    AVG(pnl) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as avg_pnl_cumulative,
    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as wins_cumulative,
    COUNT(*) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as total_trades_cumulative,
    -- 胜率
    CAST(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) AS FLOAT) / 
    COUNT(*) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as win_rate_cumulative
FROM trade_stats
ORDER BY trade_timestamp
"""
        
        model_file = self.models_path / "trades_analysis.sql"
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(trades_model)
        
        # 3. 绩效指标模型
        metrics_model = """
-- Performance Metrics Model
-- @materialize: true

SELECT 
    metric_name,
    metric_value,
    category,
    CAST(updated_at AS TIMESTAMP) as updated_at,
    -- 添加格式化显示
    CASE 
        WHEN metric_name IN ('total_return', 'annualized_return', 'volatility', 'max_drawdown', 'win_rate', 'var_95') 
        THEN CONCAT(ROUND(metric_value * 100, 2), '%')
        ELSE ROUND(metric_value, 2)::VARCHAR
    END as formatted_value,
    -- 添加基准比较
    CASE 
        WHEN metric_name = 'sharpe_ratio' AND metric_value > 1 THEN 'Good'
        WHEN metric_name = 'sharpe_ratio' AND metric_value > 0.5 THEN 'Fair'
        WHEN metric_name = 'sharpe_ratio' THEN 'Poor'
        WHEN metric_name = 'win_rate' AND metric_value > 0.6 THEN 'Excellent'
        WHEN metric_name = 'win_rate' AND metric_value > 0.5 THEN 'Good'
        WHEN metric_name = 'win_rate' THEN 'Needs Improvement'
        WHEN metric_name = 'max_drawdown' AND metric_value < 0.1 THEN 'Low Risk'
        WHEN metric_name = 'max_drawdown' AND metric_value < 0.2 THEN 'Medium Risk'
        WHEN metric_name = 'max_drawdown' THEN 'High Risk'
        ELSE 'Neutral'
    END as performance_grade
FROM read_csv_auto('data/performance_metrics.csv')
"""
        
        model_file = self.models_path / "performance_metrics.sql"
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(metrics_model)
        
        logger.info("Rill数据模型已创建")
    
    def create_rill_dashboards(self):
        """创建Rill看板"""
        logger.info("创建Rill看板...")
        
        # 1. 主看板配置
        main_dashboard = {
            'kind': 'rill.runtime.v1.Dashboard',
            'metadata': {
                'name': 'quant-performance-overview'
            },
            'spec': {
                'title': '量化策略绩效概览',
                'model': 'daily_returns',
                'default_time_range': 'P30D',
                'available_time_ranges': [
                    {'range': 'P7D', 'label': '最近7天'},
                    {'range': 'P30D', 'label': '最近30天'},
                    {'range': 'P90D', 'label': '最近90天'}
                ],
                'time_column': 'date',
                'measures': [
                    {
                        'name': 'portfolio_value',
                        'label': '组合价值',
                        'expression': 'portfolio_value',
                        'format_preset': 'currency_usd'
                    },
                    {
                        'name': 'daily_return',
                        'label': '日收益率',
                        'expression': 'daily_return',
                        'format_preset': 'percentage_2'
                    },
                    {
                        'name': 'cumulative_return',
                        'label': '累计收益率',
                        'expression': 'cumulative_return',
                        'format_preset': 'percentage_2'
                    },
                    {
                        'name': 'rolling_sharpe_30d',
                        'label': '30日滚动夏普比率',
                        'expression': 'rolling_sharpe_30d',
                        'format_preset': 'number_2'
                    }
                ],
                'dimensions': [
                    {
                        'name': 'date',
                        'label': '日期',
                        'expression': 'date'
                    }
                ]
            }
        }
        
        dashboard_file = self.dashboards_path / "main_dashboard.yaml"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            yaml.dump(main_dashboard, f, default_flow_style=False, allow_unicode=True)
        
        # 2. 交易分析看板
        trading_dashboard = {
            'kind': 'rill.runtime.v1.Dashboard',
            'metadata': {
                'name': 'trading-analysis'
            },
            'spec': {
                'title': '交易分析看板',
                'model': 'trades_analysis',
                'default_time_range': 'P30D',
                'time_column': 'trade_timestamp',
                'measures': [
                    {
                        'name': 'pnl',
                        'label': '盈亏',
                        'expression': 'pnl',
                        'format_preset': 'currency_usd'
                    },
                    {
                        'name': 'cumulative_pnl',
                        'label': '累计盈亏',
                        'expression': 'cumulative_pnl',
                        'format_preset': 'currency_usd'
                    },
                    {
                        'name': 'win_rate_cumulative',
                        'label': '累计胜率',
                        'expression': 'win_rate_cumulative',
                        'format_preset': 'percentage_2'
                    },
                    {
                        'name': 'trade_count',
                        'label': '交易次数',
                        'expression': 'COUNT(*)',
                        'format_preset': 'number_0'
                    }
                ],
                'dimensions': [
                    {
                        'name': 'symbol',
                        'label': '交易对',
                        'expression': 'symbol'
                    },
                    {
                        'name': 'side',
                        'label': '交易方向',
                        'expression': 'side'
                    },
                    {
                        'name': 'trade_result',
                        'label': '交易结果',
                        'expression': 'trade_result'
                    }
                ]
            }
        }
        
        dashboard_file = self.dashboards_path / "trading_dashboard.yaml"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            yaml.dump(trading_dashboard, f, default_flow_style=False, allow_unicode=True)
        
        logger.info("Rill看板配置已创建")
    
    def create_startup_script(self):
        """创建Rill启动脚本"""
        startup_script = f"""#!/bin/bash

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
cd {self.project_path}

# 启动Rill开发服务器
echo "📊 启动Rill看板，访问地址: http://localhost:9009"
echo "🔄 按 Ctrl+C 停止服务"
echo ""
echo "可用看板:"
echo "  • 量化策略绩效概览: http://localhost:9009/dashboard/quant-performance-overview"
echo "  • 交易分析看板: http://localhost:9009/dashboard/trading-analysis"
echo ""

rill start --verbose
"""
        
        script_file = self.project_path / "start_rill.sh"
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write(startup_script)
        
        # 设置执行权限
        os.chmod(script_file, 0o755)
        
        logger.info(f"Rill启动脚本已创建: {script_file}")
    
    def create_readme(self):
        """创建README文档"""
        readme_content = f"""# Rill Data 量化分析看板

## 项目概述

本项目使用 Rill Data 为量化分析系统创建交互式数据看板，提供实时的绩效分析和可视化。

## 项目结构

```
{self.project_path.name}/
├── rill.yaml              # Rill 配置文件
├── start_rill.sh          # 启动脚本
├── data/                  # 数据文件
│   ├── daily_returns.csv  # 日收益率数据
│   ├── trades.csv         # 交易记录
│   └── performance_metrics.csv  # 绩效指标
├── models/                # 数据模型
│   ├── daily_returns.sql  # 日收益率模型
│   ├── trades_analysis.sql # 交易分析模型
│   └── performance_metrics.sql # 绩效指标模型
└── dashboards/            # 看板配置
    ├── main_dashboard.yaml     # 主看板
    └── trading_dashboard.yaml  # 交易看板
```

## 快速开始

### 1. 安装 Rill Data

**macOS (使用 Homebrew):**
```bash
brew install rilldata/tap/rill
```

**Linux/macOS (使用安装脚本):**
```bash
curl -s https://cdn.rilldata.com/install.sh | bash
```

### 2. 启动看板

```bash
cd {self.project_path}
./start_rill.sh
```

### 3. 访问看板

打开浏览器访问: http://localhost:9009

## 可用看板

### 📈 量化策略绩效概览
- **URL**: http://localhost:9009/dashboard/quant-performance-overview
- **功能**: 
  - 组合价值趋势
  - 日收益率分析
  - 累计收益率跟踪
  - 滚动夏普比率

### 💰 交易分析看板
- **URL**: http://localhost:9009/dashboard/trading-analysis
- **功能**:
  - 交易盈亏分析
  - 累计盈亏趋势
  - 胜率统计
  - 交易对比较

## 数据模型

### daily_returns
- 日收益率数据和衍生指标
- 包含移动平均、滚动波动率等

### trades_analysis
- 交易记录和分析
- 包含累计统计和分类

### performance_metrics
- 绩效指标汇总
- 包含格式化显示和评级

## 自定义配置

### 修改数据源
编辑 `rill.yaml` 中的连接器配置:

```yaml
connectors:
  - name: performance_db
    type: duckdb
    config:
      dsn: path/to/your/database.db
```

### 添加新看板
1. 在 `dashboards/` 目录下创建新的 YAML 文件
2. 定义看板的措施和维度
3. 重启 Rill 服务

### 修改数据模型
1. 编辑 `models/` 目录下的 SQL 文件
2. 使用 DuckDB SQL 语法
3. 重启 Rill 服务以应用更改

## 故障排除

### 常见问题

1. **端口冲突**
   - 修改 `rill.yaml` 中的端口设置
   - 或停止占用 9009 端口的其他服务

2. **数据加载失败**
   - 检查 CSV 文件格式
   - 确认文件路径正确
   - 查看 Rill 日志输出

3. **看板显示异常**
   - 检查 YAML 配置语法
   - 确认模型中的字段名称
   - 重启服务

### 查看日志
```bash
rill start --verbose
```

## 更多资源

- [Rill Data 官方文档](https://docs.rilldata.com/)
- [DuckDB SQL 参考](https://duckdb.org/docs/sql/introduction)
- [YAML 语法指南](https://yaml.org/spec/1.2/spec.html)
"""
        
        readme_file = self.project_path / "README.md"
        with open(readme_file, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        
        logger.info(f"README文档已创建: {readme_file}")
    
    def setup_complete_project(self):
        """设置完整的Rill项目"""
        logger.info("设置完整的Rill Data项目...")
        
        try:
            # 1. 创建示例数据
            data_files = self.create_sample_data()
            
            # 2. 创建数据模型
            self.create_rill_models()
            
            # 3. 创建看板
            self.create_rill_dashboards()
            
            # 4. 创建启动脚本
            self.create_startup_script()
            
            # 5. 创建文档
            self.create_readme()
            
            logger.info("✅ Rill Data项目设置完成！")
            logger.info(f"📁 项目路径: {self.project_path}")
            logger.info("🚀 启动步骤:")
            logger.info("   1. 安装 Rill Data")
            logger.info(f"   2. cd {self.project_path}")
            logger.info("   3. ./start_rill.sh")
            logger.info("   4. 访问 http://localhost:9009")
            
            return True
            
        except Exception as e:
            logger.error(f"设置项目时出错: {e}")
            return False

def main():
    """主函数"""
    print("🧪 开始 Rill Data 集成...")
    
    # 创建 Rill 集成实例
    rill_integration = RillDataIntegration()
    
    # 设置完整项目
    success = rill_integration.setup_complete_project()
    
    if success:
        print("\n🎉 Rill Data 集成成功！")
        print(f"📁 项目路径: {rill_integration.project_path}")
        print("\n🚀 下一步操作:")
        print("1. 安装 Rill Data:")
        print("   # macOS")
        print("   brew install rilldata/tap/rill")
        print("   # 或使用安装脚本")
        print("   curl -s https://cdn.rilldata.com/install.sh | bash")
        print("")
        print("2. 启动看板:")
        print(f"   cd {rill_integration.project_path}")
        print("   ./start_rill.sh")
        print("")
        print("3. 访问看板:")
        print("   🌐 主页: http://localhost:9009")
        print("   📈 绩效概览: http://localhost:9009/dashboard/quant-performance-overview")
        print("   💰 交易分析: http://localhost:9009/dashboard/trading-analysis")
        print("")
        print("📚 查看详细说明:")
        print(f"   cat {rill_integration.project_path}/README.md")
    else:
        print("❌ Rill Data 集成失败")
        return False
    
    return True

if __name__ == "__main__":
    main()