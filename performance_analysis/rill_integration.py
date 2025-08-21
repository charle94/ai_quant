#!/usr/bin/env python3
"""
Rill Data集成模块 - 将绩效分析数据导入到Rill Data看板
"""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging
import pandas as pd
import yaml
from typing import Dict, List, Optional

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from performance_analysis.performance_analyzer import PerformanceAnalyzer, create_sample_performance_data

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
    
    def export_performance_data(self, analyzer: PerformanceAnalyzer) -> Dict[str, str]:
        """导出绩效数据到Rill格式"""
        logger.info("导出绩效数据到Rill格式...")
        
        exported_files = {}
        
        try:
            # 1. 导出日收益率数据
            if analyzer.daily_returns:
                daily_returns_df = pd.DataFrame([
                    {
                        'date': dr.date,
                        'portfolio_value': dr.portfolio_value,
                        'daily_return': dr.daily_return,
                        'cumulative_return': dr.cumulative_return,
                        'benchmark_return': dr.benchmark_return,
                        'excess_return': dr.excess_return
                    }
                    for dr in analyzer.daily_returns
                ])
                
                daily_returns_file = self.data_path / "daily_returns.parquet"
                daily_returns_df.to_parquet(daily_returns_file, index=False)
                exported_files['daily_returns'] = str(daily_returns_file)
                logger.info(f"日收益率数据已导出: {daily_returns_file}")
            
            # 2. 导出交易记录
            if analyzer.trades:
                trades_df = pd.DataFrame([
                    {
                        'trade_id': trade.id,
                        'timestamp': trade.timestamp,
                        'symbol': trade.symbol,
                        'side': trade.side,
                        'quantity': trade.quantity,
                        'price': trade.price,
                        'commission': trade.commission,
                        'pnl': trade.pnl,
                        'cumulative_pnl': trade.cumulative_pnl
                    }
                    for trade in analyzer.trades
                ])
                
                trades_file = self.data_path / "trades.parquet"
                trades_df.to_parquet(trades_file, index=False)
                exported_files['trades'] = str(trades_file)
                logger.info(f"交易记录已导出: {trades_file}")
            
            # 3. 导出绩效指标
            metrics = analyzer.calculate_performance_metrics()
            metrics_data = {
                'metric_name': [
                    'total_return', 'annualized_return', 'volatility', 'max_drawdown',
                    'sharpe_ratio', 'sortino_ratio', 'calmar_ratio', 'win_rate',
                    'total_trades', 'profit_factor', 'var_95', 'cvar_95'
                ],
                'metric_value': [
                    metrics.total_return, metrics.annualized_return, metrics.volatility,
                    metrics.max_drawdown, metrics.sharpe_ratio, metrics.sortino_ratio,
                    metrics.calmar_ratio, metrics.win_rate, metrics.total_trades,
                    metrics.profit_factor, metrics.var_95, metrics.cvar_95
                ],
                'category': [
                    'returns', 'returns', 'risk', 'risk',
                    'risk_adjusted', 'risk_adjusted', 'risk_adjusted', 'trading',
                    'trading', 'trading', 'risk', 'risk'
                ],
                'updated_at': [datetime.now()] * 12
            }
            
            metrics_df = pd.DataFrame(metrics_data)
            metrics_file = self.data_path / "performance_metrics.parquet"
            metrics_df.to_parquet(metrics_file, index=False)
            exported_files['metrics'] = str(metrics_file)
            logger.info(f"绩效指标已导出: {metrics_file}")
            
            # 4. 导出回撤数据
            drawdowns = analyzer.calculate_drawdown_periods()
            if drawdowns:
                drawdown_data = []
                for i, dd in enumerate(drawdowns):
                    drawdown_data.append({
                        'drawdown_id': i + 1,
                        'start_date': dd.start_date,
                        'end_date': dd.end_date,
                        'peak_value': dd.peak_value,
                        'trough_value': dd.trough_value,
                        'drawdown_pct': dd.drawdown_pct,
                        'duration_days': dd.duration_days,
                        'recovered': dd.recovery_date is not None,
                        'recovery_date': dd.recovery_date
                    })
                
                drawdowns_df = pd.DataFrame(drawdown_data)
                drawdowns_file = self.data_path / "drawdowns.parquet"
                drawdowns_df.to_parquet(drawdowns_file, index=False)
                exported_files['drawdowns'] = str(drawdowns_file)
                logger.info(f"回撤数据已导出: {drawdowns_file}")
            
            return exported_files
            
        except Exception as e:
            logger.error(f"导出数据时出错: {e}")
            return {}
    
    def create_rill_models(self):
        """创建Rill数据模型"""
        logger.info("创建Rill数据模型...")
        
        # 1. 日收益率模型
        daily_returns_model = """
-- Daily Returns Model
-- @materialize: true

SELECT 
    date,
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
FROM read_parquet('data/daily_returns.parquet')
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
        -- 按月分组
        DATE_TRUNC('month', timestamp) as trade_month,
        -- 按周分组
        DATE_TRUNC('week', timestamp) as trade_week,
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
    FROM read_parquet('data/trades.parquet')
)

SELECT 
    *,
    -- 计算累计统计
    COUNT(*) OVER (ORDER BY timestamp) as trade_number,
    AVG(pnl) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) as avg_pnl_cumulative,
    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) as wins_cumulative,
    COUNT(*) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) as total_trades_cumulative,
    -- 胜率
    CAST(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) AS FLOAT) / 
    COUNT(*) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING) as win_rate_cumulative
FROM trade_stats
ORDER BY timestamp
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
    updated_at,
    -- 添加格式化显示
    CASE 
        WHEN metric_name IN ('total_return', 'annualized_return', 'volatility', 'max_drawdown', 'win_rate', 'var_95', 'cvar_95') 
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
FROM read_parquet('data/performance_metrics.parquet')
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
                'name': 'quant_performance_overview'
            },
            'spec': {
                'title': '量化策略绩效概览',
                'model': 'daily_returns',
                'default_time_range': 'P30D',
                'available_time_ranges': [
                    {'range': 'P7D', 'label': '最近7天'},
                    {'range': 'P30D', 'label': '最近30天'},
                    {'range': 'P90D', 'label': '最近90天'},
                    {'range': 'P1Y', 'label': '最近1年'}
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
                        'expression': 'date',
                        'format_preset': 'date'
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
                'name': 'trading_analysis'
            },
            'spec': {
                'title': '交易分析看板',
                'model': 'trades_analysis',
                'default_time_range': 'P30D',
                'time_column': 'timestamp',
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
                    },
                    {
                        'name': 'trade_size',
                        'label': '交易规模',
                        'expression': 'trade_size'
                    }
                ]
            }
        }
        
        dashboard_file = self.dashboards_path / "trading_dashboard.yaml"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            yaml.dump(trading_dashboard, f, default_flow_style=False, allow_unicode=True)
        
        # 3. 风险分析看板
        risk_dashboard = {
            'kind': 'rill.runtime.v1.Dashboard',
            'metadata': {
                'name': 'risk_analysis'
            },
            'spec': {
                'title': '风险分析看板',
                'model': 'performance_metrics',
                'measures': [
                    {
                        'name': 'metric_value',
                        'label': '指标值',
                        'expression': 'metric_value'
                    }
                ],
                'dimensions': [
                    {
                        'name': 'metric_name',
                        'label': '指标名称',
                        'expression': 'metric_name'
                    },
                    {
                        'name': 'category',
                        'label': '指标类别',
                        'expression': 'category'
                    },
                    {
                        'name': 'performance_grade',
                        'label': '表现评级',
                        'expression': 'performance_grade'
                    }
                ]
            }
        }
        
        dashboard_file = self.dashboards_path / "risk_dashboard.yaml"
        with open(dashboard_file, 'w', encoding='utf-8') as f:
            yaml.dump(risk_dashboard, f, default_flow_style=False, allow_unicode=True)
        
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
        brew install rilldata/tap/rill
    else
        # Linux
        curl -s https://cdn.rilldata.com/install.sh | bash
    fi
fi

# 进入项目目录
cd {self.project_path}

# 启动Rill开发服务器
echo "📊 启动Rill看板，访问地址: http://localhost:9009"
echo "🔄 按 Ctrl+C 停止服务"

rill start --verbose
"""
        
        script_file = self.project_path / "start_rill.sh"
        with open(script_file, 'w', encoding='utf-8') as f:
            f.write(startup_script)
        
        # 设置执行权限
        os.chmod(script_file, 0o755)
        
        logger.info(f"Rill启动脚本已创建: {script_file}")
    
    def integrate_with_performance_analyzer(self, analyzer: PerformanceAnalyzer):
        """与绩效分析器集成"""
        logger.info("开始集成绩效分析数据到Rill Data...")
        
        try:
            # 1. 导出数据
            exported_files = self.export_performance_data(analyzer)
            
            if not exported_files:
                logger.error("数据导出失败")
                return False
            
            # 2. 创建模型
            self.create_rill_models()
            
            # 3. 创建看板
            self.create_rill_dashboards()
            
            # 4. 创建启动脚本
            self.create_startup_script()
            
            logger.info("Rill Data集成完成！")
            logger.info(f"项目路径: {self.project_path}")
            logger.info(f"启动命令: {self.project_path}/start_rill.sh")
            logger.info("看板访问地址: http://localhost:9009")
            
            return True
            
        except Exception as e:
            logger.error(f"集成过程中出错: {e}")
            return False
    
    def update_data(self, analyzer: PerformanceAnalyzer):
        """更新Rill Data中的数据"""
        logger.info("更新Rill Data数据...")
        
        try:
            # 重新导出数据
            exported_files = self.export_performance_data(analyzer)
            
            if exported_files:
                logger.info("数据更新成功")
                return True
            else:
                logger.error("数据更新失败")
                return False
                
        except Exception as e:
            logger.error(f"更新数据时出错: {e}")
            return False

def main():
    """测试Rill Data集成"""
    print("🧪 开始Rill Data集成测试...")
    
    # 创建示例绩效数据
    analyzer = create_sample_performance_data()
    print(f"📊 创建了 {len(analyzer.daily_returns)} 天的绩效数据")
    print(f"📊 创建了 {len(analyzer.trades)} 笔交易记录")
    
    # 创建Rill集成实例
    rill_integration = RillDataIntegration()
    
    # 执行集成
    success = rill_integration.integrate_with_performance_analyzer(analyzer)
    
    if success:
        print("\n🎉 Rill Data集成成功！")
        print(f"📁 项目路径: {rill_integration.project_path}")
        print("🚀 启动步骤:")
        print("   1. 安装Rill Data (如果尚未安装)")
        print("   2. 运行启动脚本:")
        print(f"      cd {rill_integration.project_path}")
        print("      ./start_rill.sh")
        print("   3. 访问看板: http://localhost:9009")
        print("\n📊 可用看板:")
        print("   • 量化策略绩效概览")
        print("   • 交易分析看板")
        print("   • 风险分析看板")
    else:
        print("❌ Rill Data集成失败")
    
    return success

if __name__ == "__main__":
    main()