# 增强版回测系统

## 概述

这是一个集成了RuleGo决策引擎和Feast特征存储的增强版回测系统，支持离线历史数据回测。

## 主要特性

### 🚀 核心功能
- **RuleGo集成**: 使用RuleGo规则引擎进行交易决策
- **Feast集成**: 从Feast特征存储获取历史特征数据
- **离线回测**: 支持完整的离线历史数据回测
- **风险管理**: 内置止损、止盈、仓位控制等风险管理功能
- **详细统计**: 提供完整的回测统计分析

### 📊 支持的特征
- 技术指标：MA、RSI、布林带、动量指标等
- 价格特征：开高低收、成交量等
- Alpha101因子：支持WorldQuant Alpha101因子库
- 自定义特征：支持用户自定义特征

### 🛡️ 风险控制
- 止损止盈：可配置的止损止盈比例
- 仓位控制：最大持仓比例限制
- 信号过滤：基于置信度的信号过滤
- 滑点模拟：真实的交易成本模拟

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Feast Store   │    │   RuleGo Engine │    │  Backtest Engine│
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Historical  │ │    │ │ Trading     │ │    │ │ Portfolio   │ │
│ │ Features    │ │────▶│ │ Rules       │ │────▶│ │ Management  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Alpha101    │ │    │ │ Signal      │ │    │ │ Risk        │ │
│ │ Factors     │ │    │ │ Generation  │ │    │ │ Management  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 快速开始

### 1. 环境准备

确保已安装所需依赖：

```bash
# 安装Python依赖
pip install feast pandas numpy requests

# 启动RuleGo服务（可选，可使用模拟模式）
cd decision_engine
go run main.go
```

### 2. 配置Feast

确保Feast特征仓库已正确配置：

```bash
cd feast_config/feature_repo
feast apply
```

### 3. 运行回测

#### 使用命令行工具

```bash
# 基本回测
python backtest/run_backtest.py \
  --trading-pairs BTCUSDT ETHUSDT \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --initial-capital 100000

# 使用模拟模式（不需要RuleGo服务）
python backtest/run_backtest.py \
  --trading-pairs BTCUSDT \
  --start-date 2024-01-01 \
  --end-date 2024-01-07 \
  --use-mock \
  --output results.json
```

#### 使用Python API

```python
from datetime import datetime
from backtest.enhanced_backtest_engine import EnhancedBacktestEngine, EnhancedBacktestConfig

# 创建配置
config = EnhancedBacktestConfig(
    initial_capital=100000,
    use_mock_rulego=True,  # 使用模拟模式
    min_confidence=0.3
)

# 创建引擎
engine = EnhancedBacktestEngine(config)

# 运行回测
result, stats = engine.run_enhanced_backtest(
    trading_pairs=["BTCUSDT", "ETHUSDT"],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31)
)

# 查看结果
print(f"总收益率: {result.total_return:.2%}")
print(f"夏普比率: {result.sharpe_ratio:.2f}")
```

### 4. 查看结果

回测结果包含以下信息：

- **基础指标**: 收益率、夏普比率、最大回撤等
- **交易统计**: 交易次数、胜率、平均收益等  
- **信号分析**: 信号分布、置信度统计等
- **风险指标**: 波动率、VaR、最大连续亏损等

## 配置说明

### 回测配置 (EnhancedBacktestConfig)

```python
config = EnhancedBacktestConfig(
    # 基础配置
    initial_capital=100000.0,      # 初始资金
    commission_rate=0.001,         # 手续费率
    slippage_rate=0.0005,         # 滑点率
    position_size=0.1,            # 单次交易资金比例
    
    # 风控配置
    stop_loss_pct=0.05,           # 止损比例
    take_profit_pct=0.1,          # 止盈比例
    min_confidence=0.3,           # 最小信号置信度
    
    # 系统配置
    feast_repo_path="/path/to/feast/repo",
    rulego_endpoint="http://localhost:8080",
    use_mock_rulego=False         # 是否使用模拟模式
)
```

### YAML配置文件

也可以使用YAML配置文件：

```yaml
# config/backtest_config.yaml
backtest:
  initial_capital: 100000.0
  commission_rate: 0.001
  position_size: 0.1

risk_management:
  stop_loss_pct: 0.05
  take_profit_pct: 0.1

feast:
  repo_path: "/workspace/feast_config/feature_repo"
  
rulego:
  endpoint: "http://localhost:8080"
  use_mock: false
```

## 测试

运行测试套件：

```bash
# 运行所有测试
python tests/test_enhanced_backtest.py

# 运行特定测试
python -m unittest tests.test_enhanced_backtest.TestEnhancedBacktest.test_full_backtest_workflow
```

## 模拟模式

如果RuleGo服务不可用，可以使用模拟模式：

```python
config = EnhancedBacktestConfig(use_mock_rulego=True)
```

模拟模式使用简单的RSI策略：
- RSI < 30: 买入信号
- RSI > 70: 卖出信号
- 其他: 持有信号

## 故障排除

### 常见问题

1. **Feast连接失败**
   - 检查特征仓库路径是否正确
   - 确保已运行 `feast apply`
   - 检查数据库文件权限

2. **RuleGo连接失败**
   - 确保RuleGo服务正在运行
   - 检查端点URL是否正确
   - 可以使用 `--use-mock` 参数启用模拟模式

3. **特征数据为空**
   - 检查时间范围是否有数据
   - 确认交易对名称正确
   - 检查Feast特征定义

4. **内存不足**
   - 减少时间范围或交易对数量
   - 调整批处理大小
   - 使用更大的机器

### 日志调试

启用详细日志：

```bash
python backtest/run_backtest.py --log-level DEBUG
```

或在Python代码中：

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 扩展开发

### 添加新的特征

1. 在Feast中定义新特征：

```python
# feast_config/feature_repo/features.py
new_feature_fv = FeatureView(
    name="new_features",
    entities=[trading_pair],
    schema=[
        Field(name="custom_indicator", dtype=Float64),
    ],
    source=offline_features_source,
)
```

2. 在RuleGo适配器中使用：

```python
# backtest/rulego_backtest_adapter.py
def convert_to_rulego_message(self, row):
    message = RuleGoMessage(
        # ... existing fields ...
        custom_indicator=float(row.get('new_features__custom_indicator', 0.0))
    )
```

### 自定义策略

可以通过修改RuleGo规则或实现新的适配器来添加自定义策略。

## 性能优化

- 使用批处理减少API调用
- 缓存特征数据避免重复查询
- 并行处理多个交易对
- 使用更快的特征存储后端

## 许可证

本项目采用 MIT 许可证。