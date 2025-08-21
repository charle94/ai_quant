# Alpha 101 因子库使用指南

## 📋 概述

本项目实现了完整的WorldQuant Alpha 101因子库，包含101个经典量化因子，使用DBT宏进行模块化实现，支持DuckDB高性能计算。

## 🏗️ 架构设计

```
Alpha 101 因子系统架构
├── DBT宏层 (macros/alpha101/)
│   ├── base_operators.sql           # 基础操作符 (DELAY, DELTA, RANK等)
│   ├── alpha_factors.sql           # Alpha 001-020
│   ├── alpha_factors_21_50.sql     # Alpha 021-050  
│   ├── alpha_factors_51_75.sql     # Alpha 051-075
│   └── alpha_factors_76_101.sql    # Alpha 076-101
├── DBT模型层 (models/alpha101/)
│   ├── alpha_base_data.sql         # 基础数据预处理
│   ├── alpha_factors_001_020.sql   # 因子计算 001-020
│   ├── alpha_factors_021_050.sql   # 因子计算 021-050
│   ├── alpha_factors_051_075.sql   # 因子计算 051-075
│   ├── alpha_factors_076_101.sql   # 因子计算 076-101
│   └── alpha101_complete.sql       # 完整因子汇总
└── Feast集成层 (feast_config/)
    ├── alpha101_complete_features.py # Feast特征定义
    └── alpha101_pusher.py           # 因子推送器
```

## 🔧 核心组件

### 1. 基础操作符宏

**位置**: `dbt_project/macros/alpha101/base_operators.sql`

实现了Alpha 101因子计算所需的所有基础操作符：

#### 时间序列操作符
- `delay(column, periods)` - 获取滞后值
- `delta(column, periods)` - 计算差值
- `ts_sum(column, periods)` - 滚动求和
- `ts_mean(column, periods)` - 滚动均值
- `ts_std(column, periods)` - 滚动标准差
- `ts_min/ts_max(column, periods)` - 滚动最值
- `ts_argmin/ts_argmax(column, periods)` - 最值位置

#### 截面操作符
- `rank(column)` - 截面排序
- `scale(column)` - 标准化

#### 数学操作符
- `sign(column)` - 符号函数
- `abs_value(column)` - 绝对值
- `log_value(column)` - 对数函数
- `signed_power(base, exponent)` - 带符号幂函数

#### 高级操作符
- `ts_corr(x, y, periods)` - 滚动相关系数
- `ts_cov(x, y, periods)` - 滚动协方差
- `decay_linear(column, periods)` - 线性衰减加权
- `ts_rank(column, periods)` - 时序排序

### 2. Alpha因子实现

#### Alpha 001-020 (经典基础因子)
```sql
-- 示例：Alpha001 - 价格反转动量因子
-- Alpha001 = RANK(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5
{{ rank(ts_argmax('CASE WHEN returns < 0 THEN stddev_returns_20 ELSE close END', 5)) }} - 0.5
```

#### Alpha 021-050 (量价关系因子)
```sql
-- 示例：Alpha025 - 成交量价格综合因子
-- Alpha025 = rank(((((-1 * returns) * adv20) * vwap) * (high - close)))
{{ rank('(-1 * returns) * adv20 * vwap * (high - close)') }}
```

#### Alpha 051-075 (高级技术因子)
```sql
-- 示例：Alpha053 - 价格位置变化因子
-- Alpha053 = (-1 * delta(((close - low) / (high - low)), 9))
-1 * {{ delta(safe_divide('close - low', 'high - low'), 9) }}
```

#### Alpha 076-101 (复合高级因子)
```sql
-- 示例：Alpha101 - 经典日内动量因子
-- Alpha101 = ((close - open) / ((high - low) + .001))
{{ safe_divide('close - open', 'high - low + 0.001') }}
```

### 3. 因子分类体系

#### 按投资逻辑分类
- **动量类** (Momentum): Alpha001, 012, 019, 037, 065, 089
- **反转类** (Reversal): Alpha003, 004, 009, 023, 051, 099
- **成交量类** (Volume): Alpha006, 013, 025, 044, 075, 078
- **波动率类** (Volatility): Alpha022, 040, 053, 070, 084, 094
- **趋势类** (Trend): Alpha005, 028, 032, 046, 089, 097
- **形态类** (Pattern): Alpha041, 054, 060, 083, 101, 088

#### 按时间频率分类
- **高频因子** (HFT): Alpha012, 041, 101
- **中频因子** (MFT): Alpha001, 003, 006, 025
- **低频因子** (LFT): Alpha019, 032, 048

#### 按市场状态分类
- **趋势市场**: Alpha001, 005, 028, 032
- **震荡市场**: Alpha003, 009, 023, 051
- **高波动市场**: Alpha022, 040, 070, 084

## 🚀 快速开始

### 1. 环境准备

```bash
# 激活Python环境
source .venv/bin/activate

# 进入DBT项目目录
cd dbt_project
```

### 2. 运行Alpha因子计算

```bash
# 运行所有Alpha因子模型
dbt run --models alpha101

# 运行特定因子组
dbt run --models alpha_factors_001_020
dbt run --models alpha_factors_021_050
dbt run --models alpha_factors_051_075
dbt run --models alpha_factors_076_101

# 运行完整因子汇总
dbt run --models alpha101_complete
```

### 3. 测试因子质量

```bash
# 运行DBT测试
dbt test --models alpha101

# 运行Python测试
python tests/unit/test_alpha101_factors.py
python tests/integration/test_alpha101_integration.py
```

### 4. 集成到Feast

```bash
# 配置Feast特征
cd feast_config/feature_repo
feast apply

# 推送Alpha因子
python ../alpha101_pusher.py
```

## 📊 因子使用示例

### 1. 查询单个因子

```sql
-- 查询Alpha001因子
SELECT 
    symbol,
    timestamp,
    alpha001,
    close,
    volume
FROM alpha101_complete
WHERE symbol = 'BTCUSDT'
  AND timestamp >= '2024-01-01'
ORDER BY timestamp DESC
LIMIT 10;
```

### 2. 查询因子组合

```sql
-- 查询动量类因子组合
SELECT 
    symbol,
    timestamp,
    momentum_alpha_composite,
    alpha001,
    alpha012,
    alpha019
FROM alpha101_complete
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY momentum_alpha_composite DESC;
```

### 3. 因子表现分析

```sql
-- 分析因子五分位表现
WITH factor_quintiles AS (
    SELECT 
        symbol,
        timestamp,
        alpha001,
        NTILE(5) OVER (PARTITION BY timestamp ORDER BY alpha001) as quintile,
        LEAD(returns, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as next_return
    FROM alpha101_complete
    WHERE alpha001 IS NOT NULL
)
SELECT 
    quintile,
    COUNT(*) as observations,
    AVG(next_return) as avg_return,
    STDDEV(next_return) as return_std
FROM factor_quintiles
WHERE next_return IS NOT NULL
GROUP BY quintile
ORDER BY quintile;
```

## 🎯 因子特性说明

### 经典高效因子

#### Alpha001 - 价格反转因子
- **逻辑**: 基于价格在短期内达到最高点的位置进行反转预测
- **适用**: 震荡市场，短期反转策略
- **频率**: 日频
- **预期收益**: 负偏斜，反转效应

#### Alpha003 - 量价背离因子
- **逻辑**: 开盘价与成交量的负相关性
- **适用**: 流动性分析，市场情绪判断
- **频率**: 日频
- **预期收益**: 负相关，背离修复

#### Alpha012 - 成交量动量因子
- **逻辑**: 成交量变化方向与价格变化的反向关系
- **适用**: 高频交易，短期动量
- **频率**: 高频
- **预期收益**: 正相关，动量延续

#### Alpha041 - 几何平均偏离因子
- **逻辑**: 高低价几何平均与VWAP的偏离
- **适用**: 价格发现，套利机会
- **频率**: 日频
- **预期收益**: 均值回归

#### Alpha101 - 日内动量因子
- **逻辑**: 收盘开盘价差与日内波幅的比率
- **适用**: 日内交易，动量策略
- **频率**: 高频
- **预期收益**: 正相关，日内延续

### 因子组合策略

#### 动量组合策略
```python
# 使用动量类因子构建多头组合
momentum_factors = ['alpha001', 'alpha012', 'alpha019', 'alpha037', 'alpha065', 'alpha089']
momentum_score = sum(factors[f] for f in momentum_factors) / len(momentum_factors)

# 选择动量得分最高的股票做多
if momentum_score > 0.5:
    signal = 'BUY'
elif momentum_score < -0.5:
    signal = 'SELL'
else:
    signal = 'HOLD'
```

#### 市场中性策略
```python
# 使用多空配对消除市场风险
long_factors = ['alpha001', 'alpha005', 'alpha012', 'alpha028', 'alpha032', 'alpha041', 'alpha101']
short_factors = ['alpha003', 'alpha006', 'alpha013', 'alpha022', 'alpha040', 'alpha044', 'alpha050']

long_score = sum(factors[f] for f in long_factors) / len(long_factors)
short_score = sum(factors[f] for f in short_factors) / len(short_factors)

market_neutral_score = long_score - short_score
```

## 📈 因子验证方法

### 1. IC分析 (信息系数)
```sql
-- 计算因子IC
WITH factor_returns AS (
    SELECT 
        symbol,
        timestamp,
        alpha001,
        LEAD(returns, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as next_return
    FROM alpha101_complete
)
SELECT 
    CORR(alpha001, next_return) as alpha001_ic
FROM factor_returns
WHERE alpha001 IS NOT NULL AND next_return IS NOT NULL;
```

### 2. 因子衰减分析
```sql
-- 分析因子在不同持有期的表现
WITH factor_performance AS (
    SELECT 
        symbol,
        timestamp,
        alpha001,
        LEAD(returns, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as return_1d,
        LEAD(returns, 5) OVER (PARTITION BY symbol ORDER BY timestamp) as return_5d,
        LEAD(returns, 20) OVER (PARTITION BY symbol ORDER BY timestamp) as return_20d
    FROM alpha101_complete
)
SELECT 
    CORR(alpha001, return_1d) as ic_1d,
    CORR(alpha001, return_5d) as ic_5d,
    CORR(alpha001, return_20d) as ic_20d
FROM factor_performance;
```

### 3. 因子换手率分析
```sql
-- 计算因子排序的稳定性
WITH factor_ranks AS (
    SELECT 
        symbol,
        timestamp,
        RANK() OVER (PARTITION BY timestamp ORDER BY alpha001) as alpha001_rank,
        LAG(RANK() OVER (PARTITION BY timestamp ORDER BY alpha001)) 
            OVER (PARTITION BY symbol ORDER BY timestamp) as prev_alpha001_rank
    FROM alpha101_complete
)
SELECT 
    AVG(ABS(alpha001_rank - COALESCE(prev_alpha001_rank, alpha001_rank))) as avg_rank_change
FROM factor_ranks;
```

## 🛠️ 自定义因子开发

### 1. 添加新的基础操作符

```sql
-- 在 base_operators.sql 中添加新宏
{% macro my_custom_operator(column, param1, param2) %}
    -- 自定义操作逻辑
    CUSTOM_FUNCTION({{ column }}, {{ param1 }}, {{ param2 }})
{% endmacro %}
```

### 2. 实现自定义Alpha因子

```sql
-- 在新的宏文件中定义
{% macro alpha_custom_001() %}
    -- 自定义因子逻辑
    {{ rank(ts_corr('custom_feature1', 'custom_feature2', 10)) }} * 
    {{ sign(delta('price', 1)) }}
{% endmacro %}
```

### 3. 在模型中使用自定义因子

```sql
-- 在模型文件中调用
SELECT 
    symbol,
    timestamp,
    {{ alpha_custom_001() }} as custom_alpha_001
FROM base_data
```

## 📊 因子监控和维护

### 1. 因子有效性监控

```sql
-- 监控因子的有效性
SELECT 
    symbol,
    timestamp,
    total_valid_factors,
    factor_strength,
    momentum_consistency,
    reversal_consistency
FROM alpha101_complete
WHERE total_valid_factors < 80  -- 有效因子数量过少
   OR factor_strength < 0.1     -- 因子强度过弱
ORDER BY timestamp DESC;
```

### 2. 因子异常值检测

```sql
-- 检测因子异常值
WITH factor_stats AS (
    SELECT 
        timestamp,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY alpha001) as alpha001_p1,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY alpha001) as alpha001_p99
    FROM alpha101_complete
    GROUP BY timestamp
)
SELECT 
    a.symbol,
    a.timestamp,
    a.alpha001
FROM alpha101_complete a
JOIN factor_stats s ON a.timestamp = s.timestamp
WHERE a.alpha001 < s.alpha001_p1 OR a.alpha001 > s.alpha001_p99;
```

### 3. 因子相关性监控

```sql
-- 监控因子间相关性
SELECT 
    timestamp,
    CORR(alpha001, alpha003) as corr_001_003,
    CORR(alpha001, alpha012) as corr_001_012,
    CORR(momentum_alpha_composite, reversal_alpha_composite) as corr_mom_rev
FROM alpha101_complete
GROUP BY timestamp
HAVING ABS(CORR(alpha001, alpha003)) > 0.8  -- 相关性过高
ORDER BY timestamp DESC;
```

## 🔄 实时因子计算

### 1. 实时因子推送

```python
from feast_config.alpha101_pusher import Alpha101FactorPusher

# 创建推送器
pusher = Alpha101FactorPusher()

# 准备市场数据
market_data = {
    'symbol': 'BTCUSDT',
    'prices': [45000, 45100, 44950, 45200, 45150],  # 最近5期价格
    'volumes': [1000000, 1100000, 950000, 1200000, 1050000],  # 最近5期成交量
    'timestamp': datetime.now()
}

# 推送Alpha因子
success = pusher.push_alpha_factors('BTCUSDT', market_data)
```

### 2. 获取实时Alpha因子

```python
# 获取实时Alpha因子用于决策
features = pusher.get_alpha_features_for_decision(
    trading_pairs=['BTCUSDT', 'ETHUSDT'],
    feature_set='composite'  # basic, composite, selected
)

print(f"获取到的Alpha因子: {features}")
```

## 🧪 测试和验证

### 运行完整测试套件

```bash
# 运行Alpha因子单元测试
python tests/unit/test_alpha101_factors.py

# 运行Alpha因子集成测试  
python tests/integration/test_alpha101_integration.py

# 运行DBT测试
cd dbt_project
dbt test --models alpha101
```

### 性能基准测试

```bash
# 测试因子计算性能
time dbt run --models alpha101_complete

# 测试因子推送性能
python -m cProfile feast_config/alpha101_pusher.py
```

## 📚 最佳实践

### 1. 因子使用建议

- **因子选择**: 优先使用经过验证的经典因子 (Alpha001, 003, 012, 041, 101)
- **组合使用**: 避免使用高相关性因子，建议使用不同类别的因子组合
- **风险控制**: 使用风险调整后的因子，考虑波动率影响
- **市场适应**: 根据市场状态选择合适的因子类别

### 2. 性能优化

- **数据预处理**: 使用`alpha_base_data`模型预计算常用指标
- **增量计算**: 使用DBT的增量模型减少计算量
- **索引优化**: 在symbol和timestamp上创建合适的索引
- **内存管理**: 合理设置DuckDB的内存参数

### 3. 监控告警

- **数据质量**: 监控因子的空值率和异常值
- **计算性能**: 监控因子计算的执行时间
- **预测效果**: 定期评估因子的IC和换手率
- **系统稳定**: 监控Feast推送的成功率

## 🔍 故障排除

### 常见问题

1. **因子值为NULL**
   - 检查基础数据完整性
   - 确认窗口期内有足够的历史数据
   - 验证除零保护逻辑

2. **因子值异常**
   - 检查极值处理逻辑
   - 验证数据类型转换
   - 确认数学运算的边界条件

3. **性能问题**
   - 优化SQL查询逻辑
   - 增加必要的索引
   - 调整DuckDB配置参数

4. **Feast集成问题**
   - 检查特征定义的数据类型
   - 验证推送数据格式
   - 确认Redis连接状态

## 📞 技术支持

如有问题，请：

1. 查看本文档的相关章节
2. 运行相应的测试脚本
3. 检查系统日志输出
4. 提交GitHub Issue并附上：
   - 错误日志
   - 数据样例
   - 系统配置信息

---

**注意**: Alpha 101因子基于历史数据统计规律，实际使用时请充分回测验证，并考虑市场环境变化对因子有效性的影响。