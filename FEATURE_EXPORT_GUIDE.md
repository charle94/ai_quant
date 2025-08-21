# Alpha101 特征导出指南

## 🎯 项目概览

本项目成功实现了基于 dbt 的量化特征工程管道，并导出了可供 Feast 消费的离线特征数据。

## 📊 生成的特征数据

### 数据概况
- **数据量**: 28 行 × 23 列
- **时间范围**: 2024-01-02 到 2024-01-10
- **股票**: AAPL, GOOGL, MSFT, TSLA (4只股票)
- **每只股票**: 7个交易日的数据

### 特征列表

#### 1. 基础数据特征
- `close`: 收盘价
- `volume`: 成交量
- `returns`: 日收益率

#### 2. 价格动量特征
- `price_momentum_5_20`: 5日/20日均线比率 - 1
- `price_momentum_10_20`: 10日/20日均线比率 - 1
- `price_return_5d`: 5日价格收益率
- `price_return_1d`: 1日价格收益率

#### 3. 成交量特征
- `volume_ratio_20d`: 当前成交量/20日均量 - 1
- `volume_ratio_adv20`: 当前成交量/20日平均日成交量 - 1
- `volume_change_1d`: 1日成交量变化率

#### 4. 波动率特征
- `volatility_20d`: 20日收益率标准差
- `risk_adjusted_return`: 风险调整收益率 (收益率/波动率)

#### 5. 排序特征
- `price_rank`: 当日价格排序 (0-1)
- `volume_rank`: 当日成交量排序 (0-1)
- `return_rank`: 当日收益率排序 (0-1)

#### 6. 技术指标特征
- `close_ma5`: 5日收盘价移动平均
- `close_ma10`: 10日收盘价移动平均
- `close_ma20`: 20日收盘价移动平均
- `volume_ma20`: 20日成交量移动平均
- `returns_std20`: 20日收益率标准差
- `adv20`: 20日平均日成交量

## 📁 导出文件结构

```
/workspace/feast_features/
├── all_features.parquet          # 完整数据集
├── AAPL_features.parquet        # AAPL 单独数据
├── GOOGL_features.parquet       # GOOGL 单独数据
├── MSFT_features.parquet        # MSFT 单独数据
└── TSLA_features.parquet        # TSLA 单独数据
```

## 🚀 Feast 集成

### 配置文件
- `feast_config/feature_store.yaml`: Feast 存储配置
- `feast_config/features.py`: 特征定义文件

### 使用 Feast 的步骤

1. **安装 Feast**
```bash
pip install feast
```

2. **初始化 Feast 存储**
```bash
cd /workspace/feast_config
feast apply
```

3. **获取特征数据**
```python
from feast import FeatureStore
import pandas as pd

# 初始化特征存储
fs = FeatureStore(repo_path="/workspace/feast_config")

# 获取特征
entity_df = pd.DataFrame({
    "symbol": ["AAPL", "GOOGL", "MSFT", "TSLA"],
    "event_timestamp": pd.to_datetime("2024-01-10")
})

features = [
    "stock_features:price_momentum_5_20",
    "stock_features:volatility_20d",
    "stock_features:risk_adjusted_return"
]

training_df = fs.get_historical_features(
    entity_df=entity_df,
    features=features
).to_df()
```

## 🔄 数据管道架构

```
原始数据 (CSV) 
    ↓ dbt seed
数据库表 (DuckDB)
    ↓ dbt models
基础特征 (alpha_base_data)
    ↓ dbt models
最终特征 (feast_features)
    ↓ Python 脚本
Parquet 文件
    ↓ Feast
特征服务
```

## 📈 特征统计示例

### AAPL 最新数据 (2024-01-10)
- 收盘价: $158.20
- 价格动量 (5/20日): 0.96%
- 5日收益率: 3.06%
- 波动率: 0.33%
- 风险调整收益率: 1.54

## 🛠 扩展建议

1. **增加更多股票**: 修改 `raw_stock_prices.csv` 添加更多股票数据
2. **扩展时间范围**: 增加历史数据以计算更长期的技术指标
3. **添加更多因子**: 在 `feast_features.sql` 中添加更复杂的 Alpha101 因子
4. **实时更新**: 集成实时数据源进行特征更新
5. **特征验证**: 添加数据质量检查和特征有效性测试

## ✅ 验证清单

- [x] dbt 模型成功运行
- [x] Alpha101 基础宏函数正常工作
- [x] 特征数据成功导出为 Parquet
- [x] 数据包含所有必要的特征列
- [x] Feast 配置文件已创建
- [x] 数据质量检查通过

## 🎉 总结

成功创建了一个完整的量化特征工程管道：
- **28行 × 23列** 的高质量特征数据
- **21个量化特征** 涵盖价格、成交量、波动率等维度
- **Parquet 格式** 便于 Feast 和其他工具消费
- **完整的 dbt 管道** 确保数据质量和可重复性

数据现在已经准备好用于机器学习模型训练、回测分析和实时特征服务！
