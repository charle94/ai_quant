from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, String, UnixTimestamp
from datetime import timedelta

# 定义股票实体
stock = Entity(
    name="symbol",
    value_type=ValueType.STRING,
    description="股票代码"
)

# 定义数据源
stock_features_source = FileSource(
    path="/workspace/feast_features/all_features.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="event_timestamp",
)

# 定义特征视图
stock_features_fv = FeatureView(
    name="stock_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="close", dtype=Float64),
        Field(name="volume", dtype=Float64),
        Field(name="price_momentum_5_20", dtype=Float64),
        Field(name="price_momentum_10_20", dtype=Float64),
        Field(name="price_return_5d", dtype=Float64),
        Field(name="price_return_1d", dtype=Float64),
        Field(name="volume_ratio_20d", dtype=Float64),
        Field(name="volume_ratio_adv20", dtype=Float64),
        Field(name="volume_change_1d", dtype=Float64),
        Field(name="volatility_20d", dtype=Float64),
        Field(name="risk_adjusted_return", dtype=Float64),
        Field(name="price_rank", dtype=Float64),
        Field(name="volume_rank", dtype=Float64),
        Field(name="return_rank", dtype=Float64),
        Field(name="returns", dtype=Float64),
        Field(name="close_ma5", dtype=Float64),
        Field(name="close_ma10", dtype=Float64),
        Field(name="close_ma20", dtype=Float64),
        Field(name="volume_ma20", dtype=Float64),
        Field(name="returns_std20", dtype=Float64),
        Field(name="adv20", dtype=Float64),
    ],
    source=stock_features_source,
    tags={"team": "quant", "data_source": "dbt"},
)
