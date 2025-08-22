
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, String
from datetime import timedelta

# 定义实体
symbol_entity = Entity(
    name="symbol_entity",
    value_type=String,
    description="股票/加密货币交易对",
)

# 定义数据源
quant_features_source = FileSource(
    path="/workspace/data/feast_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="event_timestamp",
)

# 定义特征视图
quant_features_fv = FeatureView(
    name="quant_features",
    entities=[symbol_entity],
    ttl=timedelta(days=1),
    schema=[
        Field(name="price", dtype=Float64),
        Field(name="ma_5", dtype=Float64),
        Field(name="ma_20", dtype=Float64),
        Field(name="rsi_14", dtype=Float64),
        Field(name="volume_ratio", dtype=Float64),
        Field(name="momentum_5d", dtype=Float64),
        Field(name="volatility_20d", dtype=Float64),
    ],
    source=quant_features_source,
    tags={"team": "quant_team"},
)
