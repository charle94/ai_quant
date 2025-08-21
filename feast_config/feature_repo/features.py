from datetime import timedelta
from feast import FeatureView, Field, PushSource
from feast.types import Float64, Int64, String
from entities import trading_pair
from data_sources import offline_features_source, realtime_features_source

# 技术指标特征视图
technical_indicators_fv = FeatureView(
    name="technical_indicators",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        Field(name="price", dtype=Float64),
        Field(name="daily_return", dtype=Float64),
        Field(name="volatility_20d", dtype=Float64),
        Field(name="ma_5", dtype=Float64),
        Field(name="ma_10", dtype=Float64),
        Field(name="ma_20", dtype=Float64),
        Field(name="rsi_14", dtype=Float64),
        Field(name="stoch_k_14", dtype=Float64),
        Field(name="momentum_5d", dtype=Float64),
        Field(name="momentum_10d", dtype=Float64),
        Field(name="volume_ratio", dtype=Float64),
        Field(name="bb_position", dtype=Float64),
        Field(name="price_above_ma5", dtype=Int64),
        Field(name="price_above_ma10", dtype=Int64),
        Field(name="price_above_ma20", dtype=Int64),
        Field(name="rsi_overbought", dtype=Int64),
        Field(name="rsi_oversold", dtype=Int64),
        Field(name="high_volume", dtype=Int64),
        Field(name="double_overbought", dtype=Int64),
        Field(name="double_oversold", dtype=Int64),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "technical_analysis"},
)

# 实时特征推送源
realtime_push_source = PushSource(
    name="realtime_features_push_source",
    schema=[
        Field(name="symbol", dtype=String),
        Field(name="price", dtype=Float64),
        Field(name="volume", dtype=Int64),
        Field(name="ma_5", dtype=Float64),
        Field(name="ma_10", dtype=Float64),
        Field(name="rsi_14", dtype=Float64),
        Field(name="volatility", dtype=Float64),
        Field(name="volume_ratio", dtype=Float64),
        Field(name="momentum_5d", dtype=Float64),
    ],
)

# 实时特征视图
realtime_features_fv = FeatureView(
    name="realtime_features",
    entities=[trading_pair],
    ttl=timedelta(minutes=10),
    schema=[
        Field(name="price", dtype=Float64),
        Field(name="volume", dtype=Int64),
        Field(name="ma_5", dtype=Float64),
        Field(name="ma_10", dtype=Float64),
        Field(name="rsi_14", dtype=Float64),
        Field(name="volatility", dtype=Float64),
        Field(name="volume_ratio", dtype=Float64),
        Field(name="momentum_5d", dtype=Float64),
    ],
    source=realtime_push_source,
    tags={"team": "quant", "type": "realtime"},
)