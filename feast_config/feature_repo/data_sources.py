from feast import FileSource
from feast.data_source import DataSource
from feast.infra.offline_stores.duckdb_source import DuckDBSource

# DuckDB离线数据源
offline_features_source = DuckDBSource(
    path="/workspace/data/quant_features.duckdb",
    query="SELECT * FROM main.features_ohlc_technical",
    timestamp_field="event_timestamp",
    created_timestamp_column="timestamp",
)

# 实时特征数据源
realtime_features_source = DuckDBSource(
    path="/workspace/data/realtime_features.duckdb", 
    query="SELECT * FROM main.realtime_features",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)