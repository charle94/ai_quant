-- 宏：设置原始数据表
{% macro setup_raw_data() %}
  
  -- 创建raw schema
  CREATE SCHEMA IF NOT EXISTS raw;
  
  -- 创建OHLC数据表
  CREATE TABLE IF NOT EXISTS raw.ohlc_data (
    symbol VARCHAR,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
  );
  
  -- 创建索引以提高查询性能
  CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_timestamp 
  ON raw.ohlc_data (symbol, timestamp);
  
{% endmacro %}