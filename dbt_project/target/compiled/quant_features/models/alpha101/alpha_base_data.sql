

-- Alpha 101 基础数据准备
-- 为Alpha因子计算准备所有必要的基础数据

WITH base_ohlc AS (
    SELECT 
        symbol,
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        -- 计算VWAP (简化版本，假设等权重)
        (high + low + close) / 3 AS vwap,
        -- 计算returns
        CASE 
            WHEN LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) IS NOT NULL
            THEN (close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) / 
                 LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)
            ELSE 0
        END AS returns
    FROM "quant_features"."main"."stg_ohlc_data"
    WHERE timestamp >= '2020-01-01'
      AND timestamp <= '2024-12-31'
),

enhanced_data AS (
    SELECT 
        *,
        -- 计算ADV (Average Daily Volume)
        
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

 AS adv20,
        
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

 AS adv10,
        
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

 AS adv5,
        
        -- 预计算一些常用的时间序列指标
        
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS close_ma5,
        
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS close_ma10,
        
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS close_ma20,
        
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS volume_ma20,
        
        -- 预计算滚动标准差
        
    STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS close_std20,
        
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS returns_std20,
        
        -- 预计算一些延迟项
        
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS close_lag1,
        
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS close_lag2,
        
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS close_lag5,
        
    LAG(close, 10) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS close_lag10,
        
    LAG(close, 20) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS close_lag20,
        
    LAG(volume, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS volume_lag1,
        
    LAG(high, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS high_lag1,
        
    LAG(low, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS low_lag1,
        
    LAG(vwap, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS vwap_lag5,
        
        -- 预计算一些差值项
        
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta1,
        
    close - 
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta2,
        
    close - 
    LAG(close, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta3,
        
    close - 
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta5,
        
    close - 
    LAG(close, 7) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta7,
        
    close - 
    LAG(close, 10) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS close_delta10,
        
    volume - 
    LAG(volume, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS volume_delta1,
        
    volume - 
    LAG(volume, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS volume_delta3,
        
    high - 
    LAG(high, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS high_delta2,
        
        -- 预计算一些排序项
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close
    )
 AS close_rank,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
 AS volume_rank,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high
    )
 AS high_rank,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY low
    )
 AS low_rank,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap
    )
 AS vwap_rank,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY returns
    )
 AS returns_rank,
        
        -- 预计算时间序列排序
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY close
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS close_ts_rank10,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS volume_ts_rank5,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY high
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS high_ts_rank5,
        
        -- 预计算一些最值项
        
    MIN(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    )
 AS close_min100,
        
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS close_max3,
        
    MIN(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS close_min5,
        
    MAX(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS volume_max5,
        
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS low_min5,
        
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS high_max3,
        
        -- 预计算一些求和项
        
    SUM(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS close_sum5,
        
    SUM(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 AS close_sum8,
        
    SUM(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS close_sum20,
        
    SUM(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    )
 AS close_sum100,
        
    SUM(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    )
 AS close_sum200,
        
    SUM(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS volume_sum5,
        
    SUM(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS volume_sum20,
        
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )
 AS returns_sum250,
        
    SUM(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS high_sum5,
        
    SUM(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS high_sum20,
        
        -- 预计算一些相关性
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS corr_close_volume_10,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(open, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS corr_open_volume_10,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(high, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS corr_high_volume_5,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
 AS corr_vwap_volume_6,
        
        -- 预计算一些协方差
        
    COVAR_SAMP(close, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS cov_close_volume_5,
        
    COVAR_SAMP(high, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS cov_high_volume_5
        
    FROM base_ohlc
    WHERE timestamp >= '2020-01-01' - INTERVAL '250 days'  -- 扩展时间范围以确保有足够的历史数据
),

-- 过滤回原始时间范围
final_data AS (
    SELECT *
    FROM enhanced_data
    WHERE timestamp >= '2020-01-01'
      AND timestamp <= '2024-12-31'
)

SELECT * FROM final_data