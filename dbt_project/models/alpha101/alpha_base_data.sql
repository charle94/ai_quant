{{ config(materialized='table') }}

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
    FROM {{ ref('stg_ohlc_data') }}
    WHERE timestamp >= CAST('{{ var("start_date") }}' AS DATE)
      AND timestamp <= CAST('{{ var("end_date") }}' AS DATE)
),

enhanced_data AS (
    SELECT 
        *,
        -- 计算ADV (Average Daily Volume)
        {{ adv(20) }} AS adv20,
        {{ adv(10) }} AS adv10,
        {{ adv(5) }} AS adv5,
        
        -- 预计算一些常用的时间序列指标
        {{ ts_mean('close', 5) }} AS close_ma5,
        {{ ts_mean('close', 10) }} AS close_ma10,
        {{ ts_mean('close', 20) }} AS close_ma20,
        {{ ts_mean('volume', 20) }} AS volume_ma20,
        
        -- 预计算滚动标准差
        {{ ts_std('close', 20) }} AS close_std20,
        {{ ts_std('returns', 20) }} AS returns_std20,
        
        -- 预计算一些延迟项
        {{ delay('close', 1) }} AS close_lag1,
        {{ delay('close', 2) }} AS close_lag2,
        {{ delay('close', 5) }} AS close_lag5,
        {{ delay('close', 10) }} AS close_lag10,
        {{ delay('close', 20) }} AS close_lag20,
        {{ delay('volume', 1) }} AS volume_lag1,
        {{ delay('high', 1) }} AS high_lag1,
        {{ delay('low', 1) }} AS low_lag1,
        {{ delay('vwap', 5) }} AS vwap_lag5,
        
        -- 预计算一些差值项
        {{ delta('close', 1) }} AS close_delta1,
        {{ delta('close', 2) }} AS close_delta2,
        {{ delta('close', 3) }} AS close_delta3,
        {{ delta('close', 5) }} AS close_delta5,
        {{ delta('close', 7) }} AS close_delta7,
        {{ delta('close', 10) }} AS close_delta10,
        {{ delta('volume', 1) }} AS volume_delta1,
        {{ delta('volume', 3) }} AS volume_delta3,
        {{ delta('high', 2) }} AS high_delta2,
        
        -- 预计算一些排序项
        {{ rank('close') }} AS close_rank,
        {{ rank('volume') }} AS volume_rank,
        {{ rank('high') }} AS high_rank,
        {{ rank('low') }} AS low_rank,
        {{ rank('vwap') }} AS vwap_rank,
        {{ rank('returns') }} AS returns_rank,
        
        -- 预计算时间序列排序
        {{ ts_rank('close', 10) }} AS close_ts_rank10,
        {{ ts_rank('volume', 5) }} AS volume_ts_rank5,
        {{ ts_rank('high', 5) }} AS high_ts_rank5,
        
        -- 预计算一些最值项
        {{ ts_min('close', 100) }} AS close_min100,
        {{ ts_max('close', 3) }} AS close_max3,
        {{ ts_min('close', 5) }} AS close_min5,
        {{ ts_max('volume', 5) }} AS volume_max5,
        {{ ts_min('low', 5) }} AS low_min5,
        {{ ts_max('high', 3) }} AS high_max3,
        
        -- 预计算一些求和项
        {{ ts_sum('close', 5) }} AS close_sum5,
        {{ ts_sum('close', 8) }} AS close_sum8,
        {{ ts_sum('close', 20) }} AS close_sum20,
        {{ ts_sum('close', 100) }} AS close_sum100,
        {{ ts_sum('close', 200) }} AS close_sum200,
        {{ ts_sum('volume', 5) }} AS volume_sum5,
        {{ ts_sum('volume', 20) }} AS volume_sum20,
        {{ ts_sum('returns', 250) }} AS returns_sum250,
        {{ ts_sum('high', 5) }} AS high_sum5,
        {{ ts_sum('high', 20) }} AS high_sum20,
        
        -- 预计算一些相关性
        {{ ts_corr('close', 'volume', 10) }} AS corr_close_volume_10,
        {{ ts_corr('open', 'volume', 10) }} AS corr_open_volume_10,
        {{ ts_corr('high', 'volume', 5) }} AS corr_high_volume_5,
        {{ ts_corr('vwap', 'volume', 6) }} AS corr_vwap_volume_6,
        
        -- 预计算一些协方差
        {{ ts_cov('close', 'volume', 5) }} AS cov_close_volume_5,
        {{ ts_cov('high', 'volume', 5) }} AS cov_high_volume_5
        
    FROM base_ohlc
    WHERE timestamp >= CAST(CAST('{{ var("start_date") }}' AS DATE) AS DATE) - INTERVAL '250 days'  -- 扩展时间范围以确保有足够的历史数据
),

-- 过滤回原始时间范围
final_data AS (
    SELECT *
    FROM enhanced_data
    WHERE timestamp >= CAST('{{ var("start_date") }}' AS DATE)
      AND timestamp <= CAST('{{ var("end_date") }}' AS DATE)
)

SELECT * FROM final_data