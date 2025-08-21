{{ config(materialized='table') }}

-- Alpha 101 因子计算 (081-101)

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 081: 成交量相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha081,
        
        -- Alpha 082: 价格排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha082,
        
        -- Alpha 083: 高低价差异
        (high - low) / NULLIF({{ ts_mean('close', 5) }}, 0) AS alpha083,
        
        -- Alpha 084: 价格相对强度
        {{ signed_power('vwap - close', 1) }} AS alpha084,
        
        -- Alpha 085: 成交量排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha085,
        
        -- Alpha 086: 价格延迟
        CASE 
            WHEN {{ ts_mean('close', 20) }} > close_lag10 THEN -1
            ELSE 1
        END AS alpha086,
        
        -- Alpha 087: 价格相关性
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha087,
        
        -- Alpha 088: 价格相对强度
        (close - close_lag20) / NULLIF(close_lag20, 0) AS alpha088,
        
        -- Alpha 089: 价格趋势
        (close - close_lag5) / NULLIF(close_lag5, 0) AS alpha089,
        
        -- Alpha 090: 价格排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha090,
        
        -- Alpha 091: 成交量相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha091,
        
        -- Alpha 092: 价格相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high + low) AS alpha092,
        
        -- Alpha 093: 成交量排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha093,
        
        -- Alpha 094: 成交量相关性
        {{ ts_corr('close', 'volume', 30) }} AS alpha094,
        
        -- Alpha 095: 价格标准化
        {{ scale('volume') }} AS alpha095,
        
        -- Alpha 096: 价格相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha096,
        
        -- Alpha 097: 成交量标准化
        {{ scale('volume') }} AS alpha097,
        
        -- Alpha 098: 价格相关性
        {{ ts_corr('vwap', 'close', 5) }} AS alpha098,
        
        -- Alpha 099: 成交量相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha099,
        
        -- Alpha 100: 价格标准化
        {{ scale('close') }} AS alpha100,
        
        -- Alpha 101: 收盘价相对强度
        (close - open) / NULLIF(high - low, 0) AS alpha101
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors