{{ config(materialized='table') }}

-- Alpha 101 因子计算 (061-080)

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 061: 成交量排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha061,
        
        -- Alpha 062: 高价相对强度
        -1 * {{ ts_corr('high', 'volume', 5) }} AS alpha062,
        
        -- Alpha 063: 价格动量
        {{ signed_power('close / close_lag1 - 1', 1) }} AS alpha063,
        
        -- Alpha 064: 成交量相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume / NULLIF(adv20, 0)) AS alpha064,
        
        -- Alpha 065: 价格相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close / NULLIF(close_lag1, 0)) AS alpha065,
        
        -- Alpha 066: 低价和VWAP差异
        (low - vwap) / NULLIF((open + high) / 2 - (high + low) / 2, 0) AS alpha066,
        
        -- Alpha 067: 价格排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high) AS alpha067,
        
        -- Alpha 068: 高价相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high) * volume AS alpha068,
        
        -- Alpha 069: 价格变化
        POWER(close_delta1 / NULLIF(close_lag1, 0), 2) AS alpha069,
        
        -- Alpha 070: 价格标准化
        {{ ts_std('close', 20) }} AS alpha070,
        
        -- Alpha 071: 价格相对强度
        (close - close_ma20) / NULLIF(close_ma20, 0) AS alpha071,
        
        -- Alpha 072: 成交量相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume / NULLIF(adv20, 0)) AS alpha072,
        
        -- Alpha 073: 价格排序
        -1 * PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha073,
        
        -- Alpha 074: 高价和低价关系
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high) + 
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY low) AS alpha074,
        
        -- Alpha 075: 成交量相关性
        {{ ts_corr('vwap', 'volume', 4) }} AS alpha075,
        
        -- Alpha 076: 价格标准化
        {{ scale('volume / adv20') }} AS alpha076,
        
        -- Alpha 077: 高价相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high) AS alpha077,
        
        -- Alpha 078: 低价相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY low) AS alpha078,
        
        -- Alpha 079: 价格变化
        {{ sign('close - close_lag1') }} AS alpha079,
        
        -- Alpha 080: 成交量变化
        volume / NULLIF(volume_lag1, 0) - 1 AS alpha080
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors