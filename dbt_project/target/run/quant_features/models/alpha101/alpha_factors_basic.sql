
  
    
    

    create  table
      "quant_features"."main"."alpha_factors_basic__dbt_tmp"
  
    as (
      

-- 基础版 Alpha 101 因子计算
-- 避免复杂的嵌套窗口函数，使用最简单的实现

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 001: 基于收益率的简单排序
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY returns
        ) AS alpha001,
        
        -- Alpha 002: 成交量变化排序  
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY volume_delta1
        ) AS alpha002,
        
        -- Alpha 003: 收盘价和成交量的简单比率
        close / NULLIF(volume, 0) AS alpha003,
        
        -- Alpha 004: 成交量相对强度
        volume / NULLIF(adv20, 0) AS alpha004,
        
        -- Alpha 005: 开盘价相对于VWAP的强度
        (open - vwap) / NULLIF(vwap, 0) AS alpha005,
        
        -- Alpha 006: 开盘价和成交量的关系
        open * volume AS alpha006,
        
        -- Alpha 007: 价格变化幅度
        ABS(close_delta7) AS alpha007,
        
        -- Alpha 008: 开盘价动量
        (open - close_lag1) / NULLIF(close_lag1, 0) AS alpha008,
        
        -- Alpha 009: 价格变化方向
        CASE 
            WHEN close_delta1 > 0 THEN 1
            WHEN close_delta1 < 0 THEN -1
            ELSE 0
        END AS alpha009,
        
        -- Alpha 010: 收盘价变化
        close_delta1 AS alpha010,
        
        -- Alpha 011: VWAP和收盘价差异
        vwap - close AS alpha011,
        
        -- Alpha 012: 成交量和价格变化的符号关系
        CASE 
            WHEN volume_delta1 > 0 AND close_delta1 > 0 THEN 1
            WHEN volume_delta1 < 0 AND close_delta1 < 0 THEN 1
            WHEN volume_delta1 > 0 AND close_delta1 < 0 THEN -1
            WHEN volume_delta1 < 0 AND close_delta1 > 0 THEN -1
            ELSE 0
        END AS alpha012,
        
        -- Alpha 013: 收盘价排序
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY close
        ) AS alpha013,
        
        -- Alpha 014: 收益率相对强度
        returns / NULLIF(returns_std20, 0) AS alpha014,
        
        -- Alpha 015: 高价排序
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY high
        ) AS alpha015,
        
        -- Alpha 016: 高价的负值
        -1 * high AS alpha016,
        
        -- Alpha 017: VWAP相对强度
        (vwap - close) / NULLIF(close, 0) AS alpha017,
        
        -- Alpha 018: 价格波动率
        ABS(close - open) / NULLIF(close, 0) AS alpha018,
        
        -- Alpha 019: 收盘价长期变化
        (close - close_lag5) / NULLIF(close_lag5, 0) AS alpha019,
        
        -- Alpha 020: 开盘价相对强度
        (open - close_lag1) / NULLIF(close_lag1, 0) * 
        (open - close) / NULLIF(close, 0) AS alpha020
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors
    );
  
  