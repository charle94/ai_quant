{{ config(materialized='table') }}

-- 简化版 Alpha 101 因子计算 (001-020)
-- 避免复杂的嵌套窗口函数

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 001 简化版: 基于收益率和波动率的排序
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY CASE WHEN returns < 0 THEN returns_std20 ELSE close END
        ) - 0.5 AS alpha001,
        
        -- Alpha 002 简化版: 成交量变化和收益率的相关性
        {{ ts_corr('volume_delta1', 'returns', 6) }} * -1 AS alpha002,
        
        -- Alpha 003 简化版: 收盘价相关性
        {{ ts_corr('close', 'volume', 10) }} * -1 AS alpha003,
        
        -- Alpha 004 简化版: 成交量趋势
        {{ ts_rank('volume', 5) }} * -1 AS alpha004,
        
        -- Alpha 005 简化版: VWAP相对强度
        {{ rank('open - vwap') }} * (-1 * {{ abs_value(rank('close - vwap')) }}) AS alpha005,
        
        -- Alpha 006 简化版: 开盘价相关性
        {{ ts_corr('open', 'volume', 10) }} * -1 AS alpha006,
        
        -- Alpha 007 简化版: 价格变化排序
        CASE 
            WHEN adv20 < volume THEN (-1 * {{ ts_rank(abs_value('close_delta7'), 60) }}) * {{ sign('close_delta7') }}
            ELSE -1
        END AS alpha007,
        
        -- Alpha 008 简化版: 开盘价和收益率的组合
        {{ rank('(open * 0.35 + returns * 0.65)') }} AS alpha008,
        
        -- Alpha 009 简化版: 价格变化逻辑
        CASE 
            WHEN {{ ts_min('close_delta1', 5) }} > 0 THEN close_delta1
            WHEN {{ ts_max('close_delta1', 5) }} < 0 THEN close_delta1
            ELSE -1 * close_delta1
        END AS alpha009,
        
        -- Alpha 010 简化版: 收盘价排序
        {{ rank('close_delta1') }} AS alpha010,
        
        -- Alpha 011 简化版: VWAP和收盘价差异
        {{ ts_max('vwap - close', 3) }} AS alpha011,
        
        -- Alpha 012 简化版: 成交量和价格变化
        {{ sign('volume_delta1') }} * (-1 * close_delta1) AS alpha012,
        
        -- Alpha 013 简化版: 收盘价和成交量相关性
        -1 * {{ rank(ts_corr('close', 'volume', 5)) }} AS alpha013,
        
        -- Alpha 014 简化版: 收益率变化
        (-1 * {{ rank('returns_delta3') }}) * {{ ts_corr('open', 'volume', 10) }} AS alpha014,
        
        -- Alpha 015 简化版: 高价和成交量相关性
        -1 * {{ ts_sum(rank(ts_corr('high_rank', 'volume_rank', 3)), 3) }} AS alpha015,
        
        -- Alpha 016 简化版: 高价排序
        -1 * {{ rank('high') }} AS alpha016,
        
        -- Alpha 017 简化版: 成交量相对强度
        {{ rank('vwap - close') }} AS alpha017,
        
        -- Alpha 018 简化版: 收盘价开盘价差异标准差
        {{ ts_std(abs_value('close - open'), 5) }} / close AS alpha018,
        
        -- Alpha 019 简化版: 收盘价变化和收益率
        (-1 * {{ sign('close_delta7 + close_delta7') }}) * 
        (1 + {{ rank('1 + returns_sum250') }}) AS alpha019,
        
        -- Alpha 020 简化版: 开盘价相对强度
        -1 * {{ rank('open - close_lag1') }} * {{ rank('open - close') }} AS alpha020
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors