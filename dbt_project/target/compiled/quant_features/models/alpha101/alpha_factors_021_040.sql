

-- Alpha 101 因子计算 (021-040)
-- 基础版本，避免复杂嵌套

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 021: 成交量相对强度
        volume / NULLIF(adv20, 0) AS alpha021,
        
        -- Alpha 022: 高价变化率
        (high - close_lag1) / NULLIF(close_lag1, 0) AS alpha022,
        
        -- Alpha 023: 低价相对强度  
        CASE 
            WHEN low < close_lag1 THEN -1 * (close - low)
            ELSE 0
        END AS alpha023,
        
        -- Alpha 024: 移动平均相对强度
        (close_ma5 - close_ma20) / NULLIF(close_ma20, 0) AS alpha024,
        
        -- Alpha 025: 成交量排序
        PERCENT_RANK() OVER (
            PARTITION BY timestamp 
            ORDER BY volume
        ) AS alpha025,
        
        -- Alpha 026: VWAP动量
        (vwap - close_lag5) / NULLIF(close_lag5, 0) AS alpha026,
        
        -- Alpha 027: 成交量变化率
        volume_delta1 / NULLIF(volume_lag1, 0) AS alpha027,
        
        -- Alpha 028: 价格趋势强度
        (close_ma5 - close_ma10) / NULLIF(close_ma10, 0) AS alpha028,
        
        -- Alpha 029: 收盘价相对位置
        (close - low) / NULLIF(high - low, 0) AS alpha029,
        
        -- Alpha 030: 多重信号组合
        CASE WHEN close > close_lag1 THEN 1 ELSE 0 END +
        CASE WHEN close_lag1 > close_lag2 THEN 1 ELSE 0 END +
        CASE WHEN volume > volume_lag1 THEN 1 ELSE 0 END AS alpha030,
        
        -- Alpha 031: 低价相关性
        (low - close) / NULLIF(close, 0) AS alpha031,
        
        -- Alpha 032: 价格动量
        (close - close_lag10) / NULLIF(close_lag10, 0) AS alpha032,
        
        -- Alpha 033: 开盘价相对强度
        1 - open / NULLIF(close, 0) AS alpha033,
        
        -- Alpha 034: 波动率比率
        returns_std20 / NULLIF(ABS(returns), 0.001) AS alpha034,
        
        -- Alpha 035: 成交量动量
        volume / NULLIF(volume_ma20, 0) AS alpha035,
        
        -- Alpha 036: VWAP和成交量关系
        ABS(vwap - close) * volume AS alpha036,
        
        -- Alpha 037: 收盘价相对强度
        (close - open) / NULLIF(open, 0) AS alpha037,
        
        -- Alpha 038: 开盘收盘比
        close / NULLIF(open, 0) AS alpha038,
        
        -- Alpha 039: 价格变化加权
        close_delta7 * (1 - volume / NULLIF(adv20, 0)) AS alpha039,
        
        -- Alpha 040: 成交量标准化
        (volume - volume_ma20) / NULLIF(volume_ma20, 0) AS alpha040
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors