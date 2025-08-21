
  
    
    

    create  table
      "quant_features"."main"."alpha_factors_041_060__dbt_tmp"
  
    as (
      

-- Alpha 101 因子计算 (041-060)

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- Alpha 041: VWAP相对强度
        (vwap - close) / NULLIF(close, 0) AS alpha041,
        
        -- Alpha 042: VWAP排序比率
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY vwap - close) /
        NULLIF(PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY vwap + close), 0) AS alpha042,
        
        -- Alpha 043: 成交量时间排序
        CASE 
            WHEN volume > volume_lag1 THEN volume / NULLIF(adv20, 0)
            ELSE 0
        END AS alpha043,
        
        -- Alpha 044: 低价和成交量关系
        low * volume AS alpha044,
        
        -- Alpha 045: 成交量加权价格
        (close * volume + open * volume) / NULLIF(2 * volume, 0) AS alpha045,
        
        -- Alpha 046: 价格动量
        (close_ma10 - close_ma20) / NULLIF(close_ma20, 0) AS alpha046,
        
        -- Alpha 047: 复杂价格成交量关系
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY 1 / NULLIF(close, 0)) * 
        volume / NULLIF(adv20, 0) * high * 
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY high - close) AS alpha047,
        
        -- Alpha 048: 收益率标准化
        returns / NULLIF(returns_std20, 0) AS alpha048,
        
        -- Alpha 049: 价格变化比率
        CASE 
            WHEN close_lag1 != 0 AND close_lag10 != 0 
            THEN (close - close_lag1) / close_lag1 - (close_lag1 - close_lag10) / close_lag10
            ELSE 0
        END AS alpha049,
        
        -- Alpha 050: 成交量相对强度
        volume / NULLIF(volume_ma20, 0) AS alpha050,
        
        -- Alpha 051: 价格变化逻辑
        CASE 
            WHEN close_delta1 < 0 THEN close_delta1
            ELSE -1 * close_delta1
        END AS alpha051,
        
        -- Alpha 052: 价格动量
        (close - close_lag5) / NULLIF(close_lag5, 0) AS alpha052,
        
        -- Alpha 053: 价格位置
        (close - low) / NULLIF(high - low, 0) AS alpha053,
        
        -- Alpha 054: 开盘价相对强度
        -1 * (low - close) * POWER(open, 5) / 
        NULLIF((low - high) * POWER(close, 5), 0) AS alpha054,
        
        -- Alpha 055: 随机指标样式
        (close - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
) / 
        NULLIF(
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
, 0) AS alpha055,
        
        -- Alpha 056: 收益率比率
        returns / NULLIF(ABS(returns), 0.001) AS alpha056,
        
        -- Alpha 057: VWAP相对强度
        -1 * (close - vwap) / NULLIF(close_ma5 - close_ma20, 0) AS alpha057,
        
        -- Alpha 058: 价格相对强度
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY close) AS alpha058,
        
        -- Alpha 059: 成交量排序
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha059,
        
        -- Alpha 060: 价格成交量乘积
        (close - low - (high - close)) / NULLIF(high - low, 0) * volume AS alpha060
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors
    );
  
  