

-- Feast 特征表
-- 整合所有特征用于 Feast 消费

WITH base_features AS (
    SELECT 
        symbol,
        timestamp as event_timestamp,
        close,
        volume,
        returns,
        close_ma5,
        close_ma10,
        close_ma20,
        volume_ma20,
        returns_std20,
        adv20,
        adv10,
        adv5,
        close_lag1,
        close_lag5,
        volume_lag1,
        close_delta1,
        close_delta5,
        volume_delta1
    FROM "quant_features"."main"."alpha_base_data"
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
),

technical_features AS (
    SELECT 
        symbol,
        event_timestamp,
        close,
        volume,
        
        -- 价格特征
        close_ma5 / NULLIF(close_ma20, 0) - 1 as price_momentum_5_20,
        close_ma10 / NULLIF(close_ma20, 0) - 1 as price_momentum_10_20,
        (close - close_lag5) / NULLIF(close_lag5, 0) as price_return_5d,
        close_delta1 / NULLIF(close_lag1, 0) as price_return_1d,
        
        -- 成交量特征
        volume / NULLIF(volume_ma20, 0) - 1 as volume_ratio_20d,
        volume / NULLIF(adv20, 0) - 1 as volume_ratio_adv20,
        (volume - volume_lag1) / NULLIF(volume_lag1, 0) as volume_change_1d,
        
        -- 波动率特征
        returns_std20 as volatility_20d,
        returns / NULLIF(returns_std20, 0) as risk_adjusted_return,
        
        -- 排序特征
        PERCENT_RANK() OVER (PARTITION BY event_timestamp ORDER BY close) as price_rank,
        PERCENT_RANK() OVER (PARTITION BY event_timestamp ORDER BY volume) as volume_rank,
        PERCENT_RANK() OVER (PARTITION BY event_timestamp ORDER BY returns) as return_rank,
        
        -- 原始数据
        returns,
        close_ma5,
        close_ma10,
        close_ma20,
        volume_ma20,
        returns_std20,
        adv20
        
    FROM base_features
)

SELECT * FROM technical_features