

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        close,
        volume,
        returns,
        
        -- 简化的 Alpha 因子
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY returns) AS alpha001,
        PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY volume) AS alpha002,
        close_ma5 / NULLIF(close_ma20, 0) - 1 AS alpha003,
        volume / NULLIF(volume_ma20, 0) - 1 AS alpha004,
        (close - close_lag5) / NULLIF(close_lag5, 0) AS alpha005,
        returns / NULLIF(returns_std20, 0) AS alpha006,
        close_ma5 / NULLIF(close_ma10, 0) - 1 AS momentum_5_10,
        returns_std20 AS volatility_20d,
        volume / NULLIF(adv20, 0) - 1 AS volume_ratio_adv20
        
    FROM base_data
    WHERE close_ma20 IS NOT NULL
      AND returns_std20 IS NOT NULL
      AND adv20 IS NOT NULL
)

SELECT * FROM alpha_factors