
  
    
    

    create  table
      "quant_features"."main"."alpha_factors_051_075__dbt_tmp"
  
    as (
      

-- Alpha 101 因子计算 (051-075)

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha051 相关计算
        (close_lag20 - close_lag10) / 10 - (close_lag10 - close) / 10 AS alpha051_slope_diff,
        
        -- Alpha052 相关计算
        
    LAG(
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS alpha052_delay_min_low,
        (returns_sum250 - close_sum20) / 220 AS alpha052_returns_diff,
        
        -- Alpha053 相关计算
        
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE close - low / high - low
    END
 AS alpha053_hl_position,
        
        -- Alpha054 相关计算
        POWER(open, 5) AS alpha054_open_power5,
        POWER(close, 5) AS alpha054_close_power5,
        
        -- Alpha055 相关计算
        
    CASE 
        WHEN 
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
 = 0 OR 
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
 IS NULL THEN NULL
        WHEN ABS(
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
) < 1e-10 THEN NULL
        ELSE close - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 / 
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

    END
 AS alpha055_stoch_like,
        
        -- Alpha056 相关计算
        
    CASE 
        WHEN 
    SUM(
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    SUM(
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    SUM(
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE 
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 / 
    SUM(
    SUM(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )

    END
 AS alpha056_returns_ratio,
        returns * volume * close AS alpha056_weighted_returns,
        
        -- Alpha057 相关计算
        
    -- 使用ROW_NUMBER()来找到最大值的位置
    (30 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (close = 
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )
 AS alpha057_argmax_close,
        
        -- Alpha060 相关计算
        
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE (close - low) - (high - close) / high - low
    END
 * volume AS alpha060_price_vol_product,
        
        -- Alpha061 相关计算
        vwap - 
    MIN(vwap) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    )
 AS alpha061_vwap_min_diff,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 17 PRECEDING AND CURRENT ROW
    )
 AS alpha061_vwap_adv_corr,
        
        -- Alpha062 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 21 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS alpha062_corr1,
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
) < (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY (high + low) / 2
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high
    )
) AS alpha062_condition,
        
        -- Alpha063 相关计算
        close * 0.607 + open * 0.393 AS alpha063_weighted_price,
        vwap * 0.318 + open * 0.682 AS alpha063_weighted_vwap_open,
        
        -- Alpha064 相关计算
        open * 0.178 + low * 0.822 AS alpha064_weighted_open_low,
        (high + low) / 2 * 0.178 + vwap * 0.822 AS alpha064_weighted_hl_vwap,
        
        -- Alpha065 相关计算
        open * 0.008 + vwap * 0.992 AS alpha065_weighted_open_vwap,
        open - 
    MIN(open) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 AS alpha065_open_min_diff,
        
        -- Alpha066 相关计算
        low - vwap AS alpha066_low_vwap_diff,
        open - (high + low) / 2 AS alpha066_open_hl_mid_diff,
        
        -- Alpha067 相关计算
        high - 
    MIN(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
 AS alpha067_high_min_diff,
        
        -- Alpha068 相关计算
        close * 0.518 + low * 0.482 AS alpha068_weighted_close_low,
        
        -- Alpha069 相关计算
        close * 0.491 + vwap * 0.509 AS alpha069_weighted_close_vwap,
        
        -- Alpha070-075 相关计算
        low + open - 2 * vwap AS alpha071_price_diff,
        (high + low) / 2 AS alpha072_hl_mid,
        open * 0.147 + low * 0.853 AS alpha073_weighted_open_low,
        high * 0.026 + vwap * 0.974 AS alpha074_weighted_high_vwap,
        
        -- 预计算一些复杂的衰减线性加权值
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    vwap - 
    LAG(vwap, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)
 AS alpha073_decay_vwap_delta,
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
 * 
        (8 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (8 * (8 + 1) / 2)
 AS alpha058_decay_corr,
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR((high + low) / 2, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 * 
        (6 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (6 * (6 + 1) / 2)
 AS alpha077_decay_corr
        
    FROM base_data
),

-- 计算Alpha因子 (051-075)
alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 051: 价格趋势变化因子
        CASE 
            WHEN alpha051_slope_diff < -0.05 THEN 1
            ELSE -1 * (close - close_lag1)
        END AS alpha051,
        
        -- Alpha 052: 低价位与收益率关系因子
        ((-1 * 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
) + alpha052_delay_min_low) * 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha052_returns_diff
    )
 * 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha052,
        
        -- Alpha 053: 价格位置变化因子
        -1 * 
    alpha053_hl_position - 
    LAG(alpha053_hl_position, 9) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS alpha053,
        
        -- Alpha 054: 开盘收盘价格关系因子
        
    CASE 
        WHEN (low - high) * alpha054_close_power5 = 0 OR (low - high) * alpha054_close_power5 IS NULL THEN NULL
        WHEN ABS((low - high) * alpha054_close_power5) < 1e-10 THEN NULL
        ELSE -1 * (low - close) * alpha054_open_power5 / (low - high) * alpha054_close_power5
    END
 AS alpha054,
        
        -- Alpha 055: 随机指标与成交量相关性
        -1 * 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha055_stoch_like
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
 AS alpha055,
        
        -- Alpha 056: 收益率比率与加权收益因子
        0 - (1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha056_returns_ratio
    )
 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha056_weighted_returns
    )
) AS alpha056,
        
        -- Alpha 057: VWAP偏离与价格位置因子
        0 - (1 * 
    CASE 
        WHEN 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha057_argmax_close
    )
 * 
        (2 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (2 * (2 + 1) / 2)
 = 0 OR 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha057_argmax_close
    )
 * 
        (2 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (2 * (2 + 1) / 2)
 IS NULL THEN NULL
        WHEN ABS(
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha057_argmax_close
    )
 * 
        (2 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (2 * (2 + 1) / 2)
) < 1e-10 THEN NULL
        ELSE close - vwap / 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha057_argmax_close
    )
 * 
        (2 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (2 * (2 + 1) / 2)

    END
) AS alpha057,
        
        -- Alpha 058: 简化的衰减相关性因子
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha058_decay_corr
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
 AS alpha058,
        
        -- Alpha 059: 简化版本
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha058_decay_corr
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 AS alpha059,
        
        -- Alpha 060: 价格位置与成交量综合因子
        0 - (1 * (2 * 
    
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha060_price_vol_product
    )
 / NULLIF(
        SUM(ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha060_price_vol_product
    )
)) OVER (PARTITION BY timestamp), 0
    )
 - 
        
    
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用ROW_NUMBER()来找到最大值的位置
    (10 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (close = 
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )

    )
 / NULLIF(
        SUM(ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用ROW_NUMBER()来找到最大值的位置
    (10 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (close = 
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )

    )
)) OVER (PARTITION BY timestamp), 0
    )
)) AS alpha060,
        
        -- Alpha 061: VWAP最小值比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha061_vwap_min_diff
    )
 < 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha061_vwap_adv_corr
    )

            THEN 1 
            ELSE 0 
        END AS alpha061,
        
        -- Alpha 062: 复合条件因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha062_corr1
    )
 < 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha062_condition
    )

            THEN -1 
            ELSE 1 
        END AS alpha062,
        
        -- Alpha 063: 加权价格衰减因子
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha063_weighted_price - 
    LAG(alpha063_weighted_price, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (8 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (8 * (8 + 1) / 2)

    )
 - 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha063_weighted_vwap_open, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 36 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 * 
        (12 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (12 * (12 + 1) / 2)

    )
) * -1 AS alpha063,
        
        -- Alpha 064: 复合加权因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    SUM(alpha064_weighted_open_low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )
, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    )

    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    alpha064_weighted_hl_vwap - 
    LAG(alpha064_weighted_hl_vwap, 4) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )

            THEN -1 
            ELSE 1 
        END AS alpha064,
        
        -- Alpha 065: 开盘价最小值比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha065_weighted_open_vwap, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha065_open_min_diff
    )

            THEN -1 
            ELSE 1 
        END AS alpha065,
        
        -- Alpha 066: VWAP衰减与低价关系因子
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    vwap - 
    LAG(vwap, 4) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (7 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (7 * (7 + 1) / 2)

    )
 + 
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    CASE 
        WHEN alpha066_open_hl_mid_diff = 0 OR alpha066_open_hl_mid_diff IS NULL THEN NULL
        WHEN ABS(alpha066_open_hl_mid_diff) < 1e-10 THEN NULL
        ELSE alpha066_low_vwap_diff / alpha066_open_hl_mid_diff
    END
 * 
        (11 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (11 * (11 + 1) / 2)

        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
) * -1 AS alpha066,
        
        -- Alpha 067: 高价最小值幂函数因子
        POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha067_high_min_diff
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

    )
) * -1 AS alpha067,
        
        -- Alpha 068: 高价与平均日成交量关系因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    alpha068_weighted_close_low - 
    LAG(alpha068_weighted_close_low, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )

            THEN -1 
            ELSE 1 
        END AS alpha068,
        
        -- Alpha 069: VWAP最大值幂函数因子
        POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    MAX(
    vwap - 
    LAG(vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

    )
, 
              
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha069_weighted_close_vwap, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
) * -1 AS alpha069,
        
        -- Alpha 070: VWAP变化幂函数因子
        POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    vwap - 
    LAG(vwap, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(close, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 17 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 17 PRECEDING AND CURRENT ROW
    )
) * -1 AS alpha070,
        
        -- Alpha 071: 复合最大值因子
        GREATEST(
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY close
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 17 PRECEDING AND CURRENT ROW
    )
 * 
        (4 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (4 * (4 + 1) / 2)

        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha071_price_diff
    )
, 2) * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )

        ) AS alpha071,
        
        -- Alpha 072: 中价与VWAP关系比率因子
        
    CASE 
        WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

    )
 = 0 OR 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

    )
 IS NULL THEN NULL
        WHEN ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

    )
) < 1e-10 THEN NULL
        ELSE 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha072_hl_mid, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
 * 
        (10 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (10 * (10 + 1) / 2)

    )
 / 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

    )

    END
 AS alpha072,
        
        -- Alpha 073: 复合最大值衰减因子
        GREATEST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha073_decay_vwap_delta
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    CASE 
        WHEN alpha073_weighted_open_low = 0 OR alpha073_weighted_open_low IS NULL THEN NULL
        WHEN ABS(alpha073_weighted_open_low) < 1e-10 THEN NULL
        ELSE 
    alpha073_weighted_open_low - 
    LAG(alpha073_weighted_open_low, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 / alpha073_weighted_open_low
    END
 * -1 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha073,
        
        -- Alpha 074: 收盘价与高价VWAP关系比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(close, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 36 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )

    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha074_weighted_high_vwap
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    )

    )

            THEN -1 
            ELSE 1 
        END AS alpha074,
        
        -- Alpha 075: VWAP成交量与低价平均日成交量关系因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )

    )
 < 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY low
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )

    )

            THEN 1 
            ELSE 0 
        END AS alpha075
        
    FROM intermediate_calcs
)

SELECT * FROM alpha_factors
    );
  
  