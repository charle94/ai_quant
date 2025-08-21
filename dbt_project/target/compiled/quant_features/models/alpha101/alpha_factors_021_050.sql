

-- Alpha 101 因子计算 (021-050)

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha021 相关计算
        close_ma8 + close_std20 AS alpha021_ma8_plus_std,
        close_ma2 AS alpha021_ma2,
        close_ma8 - close_std20 AS alpha021_ma8_minus_std,
        
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END
 AS alpha021_vol_adv_ratio,
        
        -- Alpha022 相关计算
        
    
    -- 使用DuckDB的CORR窗口函数
    CORR(high, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 - 
    LAG(
    -- 使用DuckDB的CORR窗口函数
    CORR(high, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS alpha022_delta_corr,
        
        -- Alpha024 相关计算
        
    close_ma100 - 
    LAG(close_ma100, 100) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS alpha024_delta_ma100,
        
    CASE 
        WHEN 
    LAG(close, 100) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 = 0 OR 
    LAG(close, 100) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 IS NULL THEN NULL
        WHEN ABS(
    LAG(close, 100) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) < 1e-10 THEN NULL
        ELSE alpha024_delta_ma100 / 
    LAG(close, 100) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

    END
 AS alpha024_ratio,
        
        -- Alpha025 相关计算
        (-1 * returns) * adv20 * vwap * (high - close) AS alpha025_product,
        
        -- Alpha026 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY high
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha026_corr_ts_ranks,
        
        -- Alpha027 相关计算
        
    AVG(
    -- 使用DuckDB的CORR窗口函数
    CORR(volume_rank, vwap_rank) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
 AS alpha027_mean_corr,
        
        -- Alpha028 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(adv20, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 + (high + low) / 2 - close AS alpha028_expression,
        
        -- Alpha030 相关计算
        
    CASE 
        WHEN close_delta1 > 0 THEN 1
        WHEN close_delta1 < 0 THEN -1
        ELSE 0
    END
 + 
    CASE 
        WHEN 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 - 
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 > 0 THEN 1
        WHEN 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 - 
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 < 0 THEN -1
        ELSE 0
    END
 + 
        
    CASE 
        WHEN 
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 - 
    LAG(close, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 > 0 THEN 1
        WHEN 
    LAG(close, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 - 
    LAG(close, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 < 0 THEN -1
        ELSE 0
    END
 AS alpha030_sign_sum,
        
        -- Alpha032 相关计算
        close_ma7 - close AS alpha032_ma_diff,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 229 PRECEDING AND CURRENT ROW
    )
 AS alpha032_corr_vwap_delay,
        
        -- Alpha033 相关计算
        1 - 
    CASE 
        WHEN close = 0 OR close IS NULL THEN NULL
        WHEN ABS(close) < 1e-10 THEN NULL
        ELSE open / close
    END
 AS alpha033_open_close_ratio,
        
        -- Alpha034 相关计算
        
    CASE 
        WHEN 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
 / 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

    END
 AS alpha034_std_ratio,
        
        -- Alpha035 相关计算
        close + high - low AS alpha035_price_sum,
        
        -- Alpha036 相关计算 (简化版)
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close - open, 
    LAG(volume, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )
 AS alpha036_corr1,
        open - close AS alpha036_open_close,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    LAG(-1 * returns, 6) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha036_ts_rank,
        
    ABS(
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, adv20) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
)
 AS alpha036_abs_corr,
        (close_sum200 / 200 - open) * (close - open) AS alpha036_price_product,
        
        -- Alpha037 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    LAG(open - close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
, close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    )
 AS alpha037_corr_delay,
        
        -- Alpha038 相关计算
        
    CASE 
        WHEN open = 0 OR open IS NULL THEN NULL
        WHEN ABS(open) < 1e-10 THEN NULL
        ELSE close / open
    END
 AS alpha038_close_open_ratio,
        
        -- Alpha039 相关计算
        close_delta7 * (1 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END
 * 
        (9 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (9 * (9 + 1) / 2)

    )
) AS alpha039_weighted_delta,
        
        -- Alpha043 相关计算
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END

        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS alpha043_ts_rank_vol,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY -1 * close_delta7
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 AS alpha043_ts_rank_delta,
        
        -- Alpha045 相关计算
        
    AVG(
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS alpha045_mean_delay_close,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
 AS alpha045_corr_close_vol,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close_sum5, close_sum20) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
 AS alpha045_corr_sums,
        
        -- Alpha046-049 相关计算
        (close_lag20 - close_lag10) / 10 - (close_lag10 - close) / 10 AS alpha046_slope_diff,
        
        -- Alpha047 相关计算
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN close = 0 OR close IS NULL THEN NULL
        WHEN ABS(close) < 1e-10 THEN NULL
        ELSE 1 / close
    END

    )
 * volume / adv20 * high * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high - close
    )
 / (high_sum5 / 5) AS alpha047_complex1,
        vwap - vwap_lag5 AS alpha047_vwap_diff
        
    FROM base_data
),

-- 添加更多预计算的中间变量
more_intermediate AS (
    SELECT 
        *,
        -- 为Alpha036添加更多计算
        2.21 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_corr1
    )
 + 
        0.7 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_open_close
    )
 + 
        0.73 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_ts_rank
    )
 + 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_abs_corr
    )
 + 
        0.6 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_price_product
    )
 AS alpha036_combination
        
    FROM intermediate_calcs
),

-- 计算Alpha因子
alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 021
        CASE 
            WHEN alpha021_ma8_plus_std < alpha021_ma2 THEN -1
            WHEN alpha021_ma2 < alpha021_ma8_minus_std THEN 1
            WHEN alpha021_vol_adv_ratio >= 1 THEN 1
            ELSE -1
        END AS alpha021,
        
        -- Alpha 022: (-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20))))
        -1 * alpha022_delta_corr * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close_std20
    )
 AS alpha022,
        
        -- Alpha 023: (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)
        CASE 
            WHEN (high_sum20 / 20) < high THEN -1 * high_delta2
            ELSE 0
        END AS alpha023,
        
        -- Alpha 024
        CASE 
            WHEN alpha024_ratio <= 0.05 THEN -1 * (close - close_min100)
            ELSE -1 * close_delta3
        END AS alpha024,
        
        -- Alpha 025: rank(((((-1 * returns) * adv20) * vwap) * (high - close)))
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha025_product
    )
 AS alpha025,
        
        -- Alpha 026: (-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))
        -1 * 
    MAX(alpha026_corr_ts_ranks) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha026,
        
        -- Alpha 027
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha027_mean_corr
    )
 > 0.5 THEN -1
            ELSE 1
        END AS alpha027,
        
        -- Alpha 028: scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))
        
    alpha028_expression / NULLIF(
        SUM(ABS(alpha028_expression)) OVER (PARTITION BY timestamp), 0
    )
 AS alpha028,
        
        -- Alpha 029: 简化版本
        
    MIN(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    LAG(-1 * returns, 6) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha029,
        
        -- Alpha 030
        (1.0 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha030_sign_sum
    )
) * 
    CASE 
        WHEN volume_sum20 = 0 OR volume_sum20 IS NULL THEN NULL
        WHEN ABS(volume_sum20) < 1e-10 THEN NULL
        ELSE volume_sum5 / volume_sum20
    END
 AS alpha030,
        
        -- Alpha 031: 简化版本
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close_delta10
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY -1 * close_delta3
    )
 + 
        
    CASE 
        WHEN 
    
    -- 使用DuckDB的CORR窗口函数
    CORR(adv20, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 / NULLIF(
        SUM(ABS(
    -- 使用DuckDB的CORR窗口函数
    CORR(adv20, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
)) OVER (PARTITION BY timestamp), 0
    )
 > 0 THEN 1
        WHEN 
    
    -- 使用DuckDB的CORR窗口函数
    CORR(adv20, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 / NULLIF(
        SUM(ABS(
    -- 使用DuckDB的CORR窗口函数
    CORR(adv20, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
)) OVER (PARTITION BY timestamp), 0
    )
 < 0 THEN -1
        ELSE 0
    END
 AS alpha031,
        
        -- Alpha 032
        
    alpha032_ma_diff / NULLIF(
        SUM(ABS(alpha032_ma_diff)) OVER (PARTITION BY timestamp), 0
    )
 + 20 * 
    alpha032_corr_vwap_delay / NULLIF(
        SUM(ABS(alpha032_corr_vwap_delay)) OVER (PARTITION BY timestamp), 0
    )
 AS alpha032,
        
        -- Alpha 033: rank((-1 * ((1 - (open / close))^1)))
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY -1 * alpha033_open_close_ratio
    )
 AS alpha033,
        
        -- Alpha 034
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY (1 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha034_std_ratio
    )
) + (1 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close_delta1
    )
)
    )
 AS alpha034,
        
        -- Alpha 035
        volume_ts_rank5 * (1 - 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha035_price_sum
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    )
) * 
        (1 - 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY returns
        ROWS BETWEEN 31 PRECEDING AND CURRENT ROW
    )
) AS alpha035,
        
        -- Alpha 036: 复杂组合因子
        alpha036_combination AS alpha036,
        
        -- Alpha 037
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha037_corr_delay
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha036_open_close
    )
 AS alpha037,
        
        -- Alpha 038
        (-1 * close_ts_rank10) * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha038_close_open_ratio
    )
 AS alpha038,
        
        -- Alpha 039
        (-1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha039_weighted_delta
    )
) * (1 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY returns_sum250
    )
) AS alpha039,
        
        -- Alpha 040: ((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))
        (-1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    STDDEV(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

    )
) * corr_high_volume_5 AS alpha040,
        
        -- Alpha 041: (((high * low)^0.5) - vwap)
        SQRT(high * low) - vwap AS alpha041,
        
        -- Alpha 042: (rank((vwap - close)) / rank((vwap + close)))
        
    CASE 
        WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap + close
    )
 = 0 OR 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap + close
    )
 IS NULL THEN NULL
        WHEN ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap + close
    )
) < 1e-10 THEN NULL
        ELSE 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap - close
    )
 / 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap + close
    )

    END
 AS alpha042,
        
        -- Alpha 043
        alpha043_ts_rank_vol * alpha043_ts_rank_delta AS alpha043,
        
        -- Alpha 044: (-1 * correlation(high, rank(volume), 5))
        -1 * 
    -- 使用DuckDB的CORR窗口函数
    CORR(high, volume_rank) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha044,
        
        -- Alpha 045
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha045_mean_delay_close
    )
 * alpha045_corr_close_vol * 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha045_corr_sums
    )
 AS alpha045,
        
        -- Alpha 046
        CASE 
            WHEN alpha046_slope_diff > 0.25 THEN -1
            WHEN alpha046_slope_diff < 0 THEN 1
            ELSE -1 * (close - close_lag1)
        END AS alpha046,
        
        -- Alpha 047
        alpha047_complex1 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha047_vwap_diff
    )
 AS alpha047,
        
        -- Alpha 048: 简化版本
        
    CASE 
        WHEN 
    SUM(POWER(
    CASE 
        WHEN close_lag1 = 0 OR close_lag1 IS NULL THEN NULL
        WHEN ABS(close_lag1) < 1e-10 THEN NULL
        ELSE close_delta1 / close_lag1
    END
, 2)) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    SUM(POWER(
    CASE 
        WHEN close_lag1 = 0 OR close_lag1 IS NULL THEN NULL
        WHEN ABS(close_lag1) < 1e-10 THEN NULL
        ELSE close_delta1 / close_lag1
    END
, 2)) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    SUM(POWER(
    CASE 
        WHEN close_lag1 = 0 OR close_lag1 IS NULL THEN NULL
        WHEN ABS(close_lag1) < 1e-10 THEN NULL
        ELSE close_delta1 / close_lag1
    END
, 2)) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE 
    -- 使用DuckDB的CORR窗口函数
    CORR(close_delta1, 
    close_lag1 - 
    LAG(close_lag1, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )
 * close_delta1 / close / 
    SUM(POWER(
    CASE 
        WHEN close_lag1 = 0 OR close_lag1 IS NULL THEN NULL
        WHEN ABS(close_lag1) < 1e-10 THEN NULL
        ELSE close_delta1 / close_lag1
    END
, 2)) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    )

    END
 AS alpha048,
        
        -- Alpha 049
        CASE 
            WHEN alpha046_slope_diff < -0.1 THEN 1
            ELSE -1 * (close - close_lag1)
        END AS alpha049,
        
        -- Alpha 050: (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))
        -1 * 
    MAX(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(volume_rank, vwap_rank) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha050
        
    FROM more_intermediate
)

SELECT * FROM alpha_factors