
  
    
    

    create  table
      "quant_features"."main"."alpha_factors_001_020__dbt_tmp"
  
    as (
      

-- Alpha 101 因子计算 (001-020)
-- 基于预处理的基础数据计算前20个Alpha因子

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

-- 预计算一些复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha001 相关计算
        
    -- 使用ROW_NUMBER()来找到最大值的位置
    (5 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (CASE WHEN returns < 0 THEN returns_std20 ELSE close END = 
    MAX(CASE WHEN returns < 0 THEN returns_std20 ELSE close END) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )
 AS alpha001_argmax,
        
        -- Alpha002 相关计算
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    CASE 
        WHEN volume > 0 THEN LN(volume)
        ELSE NULL
    END
 - 
    LAG(
    CASE 
        WHEN volume > 0 THEN LN(volume)
        ELSE NULL
    END
, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 AS alpha002_rank_delta_log_vol,
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN open = 0 OR open IS NULL THEN NULL
        WHEN ABS(open) < 1e-10 THEN NULL
        ELSE close - open / open
    END

    )
 AS alpha002_rank_ret,
        
        -- Alpha005 相关计算
        close_ma10 AS alpha005_mean_vwap,
        
        -- Alpha007 相关计算
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    ABS(close_delta7)

        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )
 AS alpha007_ts_rank,
        
    CASE 
        WHEN close_delta7 > 0 THEN 1
        WHEN close_delta7 < 0 THEN -1
        ELSE 0
    END
 AS alpha007_sign,
        
        -- Alpha008 相关计算
        (open * 5 + returns_sum250 / 50) AS alpha008_sum_open_returns,  -- 简化计算
        
    LAG((open * 5 + returns_sum250 / 50), 10) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 AS alpha008_delay_sum,
        
        -- Alpha009-010 逻辑
        CASE 
            WHEN 
    MIN(close_delta1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 > 0 THEN close_delta1
            WHEN 
    MAX(close_delta1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 < 0 THEN close_delta1
            ELSE -1 * close_delta1
        END AS alpha009_logic,
        
        -- Alpha011 相关计算
        
    MAX(vwap - close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha011_max_vwap_close,
        
    MIN(vwap - close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha011_min_vwap_close,
        
        -- Alpha014 相关计算
        
    returns - 
    LAG(returns, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS alpha014_delta_returns,
        
        -- Alpha015 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(high_rank, volume_rank) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha015_corr_high_vol,
        
        -- Alpha017 相关计算
        
    close_delta1 - 
    LAG(close_delta1, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 AS alpha017_delta_delta_close,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END

        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha017_ts_rank_vol_adv,
        
        -- Alpha018 相关计算
        
    STDDEV(
    ABS(close - open)
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha018_stddev,
        close - open AS alpha018_close_open_diff,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, open) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS alpha018_corr_close_open,
        
        -- Alpha019 相关计算
        (close - close_lag7) + close_delta7 AS alpha019_close_diff_plus_delta,
        1 + returns_sum250 AS alpha019_sum_returns,
        
        -- Alpha020 相关计算
        open - high_lag1 AS alpha020_open_delay_high,
        open - close_lag1 AS alpha020_open_delay_close,
        open - low_lag1 AS alpha020_open_delay_low
        
    FROM base_data
),

-- 计算Alpha因子
alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 001: RANK(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha001_argmax
    )
 - 0.5 AS alpha001,
        
        -- Alpha 002: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
        -1 * 
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha002_rank_delta_log_vol, alpha002_rank_ret) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )
 AS alpha002,
        
        -- Alpha 003: (-1 * correlation(rank(open), rank(volume), 10))
        -1 * corr_open_volume_10 AS alpha003,
        
        -- Alpha 004: (-1 * Ts_Rank(rank(low), 9))
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY low_rank
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
 AS alpha004,
        
        -- Alpha 005: (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open - alpha005_mean_vwap
    )
 * (-1 * 
    ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close - vwap
    )
)
) AS alpha005,
        
        -- Alpha 006: (-1 * correlation(open, volume, 10))
        -1 * corr_open_volume_10 AS alpha006,
        
        -- Alpha 007: ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1))
        CASE 
            WHEN adv20 < volume THEN (-1 * alpha007_ts_rank) * alpha007_sign
            ELSE -1
        END AS alpha007,
        
        -- Alpha 008: (-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)), 10))))
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha008_sum_open_returns - alpha008_delay_sum
    )
 AS alpha008,
        
        -- Alpha 009: ((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : ((ts_max(delta(close, 1), 5) < 0) ? delta(close, 1) : (-1 * delta(close, 1))))
        alpha009_logic AS alpha009,
        
        -- Alpha 010: rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : ((ts_max(delta(close, 1), 4) < 0) ? delta(close, 1) : (-1 * delta(close, 1)))))
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha009_logic
    )
 AS alpha010,
        
        -- Alpha 011: ((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) * rank(delta(volume, 3)))
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha011_max_vwap_close
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha011_min_vwap_close
    )
) * 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume_delta3
    )
 AS alpha011,
        
        -- Alpha 012: (sign(delta(volume, 1)) * (-1 * delta(close, 1)))
        
    CASE 
        WHEN volume_delta1 > 0 THEN 1
        WHEN volume_delta1 < 0 THEN -1
        ELSE 0
    END
 * (-1 * close_delta1) AS alpha012,
        
        -- Alpha 013: (-1 * rank(covariance(rank(close), rank(volume), 5)))
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY cov_close_volume_5
    )
 AS alpha013,
        
        -- Alpha 014: ((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))
        (-1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha014_delta_returns
    )
) * corr_open_volume_10 AS alpha014,
        
        -- Alpha 015: (-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))
        -1 * 
    SUM(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha015_corr_high_vol
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha015,
        
        -- Alpha 016: (-1 * rank(covariance(rank(high), rank(volume), 5)))
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY cov_high_volume_5
    )
 AS alpha016,
        
        -- Alpha 017: (((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) * rank(ts_rank((volume / adv20), 5)))
        ((-1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close_ts_rank10
    )
) * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha017_delta_delta_close
    )
) * 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha017_ts_rank_vol_adv
    )
 AS alpha017,
        
        -- Alpha 018: (-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) + correlation(close, open, 10))))
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha018_stddev + alpha018_close_open_diff + alpha018_corr_close_open
    )
 AS alpha018,
        
        -- Alpha 019: ((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) * (1 + rank((1 + sum(returns, 250)))))
        (-1 * 
    CASE 
        WHEN alpha019_close_diff_plus_delta > 0 THEN 1
        WHEN alpha019_close_diff_plus_delta < 0 THEN -1
        ELSE 0
    END
) * 
        (1 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha019_sum_returns
    )
) AS alpha019,
        
        -- Alpha 020: (((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open - delay(low, 1))))
        ((-1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha020_open_delay_high
    )
) * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha020_open_delay_close
    )
) * 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha020_open_delay_low
    )
 AS alpha020
        
    FROM intermediate_calcs
)

SELECT * FROM alpha_factors
    );
  
  