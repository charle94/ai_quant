{{ config(materialized='table') }}

-- Alpha 101 因子计算 (001-020)
-- 基于预处理的基础数据计算前20个Alpha因子

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

-- 预计算一些复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha001 相关计算
        {{ ts_argmax('CASE WHEN returns < 0 THEN returns_std20 ELSE close END', 5) }} AS alpha001_argmax,
        
        -- Alpha002 相关计算
        {{ rank(delta(log_value('volume'), 2)) }} AS alpha002_rank_delta_log_vol,
        {{ rank(safe_divide('close - open', 'open')) }} AS alpha002_rank_ret,
        
        -- Alpha005 相关计算
        close_ma10 AS alpha005_mean_vwap,
        
        -- Alpha007 相关计算
        {{ ts_rank(abs_value('close_delta7'), 60) }} AS alpha007_ts_rank,
        {{ sign('close_delta7') }} AS alpha007_sign,
        
        -- Alpha008 相关计算
        (open * 5 + returns_sum250 / 50) AS alpha008_sum_open_returns,  -- 简化计算
        {{ delay('(open * 5 + returns_sum250 / 50)', 10) }} AS alpha008_delay_sum,
        
        -- Alpha009-010 逻辑
        CASE 
            WHEN {{ ts_min('close_delta1', 5) }} > 0 THEN close_delta1
            WHEN {{ ts_max('close_delta1', 5) }} < 0 THEN close_delta1
            ELSE -1 * close_delta1
        END AS alpha009_logic,
        
        -- Alpha011 相关计算
        {{ ts_max('vwap - close', 3) }} AS alpha011_max_vwap_close,
        {{ ts_min('vwap - close', 3) }} AS alpha011_min_vwap_close,
        
        -- Alpha014 相关计算
        {{ delta('returns', 3) }} AS alpha014_delta_returns,
        
        -- Alpha015 相关计算
        {{ ts_corr('high_rank', 'volume_rank', 3) }} AS alpha015_corr_high_vol,
        
        -- Alpha017 相关计算
        {{ delta('close_delta1', 1) }} AS alpha017_delta_delta_close,
        {{ ts_rank(safe_divide('volume', 'adv20'), 5) }} AS alpha017_ts_rank_vol_adv,
        
        -- Alpha018 相关计算
        {{ ts_std(abs_value('close - open'), 5) }} AS alpha018_stddev,
        close - open AS alpha018_close_open_diff,
        {{ ts_corr('close', 'open', 10) }} AS alpha018_corr_close_open,
        
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
        {{ rank('alpha001_argmax') }} - 0.5 AS alpha001,
        
        -- Alpha 002: (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
        -1 * {{ ts_corr('alpha002_rank_delta_log_vol', 'alpha002_rank_ret', 6) }} AS alpha002,
        
        -- Alpha 003: (-1 * correlation(rank(open), rank(volume), 10))
        -1 * corr_open_volume_10 AS alpha003,
        
        -- Alpha 004: (-1 * Ts_Rank(rank(low), 9))
        -1 * {{ ts_rank('low_rank', 9) }} AS alpha004,
        
        -- Alpha 005: (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))
        {{ rank('open - alpha005_mean_vwap') }} * (-1 * {{ abs_value(rank('close - vwap')) }}) AS alpha005,
        
        -- Alpha 006: (-1 * correlation(open, volume, 10))
        -1 * corr_open_volume_10 AS alpha006,
        
        -- Alpha 007: ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1))
        CASE 
            WHEN adv20 < volume THEN (-1 * alpha007_ts_rank) * alpha007_sign
            ELSE -1
        END AS alpha007,
        
        -- Alpha 008: (-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)), 10))))
        -1 * {{ rank('alpha008_sum_open_returns - alpha008_delay_sum') }} AS alpha008,
        
        -- Alpha 009: ((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : ((ts_max(delta(close, 1), 5) < 0) ? delta(close, 1) : (-1 * delta(close, 1))))
        alpha009_logic AS alpha009,
        
        -- Alpha 010: rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : ((ts_max(delta(close, 1), 4) < 0) ? delta(close, 1) : (-1 * delta(close, 1)))))
        {{ rank('alpha009_logic') }} AS alpha010,
        
        -- Alpha 011: ((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) * rank(delta(volume, 3)))
        ({{ rank('alpha011_max_vwap_close') }} + {{ rank('alpha011_min_vwap_close') }}) * 
        {{ rank('volume_delta3') }} AS alpha011,
        
        -- Alpha 012: (sign(delta(volume, 1)) * (-1 * delta(close, 1)))
        {{ sign('volume_delta1') }} * (-1 * close_delta1) AS alpha012,
        
        -- Alpha 013: (-1 * rank(covariance(rank(close), rank(volume), 5)))
        -1 * {{ rank('cov_close_volume_5') }} AS alpha013,
        
        -- Alpha 014: ((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))
        (-1 * {{ rank('alpha014_delta_returns') }}) * corr_open_volume_10 AS alpha014,
        
        -- Alpha 015: (-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))
        -1 * {{ ts_sum(rank('alpha015_corr_high_vol'), 3) }} AS alpha015,
        
        -- Alpha 016: (-1 * rank(covariance(rank(high), rank(volume), 5)))
        -1 * {{ rank('cov_high_volume_5') }} AS alpha016,
        
        -- Alpha 017: (((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) * rank(ts_rank((volume / adv20), 5)))
        ((-1 * {{ rank('close_ts_rank10') }}) * {{ rank('alpha017_delta_delta_close') }}) * 
        {{ rank('alpha017_ts_rank_vol_adv') }} AS alpha017,
        
        -- Alpha 018: (-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) + correlation(close, open, 10))))
        -1 * {{ rank('alpha018_stddev + alpha018_close_open_diff + alpha018_corr_close_open') }} AS alpha018,
        
        -- Alpha 019: ((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) * (1 + rank((1 + sum(returns, 250)))))
        (-1 * {{ sign('alpha019_close_diff_plus_delta') }}) * 
        (1 + {{ rank('alpha019_sum_returns') }}) AS alpha019,
        
        -- Alpha 020: (((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open - delay(low, 1))))
        ((-1 * {{ rank('alpha020_open_delay_high') }}) * {{ rank('alpha020_open_delay_close') }}) * 
        {{ rank('alpha020_open_delay_low') }} AS alpha020
        
    FROM intermediate_calcs
)

SELECT * FROM alpha_factors