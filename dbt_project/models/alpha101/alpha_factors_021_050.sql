{{ config(materialized='table') }}

-- Alpha 101 因子计算 (021-050)

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha021 相关计算
        close_ma8 + close_std20 AS alpha021_ma8_plus_std,
        close_ma2 AS alpha021_ma2,
        close_ma8 - close_std20 AS alpha021_ma8_minus_std,
        {{ safe_divide('volume', 'adv20') }} AS alpha021_vol_adv_ratio,
        
        -- Alpha022 相关计算
        {{ delta(ts_corr('high', 'volume', 5), 5) }} AS alpha022_delta_corr,
        
        -- Alpha024 相关计算
        {{ delta('close_ma100', 100) }} AS alpha024_delta_ma100,
        {{ safe_divide('alpha024_delta_ma100', delay('close', 100)) }} AS alpha024_ratio,
        
        -- Alpha025 相关计算
        (-1 * returns) * adv20 * vwap * (high - close) AS alpha025_product,
        
        -- Alpha026 相关计算
        {{ ts_corr(ts_rank('volume', 5), ts_rank('high', 5), 5) }} AS alpha026_corr_ts_ranks,
        
        -- Alpha027 相关计算
        {{ ts_mean(ts_corr('volume_rank', 'vwap_rank', 6), 2) }} AS alpha027_mean_corr,
        
        -- Alpha028 相关计算
        {{ ts_corr('adv20', 'low', 5) }} + (high + low) / 2 - close AS alpha028_expression,
        
        -- Alpha030 相关计算
        {{ sign('close_delta1') }} + {{ sign(delay('close', 1) ~ ' - ' ~ delay('close', 2)) }} + 
        {{ sign(delay('close', 2) ~ ' - ' ~ delay('close', 3)) }} AS alpha030_sign_sum,
        
        -- Alpha032 相关计算
        close_ma7 - close AS alpha032_ma_diff,
        {{ ts_corr('vwap', delay('close', 5), 230) }} AS alpha032_corr_vwap_delay,
        
        -- Alpha033 相关计算
        1 - {{ safe_divide('open', 'close') }} AS alpha033_open_close_ratio,
        
        -- Alpha034 相关计算
        {{ safe_divide(ts_std('returns', 2), ts_std('returns', 5)) }} AS alpha034_std_ratio,
        
        -- Alpha035 相关计算
        close + high - low AS alpha035_price_sum,
        
        -- Alpha036 相关计算 (简化版)
        {{ ts_corr('close - open', delay('volume', 1), 15) }} AS alpha036_corr1,
        open - close AS alpha036_open_close,
        {{ ts_rank(delay('-1 * returns', 6), 5) }} AS alpha036_ts_rank,
        {{ abs_value(ts_corr('vwap', 'adv20', 6)) }} AS alpha036_abs_corr,
        (close_sum200 / 200 - open) * (close - open) AS alpha036_price_product,
        
        -- Alpha037 相关计算
        {{ ts_corr(delay('open - close', 1), 'close', 200) }} AS alpha037_corr_delay,
        
        -- Alpha038 相关计算
        {{ safe_divide('close', 'open') }} AS alpha038_close_open_ratio,
        
        -- Alpha039 相关计算
        close_delta7 * (1 - {{ rank(decay_linear(safe_divide('volume', 'adv20'), 9)) }}) AS alpha039_weighted_delta,
        
        -- Alpha043 相关计算
        {{ ts_rank(safe_divide('volume', 'adv20'), 20) }} AS alpha043_ts_rank_vol,
        {{ ts_rank('-1 * close_delta7', 8) }} AS alpha043_ts_rank_delta,
        
        -- Alpha045 相关计算
        {{ ts_mean(delay('close', 5), 20) }} AS alpha045_mean_delay_close,
        {{ ts_corr('close', 'volume', 2) }} AS alpha045_corr_close_vol,
        {{ ts_corr('close_sum5', 'close_sum20', 2) }} AS alpha045_corr_sums,
        
        -- Alpha046-049 相关计算
        (close_lag20 - close_lag10) / 10 - (close_lag10 - close) / 10 AS alpha046_slope_diff,
        
        -- Alpha047 相关计算
        {{ rank(safe_divide('1', 'close')) }} * volume / adv20 * high * {{ rank('high - close') }} / (high_sum5 / 5) AS alpha047_complex1,
        vwap - vwap_lag5 AS alpha047_vwap_diff
        
    FROM base_data
),

-- 添加更多预计算的中间变量
more_intermediate AS (
    SELECT 
        *,
        -- 为Alpha036添加更多计算
        2.21 * {{ rank('alpha036_corr1') }} + 
        0.7 * {{ rank('alpha036_open_close') }} + 
        0.73 * {{ rank('alpha036_ts_rank') }} + 
        {{ rank('alpha036_abs_corr') }} + 
        0.6 * {{ rank('alpha036_price_product') }} AS alpha036_combination
        
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
        -1 * alpha022_delta_corr * {{ rank('close_std20') }} AS alpha022,
        
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
        {{ rank('alpha025_product') }} AS alpha025,
        
        -- Alpha 026: (-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))
        -1 * {{ ts_max('alpha026_corr_ts_ranks', 3) }} AS alpha026,
        
        -- Alpha 027
        CASE 
            WHEN {{ rank('alpha027_mean_corr') }} > 0.5 THEN -1
            ELSE 1
        END AS alpha027,
        
        -- Alpha 028: scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))
        {{ scale('alpha028_expression') }} AS alpha028,
        
        -- Alpha 029: 简化版本
        {{ ts_min(rank('close'), 5) }} + {{ ts_rank(delay('-1 * returns', 6), 5) }} AS alpha029,
        
        -- Alpha 030
        (1.0 - {{ rank('alpha030_sign_sum') }}) * {{ safe_divide('volume_sum5', 'volume_sum20') }} AS alpha030,
        
        -- Alpha 031: 简化版本
        {{ rank('close_delta10') }} + {{ rank('-1 * close_delta3') }} + 
        {{ sign(scale(ts_corr('adv20', 'low', 12))) }} AS alpha031,
        
        -- Alpha 032
        {{ scale('alpha032_ma_diff') }} + 20 * {{ scale('alpha032_corr_vwap_delay') }} AS alpha032,
        
        -- Alpha 033: rank((-1 * ((1 - (open / close))^1)))
        {{ rank('-1 * alpha033_open_close_ratio') }} AS alpha033,
        
        -- Alpha 034
        {{ rank('(1 - ' ~ rank('alpha034_std_ratio') ~ ') + (1 - ' ~ rank('close_delta1') ~ ')') }} AS alpha034,
        
        -- Alpha 035
        volume_ts_rank5 * (1 - {{ ts_rank('alpha035_price_sum', 16) }}) * 
        (1 - {{ ts_rank('returns', 32) }}) AS alpha035,
        
        -- Alpha 036: 复杂组合因子
        alpha036_combination AS alpha036,
        
        -- Alpha 037
        {{ rank('alpha037_corr_delay') }} + {{ rank('alpha036_open_close') }} AS alpha037,
        
        -- Alpha 038
        (-1 * close_ts_rank10) * {{ rank('alpha038_close_open_ratio') }} AS alpha038,
        
        -- Alpha 039
        (-1 * {{ rank('alpha039_weighted_delta') }}) * (1 + {{ rank('returns_sum250') }}) AS alpha039,
        
        -- Alpha 040: ((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))
        (-1 * {{ rank(ts_std('high', 10)) }}) * corr_high_volume_5 AS alpha040,
        
        -- Alpha 041: (((high * low)^0.5) - vwap)
        SQRT(high * low) - vwap AS alpha041,
        
        -- Alpha 042: (rank((vwap - close)) / rank((vwap + close)))
        {{ safe_divide(rank('vwap - close'), rank('vwap + close')) }} AS alpha042,
        
        -- Alpha 043
        alpha043_ts_rank_vol * alpha043_ts_rank_delta AS alpha043,
        
        -- Alpha 044: (-1 * correlation(high, rank(volume), 5))
        -1 * {{ ts_corr('high', 'volume_rank', 5) }} AS alpha044,
        
        -- Alpha 045
        -1 * {{ rank('alpha045_mean_delay_close') }} * alpha045_corr_close_vol * 
        {{ rank('alpha045_corr_sums') }} AS alpha045,
        
        -- Alpha 046
        CASE 
            WHEN alpha046_slope_diff > 0.25 THEN -1
            WHEN alpha046_slope_diff < 0 THEN 1
            ELSE -1 * (close - close_lag1)
        END AS alpha046,
        
        -- Alpha 047
        alpha047_complex1 - {{ rank('alpha047_vwap_diff') }} AS alpha047,
        
        -- Alpha 048: 简化版本
        {{ safe_divide(
            ts_corr('close_delta1', delta('close_lag1', 1), 250) ~ ' * close_delta1 / close',
            ts_sum('POWER(' ~ safe_divide('close_delta1', 'close_lag1') ~ ', 2)', 250)
        ) }} AS alpha048,
        
        -- Alpha 049
        CASE 
            WHEN alpha046_slope_diff < -0.1 THEN 1
            ELSE -1 * (close - close_lag1)
        END AS alpha049,
        
        -- Alpha 050: (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))
        -1 * {{ ts_max(rank(ts_corr('volume_rank', 'vwap_rank', 5)), 5) }} AS alpha050
        
    FROM more_intermediate
)

SELECT * FROM alpha_factors