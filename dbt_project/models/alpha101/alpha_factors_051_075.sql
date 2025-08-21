{{ config(materialized='table') }}

-- Alpha 101 因子计算 (051-075)

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha051 相关计算
        (close_lag20 - close_lag10) / 10 - (close_lag10 - close) / 10 AS alpha051_slope_diff,
        
        -- Alpha052 相关计算
        {{ delay(ts_min('low', 5), 5) }} AS alpha052_delay_min_low,
        (returns_sum250 - close_sum20) / 220 AS alpha052_returns_diff,
        
        -- Alpha053 相关计算
        {{ safe_divide('close - low', 'high - low') }} AS alpha053_hl_position,
        
        -- Alpha054 相关计算
        POWER(open, 5) AS alpha054_open_power5,
        POWER(close, 5) AS alpha054_close_power5,
        
        -- Alpha055 相关计算
        {{ safe_divide('close - ' ~ ts_min('low', 12), ts_max('high', 12) ~ ' - ' ~ ts_min('low', 12)) }} AS alpha055_stoch_like,
        
        -- Alpha056 相关计算
        {{ safe_divide(ts_sum('returns', 10), ts_sum(ts_sum('returns', 2), 3)) }} AS alpha056_returns_ratio,
        returns * volume * close AS alpha056_weighted_returns,
        
        -- Alpha057 相关计算
        {{ ts_argmax('close', 30) }} AS alpha057_argmax_close,
        
        -- Alpha060 相关计算
        {{ safe_divide('(close - low) - (high - close)', 'high - low') }} * volume AS alpha060_price_vol_product,
        
        -- Alpha061 相关计算
        vwap - {{ ts_min('vwap', 16) }} AS alpha061_vwap_min_diff,
        {{ ts_corr('vwap', adv(180), 18) }} AS alpha061_vwap_adv_corr,
        
        -- Alpha062 相关计算
        {{ ts_corr('vwap', ts_sum(adv(20), 22), 10) }} AS alpha062_corr1,
        ({{ rank('open') }} + {{ rank('open') }}) < ({{ rank('(high + low) / 2') }} + {{ rank('high') }}) AS alpha062_condition,
        
        -- Alpha063 相关计算
        close * 0.607 + open * 0.393 AS alpha063_weighted_price,
        vwap * 0.318 + open * 0.682 AS alpha063_weighted_vwap_open,
        
        -- Alpha064 相关计算
        open * 0.178 + low * 0.822 AS alpha064_weighted_open_low,
        (high + low) / 2 * 0.178 + vwap * 0.822 AS alpha064_weighted_hl_vwap,
        
        -- Alpha065 相关计算
        open * 0.008 + vwap * 0.992 AS alpha065_weighted_open_vwap,
        open - {{ ts_min('open', 14) }} AS alpha065_open_min_diff,
        
        -- Alpha066 相关计算
        low - vwap AS alpha066_low_vwap_diff,
        open - (high + low) / 2 AS alpha066_open_hl_mid_diff,
        
        -- Alpha067 相关计算
        high - {{ ts_min('high', 2) }} AS alpha067_high_min_diff,
        
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
        {{ decay_linear(delta('vwap', 5), 3) }} AS alpha073_decay_vwap_delta,
        {{ decay_linear(ts_corr('vwap', 'volume', 4), 8) }} AS alpha058_decay_corr,
        {{ decay_linear(ts_corr('(high + low) / 2', adv(40), 3), 6) }} AS alpha077_decay_corr
        
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
        ((-1 * {{ ts_min('low', 5) }}) + alpha052_delay_min_low) * 
        {{ rank('alpha052_returns_diff') }} * {{ ts_rank('volume', 5) }} AS alpha052,
        
        -- Alpha 053: 价格位置变化因子
        -1 * {{ delta('alpha053_hl_position', 9) }} AS alpha053,
        
        -- Alpha 054: 开盘收盘价格关系因子
        {{ safe_divide('-1 * (low - close) * alpha054_open_power5', '(low - high) * alpha054_close_power5') }} AS alpha054,
        
        -- Alpha 055: 随机指标与成交量相关性
        -1 * {{ ts_corr(rank('alpha055_stoch_like'), rank('volume'), 6) }} AS alpha055,
        
        -- Alpha 056: 收益率比率与加权收益因子
        0 - (1 * {{ rank('alpha056_returns_ratio') }} * {{ rank('alpha056_weighted_returns') }}) AS alpha056,
        
        -- Alpha 057: VWAP偏离与价格位置因子
        0 - (1 * {{ safe_divide('close - vwap', decay_linear(rank('alpha057_argmax_close'), 2)) }}) AS alpha057,
        
        -- Alpha 058: 简化的衰减相关性因子
        -1 * {{ ts_rank('alpha058_decay_corr', 6) }} AS alpha058,
        
        -- Alpha 059: 简化版本
        -1 * {{ ts_rank('alpha058_decay_corr', 8) }} AS alpha059,
        
        -- Alpha 060: 价格位置与成交量综合因子
        0 - (1 * (2 * {{ scale(rank('alpha060_price_vol_product')) }} - 
        {{ scale(rank(ts_argmax('close', 10))) }})) AS alpha060,
        
        -- Alpha 061: VWAP最小值比较因子
        CASE 
            WHEN {{ rank('alpha061_vwap_min_diff') }} < {{ rank('alpha061_vwap_adv_corr') }}
            THEN 1 
            ELSE 0 
        END AS alpha061,
        
        -- Alpha 062: 复合条件因子
        CASE 
            WHEN {{ rank('alpha062_corr1') }} < {{ rank('alpha062_condition') }}
            THEN -1 
            ELSE 1 
        END AS alpha062,
        
        -- Alpha 063: 加权价格衰减因子
        ({{ rank(decay_linear(delta('alpha063_weighted_price', 2), 8)) }} - 
        {{ rank(decay_linear(ts_corr('alpha063_weighted_vwap_open', ts_sum(adv(180), 37), 14), 12)) }}) * -1 AS alpha063,
        
        -- Alpha 064: 复合加权因子
        CASE 
            WHEN {{ rank(ts_corr(ts_sum('alpha064_weighted_open_low', 13), ts_sum(adv(120), 13), 17)) }} < 
                 {{ rank(delta('alpha064_weighted_hl_vwap', 4)) }}
            THEN -1 
            ELSE 1 
        END AS alpha064,
        
        -- Alpha 065: 开盘价最小值比较因子
        CASE 
            WHEN {{ rank(ts_corr('alpha065_weighted_open_vwap', ts_sum(adv(60), 9), 6)) }} < 
                 {{ rank('alpha065_open_min_diff') }}
            THEN -1 
            ELSE 1 
        END AS alpha065,
        
        -- Alpha 066: VWAP衰减与低价关系因子
        ({{ rank(decay_linear(delta('vwap', 4), 7)) }} + 
        {{ ts_rank(decay_linear(safe_divide('alpha066_low_vwap_diff', 'alpha066_open_hl_mid_diff'), 11), 7) }}) * -1 AS alpha066,
        
        -- Alpha 067: 高价最小值幂函数因子
        POWER({{ rank('alpha067_high_min_diff') }}, {{ rank(ts_corr('vwap', adv(20), 6)) }}) * -1 AS alpha067,
        
        -- Alpha 068: 高价与平均日成交量关系因子
        CASE 
            WHEN {{ ts_rank(ts_corr(rank('high'), rank(adv(15)), 9), 14) }} < 
                 {{ rank(delta('alpha068_weighted_close_low', 1)) }}
            THEN -1 
            ELSE 1 
        END AS alpha068,
        
        -- Alpha 069: VWAP最大值幂函数因子
        POWER({{ rank(ts_max(delta('vwap', 3), 5)) }}, 
              {{ ts_rank(ts_corr('alpha069_weighted_close_vwap', adv(20), 5), 9) }}) * -1 AS alpha069,
        
        -- Alpha 070: VWAP变化幂函数因子
        POWER({{ rank(delta('vwap', 1)) }}, {{ ts_rank(ts_corr('close', adv(50), 18), 18) }}) * -1 AS alpha070,
        
        -- Alpha 071: 复合最大值因子
        GREATEST(
            {{ ts_rank(decay_linear(ts_corr(ts_rank('close', 3), ts_rank(adv(180), 12), 18), 4), 16) }},
            {{ ts_rank(decay_linear('POWER(' ~ rank('alpha071_price_diff') ~ ', 2)', 16), 4) }}
        ) AS alpha071,
        
        -- Alpha 072: 中价与VWAP关系比率因子
        {{ safe_divide(
            rank(decay_linear(ts_corr('alpha072_hl_mid', adv(40), 9), 10)),
            rank(decay_linear(ts_corr(ts_rank('vwap', 4), ts_rank('volume', 19), 7), 3))
        ) }} AS alpha072,
        
        -- Alpha 073: 复合最大值衰减因子
        GREATEST(
            {{ rank('alpha073_decay_vwap_delta') }},
            {{ ts_rank(decay_linear(safe_divide(delta('alpha073_weighted_open_low', 2), 'alpha073_weighted_open_low') ~ ' * -1', 3), 17) }}
        ) * -1 AS alpha073,
        
        -- Alpha 074: 收盘价与高价VWAP关系比较因子
        CASE 
            WHEN {{ rank(ts_corr('close', ts_sum(adv(30), 37), 15)) }} < 
                 {{ rank(ts_corr(rank('alpha074_weighted_high_vwap'), rank('volume'), 11)) }}
            THEN -1 
            ELSE 1 
        END AS alpha074,
        
        -- Alpha 075: VWAP成交量与低价平均日成交量关系因子
        CASE 
            WHEN {{ rank(ts_corr('vwap', 'volume', 4)) }} < {{ rank(ts_corr(rank('low'), rank(adv(50)), 12)) }}
            THEN 1 
            ELSE 0 
        END AS alpha075
        
    FROM intermediate_calcs
)

SELECT * FROM alpha_factors