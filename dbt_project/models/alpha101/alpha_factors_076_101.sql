{{ config(materialized='table') }}

-- Alpha 101 因子计算 (076-101)

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha076 相关计算
        {{ decay_linear(delta('vwap', 1), 12) }} AS alpha076_decay_vwap_delta,
        {{ ts_rank(ts_corr('low', adv(81), 8), 20) }} AS alpha076_ts_rank_corr,
        
        -- Alpha077 相关计算
        ((high + low) / 2 + high) - (vwap + high) AS alpha077_price_diff,
        {{ ts_corr('(high + low) / 2', adv(40), 3) }} AS alpha077_corr_hl_adv,
        
        -- Alpha078 相关计算
        low * 0.352 + vwap * 0.648 AS alpha078_weighted_low_vwap,
        
        -- Alpha079 相关计算
        close * 0.607 + open * 0.393 AS alpha079_weighted_close_open,
        
        -- Alpha080 相关计算
        open * 0.868 + high * 0.132 AS alpha080_weighted_open_high,
        
        -- Alpha081 相关计算
        POWER({{ rank(ts_corr('vwap', ts_sum(adv(10), 50), 8)) }}, 4) AS alpha081_rank_corr_power,
        
        -- Alpha082 相关计算
        {{ decay_linear(delta('open', 1), 15) }} AS alpha082_decay_open_delta,
        {{ ts_corr('volume', 'open', 17) }} AS alpha082_corr_vol_open,
        
        -- Alpha083 相关计算
        {{ safe_divide('high - low', ts_mean('close', 5)) }} AS alpha083_hl_close_ratio,
        vwap - close AS alpha083_vwap_close_diff,
        
        -- Alpha084 相关计算
        vwap - {{ ts_max('vwap', 15) }} AS alpha084_vwap_max_diff,
        
        -- Alpha085 相关计算
        high * 0.877 + close * 0.123 AS alpha085_weighted_high_close,
        
        -- Alpha086 相关计算
        (open + close) - (vwap + open) AS alpha086_price_diff,
        
        -- Alpha087 相关计算
        close * 0.370 + vwap * 0.630 AS alpha087_weighted_close_vwap,
        
        -- Alpha088 相关计算
        ({{ rank('open') }} + {{ rank('low') }}) - ({{ rank('high') }} + {{ rank('close') }}) AS alpha088_rank_diff,
        
        -- Alpha089 相关计算
        low AS alpha089_low,  -- 简化，因为 low * 0.967285 + low * (1 - 0.967285) = low
        
        -- Alpha090 相关计算
        close - {{ ts_max('close', 5) }} AS alpha090_close_max_diff,
        
        -- Alpha091 相关计算
        {{ decay_linear(ts_corr('close', 'volume', 10), 16) }} AS alpha091_decay_corr1,
        {{ ts_corr(rank('close'), rank(adv(50)), 4) }} AS alpha091_corr2,
        
        -- Alpha092 相关计算
        ((high + low) / 2 + close) < (low + open) AS alpha092_condition,
        
        -- Alpha093 相关计算
        close * 0.524 + vwap * 0.476 AS alpha093_weighted_close_vwap,
        
        -- Alpha094 相关计算
        vwap - {{ ts_min('vwap', 12) }} AS alpha094_vwap_min_diff,
        
        -- Alpha095 相关计算
        open - {{ ts_min('open', 12) }} AS alpha095_open_min_diff,
        
        -- Alpha096 相关计算
        {{ ts_corr(ts_rank('close', 7), ts_rank(adv(60), 4), 4) }} AS alpha096_corr_close_adv,
        
        -- Alpha097 相关计算
        low * 0.721 + vwap * 0.279 AS alpha097_weighted_low_vwap,
        
        -- Alpha098 相关计算
        {{ ts_sum(adv(5), 26) }} AS alpha098_sum_adv5,
        {{ ts_argmin(ts_corr(rank('open'), rank(adv(15)), 21), 9) }} AS alpha098_argmin_corr,
        
        -- Alpha099 相关计算
        {{ ts_sum('(high + low) / 2', 20) }} AS alpha099_sum_hl_mid,
        {{ ts_sum(adv(60), 20) }} AS alpha099_sum_adv60,
        
        -- Alpha100 相关计算
        {{ safe_divide('(close - low) - (high - close)', 'high - low') }} * volume AS alpha100_price_vol,
        {{ ts_corr('close', rank(adv(20)), 5) }} - {{ rank(ts_argmin('close', 30)) }} AS alpha100_corr_diff,
        
        -- Alpha101 相关计算
        close - open AS alpha101_close_open_diff,
        high - low + 0.001 AS alpha101_hl_range
        
    FROM base_data
),

-- 最终Alpha因子计算
final_alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 076: VWAP变化与低价相关性最大值因子
        GREATEST(
            {{ rank('alpha076_decay_vwap_delta') }},
            {{ ts_rank(decay_linear('alpha076_ts_rank_corr', 17), 19) }}
        ) * -1 AS alpha076,
        
        -- Alpha 077: 价格差异与中价相关性最小值因子
        LEAST(
            {{ rank(decay_linear('alpha077_price_diff', 20)) }},
            {{ rank(decay_linear('alpha077_corr_hl_adv', 6)) }}
        ) AS alpha077,
        
        -- Alpha 078: 加权低价VWAP与VWAP成交量相关性幂函数因子
        POWER(
            {{ rank(ts_corr(ts_sum('alpha078_weighted_low_vwap', 20), ts_sum(adv(40), 20), 7)) }},
            {{ rank(ts_corr(rank('vwap'), rank('volume'), 6)) }}
        ) AS alpha078,
        
        -- Alpha 079: 加权收盘开盘价与VWAP关系比较因子
        CASE 
            WHEN {{ rank(delta('alpha079_weighted_close_open', 1)) }} < 
                 {{ rank(ts_corr(ts_rank('vwap', 4), ts_rank(adv(150), 9), 15)) }}
            THEN 1 
            ELSE 0 
        END AS alpha079,
        
        -- Alpha 080: 加权开盘高价符号与高价平均日成交量相关性幂函数因子
        POWER(
            {{ rank(sign(delta('alpha080_weighted_open_high', 4))) }},
            {{ ts_rank(ts_corr('high', adv(10), 5), 6) }}
        ) * -1 AS alpha080,
        
        -- Alpha 081: VWAP平均日成交量相关性对数与VWAP成交量相关性比较因子
        CASE 
            WHEN {{ rank(log_value('alpha081_rank_corr_power')) }} < 
                 {{ rank(ts_corr(rank('vwap'), rank('volume'), 5)) }}
            THEN -1 
            ELSE 1 
        END AS alpha081,
        
        -- Alpha 082: 开盘价变化与成交量相关性最小值因子
        LEAST(
            {{ rank('alpha082_decay_open_delta') }},
            {{ ts_rank(decay_linear('alpha082_corr_vol_open', 7), 13) }}
        ) * -1 AS alpha082,
        
        -- Alpha 083: 高低价收盘价比率延迟与成交量关系因子
        {{ safe_divide(
            rank(delay('alpha083_hl_close_ratio', 2)) ~ ' * ' ~ rank(rank('volume')),
            safe_divide('alpha083_hl_close_ratio', 'alpha083_vwap_close_diff')
        ) }} AS alpha083,
        
        -- Alpha 084: VWAP排序幂函数因子
        {{ signed_power(ts_rank('alpha084_vwap_max_diff', 21), delta('close', 5)) }} AS alpha084,
        
        -- Alpha 085: 加权高价收盘价与中价成交量相关性幂函数因子
        POWER(
            {{ rank(ts_corr('alpha085_weighted_high_close', adv(30), 10)) }},
            {{ rank(ts_corr(ts_rank('(high + low) / 2', 4), ts_rank('volume', 10), 7)) }}
        ) AS alpha085,
        
        -- Alpha 086: 收盘价平均日成交量相关性与价格差异比较因子
        CASE 
            WHEN {{ ts_rank(ts_corr('close', ts_sum(adv(20), 15), 6), 20) }} < 
                 {{ rank('alpha086_price_diff') }}
            THEN -1 
            ELSE 1 
        END AS alpha086,
        
        -- Alpha 087: 加权收盘VWAP与平均日成交量相关性最大值因子
        GREATEST(
            {{ rank(decay_linear(delta('alpha087_weighted_close_vwap', 2), 3)) }},
            {{ ts_rank(decay_linear(abs_value(ts_corr(adv(81), 'close', 13)), 5), 14) }}
        ) * -1 AS alpha087,
        
        -- Alpha 088: 开盘低价与高价收盘价排序差异最小值因子
        LEAST(
            {{ rank(decay_linear('alpha088_rank_diff', 8)) }},
            {{ ts_rank(decay_linear(ts_corr(ts_rank('close', 8), ts_rank(adv(60), 21), 8), 7), 3) }}
        ) AS alpha088,
        
        -- Alpha 089: 低价平均日成交量相关性与VWAP变化差异因子
        {{ ts_rank(decay_linear(ts_corr('alpha089_low', adv(10), 7), 6), 4) }} - 
        {{ ts_rank(decay_linear(delta('vwap', 3), 10), 15) }} AS alpha089,
        
        -- Alpha 090: 收盘价最大值与平均日成交量低价相关性幂函数因子
        POWER(
            {{ rank('alpha090_close_max_diff') }},
            {{ ts_rank(ts_corr(adv(40), 'low', 5), 3) }}
        ) * -1 AS alpha090,
        
        -- Alpha 091: 复合衰减相关性差异因子
        ({{ ts_rank(decay_linear('alpha091_decay_corr1', 4), 5) }} - 
        {{ rank(decay_linear('alpha091_corr2', 4)) }}) * -1 AS alpha091,
        
        -- Alpha 092: 价格条件与低价平均日成交量相关性最小值因子
        LEAST(
            {{ ts_rank(decay_linear('CASE WHEN alpha092_condition THEN 1 ELSE 0 END', 15), 19) }},
            {{ ts_rank(decay_linear(ts_corr(rank('low'), rank(adv(30)), 8), 7), 7) }}
        ) AS alpha092,
        
        -- Alpha 093: VWAP平均日成交量相关性与加权收盘VWAP变化比率因子
        {{ safe_divide(
            ts_rank(decay_linear(ts_corr('vwap', adv(81), 17), 20), 8),
            rank(decay_linear(delta('alpha093_weighted_close_vwap', 3), 16))
        ) }} AS alpha093,
        
        -- Alpha 094: VWAP最小值与VWAP平均日成交量相关性幂函数因子
        POWER(
            {{ rank('alpha094_vwap_min_diff') }},
            {{ ts_rank(ts_corr(ts_rank('vwap', 20), ts_rank(adv(60), 4), 18), 3) }}
        ) * -1 AS alpha094,
        
        -- Alpha 095: 开盘价最小值与中价平均日成交量相关性幂函数比较因子
        CASE 
            WHEN {{ rank('alpha095_open_min_diff') }} < 
                 {{ ts_rank('POWER(' ~ rank(ts_corr(ts_sum('(high + low) / 2', 19), ts_sum(adv(40), 19), 13)) ~ ', 5)', 12) }}
            THEN 1 
            ELSE 0 
        END AS alpha095,
        
        -- Alpha 096: VWAP成交量相关性与收盘价平均日成交量相关性最大值因子
        GREATEST(
            {{ ts_rank(decay_linear(ts_corr(rank('vwap'), rank('volume'), 4), 4), 8) }},
            {{ ts_rank(decay_linear(ts_argmax('alpha096_corr_close_adv', 13), 14), 13) }}
        ) * -1 AS alpha096,
        
        -- Alpha 097: 加权低价VWAP变化与低价平均日成交量相关性差异因子
        ({{ rank(decay_linear(delta('alpha097_weighted_low_vwap', 3), 20)) }} - 
        {{ ts_rank(decay_linear(ts_rank(ts_corr(ts_rank('low', 8), ts_rank(adv(60), 17), 5), 19), 16), 7) }}) * -1 AS alpha097,
        
        -- Alpha 098: VWAP平均日成交量相关性与开盘价平均日成交量相关性最小值差异因子
        {{ rank(decay_linear(ts_corr('vwap', 'alpha098_sum_adv5', 5), 7)) }} - 
        {{ rank(decay_linear(ts_rank('alpha098_argmin_corr', 7), 8)) }} AS alpha098,
        
        -- Alpha 099: 中价平均日成交量相关性与低价成交量相关性比较因子
        CASE 
            WHEN {{ rank(ts_corr('alpha099_sum_hl_mid', 'alpha099_sum_adv60', 9)) }} < 
                 {{ rank(ts_corr('low', 'volume', 6)) }}
            THEN -1 
            ELSE 1 
        END AS alpha099,
        
        -- Alpha 100: 复合标准化因子
        0 - (1 * (1.5 * {{ scale(rank('alpha100_price_vol')) }} - 
        {{ scale('alpha100_corr_diff') }} * {{ safe_divide('volume', adv(20)) }})) AS alpha100,
        
        -- Alpha 101: 收盘开盘价差与高低价范围比率因子
        {{ safe_divide('alpha101_close_open_diff', 'alpha101_hl_range') }} AS alpha101
        
    FROM base_data
),

-- 最终计算所有Alpha因子
final_alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 076-101
        alpha076, alpha077, alpha078, alpha079, alpha080,
        alpha081, alpha082, alpha083, alpha084, alpha085,
        alpha086, alpha087, alpha088, alpha089, alpha090,
        alpha091, alpha092, alpha093, alpha094, alpha095,
        alpha096, alpha097, alpha098, alpha099, alpha100,
        alpha101,
        
        -- 添加一些质量控制指标
        CASE 
            WHEN alpha076 IS NOT NULL AND ABS(alpha076) < 100 THEN 1 ELSE 0 
        END AS alpha076_valid,
        
        CASE 
            WHEN alpha085 IS NOT NULL AND ABS(alpha085) < 100 THEN 1 ELSE 0 
        END AS alpha085_valid,
        
        CASE 
            WHEN alpha101 IS NOT NULL AND ABS(alpha101) < 10 THEN 1 ELSE 0 
        END AS alpha101_valid,
        
        -- 计算有效因子数量
        (
            CASE WHEN alpha076 IS NOT NULL AND ABS(alpha076) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha077 IS NOT NULL AND ABS(alpha077) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha078 IS NOT NULL AND ABS(alpha078) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha079 IS NOT NULL AND ABS(alpha079) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha080 IS NOT NULL AND ABS(alpha080) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha081 IS NOT NULL AND ABS(alpha081) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha082 IS NOT NULL AND ABS(alpha082) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha083 IS NOT NULL AND ABS(alpha083) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha084 IS NOT NULL AND ABS(alpha084) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha085 IS NOT NULL AND ABS(alpha085) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha086 IS NOT NULL AND ABS(alpha086) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha087 IS NOT NULL AND ABS(alpha087) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha088 IS NOT NULL AND ABS(alpha088) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha089 IS NOT NULL AND ABS(alpha089) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha090 IS NOT NULL AND ABS(alpha090) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha091 IS NOT NULL AND ABS(alpha091) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha092 IS NOT NULL AND ABS(alpha092) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha093 IS NOT NULL AND ABS(alpha093) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha094 IS NOT NULL AND ABS(alpha094) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha095 IS NOT NULL AND ABS(alpha095) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha096 IS NOT NULL AND ABS(alpha096) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha097 IS NOT NULL AND ABS(alpha097) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha098 IS NOT NULL AND ABS(alpha098) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha099 IS NOT NULL AND ABS(alpha099) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha100 IS NOT NULL AND ABS(alpha100) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha101 IS NOT NULL AND ABS(alpha101) < 10 THEN 1 ELSE 0 END
        ) AS valid_factors_count
        
    FROM intermediate_calcs
)

SELECT * FROM final_alpha_factors