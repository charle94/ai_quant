-- Alpha 101 因子实现 (51-75)
-- 基于WorldQuant的Alpha 101标准定义

-- ========================================
-- Alpha 051
-- ========================================
-- Alpha051 = (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 * 0.05)) ? 1 : ((-1 * (close - delay(close, 1)))))
{% macro alpha051() %}
    CASE 
        WHEN ({{ delay('close', 20) }} - {{ delay('close', 10) }}) / 10 - ({{ delay('close', 10) }} - close) / 10 < -0.05 
        THEN 1
        ELSE -1 * (close - {{ delay('close', 1) }})
    END
{% endmacro %}

-- ========================================
-- Alpha 052
-- ========================================
-- Alpha052 = ((((-1 * ts_min(low, 5)) + delay(ts_min(low, 5), 5)) * rank(((sum(returns, 240) - sum(returns, 20)) / 220))) * ts_rank(volume, 5))
{% macro alpha052() %}
    ((-1 * {{ ts_min('low', 5) }}) + {{ delay(ts_min('low', 5), 5) }}) * 
    {{ rank('(' ~ ts_sum('returns', 240) ~ ' - ' ~ ts_sum('returns', 20) ~ ') / 220') }} * 
    {{ ts_rank('volume', 5) }}
{% endmacro %}

-- ========================================
-- Alpha 053
-- ========================================
-- Alpha053 = (-1 * delta(((close - low) / (high - low)), 9))
{% macro alpha053() %}
    -1 * {{ delta(safe_divide('close - low', 'high - low'), 9) }}
{% endmacro %}

-- ========================================
-- Alpha 054
-- ========================================
-- Alpha054 = ((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5)))
{% macro alpha054() %}
    {{ safe_divide('-1 * (low - close) * POWER(open, 5)', '(low - high) * POWER(close, 5)') }}
{% endmacro %}

-- ========================================
-- Alpha 055
-- ========================================
-- Alpha055 = (-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low, 12)))), rank(volume), 6))
{% macro alpha055() %}
    -1 * {{ ts_corr(
        rank(safe_divide('close - ' ~ ts_min('low', 12), ts_max('high', 12) ~ ' - ' ~ ts_min('low', 12))),
        rank('volume'),
        6
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 056
-- ========================================
-- Alpha056 = (0 - (1 * (rank((sum(returns, 10) / sum(sum(returns, 2), 3))) * rank((returns * cap)))))
{% macro alpha056() %}
    -- 简化版本，假设cap为市值代理 (volume * close)
    0 - (1 * {{ rank(safe_divide(ts_sum('returns', 10), ts_sum(ts_sum('returns', 2), 3))) }} * 
    {{ rank('returns * volume * close') }})
{% endmacro %}

-- ========================================
-- Alpha 057
-- ========================================
-- Alpha057 = (0 - (1 * ((close - vwap) / decay_linear(rank(ts_argmax(close, 30)), 2))))
{% macro alpha057() %}
    0 - (1 * {{ safe_divide('close - vwap', decay_linear(rank(ts_argmax('close', 30)), 2)) }})
{% endmacro %}

-- ========================================
-- Alpha 058
-- ========================================
-- Alpha058 = (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.sector), volume, 3.92593), 7.89391), 5.50322))
{% macro alpha058() %}
    -- 简化版本，忽略行业中性化
    -1 * {{ ts_rank(decay_linear(ts_corr('vwap', 'volume', 4), 8), 6) }}
{% endmacro %}

-- ========================================
-- Alpha 059
-- ========================================
-- Alpha059 = (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(((vwap * 0.728317) + (vwap * (1 - 0.728317))), volume, 4.25197), 16.2289), 8.19648))
{% macro alpha059() %}
    -- 简化版本
    -1 * {{ ts_rank(decay_linear(ts_corr('vwap', 'volume', 4), 16), 8) }}
{% endmacro %}

-- ========================================
-- Alpha 060
-- ========================================
-- Alpha060 = (0 - (1 * ((2 * scale(rank(((((close - low) - (high - close)) / (high - low)) * volume)))) - scale(rank(ts_argmax(close, 10))))))
{% macro alpha060() %}
    0 - (1 * (2 * {{ scale(rank(safe_divide('(close - low) - (high - close)', 'high - low') ~ ' * volume')) }} - 
    {{ scale(rank(ts_argmax('close', 10))) }}))
{% endmacro %}

-- ========================================
-- Alpha 061
-- ========================================
-- Alpha061 = (rank((vwap - ts_min(vwap, 16.1219))) < rank(correlation(vwap, adv180, 17.9282)))
{% macro alpha061() %}
    CASE 
        WHEN {{ rank('vwap - ' ~ ts_min('vwap', 16)) }} < {{ rank(ts_corr('vwap', adv(180), 18)) }}
        THEN 1 
        ELSE 0 
    END
{% endmacro %}

-- ========================================
-- Alpha 062
-- ========================================
-- Alpha062 = ((rank(correlation(vwap, sum(adv20, 22.4101), 9.91009)) < rank(((rank(open) + rank(open)) < (rank(((high + low) / 2)) + rank(high))))) * -1)
{% macro alpha062() %}
    CASE 
        WHEN {{ rank(ts_corr('vwap', ts_sum(adv(20), 22), 10)) }} < 
             {{ rank('(' ~ rank('open') ~ ' + ' ~ rank('open') ~ ') < (' ~ rank('(high + low) / 2') ~ ' + ' ~ rank('high') ~ ')') }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 063
-- ========================================
-- Alpha063 = ((rank(decay_linear(delta(IndNeutralize(close, IndClass.industry), 2.25164), 8.22237)) - rank(decay_linear(correlation(((vwap * 0.318108) + (open * (1 - 0.318108))), sum(adv180, 37.2467), 13.557), 12.2883))) * -1)
{% macro alpha063() %}
    -- 简化版本
    ({{ rank(decay_linear(delta('close', 2), 8)) }} - 
    {{ rank(decay_linear(ts_corr('vwap * 0.318 + open * 0.682', ts_sum(adv(180), 37), 14), 12)) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 064
-- ========================================
-- Alpha064 = ((rank(correlation(sum(((open * 0.178404) + (low * (1 - 0.178404))), 12.7054), sum(adv120, 12.7054), 16.6208)) < rank(delta(((((high + low) / 2) * 0.178404) + (vwap * (1 - 0.178404))), 3.69741))) * -1)
{% macro alpha064() %}
    CASE 
        WHEN {{ rank(ts_corr(ts_sum('open * 0.178 + low * 0.822', 13), ts_sum(adv(120), 13), 17)) }} < 
             {{ rank(delta('((high + low) / 2) * 0.178 + vwap * 0.822', 4)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 065
-- ========================================
-- Alpha065 = ((rank(correlation(((open * 0.00817205) + (vwap * (1 - 0.00817205))), sum(adv60, 8.6911), 6.40374)) < rank((open - ts_min(open, 13.635)))) * -1)
{% macro alpha065() %}
    CASE 
        WHEN {{ rank(ts_corr('open * 0.008 + vwap * 0.992', ts_sum(adv(60), 9), 6)) }} < 
             {{ rank('open - ' ~ ts_min('open', 14)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 066
-- ========================================
-- Alpha066 = ((rank(decay_linear(delta(vwap, 3.51013), 7.23052)) + Ts_Rank(decay_linear(((((low * 0.96633) + (low * (1 - 0.96633))) - vwap) / (open - ((high + low) / 2))), 11.4157), 6.72611)) * -1)
{% macro alpha066() %}
    ({{ rank(decay_linear(delta('vwap', 4), 7)) }} + 
    {{ ts_rank(decay_linear(safe_divide('low - vwap', 'open - (high + low) / 2'), 11), 7) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 067
-- ========================================
-- Alpha067 = ((rank((high - ts_min(high, 2.14593)))^rank(correlation(IndNeutralize(vwap, IndClass.sector), IndNeutralize(adv20, IndClass.sector), 6.02936))) * -1)
{% macro alpha067() %}
    -- 简化版本
    POWER({{ rank('high - ' ~ ts_min('high', 2)) }}, {{ rank(ts_corr('vwap', adv(20), 6)) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 068
-- ========================================
-- Alpha068 = ((Ts_Rank(correlation(rank(high), rank(adv15), 8.91644), 13.9333) < rank(delta(((close * 0.518371) + (low * (1 - 0.518371))), 1.06157))) * -1)
{% macro alpha068() %}
    CASE 
        WHEN {{ ts_rank(ts_corr(rank('high'), rank(adv(15)), 9), 14) }} < 
             {{ rank(delta('close * 0.518 + low * 0.482', 1)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 069
-- ========================================
-- Alpha069 = ((rank(ts_max(delta(IndNeutralize(vwap, IndClass.industry), 2.72412), 4.79344))^Ts_Rank(correlation(((close * 0.490655) + (vwap * (1 - 0.490655))), adv20, 4.92416), 9.0615)) * -1)
{% macro alpha069() %}
    -- 简化版本
    POWER({{ rank(ts_max(delta('vwap', 3), 5)) }}, {{ ts_rank(ts_corr('close * 0.491 + vwap * 0.509', adv(20), 5), 9) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 070
-- ========================================
-- Alpha070 = ((rank(delta(vwap, 1.29456))^Ts_Rank(correlation(IndNeutralize(close, IndClass.industry), adv50, 17.8256), 17.9171)) * -1)
{% macro alpha070() %}
    -- 简化版本
    POWER({{ rank(delta('vwap', 1)) }}, {{ ts_rank(ts_corr('close', adv(50), 18), 18) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 071
-- ========================================
-- Alpha071 = max(Ts_Rank(decay_linear(correlation(Ts_Rank(close, 3.43976), Ts_Rank(adv180, 12.0647), 18.0175), 4.20501), 15.6948), Ts_Rank(decay_linear((rank(((low + open) - (vwap + vwap)))^2), 16.4662), 4.4388))
{% macro alpha071() %}
    GREATEST(
        {{ ts_rank(decay_linear(ts_corr(ts_rank('close', 3), ts_rank(adv(180), 12), 18), 4), 16) }},
        {{ ts_rank(decay_linear('POWER(' ~ rank('low + open - 2 * vwap') ~ ', 2)', 16), 4) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 072
-- ========================================
-- Alpha072 = (rank(decay_linear(correlation(((high + low) / 2), adv40, 8.93345), 10.1519)) / rank(decay_linear(correlation(Ts_Rank(vwap, 3.72469), Ts_Rank(volume, 18.5188), 6.86671), 2.95011)))
{% macro alpha072() %}
    {{ safe_divide(
        rank(decay_linear(ts_corr('(high + low) / 2', adv(40), 9), 10)),
        rank(decay_linear(ts_corr(ts_rank('vwap', 4), ts_rank('volume', 19), 7), 3))
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 073
-- ========================================
-- Alpha073 = (max(rank(decay_linear(delta(vwap, 4.72775), 2.91864)), Ts_Rank(decay_linear(((delta(((open * 0.147155) + (low * (1 - 0.147155))), 2.03608) / ((open * 0.147155) + (low * (1 - 0.147155)))) * -1), 3.33829), 16.7411)) * -1)
{% macro alpha073() %}
    GREATEST(
        {{ rank(decay_linear(delta('vwap', 5), 3)) }},
        {{ ts_rank(decay_linear(safe_divide(delta('open * 0.147 + low * 0.853', 2), 'open * 0.147 + low * 0.853') ~ ' * -1', 3), 17) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 074
-- ========================================
-- Alpha074 = ((rank(correlation(close, sum(adv30, 37.4843), 15.1365)) < rank(correlation(rank(((high * 0.0261661) + (vwap * (1 - 0.0261661)))), rank(volume), 11.4791))) * -1)
{% macro alpha074() %}
    CASE 
        WHEN {{ rank(ts_corr('close', ts_sum(adv(30), 37), 15)) }} < 
             {{ rank(ts_corr(rank('high * 0.026 + vwap * 0.974'), rank('volume'), 11)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 075
-- ========================================
-- Alpha075 = (rank(correlation(vwap, volume, 4.24304)) < rank(correlation(rank(low), rank(adv50), 12.4413)))
{% macro alpha075() %}
    CASE 
        WHEN {{ rank(ts_corr('vwap', 'volume', 4)) }} < {{ rank(ts_corr(rank('low'), rank(adv(50)), 12)) }}
        THEN 1 
        ELSE 0 
    END
{% endmacro %}