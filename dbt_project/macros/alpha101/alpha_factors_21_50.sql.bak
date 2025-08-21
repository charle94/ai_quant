-- Alpha 101 因子实现 (21-50)

-- ========================================
-- Alpha 021
-- ========================================
-- Alpha021 = ((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1) : (((sum(close, 2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : (((1 < (volume / adv20)) || ((volume / adv20) == 1)) ? 1 : (-1))))
{% macro alpha021() %}
    CASE 
        WHEN {{ ts_mean('close', 8) }} + {{ ts_std('close', 8) }} < {{ ts_mean('close', 2) }} THEN -1
        WHEN {{ ts_mean('close', 2) }} < {{ ts_mean('close', 8) }} - {{ ts_std('close', 8) }} THEN 1
        WHEN {{ safe_divide('volume', adv(20)) }} >= 1 THEN 1
        ELSE -1
    END
{% endmacro %}

-- ========================================
-- Alpha 022
-- ========================================
-- Alpha022 = (-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20))))
{% macro alpha022() %}
    -1 * {{ delta(ts_corr('high', 'volume', 5), 5) }} * {{ rank(ts_std('close', 20)) }}
{% endmacro %}

-- ========================================
-- Alpha 023
-- ========================================
-- Alpha023 = (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)
{% macro alpha023() %}
    {{ condition_if(
        ts_mean('high', 20) ~ ' < high',
        '-1 * ' ~ delta('high', 2),
        '0'
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 024
-- ========================================
-- Alpha024 = ((((delta((sum(close, 100) / 100), 100) / delay(close, 100)) < 0.05) || ((delta((sum(close, 100) / 100), 100) / delay(close, 100)) == 0.05)) ? (-1 * (close - ts_min(close, 100))) : (-1 * delta(close, 3)))
{% macro alpha024() %}
    CASE 
        WHEN {{ safe_divide(delta(ts_mean('close', 100), 100), delay('close', 100)) }} <= 0.05 
        THEN -1 * (close - {{ ts_min('close', 100) }})
        ELSE -1 * {{ delta('close', 3) }}
    END
{% endmacro %}

-- ========================================
-- Alpha 025
-- ========================================
-- Alpha025 = rank(((((-1 * returns) * adv20) * vwap) * (high - close)))
{% macro alpha025() %}
    {{ rank('(-1 * returns) * adv20 * vwap * (high - close)') }}
{% endmacro %}

-- ========================================
-- Alpha 026
-- ========================================
-- Alpha026 = (-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))
{% macro alpha026() %}
    -1 * {{ ts_max(ts_corr(ts_rank('volume', 5), ts_rank('high', 5), 5), 3) }}
{% endmacro %}

-- ========================================
-- Alpha 027
-- ========================================
-- Alpha027 = ((0.5 < rank((sum(correlation(rank(volume), rank(vwap), 6), 2) / 2.0))) ? (-1) : 1)
{% macro alpha027() %}
    {{ condition_if(
        rank(ts_mean(ts_corr(rank('volume'), rank('vwap'), 6), 2)) ~ ' > 0.5',
        '-1',
        '1'
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 028
-- ========================================
-- Alpha028 = scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))
{% macro alpha028() %}
    {{ scale(ts_corr(adv(20), 'low', 5) ~ ' + (high + low) / 2 - close') }}
{% endmacro %}

-- ========================================
-- Alpha 029
-- ========================================
-- Alpha029 = (ts_min(product(rank(rank(scale(log(sum(ts_min(rank(rank((-1 * rank(delta(close - 1, 5))))), 2), 1))))), 1), 5) + ts_rank(delay((-1 * returns), 6), 5))
{% macro alpha029() %}
    -- 简化版本，原公式过于复杂
    {{ ts_min('rank_scaled_log_sum', 5) }} + {{ ts_rank(delay('-1 * returns', 6), 5) }}
{% endmacro %}

-- ========================================
-- Alpha 030
-- ========================================
-- Alpha030 = (((1.0 - rank(((sign((close - delay(close, 1))) + sign((delay(close, 1) - delay(close, 2)))) + sign((delay(close, 2) - delay(close, 3)))))) * sum(volume, 5)) / sum(volume, 20))
{% macro alpha030() %}
    (1.0 - {{ rank('sign_sum') }}) * {{ safe_divide(ts_sum('volume', 5), ts_sum('volume', 20)) }}
{% endmacro %}

-- ========================================
-- Alpha 031
-- ========================================
-- Alpha031 = ((rank(rank(rank(decay_linear((-1 * rank(rank(delta(close, 10)))), 10)))) + rank((-1 * delta(close, 3)))) + sign(scale(correlation(adv20, low, 12))))
{% macro alpha031() %}
    {{ rank(rank(rank(decay_linear('-1 * ' ~ rank(rank(delta('close', 10))), 10)))) }} + 
    {{ rank('-1 * ' ~ delta('close', 3)) }} + 
    {{ sign(scale(ts_corr(adv(20), 'low', 12))) }}
{% endmacro %}

-- ========================================
-- Alpha 032
-- ========================================
-- Alpha032 = (scale(((sum(close, 7) / 7) - close)) + (20 * scale(correlation(vwap, delay(close, 5), 230))))
{% macro alpha032() %}
    {{ scale('(' ~ ts_mean('close', 7) ~ ') - close') }} + 
    20 * {{ scale(ts_corr('vwap', delay('close', 5), 230)) }}
{% endmacro %}

-- ========================================
-- Alpha 033
-- ========================================
-- Alpha033 = rank((-1 * ((1 - (open / close))^1)))
{% macro alpha033() %}
    {{ rank('-1 * (1 - ' ~ safe_divide('open', 'close') ~ ')') }}
{% endmacro %}

-- ========================================
-- Alpha 034
-- ========================================
-- Alpha034 = rank(((1 - rank((stddev(returns, 2) / stddev(returns, 5)))) + (1 - rank(delta(close, 1)))))
{% macro alpha034() %}
    {{ rank('(1 - ' ~ rank(safe_divide(ts_std('returns', 2), ts_std('returns', 5))) ~ ') + (1 - ' ~ rank(delta('close', 1)) ~ ')') }}
{% endmacro %}

-- ========================================
-- Alpha 035
-- ========================================
-- Alpha035 = ((Ts_Rank(volume, 32) * (1 - Ts_Rank(((close + high) - low), 16))) * (1 - Ts_Rank(returns, 32)))
{% macro alpha035() %}
    {{ ts_rank('volume', 32) }} * 
    (1 - {{ ts_rank('close + high - low', 16) }}) * 
    (1 - {{ ts_rank('returns', 32) }})
{% endmacro %}

-- ========================================
-- Alpha 036
-- ========================================
-- Alpha036 = (((((2.21 * rank(correlation((close - open), delay(volume, 1), 15))) + (0.7 * rank((open - close)))) + (0.73 * rank(Ts_Rank(delay((-1 * returns), 6), 5)))) + rank(abs(correlation(vwap, adv20, 6)))) + (0.6 * rank((((sum(close, 200) / 200) - open) * (close - open)))))
{% macro alpha036() %}
    2.21 * {{ rank(ts_corr('close - open', delay('volume', 1), 15)) }} +
    0.7 * {{ rank('open - close') }} +
    0.73 * {{ rank(ts_rank(delay('-1 * returns', 6), 5)) }} +
    {{ rank(abs_value(ts_corr('vwap', adv(20), 6))) }} +
    0.6 * {{ rank('(' ~ ts_mean('close', 200) ~ ' - open) * (close - open)') }}
{% endmacro %}

-- ========================================
-- Alpha 037
-- ========================================
-- Alpha037 = (rank(correlation(delay((open - close), 1), close, 200)) + rank((open - close)))
{% macro alpha037() %}
    {{ rank(ts_corr(delay('open - close', 1), 'close', 200)) }} + {{ rank('open - close') }}
{% endmacro %}

-- ========================================
-- Alpha 038
-- ========================================
-- Alpha038 = ((-1 * rank(Ts_Rank(close, 10))) * rank((close / open)))
{% macro alpha038() %}
    (-1 * {{ rank(ts_rank('close', 10)) }}) * {{ rank(safe_divide('close', 'open')) }}
{% endmacro %}

-- ========================================
-- Alpha 039
-- ========================================
-- Alpha039 = ((-1 * rank((delta(close, 7) * (1 - rank(decay_linear((volume / adv20), 9)))))) * (1 + rank(sum(returns, 250))))
{% macro alpha039() %}
    (-1 * {{ rank(delta('close', 7) ~ ' * (1 - ' ~ rank(decay_linear(safe_divide('volume', adv(20)), 9)) ~ ')') }}) * 
    (1 + {{ rank(ts_sum('returns', 250)) }})
{% endmacro %}

-- ========================================
-- Alpha 040
-- ========================================
-- Alpha040 = ((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))
{% macro alpha040() %}
    (-1 * {{ rank(ts_std('high', 10)) }}) * {{ ts_corr('high', 'volume', 10) }}
{% endmacro %}

-- ========================================
-- Alpha 041
-- ========================================
-- Alpha041 = (((high * low)^0.5) - vwap)
{% macro alpha041() %}
    SQRT(high * low) - vwap
{% endmacro %}

-- ========================================
-- Alpha 042
-- ========================================
-- Alpha042 = (rank((vwap - close)) / rank((vwap + close)))
{% macro alpha042() %}
    {{ safe_divide(rank('vwap - close'), rank('vwap + close')) }}
{% endmacro %}

-- ========================================
-- Alpha 043
-- ========================================
-- Alpha043 = (ts_rank((volume / adv20), 20) * ts_rank((-1 * delta(close, 7)), 8))
{% macro alpha043() %}
    {{ ts_rank(safe_divide('volume', adv(20)), 20) }} * {{ ts_rank('-1 * ' ~ delta('close', 7), 8) }}
{% endmacro %}

-- ========================================
-- Alpha 044
-- ========================================
-- Alpha044 = (-1 * correlation(high, rank(volume), 5))
{% macro alpha044() %}
    -1 * {{ ts_corr('high', rank('volume'), 5) }}
{% endmacro %}

-- ========================================
-- Alpha 045
-- ========================================
-- Alpha045 = (-1 * ((rank((sum(delay(close, 5), 20) / 20)) * correlation(close, volume, 2)) * rank(correlation(sum(close, 5), sum(close, 20), 2))))
{% macro alpha045() %}
    -1 * {{ rank(ts_mean(delay('close', 5), 20)) }} * 
    {{ ts_corr('close', 'volume', 2) }} * 
    {{ rank(ts_corr(ts_sum('close', 5), ts_sum('close', 20), 2)) }}
{% endmacro %}

-- ========================================
-- Alpha 046
-- ========================================
-- Alpha046 = ((0.25 < (((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10))) ? (-1) : (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < 0) ? 1 : ((-1 * (close - delay(close, 1))))))
{% macro alpha046() %}
    CASE 
        WHEN ({{ delay('close', 20) }} - {{ delay('close', 10) }}) / 10 - ({{ delay('close', 10) }} - close) / 10 > 0.25 THEN -1
        WHEN ({{ delay('close', 20) }} - {{ delay('close', 10) }}) / 10 - ({{ delay('close', 10) }} - close) / 10 < 0 THEN 1
        ELSE -1 * (close - {{ delay('close', 1) }})
    END
{% endmacro %}

-- ========================================
-- Alpha 047
-- ========================================
-- Alpha047 = ((((rank((1 / close)) * volume) / adv20) * ((high * rank((high - close))) / (sum(high, 5) / 5))) - rank((vwap - delay(vwap, 5))))
{% macro alpha047() %}
    ({{ rank(safe_divide('1', 'close')) }} * volume / {{ adv(20) }} * 
    high * {{ rank('high - close') }} / {{ ts_mean('high', 5) }}) - 
    {{ rank('vwap - ' ~ delay('vwap', 5)) }}
{% endmacro %}

-- ========================================
-- Alpha 048
-- ========================================
-- Alpha048 = (indneutralize(((correlation(delta(close, 1), delta(delay(close, 1), 1), 250) * delta(close, 1)) / close), IndClass.subindustry) / sum(((delta(close, 1) / delay(close, 1))^2), 250))
{% macro alpha048() %}
    -- 简化版本，忽略行业中性化
    {{ safe_divide(
        ts_corr(delta('close', 1), delta(delay('close', 1), 1), 250) ~ ' * ' ~ delta('close', 1) ~ ' / close',
        ts_sum('POWER(' ~ safe_divide(delta('close', 1), delay('close', 1)) ~ ', 2)', 250)
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 049
-- ========================================
-- Alpha049 = (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 * 0.1)) ? 1 : ((-1 * (close - delay(close, 1)))))
{% macro alpha049() %}
    CASE 
        WHEN ({{ delay('close', 20) }} - {{ delay('close', 10) }}) / 10 - ({{ delay('close', 10) }} - close) / 10 < -0.1 
        THEN 1
        ELSE -1 * (close - {{ delay('close', 1) }})
    END
{% endmacro %}

-- ========================================
-- Alpha 050
-- ========================================
-- Alpha050 = (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))
{% macro alpha050() %}
    -1 * {{ ts_max(rank(ts_corr(rank('volume'), rank('vwap'), 5)), 5) }}
{% endmacro %}