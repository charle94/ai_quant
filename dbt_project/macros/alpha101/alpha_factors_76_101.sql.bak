-- Alpha 101 因子实现 (76-101)
-- 基于WorldQuant的Alpha 101标准定义

-- ========================================
-- Alpha 076
-- ========================================
-- Alpha076 = (max(rank(decay_linear(delta(vwap, 1.24383), 11.8259)), Ts_Rank(decay_linear(Ts_Rank(correlation(IndNeutralize(low, IndClass.sector), adv81, 8.14941), 19.569), 17.1543), 19.383)) * -1)
{% macro alpha076() %}
    GREATEST(
        {{ rank(decay_linear(delta('vwap', 1), 12)) }},
        {{ ts_rank(decay_linear(ts_rank(ts_corr('low', adv(81), 8), 20), 17), 19) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 077
-- ========================================
-- Alpha077 = min(rank(decay_linear(((((high + low) / 2) + high) - (vwap + high)), 20.0451)), rank(decay_linear(correlation(((high + low) / 2), adv40, 3.1614), 5.64125)))
{% macro alpha077() %}
    LEAST(
        {{ rank(decay_linear('((high + low) / 2 + high) - (vwap + high)', 20)) }},
        {{ rank(decay_linear(ts_corr('(high + low) / 2', adv(40), 3), 6)) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 078
-- ========================================
-- Alpha078 = (rank(correlation(sum(((low * 0.352233) + (vwap * (1 - 0.352233))), 19.7428), sum(adv40, 19.7428), 6.83313))^rank(correlation(rank(vwap), rank(volume), 5.77492)))
{% macro alpha078() %}
    POWER(
        {{ rank(ts_corr(ts_sum('low * 0.352 + vwap * 0.648', 20), ts_sum(adv(40), 20), 7)) }},
        {{ rank(ts_corr(rank('vwap'), rank('volume'), 6)) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 079
-- ========================================
-- Alpha079 = (rank(delta(IndNeutralize(((close * 0.60733) + (open * (1 - 0.60733))), IndClass.sector), 1.23438)) < rank(correlation(Ts_Rank(vwap, 3.60973), Ts_Rank(adv150, 9.18637), 14.6644)))
{% macro alpha079() %}
    CASE 
        WHEN {{ rank(delta('close * 0.607 + open * 0.393', 1)) }} < 
             {{ rank(ts_corr(ts_rank('vwap', 4), ts_rank(adv(150), 9), 15)) }}
        THEN 1 
        ELSE 0 
    END
{% endmacro %}

-- ========================================
-- Alpha 080
-- ========================================
-- Alpha080 = ((rank(Sign(delta(IndNeutralize(((open * 0.868128) + (high * (1 - 0.868128))), IndClass.industry), 4.04545)))^Ts_Rank(correlation(high, adv10, 5.11456), 5.53756)) * -1)
{% macro alpha080() %}
    POWER(
        {{ rank(sign(delta('open * 0.868 + high * 0.132', 4))) }},
        {{ ts_rank(ts_corr('high', adv(10), 5), 6) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 081
-- ========================================
-- Alpha081 = ((rank(Log(product(rank((rank(correlation(vwap, sum(adv10, 49.6054), 8.47743))^4)), 14.9655))) < rank(correlation(rank(vwap), rank(volume), 5.07914))) * -1)
{% macro alpha081() %}
    -- 简化版本
    CASE 
        WHEN {{ rank(log_value('POWER(' ~ rank(ts_corr('vwap', ts_sum(adv(10), 50), 8)) ~ ', 4)')) }} < 
             {{ rank(ts_corr(rank('vwap'), rank('volume'), 5)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 082
-- ========================================
-- Alpha082 = (min(rank(decay_linear(delta(open, 1.46063), 14.8717)), Ts_Rank(decay_linear(correlation(IndNeutralize(volume, IndClass.sector), ((open * 0.634196) + (open * (1 - 0.634196))), 17.4842), 6.92131), 13.4283)) * -1)
{% macro alpha082() %}
    LEAST(
        {{ rank(decay_linear(delta('open', 1), 15)) }},
        {{ ts_rank(decay_linear(ts_corr('volume', 'open', 17), 7), 13) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 083
-- ========================================
-- Alpha083 = ((rank(delay(((high - low) / (sum(close, 5) / 5)), 2)) * rank(rank(volume))) / (((high - low) / (sum(close, 5) / 5)) / (vwap - close)))
{% macro alpha083() %}
    {{ safe_divide(
        rank(delay(safe_divide('high - low', ts_mean('close', 5)), 2)) ~ ' * ' ~ rank(rank('volume')),
        safe_divide(safe_divide('high - low', ts_mean('close', 5)), 'vwap - close')
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 084
-- ========================================
-- Alpha084 = SignedPower(Ts_Rank((vwap - ts_max(vwap, 15.3217)), 20.7127), delta(close, 4.96796))
{% macro alpha084() %}
    {{ signed_power(ts_rank('vwap - ' ~ ts_max('vwap', 15), 21), delta('close', 5)) }}
{% endmacro %}

-- ========================================
-- Alpha 085
-- ========================================
-- Alpha085 = (rank(correlation(((high * 0.876703) + (close * (1 - 0.876703))), adv30, 9.61331))^rank(correlation(Ts_Rank(((high + low) / 2), 3.70596), Ts_Rank(volume, 10.1595), 7.11408)))
{% macro alpha085() %}
    POWER(
        {{ rank(ts_corr('high * 0.877 + close * 0.123', adv(30), 10)) }},
        {{ rank(ts_corr(ts_rank('(high + low) / 2', 4), ts_rank('volume', 10), 7)) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 086
-- ========================================
-- Alpha086 = ((Ts_Rank(correlation(close, sum(adv20, 14.7444), 6.00049), 20.4195) < rank(((open + close) - (vwap + open)))) * -1)
{% macro alpha086() %}
    CASE 
        WHEN {{ ts_rank(ts_corr('close', ts_sum(adv(20), 15), 6), 20) }} < 
             {{ rank('(open + close) - (vwap + open)') }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 087
-- ========================================
-- Alpha087 = (max(rank(decay_linear(delta(((close * 0.369701) + (vwap * (1 - 0.369701))), 1.91233), 2.65461)), Ts_Rank(decay_linear(abs(correlation(IndNeutralize(adv81, IndClass.industry), close, 13.4132)), 4.89768), 14.4535)) * -1)
{% macro alpha087() %}
    GREATEST(
        {{ rank(decay_linear(delta('close * 0.370 + vwap * 0.630', 2), 3)) }},
        {{ ts_rank(decay_linear(abs_value(ts_corr(adv(81), 'close', 13)), 5), 14) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 088
-- ========================================
-- Alpha088 = min(rank(decay_linear(((rank(open) + rank(low)) - (rank(high) + rank(close))), 8.06882)), Ts_Rank(decay_linear(correlation(Ts_Rank(close, 8.44728), Ts_Rank(adv60, 20.6966), 8.01183), 6.65053), 2.61957))
{% macro alpha088() %}
    LEAST(
        {{ rank(decay_linear('(' ~ rank('open') ~ ' + ' ~ rank('low') ~ ') - (' ~ rank('high') ~ ' + ' ~ rank('close') ~ ')', 8)) }},
        {{ ts_rank(decay_linear(ts_corr(ts_rank('close', 8), ts_rank(adv(60), 21), 8), 7), 3) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 089
-- ========================================
-- Alpha089 = (Ts_Rank(decay_linear(correlation(((low * 0.967285) + (low * (1 - 0.967285))), adv10, 6.94279), 5.51607), 3.79744) - Ts_Rank(decay_linear(delta(IndNeutralize(vwap, IndClass.industry), 3.48158), 10.1466), 15.3012))
{% macro alpha089() %}
    {{ ts_rank(decay_linear(ts_corr('low', adv(10), 7), 6), 4) }} - 
    {{ ts_rank(decay_linear(delta('vwap', 3), 10), 15) }}
{% endmacro %}

-- ========================================
-- Alpha 090
-- ========================================
-- Alpha090 = ((rank((close - ts_max(close, 4.66719)))^Ts_Rank(correlation(IndNeutralize(adv40, IndClass.subindustry), low, 5.38375), 3.21856)) * -1)
{% macro alpha090() %}
    POWER(
        {{ rank('close - ' ~ ts_max('close', 5)) }},
        {{ ts_rank(ts_corr(adv(40), 'low', 5), 3) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 091
-- ========================================
-- Alpha091 = ((Ts_Rank(decay_linear(decay_linear(correlation(IndNeutralize(close, IndClass.industry), volume, 9.74928), 16.398), 3.83219), 4.8667) - rank(decay_linear(correlation(rank(close), rank(adv50), 4.37226), 3.80406))) * -1)
{% macro alpha091() %}
    ({{ ts_rank(decay_linear(decay_linear(ts_corr('close', 'volume', 10), 16), 4), 5) }} - 
    {{ rank(decay_linear(ts_corr(rank('close'), rank(adv(50)), 4), 4)) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 092
-- ========================================
-- Alpha092 = min(Ts_Rank(decay_linear(((((high + low) / 2) + close) < (low + open)), 14.7221), 18.8683), Ts_Rank(decay_linear(correlation(rank(low), rank(adv30), 7.58555), 6.94024), 6.80584))
{% macro alpha092() %}
    LEAST(
        {{ ts_rank(decay_linear('CASE WHEN ((high + low) / 2 + close) < (low + open) THEN 1 ELSE 0 END', 15), 19) }},
        {{ ts_rank(decay_linear(ts_corr(rank('low'), rank(adv(30)), 8), 7), 7) }}
    )
{% endmacro %}

-- ========================================
-- Alpha 093
-- ========================================
-- Alpha093 = (Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.industry), adv81, 17.4193), 19.848), 7.54455) / rank(decay_linear(delta(((close * 0.524434) + (vwap * (1 - 0.524434))), 2.77377), 16.2664)))
{% macro alpha093() %}
    {{ safe_divide(
        ts_rank(decay_linear(ts_corr('vwap', adv(81), 17), 20), 8),
        rank(decay_linear(delta('close * 0.524 + vwap * 0.476', 3), 16))
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 094
-- ========================================
-- Alpha094 = ((rank((vwap - ts_min(vwap, 11.5783)))^Ts_Rank(correlation(Ts_Rank(vwap, 19.6462), Ts_Rank(adv60, 4.02992), 18.0926), 2.70756)) * -1)
{% macro alpha094() %}
    POWER(
        {{ rank('vwap - ' ~ ts_min('vwap', 12)) }},
        {{ ts_rank(ts_corr(ts_rank('vwap', 20), ts_rank(adv(60), 4), 18), 3) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 095
-- ========================================
-- Alpha095 = (rank((open - ts_min(open, 12.4105))) < Ts_Rank((rank(correlation(sum(((high + low) / 2), 19.1351), sum(adv40, 19.1351), 12.8742))^5), 11.7584))
{% macro alpha095() %}
    CASE 
        WHEN {{ rank('open - ' ~ ts_min('open', 12)) }} < 
             {{ ts_rank('POWER(' ~ rank(ts_corr(ts_sum('(high + low) / 2', 19), ts_sum(adv(40), 19), 13)) ~ ', 5)', 12) }}
        THEN 1 
        ELSE 0 
    END
{% endmacro %}

-- ========================================
-- Alpha 096
-- ========================================
-- Alpha096 = (max(Ts_Rank(decay_linear(correlation(rank(vwap), rank(volume), 3.83878), 4.16783), 8.38151), Ts_Rank(decay_linear(Ts_ArgMax(correlation(Ts_Rank(close, 7.45404), Ts_Rank(adv60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143)) * -1)
{% macro alpha096() %}
    GREATEST(
        {{ ts_rank(decay_linear(ts_corr(rank('vwap'), rank('volume'), 4), 4), 8) }},
        {{ ts_rank(decay_linear(ts_argmax(ts_corr(ts_rank('close', 7), ts_rank(adv(60), 4), 4), 13), 14), 13) }}
    ) * -1
{% endmacro %}

-- ========================================
-- Alpha 097
-- ========================================
-- Alpha097 = ((rank(decay_linear(delta(IndNeutralize(((low * 0.721001) + (vwap * (1 - 0.721001))), IndClass.industry), 3.3705), 20.4523)) - Ts_Rank(decay_linear(Ts_Rank(correlation(Ts_Rank(low, 7.87871), Ts_Rank(adv60, 17.255), 4.97547), 18.5925), 15.7152), 6.71659)) * -1)
{% macro alpha097() %}
    ({{ rank(decay_linear(delta('low * 0.721 + vwap * 0.279', 3), 20)) }} - 
    {{ ts_rank(decay_linear(ts_rank(ts_corr(ts_rank('low', 8), ts_rank(adv(60), 17), 5), 19), 16), 7) }}) * -1
{% endmacro %}

-- ========================================
-- Alpha 098
-- ========================================
-- Alpha098 = (rank(decay_linear(correlation(vwap, sum(adv5, 26.4719), 4.58418), 7.18088)) - rank(decay_linear(Ts_Rank(Ts_ArgMin(correlation(rank(open), rank(adv15), 20.8187), 8.62571), 6.95668), 8.07206)))
{% macro alpha098() %}
    {{ rank(decay_linear(ts_corr('vwap', ts_sum(adv(5), 26), 5), 7)) }} - 
    {{ rank(decay_linear(ts_rank(ts_argmin(ts_corr(rank('open'), rank(adv(15)), 21), 9), 7), 8)) }}
{% endmacro %}

-- ========================================
-- Alpha 099
-- ========================================
-- Alpha099 = ((rank(correlation(sum(((high + low) / 2), 19.8975), sum(adv60, 19.8975), 8.8136)) < rank(correlation(low, volume, 6.28259))) * -1)
{% macro alpha099() %}
    CASE 
        WHEN {{ rank(ts_corr(ts_sum('(high + low) / 2', 20), ts_sum(adv(60), 20), 9)) }} < 
             {{ rank(ts_corr('low', 'volume', 6)) }}
        THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- ========================================
-- Alpha 100
-- ========================================
-- Alpha100 = (0 - (1 * (((1.5 * scale(IndNeutralize(IndNeutralize(rank(((((close - low) - (high - close)) / (high - low)) * volume)), IndClass.subindustry), IndClass.sector))) - scale(IndNeutralize((correlation(close, rank(adv20), 5) - rank(ts_argmin(close, 30))), IndClass.sector))) * (volume / adv20))))
{% macro alpha100() %}
    -- 简化版本，忽略行业中性化
    0 - (1 * (1.5 * {{ scale(rank(safe_divide('(close - low) - (high - close)', 'high - low') ~ ' * volume')) }} - 
    {{ scale(ts_corr('close', rank(adv(20)), 5) ~ ' - ' ~ rank(ts_argmin('close', 30))) }} * 
    {{ safe_divide('volume', adv(20)) }}))
{% endmacro %}

-- ========================================
-- Alpha 101
-- ========================================
-- Alpha101 = ((close - open) / ((high - low) + .001))
{% macro alpha101() %}
    {{ safe_divide('close - open', 'high - low + 0.001') }}
{% endmacro %}

-- ========================================
-- 额外的高级操作符 (支持复杂因子)
-- ========================================

-- PRODUCT: 滚动乘积
{% macro ts_product(column, periods, partition_by='symbol', order_by='timestamp') %}
    EXP(SUM(LN(ABS({{ column }}))) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )) * 
    CASE 
        WHEN MOD(SUM(CASE WHEN {{ column }} < 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ), 2) = 1 THEN -1 
        ELSE 1 
    END
{% endmacro %}

-- INDNEUTRALIZE: 行业中性化 (简化为去均值)
{% macro indneutralize(column, timestamp_column='timestamp') %}
    {{ column }} - AVG({{ column }}) OVER (PARTITION BY {{ timestamp_column }})
{% endmacro %}

-- DECAY_LINEAR 的改进版本
{% macro decay_linear_advanced(column, periods, partition_by='symbol', order_by='timestamp') %}
    SUM(
        {{ column }} * 
        ({{ periods }} + 1 - ROW_NUMBER() OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }} DESC
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ))
    ) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    ) / 
    ({{ periods }} * ({{ periods }} + 1) / 2)
{% endmacro %}

-- 高级排序函数：支持多列排序
{% macro multi_rank(columns_list, timestamp_column='timestamp') %}
    {% set rank_expressions = [] %}
    {% for column in columns_list %}
        {% do rank_expressions.append('RANK() OVER (PARTITION BY ' ~ timestamp_column ~ ' ORDER BY ' ~ column ~ ')') %}
    {% endfor %}
    ({{ rank_expressions | join(' + ') }}) / {{ columns_list | length }}
{% endmacro %}

-- 条件排序：根据条件进行不同的排序
{% macro conditional_rank(column, condition, timestamp_column='timestamp') %}
    CASE 
        WHEN {{ condition }} 
        THEN RANK() OVER (PARTITION BY {{ timestamp_column }}, ({{ condition }}) ORDER BY {{ column }})
        ELSE RANK() OVER (PARTITION BY {{ timestamp_column }}, NOT ({{ condition }}) ORDER BY {{ column }} DESC)
    END
{% endmacro %}

-- 分位数函数
{% macro percentile(column, p, timestamp_column='timestamp') %}
    PERCENTILE_CONT({{ p }}) WITHIN GROUP (ORDER BY {{ column }}) OVER (PARTITION BY {{ timestamp_column }})
{% endmacro %}