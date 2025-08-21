-- Alpha 101 因子实现
-- 基于WorldQuant的Alpha 101因子公式

-- ========================================
-- Alpha 001
-- ========================================
-- Alpha001 = RANK(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) - 0.5
{% macro alpha001() %}
    {{ rank('ts_argmax_result') }} - 0.5
{% endmacro %}

-- 辅助宏：SignedPower
{% macro signed_power(base, exponent) %}
    {{ sign(base) }} * POWER(ABS({{ base }}), {{ exponent }})
{% endmacro %}

-- ========================================
-- Alpha 002  
-- ========================================
-- Alpha002 = (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
{% macro alpha002() %}
    -1 * {{ ts_corr(
        rank(delta(log_value('volume'), 2)),
        rank(safe_divide('close - open', 'open')),
        6
    ) }}
{% endmacro %}

-- ========================================
-- Alpha 003
-- ========================================  
-- Alpha003 = (-1 * correlation(rank(open), rank(volume), 10))
{% macro alpha003() %}
    -1 * {{ ts_corr(rank('open'), rank('volume'), 10) }}
{% endmacro %}

-- ========================================
-- Alpha 004
-- ========================================
-- Alpha004 = (-1 * Ts_Rank(rank(low), 9))
{% macro alpha004() %}
    -1 * {{ ts_rank(rank('low'), 9) }}
{% endmacro %}

-- TS_RANK宏定义
{% macro ts_rank(column, periods, partition_by='symbol', order_by='timestamp') %}
    PERCENT_RANK() OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ column }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- ========================================
-- Alpha 005
-- ========================================
-- Alpha005 = (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))
{% macro alpha005() %}
    {{ rank('open - ts_mean_vwap') }} * 
    (-1 * {{ abs_value(rank('close - vwap')) }})
{% endmacro %}

-- ========================================
-- Alpha 006
-- ========================================
-- Alpha006 = (-1 * correlation(open, volume, 10))
{% macro alpha006() %}
    -1 * {{ ts_corr('open', 'volume', 10) }}
{% endmacro %}

-- ========================================
-- Alpha 007
-- ========================================
-- Alpha007 = ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1))
{% macro alpha007() %}
    {{ condition_if(
        'adv20 < volume',
        '(-1 * ' ~ ts_rank(abs_value(delta('close', 7)), 60) ~ ') * ' ~ sign(delta('close', 7)),
        '-1'
    ) }}
{% endmacro %}

-- ADV(d): d期平均成交量
{% macro adv(periods, partition_by='symbol', order_by='timestamp') %}
    {{ ts_mean('volume', periods, partition_by, order_by) }}
{% endmacro %}

-- ========================================
-- Alpha 008
-- ========================================
-- Alpha008 = (-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)), 10))))
{% macro alpha008() %}
    -1 * {{ rank('sum_open_returns - delay_sum_open_returns') }}
{% endmacro %}

-- ========================================
-- Alpha 009
-- ========================================
-- Alpha009 = ((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : ((ts_max(delta(close, 1), 5) < 0) ? delta(close, 1) : (-1 * delta(close, 1))))
{% macro alpha009() %}
    CASE 
        WHEN {{ ts_min(delta('close', 1), 5) }} > 0 THEN {{ delta('close', 1) }}
        WHEN {{ ts_max(delta('close', 1), 5) }} < 0 THEN {{ delta('close', 1) }}
        ELSE -1 * {{ delta('close', 1) }}
    END
{% endmacro %}

-- ========================================
-- Alpha 010
-- ========================================
-- Alpha010 = rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : ((ts_max(delta(close, 1), 4) < 0) ? delta(close, 1) : (-1 * delta(close, 1)))))
{% macro alpha010() %}
    {{ rank('alpha009_logic') }}
{% endmacro %}

-- ========================================
-- Alpha 011
-- ========================================
-- Alpha011 = ((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) * rank(delta(volume, 3)))
{% macro alpha011() %}
    ({{ rank(ts_max('vwap - close', 3)) }} + {{ rank(ts_min('vwap - close', 3)) }}) * 
    {{ rank(delta('volume', 3)) }}
{% endmacro %}

-- ========================================
-- Alpha 012
-- ========================================
-- Alpha012 = (sign(delta(volume, 1)) * (-1 * delta(close, 1)))
{% macro alpha012() %}
    {{ sign(delta('volume', 1)) }} * (-1 * {{ delta('close', 1) }})
{% endmacro %}

-- ========================================
-- Alpha 013
-- ========================================
-- Alpha013 = (-1 * rank(covariance(rank(close), rank(volume), 5)))
{% macro alpha013() %}
    -1 * {{ rank(ts_cov(rank('close'), rank('volume'), 5)) }}
{% endmacro %}

-- ========================================
-- Alpha 014
-- ========================================
-- Alpha014 = ((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))
{% macro alpha014() %}
    (-1 * {{ rank(delta('returns', 3)) }}) * {{ ts_corr('open', 'volume', 10) }}
{% endmacro %}

-- ========================================
-- Alpha 015
-- ========================================
-- Alpha015 = (-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))
{% macro alpha015() %}
    -1 * {{ ts_sum(rank(ts_corr(rank('high'), rank('volume'), 3)), 3) }}
{% endmacro %}

-- ========================================
-- Alpha 016
-- ========================================
-- Alpha016 = (-1 * rank(covariance(rank(high), rank(volume), 5)))
{% macro alpha016() %}
    -1 * {{ rank(ts_cov(rank('high'), rank('volume'), 5)) }}
{% endmacro %}

-- ========================================
-- Alpha 017
-- ========================================
-- Alpha017 = (((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) * rank(ts_rank((volume / adv20), 5)))
{% macro alpha017() %}
    ((-1 * {{ rank(ts_rank('close', 10)) }}) * {{ rank(delta(delta('close', 1), 1)) }}) * 
    {{ rank(ts_rank(safe_divide('volume', 'adv20'), 5)) }}
{% endmacro %}

-- ========================================
-- Alpha 018
-- ========================================
-- Alpha018 = (-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) + correlation(close, open, 10))))
{% macro alpha018() %}
    -1 * {{ rank('stddev_close_open + close_open_diff + corr_close_open') }}
{% endmacro %}

-- ========================================
-- Alpha 019
-- ========================================
-- Alpha019 = ((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) * (1 + rank((1 + sum(returns, 250)))))
{% macro alpha019() %}
    (-1 * {{ sign('close_diff_plus_delta') }}) * 
    (1 + {{ rank('1 + sum_returns_250') }})
{% endmacro %}

-- ========================================
-- Alpha 020
-- ========================================
-- Alpha020 = (((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open - delay(low, 1))))
{% macro alpha020() %}
    ((-1 * {{ rank('open - delay_high_1') }}) * {{ rank('open - delay_close_1') }}) * 
    {{ rank('open - delay_low_1') }}
{% endmacro %}

-- ========================================
-- 复合因子宏 - 用于复杂因子的组合计算
-- ========================================

-- 计算多个因子的加权组合
{% macro alpha_combination(factors_dict, weights_dict=none) %}
    {% set factor_expressions = [] %}
    {% for factor_name, factor_expr in factors_dict.items() %}
        {% if weights_dict and factor_name in weights_dict %}
            {% set weight = weights_dict[factor_name] %}
            {% set weighted_expr = weight ~ " * (" ~ factor_expr ~ ")" %}
        {% else %}
            {% set weighted_expr = factor_expr %}
        {% endif %}
        {% do factor_expressions.append(weighted_expr) %}
    {% endfor %}
    {{ factor_expressions | join(' + ') }}
{% endmacro %}

-- 因子标准化
{% macro normalize_factor(factor_expression, method='zscore') %}
    {% if method == 'zscore' %}
        -- Z-score标准化
        ({{ factor_expression }} - AVG({{ factor_expression }}) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV({{ factor_expression }}) OVER (PARTITION BY timestamp), 0)
    {% elif method == 'rank' %}
        -- 排序标准化
        {{ rank(factor_expression) }}
    {% elif method == 'minmax' %}
        -- 最小最大标准化
        ({{ factor_expression }} - MIN({{ factor_expression }}) OVER (PARTITION BY timestamp)) / 
        NULLIF(
            MAX({{ factor_expression }}) OVER (PARTITION BY timestamp) - 
            MIN({{ factor_expression }}) OVER (PARTITION BY timestamp), 
            0
        )
    {% else %}
        {{ factor_expression }}
    {% endif %}
{% endmacro %}

-- 因子去极值处理
{% macro winsorize_factor(factor_expression, lower_quantile=0.01, upper_quantile=0.99) %}
    CASE 
        WHEN {{ factor_expression }} < PERCENTILE_CONT({{ lower_quantile }}) WITHIN GROUP (ORDER BY {{ factor_expression }}) OVER (PARTITION BY timestamp)
        THEN PERCENTILE_CONT({{ lower_quantile }}) WITHIN GROUP (ORDER BY {{ factor_expression }}) OVER (PARTITION BY timestamp)
        WHEN {{ factor_expression }} > PERCENTILE_CONT({{ upper_quantile }}) WITHIN GROUP (ORDER BY {{ factor_expression }}) OVER (PARTITION BY timestamp)
        THEN PERCENTILE_CONT({{ upper_quantile }}) WITHIN GROUP (ORDER BY {{ factor_expression }}) OVER (PARTITION BY timestamp)
        ELSE {{ factor_expression }}
    END
{% endmacro %}