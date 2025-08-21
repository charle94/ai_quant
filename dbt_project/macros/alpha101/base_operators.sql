-- Alpha 101 基础操作符宏
-- 实现Alpha 101因子计算中常用的基础操作符

-- ========================================
-- 时间序列操作符
-- ========================================

-- DELAY(X, d): 获取X的d期前的值
{% macro delay(column, periods, partition_by='symbol', order_by='timestamp') %}
    LAG({{ column }}, {{ periods }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
    )
{% endmacro %}

-- DELTA(X, d): 计算X与d期前值的差值
{% macro delta(column, periods, partition_by='symbol', order_by='timestamp') %}
    {{ column }} - {{ delay(column, periods, partition_by, order_by) }}
{% endmacro %}

-- TS_SUM(X, d): 计算X的d期滚动求和
{% macro ts_sum(column, periods, partition_by='symbol', order_by='timestamp') %}
    SUM({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_MEAN(X, d): 计算X的d期滚动均值
{% macro ts_mean(column, periods, partition_by='symbol', order_by='timestamp') %}
    AVG({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_STD(X, d): 计算X的d期滚动标准差
{% macro ts_std(column, periods, partition_by='symbol', order_by='timestamp') %}
    STDDEV({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_MIN(X, d): 计算X的d期滚动最小值
{% macro ts_min(column, periods, partition_by='symbol', order_by='timestamp') %}
    MIN({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_MAX(X, d): 计算X的d期滚动最大值
{% macro ts_max(column, periods, partition_by='symbol', order_by='timestamp') %}
    MAX({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_ARGMAX(X, d): 返回X在d期内达到最大值的相对位置
{% macro ts_argmax(column, periods, partition_by='symbol', order_by='timestamp') %}
    -- 使用ROW_NUMBER()来找到最大值的位置
    ({{ periods }} - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY {{ partition_by }}, 
            ({{ column }} = {{ ts_max(column, periods, partition_by, order_by) }})
            ORDER BY {{ order_by }} DESC
        ) - 1
    )
{% endmacro %}

-- TS_ARGMIN(X, d): 返回X在d期内达到最小值的相对位置
{% macro ts_argmin(column, periods, partition_by='symbol', order_by='timestamp') %}
    ({{ periods }} - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY {{ partition_by }}, 
            ({{ column }} = {{ ts_min(column, periods, partition_by, order_by) }})
            ORDER BY {{ order_by }} DESC
        ) - 1
    )
{% endmacro %}

-- ========================================
-- 截面操作符
-- ========================================

-- RANK(X): 对X进行截面排序，返回排名百分位
{% macro rank(column, timestamp_column='timestamp') %}
    PERCENT_RANK() OVER (
        PARTITION BY {{ timestamp_column }}
        ORDER BY {{ column }}
    )
{% endmacro %}

-- ========================================
-- 数学操作符
-- ========================================

-- SIGN(X): 返回X的符号
{% macro sign(column) %}
    CASE 
        WHEN {{ column }} > 0 THEN 1
        WHEN {{ column }} < 0 THEN -1
        ELSE 0
    END
{% endmacro %}

-- ABS(X): 返回X的绝对值
{% macro abs_value(column) %}
    ABS({{ column }})
{% endmacro %}

-- LOG(X): 返回X的自然对数
{% macro log_value(column) %}
    CASE 
        WHEN {{ column }} > 0 THEN LN({{ column }})
        ELSE NULL
    END
{% endmacro %}

-- ========================================
-- 条件操作符
-- ========================================

-- 条件函数：IF(condition, true_value, false_value)
{% macro condition_if(condition, true_value, false_value) %}
    CASE 
        WHEN {{ condition }} THEN {{ true_value }}
        ELSE {{ false_value }}
    END
{% endmacro %}

-- ========================================
-- 相关性和协方差操作符
-- ========================================

-- CORR(X, Y, d): 计算X和Y的d期滚动相关系数
{% macro ts_corr(x_column, y_column, periods, partition_by='symbol', order_by='timestamp') %}
    -- 使用DuckDB的CORR窗口函数
    CORR({{ x_column }}, {{ y_column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- COV(X, Y, d): 计算X和Y的d期滚动协方差
{% macro ts_cov(x_column, y_column, periods, partition_by='symbol', order_by='timestamp') %}
    COVAR_SAMP({{ x_column }}, {{ y_column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- ========================================
-- 回归操作符
-- ========================================

-- REGBETA(Y, X, d): 计算Y对X的d期滚动回归系数
{% macro ts_regbeta(y_column, x_column, periods, partition_by='symbol', order_by='timestamp') %}
    -- Beta = Cov(X,Y) / Var(X)
    {{ ts_cov(x_column, y_column, periods, partition_by, order_by) }} / 
    NULLIF(
        VAR_SAMP({{ x_column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ), 0
    )
{% endmacro %}

-- REGRESI(Y, X, d): 计算Y对X回归的d期滚动残差
{% macro ts_regresi(y_column, x_column, periods, partition_by='symbol', order_by='timestamp') %}
    {{ y_column }} - (
        {{ ts_mean(y_column, periods, partition_by, order_by) }} + 
        {{ ts_regbeta(y_column, x_column, periods, partition_by, order_by) }} * 
        ({{ x_column }} - {{ ts_mean(x_column, periods, partition_by, order_by) }})
    )
{% endmacro %}

-- ========================================
-- 高级操作符
-- ========================================

-- DECAYLINEAR(X, d): 线性衰减加权
{% macro decay_linear(column, periods, partition_by='symbol', order_by='timestamp') %}
    -- 实现线性衰减权重的加权平均
    SUM(
        {{ column }} * 
        ({{ periods }} - ROW_NUMBER() OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }} DESC
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    ({{ periods }} * ({{ periods }} + 1) / 2)
{% endmacro %}

-- SCALE(X): 对X进行标准化，使其和为1
{% macro scale(column, timestamp_column='timestamp') %}
    {{ column }} / NULLIF(
        SUM(ABS({{ column }})) OVER (PARTITION BY {{ timestamp_column }}), 0
    )
{% endmacro %}

-- ========================================
-- 辅助函数
-- ========================================

-- 处理NULL值和无穷大值
{% macro safe_divide(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        WHEN ABS({{ denominator }}) < 1e-10 THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}

-- 限制数值范围，防止极值
{% macro clip_values(column, min_value=null, max_value=null) %}
    {% if min_value is not none and max_value is not none %}
        GREATEST({{ min_value }}, LEAST({{ max_value }}, {{ column }}))
    {% elif min_value is not none %}
        GREATEST({{ min_value }}, {{ column }})
    {% elif max_value is not none %}
        LEAST({{ max_value }}, {{ column }})
    {% else %}
        {{ column }}
    {% endif %}
{% endmacro %}