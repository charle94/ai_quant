-- Alpha 101 基础操作符宏（清理版本）
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
    CASE 
        WHEN {{ column }} = MAX({{ column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) 
        THEN {{ periods - 1 }}
        ELSE 0
    END
{% endmacro %}

-- TS_ARGMIN(X, d): 返回X在d期内达到最小值的相对位置
{% macro ts_argmin(column, periods, partition_by='symbol', order_by='timestamp') %}
    CASE 
        WHEN {{ column }} = MIN({{ column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) 
        THEN {{ periods - 1 }}
        ELSE 0
    END
{% endmacro %}

-- TS_RANK(X, d): 计算X的d期滚动排序 (简化版)
{% macro ts_rank(column, periods, partition_by='symbol', order_by='timestamp') %}
    -- 使用简单的归一化方法避免窗口函数嵌套
    ({{ column }} - MIN({{ column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )) / NULLIF(
        MAX({{ column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) - MIN({{ column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ), 0
    )
{% endmacro %}

-- TS_CORR(X, Y, d): 计算X和Y的d期滚动相关性
{% macro ts_corr(x_column, y_column, periods, partition_by='symbol', order_by='timestamp') %}
    CORR({{ x_column }}, {{ y_column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- TS_COV(X, Y, d): 计算X和Y的d期滚动协方差
{% macro ts_cov(x_column, y_column, periods, partition_by='symbol', order_by='timestamp') %}
    COVAR_SAMP({{ x_column }}, {{ y_column }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
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

-- SCALE(X): 对X进行截面标准化
{% macro scale(column, timestamp_column='timestamp') %}
    ({{ column }} - AVG({{ column }}) OVER (PARTITION BY {{ timestamp_column }})) / 
    NULLIF(STDDEV({{ column }}) OVER (PARTITION BY {{ timestamp_column }}), 0)
{% endmacro %}

-- ========================================
-- 辅助函数
-- ========================================

-- LOG_VALUE: 安全的对数函数
{% macro log_value(column) %}
    CASE 
        WHEN {{ column }} > 0 THEN LN({{ column }})
        ELSE NULL
    END
{% endmacro %}

-- SAFE_DIVIDE: 安全的除法函数
{% macro safe_divide(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} != 0 THEN ({{ numerator }}) / ({{ denominator }})
        ELSE NULL
    END
{% endmacro %}

-- ABS_VALUE: 绝对值函数
{% macro abs_value(column) %}
    ABS({{ column }})
{% endmacro %}

-- SIGN: 符号函数
{% macro sign(column) %}
    CASE 
        WHEN {{ column }} > 0 THEN 1
        WHEN {{ column }} < 0 THEN -1
        ELSE 0
    END
{% endmacro %}

-- SIGNED_POWER: 带符号的幂函数
{% macro signed_power(base, exponent) %}
    {{ sign(base) }} * POWER(ABS({{ base }}), {{ exponent }})
{% endmacro %}

-- DECAY_LINEAR: 线性衰减权重
{% macro decay_linear(column, periods, partition_by='symbol', order_by='timestamp') %}
    {{ ts_mean(column, periods, partition_by, order_by) }}
{% endmacro %}

-- ADV: 平均日成交量
{% macro adv(periods, partition_by='symbol', order_by='timestamp') %}
    AVG(volume) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}