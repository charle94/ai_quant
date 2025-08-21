

-- Alpha 101 高级因子计算 (精选重要因子)
-- 包含一些最有效和最常用的Alpha因子

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

-- 计算一些经典的高频使用因子
classic_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- ========================================
        -- 价格动量类因子
        -- ========================================
        
        -- 价格反转因子 (类似Alpha001的简化版)
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用ROW_NUMBER()来找到最大值的位置
    (5 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (close = 
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )

    )
 - 0.5 AS momentum_reversal,
        
        -- 短期价格动量
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 AS short_momentum,
        
        -- 中期价格动量  
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    close - 
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 AS medium_momentum,
        
        -- 长期价格动量
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    close - 
    LAG(close, 20) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 AS long_momentum,
        
        -- 价格加速度因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 - 
    LAG(
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 AS price_acceleration,
        
        -- ========================================
        -- 成交量价格关系因子
        -- ========================================
        
        -- 量价背离因子 (类似Alpha003)
        -1 * 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS volume_price_divergence,
        
        -- 量价确认因子 (类似Alpha006)
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS volume_price_confirmation,
        
        -- 成交量突破因子 (类似Alpha007)
        CASE 
            WHEN adv20 < volume THEN 
    CASE 
        WHEN 
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 > 0 THEN 1
        WHEN 
    close - 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 < 0 THEN -1
        ELSE 0
    END

            ELSE 0
        END AS volume_breakout,
        
        -- 相对成交量因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END

    )
 AS relative_volume,
        
        -- ========================================
        -- 波动率相关因子
        -- ========================================
        
        -- 波动率排序因子 (类似Alpha004)
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY low
    )

        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
 AS volatility_rank,
        
        -- 价格波动率因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

    )
 AS price_volatility,
        
        -- 收益率波动率因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

    )
 AS return_volatility,
        
        -- 波动率突破因子
        CASE 
            WHEN 
    STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 > 
    STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 THEN 1
            ELSE -1
        END AS volatility_breakout,
        
        -- ========================================
        -- 趋势跟踪因子
        -- ========================================
        
        -- 移动平均趋势因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close - close_ma20
    )
 AS ma_trend,
        
        -- 多重时间框架趋势
        
    CASE 
        WHEN close - close_ma5 > 0 THEN 1
        WHEN close - close_ma5 < 0 THEN -1
        ELSE 0
    END
 + 
    CASE 
        WHEN close_ma5 - close_ma10 > 0 THEN 1
        WHEN close_ma5 - close_ma10 < 0 THEN -1
        ELSE 0
    END
 + 
        
    CASE 
        WHEN close_ma10 - close_ma20 > 0 THEN 1
        WHEN close_ma10 - close_ma20 < 0 THEN -1
        ELSE 0
    END
 AS multi_timeframe_trend,
        
        -- 趋势强度因子
        
    ABS(close - close_ma20)
 / 
    STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS trend_strength,
        
        -- ========================================
        -- 均值回归因子
        -- ========================================
        
        -- 布林带位置因子
        
    CASE 
        WHEN 4 * close_std20 = 0 OR 4 * close_std20 IS NULL THEN NULL
        WHEN ABS(4 * close_std20) < 1e-10 THEN NULL
        ELSE close - (close_ma20 - 2 * close_std20) / 4 * close_std20
    END
 AS bollinger_position,
        
        -- 价格偏离因子
        (close - close_ma20) / close_ma20 AS price_deviation,
        
        -- RSI类因子 (简化版)
        CASE 
            WHEN 
    AVG(CASE WHEN returns > 0 THEN returns ELSE 0 END) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 = 0 THEN 0
            ELSE 
    AVG(CASE WHEN returns > 0 THEN returns ELSE 0 END) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 / 
                 (
    AVG(CASE WHEN returns > 0 THEN returns ELSE 0 END) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 + 
                  
    AVG(CASE WHEN returns < 0 THEN ABS(returns) ELSE 0 END) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
)
        END AS rsi_like_factor,
        
        -- ========================================
        -- 高低价相关因子
        -- ========================================
        
        -- 高低价位置因子
        
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE close - low / high - low
    END
 AS hl_position,
        
        -- 高低价范围因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high - low
    )
 AS hl_range,
        
        -- 影线长度因子
        (high - GREATEST(open, close)) + (LEAST(open, close) - low) AS shadow_length,
        
        -- ========================================
        -- 组合技术因子
        -- ========================================
        
        -- MACD类因子
        
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 - 
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
    )
 AS macd_like,
        
        -- 随机震荡因子
        
    CASE 
        WHEN 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE close - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 / 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )

    END
 AS stoch_like,
        
        -- 威廉指标类因子
        
    CASE 
        WHEN 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - close / 
    MAX(high) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
 - 
    MIN(low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )

    END
 AS williams_like,
        
        -- ========================================
        -- 市场微观结构因子
        -- ========================================
        
        -- 开盘缺口因子
        
    CASE 
        WHEN close_lag1 = 0 OR close_lag1 IS NULL THEN NULL
        WHEN ABS(close_lag1) < 1e-10 THEN NULL
        ELSE open - close_lag1 / close_lag1
    END
 AS opening_gap,
        
        -- 收盘强度因子
        
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE close - open / high - low
    END
 AS closing_strength,
        
        -- 日内动量因子
        
    CASE 
        WHEN open = 0 OR open IS NULL THEN NULL
        WHEN ABS(open) < 1e-10 THEN NULL
        ELSE close - open / open
    END
 AS intraday_momentum,
        
        -- 日内波动率因子
        
    CASE 
        WHEN close = 0 OR close IS NULL THEN NULL
        WHEN ABS(close) < 1e-10 THEN NULL
        ELSE high - low / close
    END
 AS intraday_volatility,
        
        -- ========================================
        -- 跨期套利因子
        -- ========================================
        
        -- 期限结构因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(close, 
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

    )
 AS term_structure,
        
        -- 基差因子 (现货与期货价差的代理)
        close - 
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS basis_like,
        
        -- 便利收益因子
        
    CASE 
        WHEN 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 = 0 OR 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 IS NULL THEN NULL
        WHEN ABS(
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) < 1e-10 THEN NULL
        ELSE close - delay(close, 1) / 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

    END
 - 
        
    AVG(
    CASE 
        WHEN 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 = 0 OR 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
 IS NULL THEN NULL
        WHEN ABS(
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) < 1e-10 THEN NULL
        ELSE close - delay(close, 1) / 
    LAG(close, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

    END
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS convenience_yield,
        
        -- ========================================
        -- 情绪和行为因子
        -- ========================================
        
        -- 投资者情绪因子 (基于成交量)
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END

    )
 * 
    CASE 
        WHEN returns > 0 THEN 1
        WHEN returns < 0 THEN -1
        ELSE 0
    END
 AS sentiment_volume,
        
        -- 羊群效应因子
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY returns
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN adv20 = 0 OR adv20 IS NULL THEN NULL
        WHEN ABS(adv20) < 1e-10 THEN NULL
        ELSE volume / adv20
    END

    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 AS herding_effect,
        
        -- 过度反应因子
        
    CASE 
        WHEN returns > 0 THEN 1
        WHEN returns < 0 THEN -1
        ELSE 0
    END
 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    ABS(returns)

    )
 AS overreaction,
        
        -- ========================================
        -- 质量和盈利能力代理因子
        -- ========================================
        
        -- 价格质量因子 (价格稳定性)
        -1 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    STDDEV(returns) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )

    )
 AS price_quality,
        
        -- 流动性因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE volume * close / high - low
    END

    )
 AS liquidity,
        
        -- 效率因子 (价格发现效率)
        
    ABS(
    -- 使用DuckDB的CORR窗口函数
    CORR(returns, 
    LAG(returns, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
)
 AS efficiency
        
    FROM more_intermediate
),

-- 因子后处理：标准化和去极值
processed_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 对所有因子进行标准化处理
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY momentum_reversal
    )

    
 AS momentum_reversal_norm,
        
    
        -- Z-score标准化
        (short_momentum - AVG(short_momentum) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(short_momentum) OVER (PARTITION BY timestamp), 0)
    
 AS short_momentum_norm,
        
    
        -- Z-score标准化
        (medium_momentum - AVG(medium_momentum) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(medium_momentum) OVER (PARTITION BY timestamp), 0)
    
 AS medium_momentum_norm,
        
    
        -- Z-score标准化
        (long_momentum - AVG(long_momentum) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(long_momentum) OVER (PARTITION BY timestamp), 0)
    
 AS long_momentum_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY price_acceleration
    )

    
 AS price_acceleration_norm,
        
        
    
        -- Z-score标准化
        (volume_price_divergence - AVG(volume_price_divergence) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(volume_price_divergence) OVER (PARTITION BY timestamp), 0)
    
 AS volume_price_divergence_norm,
        
    
        -- Z-score标准化
        (volume_price_confirmation - AVG(volume_price_confirmation) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(volume_price_confirmation) OVER (PARTITION BY timestamp), 0)
    
 AS volume_price_confirmation_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume_breakout
    )

    
 AS volume_breakout_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY relative_volume
    )

    
 AS relative_volume_norm,
        
        
    
        -- Z-score标准化
        (volatility_rank - AVG(volatility_rank) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(volatility_rank) OVER (PARTITION BY timestamp), 0)
    
 AS volatility_rank_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY price_volatility
    )

    
 AS price_volatility_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY return_volatility
    )

    
 AS return_volatility_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volatility_breakout
    )

    
 AS volatility_breakout_norm,
        
        
    
        -- Z-score标准化
        (ma_trend - AVG(ma_trend) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(ma_trend) OVER (PARTITION BY timestamp), 0)
    
 AS ma_trend_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY multi_timeframe_trend
    )

    
 AS multi_timeframe_trend_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY trend_strength
    )

    
 AS trend_strength_norm,
        
        
    
        -- Z-score标准化
        (bollinger_position - AVG(bollinger_position) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(bollinger_position) OVER (PARTITION BY timestamp), 0)
    
 AS bollinger_position_norm,
        
    
        -- Z-score标准化
        (price_deviation - AVG(price_deviation) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(price_deviation) OVER (PARTITION BY timestamp), 0)
    
 AS price_deviation_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY rsi_like_factor
    )

    
 AS rsi_like_factor_norm,
        
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY hl_position
    )

    
 AS hl_position_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY hl_range
    )

    
 AS hl_range_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY shadow_length
    )

    
 AS shadow_length_norm,
        
        
    
        -- Z-score标准化
        (macd_like - AVG(macd_like) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(macd_like) OVER (PARTITION BY timestamp), 0)
    
 AS macd_like_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY stoch_like
    )

    
 AS stoch_like_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY williams_like
    )

    
 AS williams_like_norm,
        
        
    
        -- Z-score标准化
        (opening_gap - AVG(opening_gap) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(opening_gap) OVER (PARTITION BY timestamp), 0)
    
 AS opening_gap_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY closing_strength
    )

    
 AS closing_strength_norm,
        
    
        -- Z-score标准化
        (intraday_momentum - AVG(intraday_momentum) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(intraday_momentum) OVER (PARTITION BY timestamp), 0)
    
 AS intraday_momentum_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY intraday_volatility
    )

    
 AS intraday_volatility_norm,
        
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY term_structure
    )

    
 AS term_structure_norm,
        
    
        -- Z-score标准化
        (basis_like - AVG(basis_like) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(basis_like) OVER (PARTITION BY timestamp), 0)
    
 AS basis_like_norm,
        
    
        -- Z-score标准化
        (convenience_yield - AVG(convenience_yield) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(convenience_yield) OVER (PARTITION BY timestamp), 0)
    
 AS convenience_yield_norm,
        
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY sentiment_volume
    )

    
 AS sentiment_volume_norm,
        
    
        -- Z-score标准化
        (herding_effect - AVG(herding_effect) OVER (PARTITION BY timestamp)) / 
        NULLIF(STDDEV(herding_effect) OVER (PARTITION BY timestamp), 0)
    
 AS herding_effect_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY overreaction
    )

    
 AS overreaction_norm,
        
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY price_quality
    )

    
 AS price_quality_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY liquidity
    )

    
 AS liquidity_norm,
        
    
        -- 排序标准化
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY efficiency
    )

    
 AS efficiency_norm,
        
        -- 保留原始因子值用于调试
        momentum_reversal,
        volume_price_divergence,
        ma_trend,
        bollinger_position,
        opening_gap,
        sentiment_volume
        
    FROM alpha_factors
)

SELECT * FROM processed_factors