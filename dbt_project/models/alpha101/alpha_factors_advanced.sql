{{ config(materialized='table') }}

-- Alpha 101 高级因子计算 (精选重要因子)
-- 包含一些最有效和最常用的Alpha因子

WITH base_data AS (
    SELECT * FROM {{ ref('alpha_base_data') }}
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
        {{ rank(ts_argmax('close', 5)) }} - 0.5 AS momentum_reversal,
        
        -- 短期价格动量
        {{ rank(delta('close', 1)) }} AS short_momentum,
        
        -- 中期价格动量  
        {{ rank(delta('close', 5)) }} AS medium_momentum,
        
        -- 长期价格动量
        {{ rank(delta('close', 20)) }} AS long_momentum,
        
        -- 价格加速度因子
        {{ rank(delta(delta('close', 1), 1)) }} AS price_acceleration,
        
        -- ========================================
        -- 成交量价格关系因子
        -- ========================================
        
        -- 量价背离因子 (类似Alpha003)
        -1 * {{ ts_corr(rank('open'), rank('volume'), 10) }} AS volume_price_divergence,
        
        -- 量价确认因子 (类似Alpha006)
        {{ ts_corr('close', 'volume', 10) }} AS volume_price_confirmation,
        
        -- 成交量突破因子 (类似Alpha007)
        CASE 
            WHEN adv20 < volume THEN {{ sign(delta('close', 1)) }}
            ELSE 0
        END AS volume_breakout,
        
        -- 相对成交量因子
        {{ rank(safe_divide('volume', 'adv20')) }} AS relative_volume,
        
        -- ========================================
        -- 波动率相关因子
        -- ========================================
        
        -- 波动率排序因子 (类似Alpha004)
        -1 * {{ ts_rank(rank('low'), 9) }} AS volatility_rank,
        
        -- 价格波动率因子
        {{ rank(ts_std('close', 20)) }} AS price_volatility,
        
        -- 收益率波动率因子
        {{ rank(ts_std('returns', 20)) }} AS return_volatility,
        
        -- 波动率突破因子
        CASE 
            WHEN {{ ts_std('close', 5) }} > {{ ts_std('close', 20) }} THEN 1
            ELSE -1
        END AS volatility_breakout,
        
        -- ========================================
        -- 趋势跟踪因子
        -- ========================================
        
        -- 移动平均趋势因子
        {{ rank('close - close_ma20') }} AS ma_trend,
        
        -- 多重时间框架趋势
        {{ sign('close - close_ma5') }} + {{ sign('close_ma5 - close_ma10') }} + 
        {{ sign('close_ma10 - close_ma20') }} AS multi_timeframe_trend,
        
        -- 趋势强度因子
        {{ abs_value('close - close_ma20') }} / {{ ts_std('close', 20) }} AS trend_strength,
        
        -- ========================================
        -- 均值回归因子
        -- ========================================
        
        -- 布林带位置因子
        {{ safe_divide('close - (close_ma20 - 2 * close_std20)', '4 * close_std20') }} AS bollinger_position,
        
        -- 价格偏离因子
        (close - close_ma20) / close_ma20 AS price_deviation,
        
        -- RSI类因子 (简化版)
        CASE 
            WHEN {{ ts_mean('CASE WHEN returns > 0 THEN returns ELSE 0 END', 14) }} = 0 THEN 0
            ELSE {{ ts_mean('CASE WHEN returns > 0 THEN returns ELSE 0 END', 14) }} / 
                 ({{ ts_mean('CASE WHEN returns > 0 THEN returns ELSE 0 END', 14) }} + 
                  {{ ts_mean('CASE WHEN returns < 0 THEN ABS(returns) ELSE 0 END', 14) }})
        END AS rsi_like_factor,
        
        -- ========================================
        -- 高低价相关因子
        -- ========================================
        
        -- 高低价位置因子
        {{ safe_divide('close - low', 'high - low') }} AS hl_position,
        
        -- 高低价范围因子
        {{ rank('high - low') }} AS hl_range,
        
        -- 影线长度因子
        (high - GREATEST(open, close)) + (LEAST(open, close) - low) AS shadow_length,
        
        -- ========================================
        -- 组合技术因子
        -- ========================================
        
        -- MACD类因子
        {{ ts_mean('close', 12) }} - {{ ts_mean('close', 26) }} AS macd_like,
        
        -- 随机震荡因子
        {{ safe_divide('close - ' ~ ts_min('low', 14), ts_max('high', 14) ~ ' - ' ~ ts_min('low', 14)) }} AS stoch_like,
        
        -- 威廉指标类因子
        {{ safe_divide(ts_max('high', 14) ~ ' - close', ts_max('high', 14) ~ ' - ' ~ ts_min('low', 14)) }} AS williams_like,
        
        -- ========================================
        -- 市场微观结构因子
        -- ========================================
        
        -- 开盘缺口因子
        {{ safe_divide('open - close_lag1', 'close_lag1') }} AS opening_gap,
        
        -- 收盘强度因子
        {{ safe_divide('close - open', 'high - low') }} AS closing_strength,
        
        -- 日内动量因子
        {{ safe_divide('close - open', 'open') }} AS intraday_momentum,
        
        -- 日内波动率因子
        {{ safe_divide('high - low', 'close') }} AS intraday_volatility,
        
        -- ========================================
        -- 跨期套利因子
        -- ========================================
        
        -- 期限结构因子
        {{ rank(ts_corr('close', delay('close', 5), 20)) }} AS term_structure,
        
        -- 基差因子 (现货与期货价差的代理)
        close - {{ ts_mean('close', 5) }} AS basis_like,
        
        -- 便利收益因子
        {{ safe_divide('close - delay(close, 1)', delay('close', 1)) }} - 
        {{ ts_mean(safe_divide('close - delay(close, 1)', delay('close', 1)), 20) }} AS convenience_yield,
        
        -- ========================================
        -- 情绪和行为因子
        -- ========================================
        
        -- 投资者情绪因子 (基于成交量)
        {{ rank(safe_divide('volume', 'adv20')) }} * {{ sign('returns') }} AS sentiment_volume,
        
        -- 羊群效应因子
        {{ ts_corr(rank('returns'), rank(safe_divide('volume', 'adv20')), 10) }} AS herding_effect,
        
        -- 过度反应因子
        {{ sign('returns') }} * {{ rank(abs_value('returns')) }} AS overreaction,
        
        -- ========================================
        -- 质量和盈利能力代理因子
        -- ========================================
        
        -- 价格质量因子 (价格稳定性)
        -1 * {{ rank(ts_std('returns', 60)) }} AS price_quality,
        
        -- 流动性因子
        {{ rank(safe_divide('volume * close', 'high - low')) }} AS liquidity,
        
        -- 效率因子 (价格发现效率)
        {{ abs_value(ts_corr('returns', delay('returns', 1), 20)) }} AS efficiency
        
    FROM more_intermediate
),

-- 因子后处理：标准化和去极值
processed_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 对所有因子进行标准化处理
        {{ normalize_factor('momentum_reversal', 'rank') }} AS momentum_reversal_norm,
        {{ normalize_factor('short_momentum', 'zscore') }} AS short_momentum_norm,
        {{ normalize_factor('medium_momentum', 'zscore') }} AS medium_momentum_norm,
        {{ normalize_factor('long_momentum', 'zscore') }} AS long_momentum_norm,
        {{ normalize_factor('price_acceleration', 'rank') }} AS price_acceleration_norm,
        
        {{ normalize_factor('volume_price_divergence', 'zscore') }} AS volume_price_divergence_norm,
        {{ normalize_factor('volume_price_confirmation', 'zscore') }} AS volume_price_confirmation_norm,
        {{ normalize_factor('volume_breakout', 'rank') }} AS volume_breakout_norm,
        {{ normalize_factor('relative_volume', 'rank') }} AS relative_volume_norm,
        
        {{ normalize_factor('volatility_rank', 'zscore') }} AS volatility_rank_norm,
        {{ normalize_factor('price_volatility', 'rank') }} AS price_volatility_norm,
        {{ normalize_factor('return_volatility', 'rank') }} AS return_volatility_norm,
        {{ normalize_factor('volatility_breakout', 'rank') }} AS volatility_breakout_norm,
        
        {{ normalize_factor('ma_trend', 'zscore') }} AS ma_trend_norm,
        {{ normalize_factor('multi_timeframe_trend', 'rank') }} AS multi_timeframe_trend_norm,
        {{ normalize_factor('trend_strength', 'rank') }} AS trend_strength_norm,
        
        {{ normalize_factor('bollinger_position', 'zscore') }} AS bollinger_position_norm,
        {{ normalize_factor('price_deviation', 'zscore') }} AS price_deviation_norm,
        {{ normalize_factor('rsi_like_factor', 'rank') }} AS rsi_like_factor_norm,
        
        {{ normalize_factor('hl_position', 'rank') }} AS hl_position_norm,
        {{ normalize_factor('hl_range', 'rank') }} AS hl_range_norm,
        {{ normalize_factor('shadow_length', 'rank') }} AS shadow_length_norm,
        
        {{ normalize_factor('macd_like', 'zscore') }} AS macd_like_norm,
        {{ normalize_factor('stoch_like', 'rank') }} AS stoch_like_norm,
        {{ normalize_factor('williams_like', 'rank') }} AS williams_like_norm,
        
        {{ normalize_factor('opening_gap', 'zscore') }} AS opening_gap_norm,
        {{ normalize_factor('closing_strength', 'rank') }} AS closing_strength_norm,
        {{ normalize_factor('intraday_momentum', 'zscore') }} AS intraday_momentum_norm,
        {{ normalize_factor('intraday_volatility', 'rank') }} AS intraday_volatility_norm,
        
        {{ normalize_factor('term_structure', 'rank') }} AS term_structure_norm,
        {{ normalize_factor('basis_like', 'zscore') }} AS basis_like_norm,
        {{ normalize_factor('convenience_yield', 'zscore') }} AS convenience_yield_norm,
        
        {{ normalize_factor('sentiment_volume', 'rank') }} AS sentiment_volume_norm,
        {{ normalize_factor('herding_effect', 'zscore') }} AS herding_effect_norm,
        {{ normalize_factor('overreaction', 'rank') }} AS overreaction_norm,
        
        {{ normalize_factor('price_quality', 'rank') }} AS price_quality_norm,
        {{ normalize_factor('liquidity', 'rank') }} AS liquidity_norm,
        {{ normalize_factor('efficiency', 'rank') }} AS efficiency_norm,
        
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