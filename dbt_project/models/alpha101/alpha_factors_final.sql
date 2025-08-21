{{ config(materialized='table') }}

-- Alpha 101 最终因子模型
-- 整合所有Alpha因子并进行最终处理

WITH factors_001_020 AS (
    SELECT * FROM {{ ref('alpha_factors_001_020') }}
),

factors_021_050 AS (
    SELECT * FROM {{ ref('alpha_factors_021_050') }}
),

advanced_factors AS (
    SELECT * FROM {{ ref('alpha_factors_advanced') }}
),

-- 合并所有因子
all_factors AS (
    SELECT 
        a.symbol,
        a.timestamp,
        
        -- 基础市场数据
        a.open,
        a.high,
        a.low,
        a.close,
        a.volume,
        a.vwap,
        a.returns,
        
        -- Alpha 001-020
        a.alpha001,
        a.alpha002,
        a.alpha003,
        a.alpha004,
        a.alpha005,
        a.alpha006,
        a.alpha007,
        a.alpha008,
        a.alpha009,
        a.alpha010,
        a.alpha011,
        a.alpha012,
        a.alpha013,
        a.alpha014,
        a.alpha015,
        a.alpha016,
        a.alpha017,
        a.alpha018,
        a.alpha019,
        a.alpha020,
        
        -- Alpha 021-050
        b.alpha021,
        b.alpha022,
        b.alpha023,
        b.alpha024,
        b.alpha025,
        b.alpha026,
        b.alpha027,
        b.alpha028,
        b.alpha029,
        b.alpha030,
        b.alpha031,
        b.alpha032,
        b.alpha033,
        b.alpha034,
        b.alpha035,
        b.alpha036,
        b.alpha037,
        b.alpha038,
        b.alpha039,
        b.alpha040,
        b.alpha041,
        b.alpha042,
        b.alpha043,
        b.alpha044,
        b.alpha045,
        b.alpha046,
        b.alpha047,
        b.alpha048,
        b.alpha049,
        b.alpha050,
        
        -- 高级因子
        c.momentum_reversal_norm,
        c.short_momentum_norm,
        c.medium_momentum_norm,
        c.long_momentum_norm,
        c.price_acceleration_norm,
        c.volume_price_divergence_norm,
        c.volume_price_confirmation_norm,
        c.volume_breakout_norm,
        c.relative_volume_norm,
        c.volatility_rank_norm,
        c.price_volatility_norm,
        c.return_volatility_norm,
        c.volatility_breakout_norm,
        c.ma_trend_norm,
        c.multi_timeframe_trend_norm,
        c.trend_strength_norm,
        c.bollinger_position_norm,
        c.price_deviation_norm,
        c.rsi_like_factor_norm,
        c.hl_position_norm,
        c.hl_range_norm,
        c.shadow_length_norm,
        c.macd_like_norm,
        c.stoch_like_norm,
        c.williams_like_norm,
        c.opening_gap_norm,
        c.closing_strength_norm,
        c.intraday_momentum_norm,
        c.intraday_volatility_norm,
        c.term_structure_norm,
        c.basis_like_norm,
        c.convenience_yield_norm,
        c.sentiment_volume_norm,
        c.herding_effect_norm,
        c.overreaction_norm,
        c.price_quality_norm,
        c.liquidity_norm,
        c.efficiency_norm
        
    FROM factors_001_020 a
    LEFT JOIN factors_021_050 b 
        ON a.symbol = b.symbol AND a.timestamp = b.timestamp
    LEFT JOIN advanced_factors c 
        ON a.symbol = c.symbol AND a.timestamp = c.timestamp
),

-- 计算因子组合和元因子
factor_combinations AS (
    SELECT 
        *,
        
        -- ========================================
        -- 因子组合 (多因子策略)
        -- ========================================
        
        -- 动量组合因子
        (COALESCE(alpha001, 0) + COALESCE(alpha012, 0) + COALESCE(short_momentum_norm, 0) + 
         COALESCE(medium_momentum_norm, 0)) / 4 AS momentum_composite,
        
        -- 反转组合因子
        (COALESCE(alpha003, 0) + COALESCE(alpha004, 0) + COALESCE(momentum_reversal_norm, 0) + 
         COALESCE(price_deviation_norm, 0)) / 4 AS reversal_composite,
        
        -- 成交量组合因子
        (COALESCE(alpha006, 0) + COALESCE(alpha025, 0) + COALESCE(volume_price_confirmation_norm, 0) + 
         COALESCE(relative_volume_norm, 0)) / 4 AS volume_composite,
        
        -- 波动率组合因子
        (COALESCE(alpha022, 0) + COALESCE(alpha040, 0) + COALESCE(price_volatility_norm, 0) + 
         COALESCE(volatility_breakout_norm, 0)) / 4 AS volatility_composite,
        
        -- 趋势组合因子
        (COALESCE(alpha005, 0) + COALESCE(alpha028, 0) + COALESCE(ma_trend_norm, 0) + 
         COALESCE(trend_strength_norm, 0)) / 4 AS trend_composite,
        
        -- ========================================
        -- 风险调整因子
        -- ========================================
        
        -- 风险调整动量
        CASE 
            WHEN COALESCE(return_volatility_norm, 1) != 0 
            THEN COALESCE(short_momentum_norm, 0) / COALESCE(return_volatility_norm, 1)
            ELSE 0
        END AS risk_adjusted_momentum,
        
        -- 风险调整反转
        CASE 
            WHEN COALESCE(price_volatility_norm, 1) != 0 
            THEN COALESCE(momentum_reversal_norm, 0) / COALESCE(price_volatility_norm, 1)
            ELSE 0
        END AS risk_adjusted_reversal,
        
        -- ========================================
        -- 市场状态因子
        -- ========================================
        
        -- 市场压力因子
        CASE 
            WHEN COALESCE(volatility_breakout_norm, 0) > 0.5 AND COALESCE(volume_breakout_norm, 0) > 0.5 
            THEN 1
            ELSE 0
        END AS market_stress,
        
        -- 市场效率因子
        COALESCE(efficiency_norm, 0) * COALESCE(liquidity_norm, 0) AS market_efficiency,
        
        -- ========================================
        -- 元因子 (因子的因子)
        -- ========================================
        
        -- 因子动量 (因子值的变化趋势)
        {{ delta('COALESCE(alpha001, 0)', 5) }} AS factor_momentum_alpha001,
        {{ delta('COALESCE(volume_composite, 0)', 5) }} AS factor_momentum_volume,
        
        -- 因子波动率
        {{ ts_std('COALESCE(alpha003, 0)', 20) }} AS factor_volatility_alpha003,
        {{ ts_std('COALESCE(momentum_composite, 0)', 20) }} AS factor_volatility_momentum,
        
        -- 因子相关性
        {{ ts_corr('COALESCE(alpha001, 0)', 'COALESCE(alpha003, 0)', 20) }} AS factor_corr_mom_rev,
        
        -- ========================================
        -- 宏观环境适应因子
        -- ========================================
        
        -- 市场状态识别
        CASE 
            WHEN COALESCE(trend_composite, 0) > 0.3 THEN 'TRENDING'
            WHEN COALESCE(trend_composite, 0) < -0.3 THEN 'MEAN_REVERTING'
            ELSE 'SIDEWAYS'
        END AS market_regime,
        
        -- 波动率状态
        CASE 
            WHEN COALESCE(volatility_composite, 0) > 0.5 THEN 'HIGH_VOL'
            WHEN COALESCE(volatility_composite, 0) < -0.5 THEN 'LOW_VOL'
            ELSE 'NORMAL_VOL'
        END AS volatility_regime,
        
        -- 成交量状态
        CASE 
            WHEN COALESCE(volume_composite, 0) > 0.5 THEN 'HIGH_VOLUME'
            WHEN COALESCE(volume_composite, 0) < -0.5 THEN 'LOW_VOLUME'
            ELSE 'NORMAL_VOLUME'
        END AS volume_regime
        
    FROM all_factors
),

-- 最终输出：包含所有因子和元数据
final_output AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 基础数据
        open, high, low, close, volume, vwap, returns,
        
        -- 原始Alpha因子 (前50个)
        alpha001, alpha002, alpha003, alpha004, alpha005,
        alpha006, alpha007, alpha008, alpha009, alpha010,
        alpha011, alpha012, alpha013, alpha014, alpha015,
        alpha016, alpha017, alpha018, alpha019, alpha020,
        alpha021, alpha022, alpha023, alpha024, alpha025,
        alpha026, alpha027, alpha028, alpha029, alpha030,
        alpha031, alpha032, alpha033, alpha034, alpha035,
        alpha036, alpha037, alpha038, alpha039, alpha040,
        alpha041, alpha042, alpha043, alpha044, alpha045,
        alpha046, alpha047, alpha048, alpha049, alpha050,
        
        -- 标准化的高级因子
        momentum_reversal_norm, short_momentum_norm, medium_momentum_norm, 
        long_momentum_norm, price_acceleration_norm,
        volume_price_divergence_norm, volume_price_confirmation_norm, 
        volume_breakout_norm, relative_volume_norm,
        volatility_rank_norm, price_volatility_norm, return_volatility_norm, 
        volatility_breakout_norm,
        ma_trend_norm, multi_timeframe_trend_norm, trend_strength_norm,
        bollinger_position_norm, price_deviation_norm, rsi_like_factor_norm,
        hl_position_norm, hl_range_norm, shadow_length_norm,
        macd_like_norm, stoch_like_norm, williams_like_norm,
        opening_gap_norm, closing_strength_norm, intraday_momentum_norm, 
        intraday_volatility_norm,
        term_structure_norm, basis_like_norm, convenience_yield_norm,
        sentiment_volume_norm, herding_effect_norm, overreaction_norm,
        price_quality_norm, liquidity_norm, efficiency_norm,
        
        -- 组合因子
        momentum_composite, reversal_composite, volume_composite, 
        volatility_composite, trend_composite,
        
        -- 风险调整因子
        risk_adjusted_momentum, risk_adjusted_reversal,
        
        -- 市场状态因子
        market_stress, market_efficiency,
        
        -- 元因子
        factor_momentum_alpha001, factor_momentum_volume,
        factor_volatility_alpha003, factor_volatility_momentum,
        factor_corr_mom_rev,
        
        -- 市场状态标识
        market_regime, volatility_regime, volume_regime,
        
        -- 元数据
        CONCAT(symbol, '_', DATE_TRUNC('day', timestamp)::STRING) AS entity_id,
        timestamp AS event_timestamp,
        CURRENT_TIMESTAMP AS created_at
        
    FROM processed_factors
)

SELECT * FROM final_output