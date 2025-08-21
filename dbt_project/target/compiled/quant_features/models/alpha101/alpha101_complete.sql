

-- Alpha 101 完整因子汇总
-- 整合所有101个Alpha因子并进行最终处理

WITH factors_001_020 AS (
    SELECT * FROM "quant_features"."main"."alpha_factors_001_020"
),

factors_021_050 AS (
    SELECT * FROM "quant_features"."main"."alpha_factors_021_050"
),

factors_051_075 AS (
    SELECT * FROM "quant_features"."main"."alpha_factors_051_075"
),

factors_076_101 AS (
    SELECT * FROM "quant_features"."main"."alpha_factors_076_101"
),

-- 合并所有Alpha因子
all_alpha_factors AS (
    SELECT 
        a.symbol,
        a.timestamp,
        
        -- 基础市场数据
        a.open, a.high, a.low, a.close, a.volume, a.vwap, a.returns,
        
        -- Alpha 001-020
        a.alpha001, a.alpha002, a.alpha003, a.alpha004, a.alpha005,
        a.alpha006, a.alpha007, a.alpha008, a.alpha009, a.alpha010,
        a.alpha011, a.alpha012, a.alpha013, a.alpha014, a.alpha015,
        a.alpha016, a.alpha017, a.alpha018, a.alpha019, a.alpha020,
        
        -- Alpha 021-050
        b.alpha021, b.alpha022, b.alpha023, b.alpha024, b.alpha025,
        b.alpha026, b.alpha027, b.alpha028, b.alpha029, b.alpha030,
        b.alpha031, b.alpha032, b.alpha033, b.alpha034, b.alpha035,
        b.alpha036, b.alpha037, b.alpha038, b.alpha039, b.alpha040,
        b.alpha041, b.alpha042, b.alpha043, b.alpha044, b.alpha045,
        b.alpha046, b.alpha047, b.alpha048, b.alpha049, b.alpha050,
        
        -- Alpha 051-075
        c.alpha051, c.alpha052, c.alpha053, c.alpha054, c.alpha055,
        c.alpha056, c.alpha057, c.alpha058, c.alpha059, c.alpha060,
        c.alpha061, c.alpha062, c.alpha063, c.alpha064, c.alpha065,
        c.alpha066, c.alpha067, c.alpha068, c.alpha069, c.alpha070,
        c.alpha071, c.alpha072, c.alpha073, c.alpha074, c.alpha075,
        
        -- Alpha 076-101
        d.alpha076, d.alpha077, d.alpha078, d.alpha079, d.alpha080,
        d.alpha081, d.alpha082, d.alpha083, d.alpha084, d.alpha085,
        d.alpha086, d.alpha087, d.alpha088, d.alpha089, d.alpha090,
        d.alpha091, d.alpha092, d.alpha093, d.alpha094, d.alpha095,
        d.alpha096, d.alpha097, d.alpha098, d.alpha099, d.alpha100,
        d.alpha101,
        
        -- 有效性统计
        d.valid_factors_count
        
    FROM factors_001_020 a
    LEFT JOIN factors_021_050 b ON a.symbol = b.symbol AND a.timestamp = b.timestamp
    LEFT JOIN factors_051_075 c ON a.symbol = c.symbol AND a.timestamp = c.timestamp
    LEFT JOIN factors_076_101 d ON a.symbol = d.symbol AND a.timestamp = d.timestamp
),

-- 因子分类和组合
factor_categories AS (
    SELECT 
        *,
        
        -- ========================================
        -- 因子分类组合
        -- ========================================
        
        -- 动量类因子组合 (基于因子特性分类)
        (COALESCE(alpha001, 0) + COALESCE(alpha012, 0) + COALESCE(alpha019, 0) + 
         COALESCE(alpha037, 0) + COALESCE(alpha065, 0)) / 5 AS momentum_alpha_composite,
        
        -- 反转类因子组合
        (COALESCE(alpha003, 0) + COALESCE(alpha004, 0) + COALESCE(alpha009, 0) + 
         COALESCE(alpha023, 0) + COALESCE(alpha051, 0)) / 5 AS reversal_alpha_composite,
        
        -- 成交量类因子组合
        (COALESCE(alpha006, 0) + COALESCE(alpha013, 0) + COALESCE(alpha025, 0) + 
         COALESCE(alpha044, 0) + COALESCE(alpha075, 0)) / 5 AS volume_alpha_composite,
        
        -- 波动率类因子组合
        (COALESCE(alpha022, 0) + COALESCE(alpha040, 0) + COALESCE(alpha053, 0) + 
         COALESCE(alpha070, 0) + COALESCE(alpha084, 0)) / 5 AS volatility_alpha_composite,
        
        -- 趋势类因子组合
        (COALESCE(alpha005, 0) + COALESCE(alpha028, 0) + COALESCE(alpha032, 0) + 
         COALESCE(alpha046, 0) + COALESCE(alpha089, 0)) / 5 AS trend_alpha_composite,
        
        -- 价格形态类因子组合
        (COALESCE(alpha041, 0) + COALESCE(alpha054, 0) + COALESCE(alpha060, 0) + 
         COALESCE(alpha083, 0) + COALESCE(alpha101, 0)) / 5 AS pattern_alpha_composite,
        
        -- ========================================
        -- 因子质量指标
        -- ========================================
        
        -- 计算非空因子数量
        (
            CASE WHEN alpha001 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha002 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha003 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha004 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha005 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha006 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha007 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha008 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha009 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha010 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha011 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha012 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha013 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha014 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha015 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha016 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha017 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha018 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha019 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha020 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha021 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha022 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha023 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha024 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha025 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha026 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha027 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha028 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha029 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha030 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha031 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha032 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha033 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha034 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha035 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha036 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha037 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha038 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha039 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha040 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha041 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha042 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha043 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha044 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha045 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha046 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha047 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha048 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha049 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha050 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha051 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha052 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha053 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha054 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha055 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha056 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha057 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha058 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha059 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha060 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha061 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha062 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha063 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha064 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha065 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha066 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha067 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha068 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha069 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha070 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha071 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha072 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha073 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha074 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha075 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha076 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha077 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha078 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha079 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha080 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha081 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha082 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha083 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha084 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha085 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha086 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha087 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha088 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha089 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha090 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha091 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha092 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha093 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha094 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha095 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha096 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha097 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha098 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha099 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha100 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN alpha101 IS NOT NULL THEN 1 ELSE 0 END
        ) AS total_valid_factors,
        
        -- Alpha 021-050
        b.alpha021, b.alpha022, b.alpha023, b.alpha024, b.alpha025,
        b.alpha026, b.alpha027, b.alpha028, b.alpha029, b.alpha030,
        b.alpha031, b.alpha032, b.alpha033, b.alpha034, b.alpha035,
        b.alpha036, b.alpha037, b.alpha038, b.alpha039, b.alpha040,
        b.alpha041, b.alpha042, b.alpha043, b.alpha044, b.alpha045,
        b.alpha046, b.alpha047, b.alpha048, b.alpha049, b.alpha050,
        
        -- Alpha 051-075
        c.alpha051, c.alpha052, c.alpha053, c.alpha054, c.alpha055,
        c.alpha056, c.alpha057, c.alpha058, c.alpha059, c.alpha060,
        c.alpha061, c.alpha062, c.alpha063, c.alpha064, c.alpha065,
        c.alpha066, c.alpha067, c.alpha068, c.alpha069, c.alpha070,
        c.alpha071, c.alpha072, c.alpha073, c.alpha074, c.alpha075,
        
        -- Alpha 076-101
        d.alpha076, d.alpha077, d.alpha078, d.alpha079, d.alpha080,
        d.alpha081, d.alpha082, d.alpha083, d.alpha084, d.alpha085,
        d.alpha086, d.alpha087, d.alpha088, d.alpha089, d.alpha090,
        d.alpha091, d.alpha092, d.alpha093, d.alpha094, d.alpha095,
        d.alpha096, d.alpha097, d.alpha098, d.alpha099, d.alpha100,
        d.alpha101
        
    FROM factors_001_020 a
    LEFT JOIN factors_021_050 b ON a.symbol = b.symbol AND a.timestamp = b.timestamp
    LEFT JOIN factors_051_075 c ON a.symbol = c.symbol AND a.timestamp = c.timestamp
    LEFT JOIN factors_076_101 d ON a.symbol = d.symbol AND a.timestamp = d.timestamp
),

-- 因子组合和元因子
factor_combinations AS (
    SELECT 
        *,
        
        -- ========================================
        -- 主要因子组合 (基于研究文献中的有效因子)
        -- ========================================
        
        -- 动量类因子组合 (经过验证的有效因子)
        (COALESCE(alpha001, 0) + COALESCE(alpha012, 0) + COALESCE(alpha019, 0) + 
         COALESCE(alpha037, 0) + COALESCE(alpha065, 0) + COALESCE(alpha089, 0)) / 6 AS momentum_alpha_composite,
        
        -- 反转类因子组合
        (COALESCE(alpha003, 0) + COALESCE(alpha004, 0) + COALESCE(alpha009, 0) + 
         COALESCE(alpha023, 0) + COALESCE(alpha051, 0) + COALESCE(alpha099, 0)) / 6 AS reversal_alpha_composite,
        
        -- 成交量类因子组合
        (COALESCE(alpha006, 0) + COALESCE(alpha013, 0) + COALESCE(alpha025, 0) + 
         COALESCE(alpha044, 0) + COALESCE(alpha075, 0) + COALESCE(alpha078, 0)) / 6 AS volume_alpha_composite,
        
        -- 波动率类因子组合
        (COALESCE(alpha022, 0) + COALESCE(alpha040, 0) + COALESCE(alpha053, 0) + 
         COALESCE(alpha070, 0) + COALESCE(alpha084, 0) + COALESCE(alpha094, 0)) / 6 AS volatility_alpha_composite,
        
        -- 趋势类因子组合
        (COALESCE(alpha005, 0) + COALESCE(alpha028, 0) + COALESCE(alpha032, 0) + 
         COALESCE(alpha046, 0) + COALESCE(alpha089, 0) + COALESCE(alpha097, 0)) / 6 AS trend_alpha_composite,
        
        -- 价格形态类因子组合
        (COALESCE(alpha041, 0) + COALESCE(alpha054, 0) + COALESCE(alpha060, 0) + 
         COALESCE(alpha083, 0) + COALESCE(alpha101, 0) + COALESCE(alpha088, 0)) / 6 AS pattern_alpha_composite,
        
        -- ========================================
        -- 高级组合因子
        -- ========================================
        
        -- 多空组合 (Long-Short Portfolio)
        -- 多头因子：选择正向预测的因子
        (COALESCE(alpha001, 0) + COALESCE(alpha005, 0) + COALESCE(alpha012, 0) + 
         COALESCE(alpha028, 0) + COALESCE(alpha032, 0) + COALESCE(alpha041, 0) + 
         COALESCE(alpha101, 0)) / 7 AS long_alpha_composite,
        
        -- 空头因子：选择负向预测的因子
        (COALESCE(alpha003, 0) + COALESCE(alpha006, 0) + COALESCE(alpha013, 0) + 
         COALESCE(alpha022, 0) + COALESCE(alpha040, 0) + COALESCE(alpha044, 0) + 
         COALESCE(alpha050, 0)) / 7 AS short_alpha_composite,
        
        -- 市场中性组合
        ((COALESCE(alpha001, 0) + COALESCE(alpha012, 0) + COALESCE(alpha028, 0)) / 3) - 
        ((COALESCE(alpha003, 0) + COALESCE(alpha006, 0) + COALESCE(alpha013, 0)) / 3) AS market_neutral_alpha,
        
        -- ========================================
        -- 因子稳健性指标
        -- ========================================
        
        -- 因子一致性 (同类因子的方向一致性)
        CASE 
            WHEN (COALESCE(alpha001, 0) > 0 AND COALESCE(alpha012, 0) > 0 AND COALESCE(alpha019, 0) > 0) OR
                 (COALESCE(alpha001, 0) < 0 AND COALESCE(alpha012, 0) < 0 AND COALESCE(alpha019, 0) < 0)
            THEN 1 ELSE 0 
        END AS momentum_consistency,
        
        CASE 
            WHEN (COALESCE(alpha003, 0) > 0 AND COALESCE(alpha004, 0) > 0 AND COALESCE(alpha009, 0) > 0) OR
                 (COALESCE(alpha003, 0) < 0 AND COALESCE(alpha004, 0) < 0 AND COALESCE(alpha009, 0) < 0)
            THEN 1 ELSE 0 
        END AS reversal_consistency,
        
        -- 因子强度 (因子绝对值的平均)
        (ABS(COALESCE(alpha001, 0)) + ABS(COALESCE(alpha003, 0)) + ABS(COALESCE(alpha006, 0)) + 
         ABS(COALESCE(alpha012, 0)) + ABS(COALESCE(alpha028, 0))) / 5 AS factor_strength,
        
        -- ========================================
        -- 特殊用途因子
        -- ========================================
        
        -- 高频交易因子 (基于短期因子)
        (COALESCE(alpha012, 0) + COALESCE(alpha041, 0) + COALESCE(alpha101, 0)) / 3 AS hft_alpha_composite,
        
        -- 低频交易因子 (基于长期因子)
        (COALESCE(alpha019, 0) + COALESCE(alpha032, 0) + COALESCE(alpha048, 0)) / 3 AS low_freq_alpha_composite,
        
        -- 风险平价因子 (考虑波动率调整)
        CASE 
            WHEN COALESCE(volatility_alpha_composite, 1) != 0 
            THEN COALESCE(momentum_alpha_composite, 0) / ABS(COALESCE(volatility_alpha_composite, 1))
            ELSE 0 
        END AS risk_parity_alpha
        
    FROM factor_categories
),

-- 最终输出
final_output AS (
    SELECT 
        symbol,
        timestamp,
        
        -- 完整的101个Alpha因子
        alpha001, alpha002, alpha003, alpha004, alpha005, alpha006, alpha007, alpha008, alpha009, alpha010,
        alpha011, alpha012, alpha013, alpha014, alpha015, alpha016, alpha017, alpha018, alpha019, alpha020,
        alpha021, alpha022, alpha023, alpha024, alpha025, alpha026, alpha027, alpha028, alpha029, alpha030,
        alpha031, alpha032, alpha033, alpha034, alpha035, alpha036, alpha037, alpha038, alpha039, alpha040,
        alpha041, alpha042, alpha043, alpha044, alpha045, alpha046, alpha047, alpha048, alpha049, alpha050,
        alpha051, alpha052, alpha053, alpha054, alpha055, alpha056, alpha057, alpha058, alpha059, alpha060,
        alpha061, alpha062, alpha063, alpha064, alpha065, alpha066, alpha067, alpha068, alpha069, alpha070,
        alpha071, alpha072, alpha073, alpha074, alpha075, alpha076, alpha077, alpha078, alpha079, alpha080,
        alpha081, alpha082, alpha083, alpha084, alpha085, alpha086, alpha087, alpha088, alpha089, alpha090,
        alpha091, alpha092, alpha093, alpha094, alpha095, alpha096, alpha097, alpha098, alpha099, alpha100,
        alpha101,
        
        -- 因子组合
        momentum_alpha_composite,
        reversal_alpha_composite,
        volume_alpha_composite,
        volatility_alpha_composite,
        trend_alpha_composite,
        pattern_alpha_composite,
        
        -- 高级组合
        long_alpha_composite,
        short_alpha_composite,
        market_neutral_alpha,
        
        -- 特殊用途因子
        hft_alpha_composite,
        low_freq_alpha_composite,
        risk_parity_alpha,
        
        -- 质量指标
        total_valid_factors,
        momentum_consistency,
        reversal_consistency,
        factor_strength,
        
        -- Feast集成字段
        CONCAT(symbol, '_', DATE_TRUNC('day', timestamp)::STRING) AS entity_id,
        timestamp AS event_timestamp,
        CURRENT_TIMESTAMP AS created_at
        
    FROM factor_combinations
)

SELECT * FROM final_output