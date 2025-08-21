{{ config(materialized='table') }}

-- Alpha 101 完整因子汇总（修正版）
-- 整合所有101个Alpha因子

WITH factors_basic AS (
    SELECT * FROM {{ ref('alpha_factors_basic') }}
),

factors_021_040 AS (
    SELECT * FROM {{ ref('alpha_factors_021_040') }}
),

factors_041_060 AS (
    SELECT * FROM {{ ref('alpha_factors_041_060') }}
),

factors_061_080 AS (
    SELECT * FROM {{ ref('alpha_factors_061_080') }}
),

factors_081_101 AS (
    SELECT * FROM {{ ref('alpha_factors_081_101') }}
),

-- 合并所有Alpha因子
all_alpha_factors AS (
    SELECT 
        a.symbol,
        a.timestamp,
        
        -- 基础市场数据
        a.open, a.high, a.low, a.close, a.volume, a.vwap, a.returns,
        
        -- Alpha 001-020 (来自基础模型)
        a.alpha001, a.alpha002, a.alpha003, a.alpha004, a.alpha005,
        a.alpha006, a.alpha007, a.alpha008, a.alpha009, a.alpha010,
        a.alpha011, a.alpha012, a.alpha013, a.alpha014, a.alpha015,
        a.alpha016, a.alpha017, a.alpha018, a.alpha019, a.alpha020,
        
        -- Alpha 021-040
        b.alpha021, b.alpha022, b.alpha023, b.alpha024, b.alpha025,
        b.alpha026, b.alpha027, b.alpha028, b.alpha029, b.alpha030,
        b.alpha031, b.alpha032, b.alpha033, b.alpha034, b.alpha035,
        b.alpha036, b.alpha037, b.alpha038, b.alpha039, b.alpha040,
        
        -- Alpha 041-060
        c.alpha041, c.alpha042, c.alpha043, c.alpha044, c.alpha045,
        c.alpha046, c.alpha047, c.alpha048, c.alpha049, c.alpha050,
        c.alpha051, c.alpha052, c.alpha053, c.alpha054, c.alpha055,
        c.alpha056, c.alpha057, c.alpha058, c.alpha059, c.alpha060,
        
        -- Alpha 061-080
        d.alpha061, d.alpha062, d.alpha063, d.alpha064, d.alpha065,
        d.alpha066, d.alpha067, d.alpha068, d.alpha069, d.alpha070,
        d.alpha071, d.alpha072, d.alpha073, d.alpha074, d.alpha075,
        d.alpha076, d.alpha077, d.alpha078, d.alpha079, d.alpha080,
        
        -- Alpha 081-101
        e.alpha081, e.alpha082, e.alpha083, e.alpha084, e.alpha085,
        e.alpha086, e.alpha087, e.alpha088, e.alpha089, e.alpha090,
        e.alpha091, e.alpha092, e.alpha093, e.alpha094, e.alpha095,
        e.alpha096, e.alpha097, e.alpha098, e.alpha099, e.alpha100,
        e.alpha101
        
    FROM factors_basic a
    LEFT JOIN factors_021_040 b ON a.symbol = b.symbol AND a.timestamp = b.timestamp
    LEFT JOIN factors_041_060 c ON a.symbol = c.symbol AND a.timestamp = c.timestamp
    LEFT JOIN factors_061_080 d ON a.symbol = d.symbol AND a.timestamp = d.timestamp
    LEFT JOIN factors_081_101 e ON a.symbol = e.symbol AND a.timestamp = e.timestamp
),

-- 因子统计和组合
factor_summary AS (
    SELECT 
        *,
        
        -- 计算有效因子数量
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
        
        -- 因子组合
        (COALESCE(alpha001, 0) + COALESCE(alpha012, 0) + COALESCE(alpha019, 0) +
         COALESCE(alpha037, 0) + COALESCE(alpha065, 0)) / 5 AS momentum_composite,
        
        (COALESCE(alpha003, 0) + COALESCE(alpha004, 0) + COALESCE(alpha009, 0) +
         COALESCE(alpha023, 0) + COALESCE(alpha051, 0)) / 5 AS reversal_composite,
        
        (COALESCE(alpha006, 0) + COALESCE(alpha013, 0) + COALESCE(alpha025, 0) +
         COALESCE(alpha044, 0) + COALESCE(alpha075, 0)) / 5 AS volume_composite,
        
        -- Feast集成字段
        timestamp AS event_timestamp,
        CURRENT_TIMESTAMP AS created_at
        
    FROM all_alpha_factors
)

SELECT * FROM factor_summary