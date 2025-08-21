

-- Alpha 101 因子计算 (076-101)

WITH base_data AS (
    SELECT * FROM "quant_features"."main"."alpha_base_data"
),

-- 预计算复杂的中间变量
intermediate_calcs AS (
    SELECT 
        *,
        -- Alpha076 相关计算
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    vwap - 
    LAG(vwap, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (12 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (12 * (12 + 1) / 2)
 AS alpha076_decay_vwap_delta,
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(low, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 80 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS alpha076_ts_rank_corr,
        
        -- Alpha077 相关计算
        ((high + low) / 2 + high) - (vwap + high) AS alpha077_price_diff,
        
    -- 使用DuckDB的CORR窗口函数
    CORR((high + low) / 2, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
 AS alpha077_corr_hl_adv,
        
        -- Alpha078 相关计算
        low * 0.352 + vwap * 0.648 AS alpha078_weighted_low_vwap,
        
        -- Alpha079 相关计算
        close * 0.607 + open * 0.393 AS alpha079_weighted_close_open,
        
        -- Alpha080 相关计算
        open * 0.868 + high * 0.132 AS alpha080_weighted_open_high,
        
        -- Alpha081 相关计算
        POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )

    )
, 4) AS alpha081_rank_corr_power,
        
        -- Alpha082 相关计算
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    open - 
    LAG(open, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (15 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (15 * (15 + 1) / 2)
 AS alpha082_decay_open_delta,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(volume, open) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    )
 AS alpha082_corr_vol_open,
        
        -- Alpha083 相关计算
        
    CASE 
        WHEN 
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 = 0 OR 
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 IS NULL THEN NULL
        WHEN ABS(
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
) < 1e-10 THEN NULL
        ELSE high - low / 
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

    END
 AS alpha083_hl_close_ratio,
        vwap - close AS alpha083_vwap_close_diff,
        
        -- Alpha084 相关计算
        vwap - 
    MAX(vwap) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )
 AS alpha084_vwap_max_diff,
        
        -- Alpha085 相关计算
        high * 0.877 + close * 0.123 AS alpha085_weighted_high_close,
        
        -- Alpha086 相关计算
        (open + close) - (vwap + open) AS alpha086_price_diff,
        
        -- Alpha087 相关计算
        close * 0.370 + vwap * 0.630 AS alpha087_weighted_close_vwap,
        
        -- Alpha088 相关计算
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY low
    )
) - (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY high
    )
 + 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close
    )
) AS alpha088_rank_diff,
        
        -- Alpha089 相关计算
        low AS alpha089_low,  -- 简化，因为 low * 0.967285 + low * (1 - 0.967285) = low
        
        -- Alpha090 相关计算
        close - 
    MAX(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 AS alpha090_close_max_diff,
        
        -- Alpha091 相关计算
        
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)
 AS alpha091_decay_corr1,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY close
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
 AS alpha091_corr2,
        
        -- Alpha092 相关计算
        ((high + low) / 2 + close) < (low + open) AS alpha092_condition,
        
        -- Alpha093 相关计算
        close * 0.524 + vwap * 0.476 AS alpha093_weighted_close_vwap,
        
        -- Alpha094 相关计算
        vwap - 
    MIN(vwap) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 AS alpha094_vwap_min_diff,
        
        -- Alpha095 相关计算
        open - 
    MIN(open) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )
 AS alpha095_open_min_diff,
        
        -- Alpha096 相关计算
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY close
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
 AS alpha096_corr_close_adv,
        
        -- Alpha097 相关计算
        low * 0.721 + vwap * 0.279 AS alpha097_weighted_low_vwap,
        
        -- Alpha098 相关计算
        
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
    )
 AS alpha098_sum_adv5,
        
    (9 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
 = 
    MIN(
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY open
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )
 AS alpha098_argmin_corr,
        
        -- Alpha099 相关计算
        
    SUM((high + low) / 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS alpha099_sum_hl_mid,
        
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 AS alpha099_sum_adv60,
        
        -- Alpha100 相关计算
        
    CASE 
        WHEN high - low = 0 OR high - low IS NULL THEN NULL
        WHEN ABS(high - low) < 1e-10 THEN NULL
        ELSE (close - low) - (high - close) / high - low
    END
 * volume AS alpha100_price_vol,
        
    -- 使用DuckDB的CORR窗口函数
    CORR(close, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 - 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    (30 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (close = 
    MIN(close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )

    )
 AS alpha100_corr_diff,
        
        -- Alpha101 相关计算
        close - open AS alpha101_close_open_diff,
        high - low + 0.001 AS alpha101_hl_range
        
    FROM base_data
),

-- 最终Alpha因子计算
final_alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 076: VWAP变化与低价相关性最大值因子
        GREATEST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha076_decay_vwap_delta
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha076_ts_rank_corr * 
        (17 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (17 * (17 + 1) / 2)

        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha076,
        
        -- Alpha 077: 价格差异与中价相关性最小值因子
        LEAST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha077_price_diff * 
        (20 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (20 * (20 + 1) / 2)

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha077_corr_hl_adv * 
        (6 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (6 * (6 + 1) / 2)

    )

        ) AS alpha077,
        
        -- Alpha 078: 加权低价VWAP与VWAP成交量相关性幂函数因子
        POWER(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    SUM(alpha078_weighted_low_vwap) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

    )

        ) AS alpha078,
        
        -- Alpha 079: 加权收盘开盘价与VWAP关系比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    alpha079_weighted_close_open - 
    LAG(alpha079_weighted_close_open, 1) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )


    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 149 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )

    )

            THEN 1 
            ELSE 0 
        END AS alpha079,
        
        -- Alpha 080: 加权开盘高价符号与高价平均日成交量相关性幂函数因子
        POWER(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN 
    alpha080_weighted_open_high - 
    LAG(alpha080_weighted_open_high, 4) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 > 0 THEN 1
        WHEN 
    alpha080_weighted_open_high - 
    LAG(alpha080_weighted_open_high, 4) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 < 0 THEN -1
        ELSE 0
    END

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(high, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha080,
        
        -- Alpha 081: VWAP平均日成交量相关性对数与VWAP成交量相关性比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    CASE 
        WHEN alpha081_rank_corr_power > 0 THEN LN(alpha081_rank_corr_power)
        ELSE NULL
    END

    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

    )

            THEN -1 
            ELSE 1 
        END AS alpha081,
        
        -- Alpha 082: 开盘价变化与成交量相关性最小值因子
        LEAST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha082_decay_open_delta
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha082_corr_vol_open * 
        (7 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (7 * (7 + 1) / 2)

        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha082,
        
        -- Alpha 083: 高低价收盘价比率延迟与成交量关系因子
        
    CASE 
        WHEN 
    CASE 
        WHEN alpha083_vwap_close_diff = 0 OR alpha083_vwap_close_diff IS NULL THEN NULL
        WHEN ABS(alpha083_vwap_close_diff) < 1e-10 THEN NULL
        ELSE alpha083_hl_close_ratio / alpha083_vwap_close_diff
    END
 = 0 OR 
    CASE 
        WHEN alpha083_vwap_close_diff = 0 OR alpha083_vwap_close_diff IS NULL THEN NULL
        WHEN ABS(alpha083_vwap_close_diff) < 1e-10 THEN NULL
        ELSE alpha083_hl_close_ratio / alpha083_vwap_close_diff
    END
 IS NULL THEN NULL
        WHEN ABS(
    CASE 
        WHEN alpha083_vwap_close_diff = 0 OR alpha083_vwap_close_diff IS NULL THEN NULL
        WHEN ABS(alpha083_vwap_close_diff) < 1e-10 THEN NULL
        ELSE alpha083_hl_close_ratio / alpha083_vwap_close_diff
    END
) < 1e-10 THEN NULL
        ELSE 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    LAG(alpha083_hl_close_ratio, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

    )
 * 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )

    )
 / 
    CASE 
        WHEN alpha083_vwap_close_diff = 0 OR alpha083_vwap_close_diff IS NULL THEN NULL
        WHEN ABS(alpha083_vwap_close_diff) < 1e-10 THEN NULL
        ELSE alpha083_hl_close_ratio / alpha083_vwap_close_diff
    END

    END
 AS alpha083,
        
        -- Alpha 084: VWAP排序幂函数因子
        
    
    CASE 
        WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha084_vwap_max_diff
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
 > 0 THEN 1
        WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha084_vwap_max_diff
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
 < 0 THEN -1
        ELSE 0
    END
 * POWER(ABS(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha084_vwap_max_diff
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
), 
    close - 
    LAG(close, 5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

)
 AS alpha084,
        
        -- Alpha 085: 加权高价收盘价与中价成交量相关性幂函数因子
        POWER(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha085_weighted_high_close, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY (high + low) / 2
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY volume
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )

    )

        ) AS alpha085,
        
        -- Alpha 086: 收盘价平均日成交量相关性与价格差异比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(close, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha086_price_diff
    )

            THEN -1 
            ELSE 1 
        END AS alpha086,
        
        -- Alpha 087: 加权收盘VWAP与平均日成交量相关性最大值因子
        GREATEST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha087_weighted_close_vwap - 
    LAG(alpha087_weighted_close_vwap, 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (3 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (3 * (3 + 1) / 2)

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    ABS(
    -- 使用DuckDB的CORR窗口函数
    CORR(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 80 PRECEDING AND CURRENT ROW
    )

, close) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )
)
 * 
        (5 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (5 * (5 + 1) / 2)

        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha087,
        
        -- Alpha 088: 开盘低价与高价收盘价排序差异最小值因子
        LEAST(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha088_rank_diff * 
        (8 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (8 * (8 + 1) / 2)

    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY close
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 * 
        (7 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (7 * (7 + 1) / 2)

        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )

        ) AS alpha088,
        
        -- Alpha 089: 低价平均日成交量相关性与VWAP变化差异因子
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha089_low, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (6 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (6 * (6 + 1) / 2)

        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
 - 
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    vwap - 
    LAG(vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (10 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (10 * (10 + 1) / 2)

        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    )
 AS alpha089,
        
        -- Alpha 090: 收盘价最大值与平均日成交量低价相关性幂函数因子
        POWER(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha090_close_max_diff
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

, low) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha090,
        
        -- Alpha 091: 复合衰减相关性差异因子
        (
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha091_decay_corr1 * 
        (4 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (4 * (4 + 1) / 2)

        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 - 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        alpha091_corr2 * 
        (4 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (4 * (4 + 1) / 2)

    )
) * -1 AS alpha091,
        
        -- Alpha 092: 价格条件与低价平均日成交量相关性最小值因子
        LEAST(
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        CASE WHEN alpha092_condition THEN 1 ELSE 0 END * 
        (15 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (15 * (15 + 1) / 2)

        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY low
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )


    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 * 
        (7 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (7 * (7 + 1) / 2)

        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )

        ) AS alpha092,
        
        -- Alpha 093: VWAP平均日成交量相关性与加权收盘VWAP变化比率因子
        
    CASE 
        WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha093_weighted_close_vwap - 
    LAG(alpha093_weighted_close_vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

    )
 = 0 OR 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha093_weighted_close_vwap - 
    LAG(alpha093_weighted_close_vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

    )
 IS NULL THEN NULL
        WHEN ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha093_weighted_close_vwap - 
    LAG(alpha093_weighted_close_vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

    )
) < 1e-10 THEN NULL
        ELSE 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 80 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    )
 * 
        (20 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (20 * (20 + 1) / 2)

        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
 / 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha093_weighted_close_vwap - 
    LAG(alpha093_weighted_close_vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

    )

    END
 AS alpha093,
        
        -- Alpha 094: VWAP最小值与VWAP平均日成交量相关性幂函数因子
        POWER(
            
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha094_vwap_min_diff
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY vwap
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 17 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha094,
        
        -- Alpha 095: 开盘价最小值与中价平均日成交量相关性幂函数比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha095_open_min_diff
    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY POWER(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    SUM((high + low) / 2) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
, 
    SUM(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 39 PRECEDING AND CURRENT ROW
    )

) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )

    )
, 5)
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )

            THEN 1 
            ELSE 0 
        END AS alpha095,
        
        -- Alpha 096: VWAP成交量相关性与收盘价平均日成交量相关性最大值因子
        GREATEST(
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY vwap
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY volume
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )
 * 
        (4 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (4 * (4 + 1) / 2)

        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
,
            
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用ROW_NUMBER()来找到最大值的位置
    (13 - 1) - (
        ROW_NUMBER() OVER (
            PARTITION BY symbol, 
            (alpha096_corr_close_adv = 
    MAX(alpha096_corr_close_adv) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )
)
            ORDER BY timestamp DESC
        ) - 1
    )
 * 
        (14 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (14 * (14 + 1) / 2)

        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    )

        ) * -1 AS alpha096,
        
        -- Alpha 097: 加权低价VWAP变化与低价平均日成交量相关性差异因子
        (
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    alpha097_weighted_low_vwap - 
    LAG(alpha097_weighted_low_vwap, 3) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
    )

 * 
        (20 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (20 * (20 + 1) / 2)

    )
 - 
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY low
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    )
, 
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    )


        ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
    )
) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )

        ROWS BETWEEN 18 PRECEDING AND CURRENT ROW
    )
 * 
        (16 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (16 * (16 + 1) / 2)

        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
) * -1 AS alpha097,
        
        -- Alpha 098: VWAP平均日成交量相关性与开盘价平均日成交量相关性最小值差异因子
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    -- 使用DuckDB的CORR窗口函数
    CORR(vwap, alpha098_sum_adv5) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )
 * 
        (7 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (7 * (7 + 1) / 2)

    )
 - 
        
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 实现线性衰减权重的加权平均
    SUM(
        
    PERCENT_RANK() OVER (
        PARTITION BY symbol
        ORDER BY alpha098_argmin_corr
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
 * 
        (8 - ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY timestamp DESC
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) + 1)
    ) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) / 
    -- 权重总和 = d*(d+1)/2
    (8 * (8 + 1) / 2)

    )
 AS alpha098,
        
        -- Alpha 099: 中价平均日成交量相关性与低价成交量相关性比较因子
        CASE 
            WHEN 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(alpha099_sum_hl_mid, alpha099_sum_adv60) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    )

    )
 < 
                 
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY 
    -- 使用DuckDB的CORR窗口函数
    CORR(low, volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    )

    )

            THEN -1 
            ELSE 1 
        END AS alpha099,
        
        -- Alpha 100: 复合标准化因子
        0 - (1 * (1.5 * 
    
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha100_price_vol
    )
 / NULLIF(
        SUM(ABS(
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY alpha100_price_vol
    )
)) OVER (PARTITION BY timestamp), 0
    )
 - 
        
    alpha100_corr_diff / NULLIF(
        SUM(ABS(alpha100_corr_diff)) OVER (PARTITION BY timestamp), 0
    )
 * 
    CASE 
        WHEN 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

 = 0 OR 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

 IS NULL THEN NULL
        WHEN ABS(
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )

) < 1e-10 THEN NULL
        ELSE volume / 
    
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )


    END
)) AS alpha100,
        
        -- Alpha 101: 收盘开盘价差与高低价范围比率因子
        
    CASE 
        WHEN alpha101_hl_range = 0 OR alpha101_hl_range IS NULL THEN NULL
        WHEN ABS(alpha101_hl_range) < 1e-10 THEN NULL
        ELSE alpha101_close_open_diff / alpha101_hl_range
    END
 AS alpha101
        
    FROM base_data
),

-- 最终计算所有Alpha因子
final_alpha_factors AS (
    SELECT 
        symbol,
        timestamp,
        
        -- Alpha 076-101
        alpha076, alpha077, alpha078, alpha079, alpha080,
        alpha081, alpha082, alpha083, alpha084, alpha085,
        alpha086, alpha087, alpha088, alpha089, alpha090,
        alpha091, alpha092, alpha093, alpha094, alpha095,
        alpha096, alpha097, alpha098, alpha099, alpha100,
        alpha101,
        
        -- 添加一些质量控制指标
        CASE 
            WHEN alpha076 IS NOT NULL AND ABS(alpha076) < 100 THEN 1 ELSE 0 
        END AS alpha076_valid,
        
        CASE 
            WHEN alpha085 IS NOT NULL AND ABS(alpha085) < 100 THEN 1 ELSE 0 
        END AS alpha085_valid,
        
        CASE 
            WHEN alpha101 IS NOT NULL AND ABS(alpha101) < 10 THEN 1 ELSE 0 
        END AS alpha101_valid,
        
        -- 计算有效因子数量
        (
            CASE WHEN alpha076 IS NOT NULL AND ABS(alpha076) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha077 IS NOT NULL AND ABS(alpha077) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha078 IS NOT NULL AND ABS(alpha078) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha079 IS NOT NULL AND ABS(alpha079) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha080 IS NOT NULL AND ABS(alpha080) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha081 IS NOT NULL AND ABS(alpha081) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha082 IS NOT NULL AND ABS(alpha082) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha083 IS NOT NULL AND ABS(alpha083) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha084 IS NOT NULL AND ABS(alpha084) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha085 IS NOT NULL AND ABS(alpha085) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha086 IS NOT NULL AND ABS(alpha086) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha087 IS NOT NULL AND ABS(alpha087) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha088 IS NOT NULL AND ABS(alpha088) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha089 IS NOT NULL AND ABS(alpha089) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha090 IS NOT NULL AND ABS(alpha090) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha091 IS NOT NULL AND ABS(alpha091) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha092 IS NOT NULL AND ABS(alpha092) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha093 IS NOT NULL AND ABS(alpha093) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha094 IS NOT NULL AND ABS(alpha094) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha095 IS NOT NULL AND ABS(alpha095) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha096 IS NOT NULL AND ABS(alpha096) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha097 IS NOT NULL AND ABS(alpha097) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha098 IS NOT NULL AND ABS(alpha098) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha099 IS NOT NULL AND ABS(alpha099) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha100 IS NOT NULL AND ABS(alpha100) < 100 THEN 1 ELSE 0 END +
            CASE WHEN alpha101 IS NOT NULL AND ABS(alpha101) < 10 THEN 1 ELSE 0 END
        ) AS valid_factors_count
        
    FROM intermediate_calcs
)

SELECT * FROM final_alpha_factors