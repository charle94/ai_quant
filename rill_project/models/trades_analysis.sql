
-- Trades Analysis Model
-- @materialize: true

WITH trade_stats AS (
    SELECT 
        *,
        CAST(timestamp AS TIMESTAMP) as trade_timestamp,
        -- 按月分组
        DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) as trade_month,
        -- 按周分组
        DATE_TRUNC('week', CAST(timestamp AS TIMESTAMP)) as trade_week,
        -- 盈亏分类
        CASE 
            WHEN pnl > 0 THEN 'Win'
            WHEN pnl < 0 THEN 'Loss'
            ELSE 'Breakeven'
        END as trade_result,
        -- 交易规模分类
        CASE 
            WHEN ABS(pnl) > 1000 THEN 'Large'
            WHEN ABS(pnl) > 500 THEN 'Medium'
            ELSE 'Small'
        END as trade_size
    FROM read_csv_auto('data/trades.csv')
)

SELECT 
    *,
    -- 计算累计统计
    ROW_NUMBER() OVER (ORDER BY trade_timestamp) as trade_number,
    AVG(pnl) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as avg_pnl_cumulative,
    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as wins_cumulative,
    COUNT(*) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as total_trades_cumulative,
    -- 胜率
    CAST(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) AS FLOAT) / 
    COUNT(*) OVER (ORDER BY trade_timestamp ROWS UNBOUNDED PRECEDING) as win_rate_cumulative
FROM trade_stats
ORDER BY trade_timestamp
