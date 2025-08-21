
-- Daily Returns Model
-- @materialize: true

SELECT 
    CAST(date AS DATE) as date,
    portfolio_value,
    daily_return,
    cumulative_return,
    benchmark_return,
    excess_return,
    -- 计算移动平均
    AVG(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as ma_7_return,
    AVG(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30_return,
    -- 计算滚动波动率
    STDDEV(daily_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_volatility_30d,
    -- 计算滚动夏普比率
    AVG(excess_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) / NULLIF(STDDEV(excess_return) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 0) as rolling_sharpe_30d
FROM read_csv_auto('data/daily_returns.csv')
ORDER BY date
