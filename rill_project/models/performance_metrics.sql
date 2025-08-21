
-- Performance Metrics Model
-- @materialize: true

SELECT 
    metric_name,
    metric_value,
    category,
    CAST(updated_at AS TIMESTAMP) as updated_at,
    -- 添加格式化显示
    CASE 
        WHEN metric_name IN ('total_return', 'annualized_return', 'volatility', 'max_drawdown', 'win_rate', 'var_95') 
        THEN CONCAT(ROUND(metric_value * 100, 2), '%')
        ELSE ROUND(metric_value, 2)::VARCHAR
    END as formatted_value,
    -- 添加基准比较
    CASE 
        WHEN metric_name = 'sharpe_ratio' AND metric_value > 1 THEN 'Good'
        WHEN metric_name = 'sharpe_ratio' AND metric_value > 0.5 THEN 'Fair'
        WHEN metric_name = 'sharpe_ratio' THEN 'Poor'
        WHEN metric_name = 'win_rate' AND metric_value > 0.6 THEN 'Excellent'
        WHEN metric_name = 'win_rate' AND metric_value > 0.5 THEN 'Good'
        WHEN metric_name = 'win_rate' THEN 'Needs Improvement'
        WHEN metric_name = 'max_drawdown' AND metric_value < 0.1 THEN 'Low Risk'
        WHEN metric_name = 'max_drawdown' AND metric_value < 0.2 THEN 'Medium Risk'
        WHEN metric_name = 'max_drawdown' THEN 'High Risk'
        ELSE 'Neutral'
    END as performance_grade
FROM read_csv_auto('data/performance_metrics.csv')
