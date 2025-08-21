

with base_data as (
    select * from "quant_features"."main"."stg_ohlc_data"
),

technical_indicators as (
    select 
        symbol,
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        typical_price,
        daily_range,
        daily_return,
        volume_price_ratio,
        
        -- 移动平均线
        avg(close) over (
            partition by symbol 
            order by timestamp 
            rows between 4 preceding and current row
        ) as ma_5,
        
        avg(close) over (
            partition by symbol 
            order by timestamp 
            rows between 9 preceding and current row
        ) as ma_10,
        
        avg(close) over (
            partition by symbol 
            order by timestamp 
            rows between 19 preceding and current row
        ) as ma_20,
        
        -- 波动率 (标准差)
        stddev(daily_return) over (
            partition by symbol 
            order by timestamp 
            rows between 19 preceding and current row
        ) as volatility_20d,
        
        -- RSI相关计算
        case when daily_return > 0 then daily_return else 0 end as gain,
        case when daily_return < 0 then abs(daily_return) else 0 end as loss,
        
        -- 价格位置指标
        (close - min(low) over (
            partition by symbol 
            order by timestamp 
            rows between 13 preceding and current row
        )) / nullif((max(high) over (
            partition by symbol 
            order by timestamp 
            rows between 13 preceding and current row
        ) - min(low) over (
            partition by symbol 
            order by timestamp 
            rows between 13 preceding and current row
        )), 0) as stoch_k_14,
        
        -- 成交量指标
        avg(volume) over (
            partition by symbol 
            order by timestamp 
            rows between 19 preceding and current row
        ) as avg_volume_20d
        
    from base_data
),

rsi_calculation as (
    select *,
        -- RSI计算
        avg(gain) over (
            partition by symbol 
            order by timestamp 
            rows between 13 preceding and current row
        ) as avg_gain_14,
        
        avg(loss) over (
            partition by symbol 
            order by timestamp 
            rows between 13 preceding and current row
        ) as avg_loss_14
    from technical_indicators
),

final_indicators as (
    select *,
        case 
            when avg_loss_14 = 0 then 100
            when avg_gain_14 = 0 then 0
            else 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
        end as rsi_14,
        
        -- 布林带
        ma_20 + (2 * stddev(close) over (
            partition by symbol 
            order by timestamp 
            rows between 19 preceding and current row
        )) as bollinger_upper,
        
        ma_20 - (2 * stddev(close) over (
            partition by symbol 
            order by timestamp 
            rows between 19 preceding and current row
        )) as bollinger_lower,
        
        -- 价格动量
        case when lag(close, 5) over (partition by symbol order by timestamp) != 0 
            then (close - lag(close, 5) over (partition by symbol order by timestamp)) / 
                 lag(close, 5) over (partition by symbol order by timestamp)
            else 0 
        end as momentum_5d,
        
        case when lag(close, 10) over (partition by symbol order by timestamp) != 0 
            then (close - lag(close, 10) over (partition by symbol order by timestamp)) / 
                 lag(close, 10) over (partition by symbol order by timestamp)
            else 0 
        end as momentum_10d
        
    from rsi_calculation
)

select * from final_indicators