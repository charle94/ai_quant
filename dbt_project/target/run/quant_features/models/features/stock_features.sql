
  
    
    

    create  table
      "quant_features"."main"."stock_features__dbt_tmp"
  
    as (
      

with daily_data as (
    select * from "quant_features"."main"."daily_stock_summary"
),

windowed_features as (
    select
        *,
        -- 移动平均
        avg(close) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) as ma_5d,
        
        avg(close) over (
            partition by symbol 
            order by date 
            rows between 9 preceding and current row
        ) as ma_10d,
        
        -- 波动率（标准差）
        stddev(daily_return) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) as volatility_5d,
        
        -- 价格相对位置
        (close - min(low) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        )) / (max(high) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) - min(low) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        )) as price_position_5d,
        
        -- 成交量相对强度
        volume / avg(volume) over (
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) as volume_ratio_5d
        
    from daily_data
),

final_features as (
    select
        *,
        -- 技术信号
        case 
            when close > ma_5d and ma_5d > ma_10d then 'Bullish'
            when close < ma_5d and ma_5d < ma_10d then 'Bearish'
            else 'Neutral'
        end as trend_signal,
        
        case 
            when volatility_5d > 0.03 then 'High'
            when volatility_5d > 0.01 then 'Medium'
            else 'Low'
        end as volatility_category
        
    from windowed_features
)

select * from final_features
    );
  
  