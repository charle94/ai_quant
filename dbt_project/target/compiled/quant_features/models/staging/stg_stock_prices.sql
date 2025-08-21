

with source_data as (
    select
        date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        -- 计算基础技术指标
        (high - low) as daily_range,
        (close - open) as daily_change,
        (close - open) / open as daily_return,
        -- 添加数据质量检查
        case 
            when high >= low and high >= open and high >= close 
                 and low <= open and low <= close 
            then true 
            else false 
        end as is_valid_ohlc
    from "quant_features"."main"."raw_stock_prices"
    where date is not null
      and symbol is not null
      and open > 0
      and high > 0
      and low > 0
      and close > 0
      and volume >= 0
)

select * from source_data
where is_valid_ohlc = true