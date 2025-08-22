
  
  create view "quant_features"."main"."stg_ohlc_data__dbt_tmp" as (
    

with raw_ohlc as (
    select 
        symbol,
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        -- 数据清洗和验证
        case 
            when open <= 0 or high <= 0 or low <= 0 or close <= 0 then null
            when high < greatest(open, close, low) then null
            when low > least(open, close, high) then null
            else timestamp
        end as valid_timestamp
    from "quant_features"."raw"."ohlc_data"
),

cleaned_ohlc as (
    select 
        symbol,
        timestamp,
        open,
        high,
        low,
        close,
        volume,
        -- 计算基础指标
        (high + low + close) / 3 as typical_price,
        (high - low) as daily_range,
        case when open != 0 then (close - open) / open else 0 end as daily_return,
        case when close != 0 then volume / close else 0 end as volume_price_ratio
    from raw_ohlc
    where valid_timestamp is not null
      and timestamp >= '2024-01-01'
      and timestamp <= '2024-01-02'
)

select * from cleaned_ohlc
  );
