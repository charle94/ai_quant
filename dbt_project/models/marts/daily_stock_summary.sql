{{ config(materialized='table') }}

with price_data as (
    select * from {{ ref('stg_stock_prices') }}
),

market_data as (
    select * from {{ ref('stg_market_data') }}
),

combined_data as (
    select
        p.date,
        p.symbol,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        p.daily_range,
        p.daily_change,
        p.daily_return,
        m.market_cap,
        m.pe_ratio,
        m.dividend_yield,
        m.sector,
        m.estimated_earnings,
        m.estimated_dividend_payout,
        -- 计算额外的技术指标
        (p.high + p.low + p.close) / 3 as typical_price,
        p.volume * p.close as dollar_volume,
        case 
            when p.daily_return > 0.05 then 'Strong Up'
            when p.daily_return > 0.02 then 'Up'
            when p.daily_return > -0.02 then 'Flat'
            when p.daily_return > -0.05 then 'Down'
            else 'Strong Down'
        end as price_movement_category
    from price_data p
    left join market_data m
        on p.date = m.date
        and p.symbol = m.symbol
)

select * from combined_data