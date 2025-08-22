

with technical_data as (
    select * from "quant_features"."main"."mart_technical_indicators"
),

feature_engineering as (
    select 
        symbol,
        timestamp,
        
        -- 基础价格特征
        close as price,
        daily_return,
        volatility_20d,
        
        -- 趋势特征
        ma_5,
        ma_10,
        ma_20,
        case when close > ma_5 then 1 else 0 end as price_above_ma5,
        case when close > ma_10 then 1 else 0 end as price_above_ma10,
        case when close > ma_20 then 1 else 0 end as price_above_ma20,
        case when ma_5 > ma_10 then 1 else 0 end as ma5_above_ma10,
        case when ma_10 > ma_20 then 1 else 0 end as ma10_above_ma20,
        
        -- 技术指标特征
        rsi_14,
        case when rsi_14 > 70 then 1 else 0 end as rsi_overbought,
        case when rsi_14 < 30 then 1 else 0 end as rsi_oversold,
        
        stoch_k_14,
        case when stoch_k_14 > 0.8 then 1 else 0 end as stoch_overbought,
        case when stoch_k_14 < 0.2 then 1 else 0 end as stoch_oversold,
        
        -- 布林带特征
        bollinger_upper,
        bollinger_lower,
        case when close > bollinger_upper then 1 else 0 end as price_above_bb_upper,
        case when close < bollinger_lower then 1 else 0 end as price_below_bb_lower,
        case 
            when bollinger_upper - bollinger_lower != 0 
            then (close - bollinger_lower) / (bollinger_upper - bollinger_lower)
            else 0.5
        end as bb_position,
        
        -- 动量特征
        momentum_5d,
        momentum_10d,
        case when momentum_5d > 0 then 1 else 0 end as momentum_5d_positive,
        case when momentum_10d > 0 then 1 else 0 end as momentum_10d_positive,
        
        -- 成交量特征
        volume,
        avg_volume_20d,
        case when avg_volume_20d != 0 then volume / avg_volume_20d else 0 end as volume_ratio,
        case when volume > avg_volume_20d * 1.5 then 1 else 0 end as high_volume,
        
        -- 价格范围特征
        daily_range,
        case when lag(close) over (partition by symbol order by timestamp) != 0 
            then daily_range / lag(close) over (partition by symbol order by timestamp)
            else 0
        end as range_ratio,
        
        -- 组合特征
        case when rsi_14 > 70 and stoch_k_14 > 0.8 then 1 else 0 end as double_overbought,
        case when rsi_14 < 30 and stoch_k_14 < 0.2 then 1 else 0 end as double_oversold,
        
        -- 时间特征
        extract(hour from timestamp) as hour,
        extract(dow from timestamp) as day_of_week,
        extract(month from timestamp) as month,
        
        -- 标识特征用于Feast
        concat(symbol, '_', date_trunc('day', timestamp)::string) as entity_id,
        timestamp as event_timestamp
        
    from technical_data
    -- 移除时间过滤，使用所有可用数据
    -- where timestamp >= current_date - interval '20' days
)

select * from feature_engineering