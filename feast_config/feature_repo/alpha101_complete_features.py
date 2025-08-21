from datetime import timedelta
from feast import FeatureView, Field, PushSource
from feast.types import Float64, Int64, String
from entities import trading_pair
from data_sources import offline_features_source

# Alpha 101 完整因子特征视图 (001-050)
alpha101_basic_features_fv = FeatureView(
    name="alpha101_basic_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # Alpha 001-025
        Field(name="alpha001", dtype=Float64, description="价格反转动量因子"),
        Field(name="alpha002", dtype=Float64, description="成交量对数变化与价格变化相关性"),
        Field(name="alpha003", dtype=Float64, description="开盘价与成交量负相关"),
        Field(name="alpha004", dtype=Float64, description="低价时序排序"),
        Field(name="alpha005", dtype=Float64, description="开盘价与VWAP偏离"),
        Field(name="alpha006", dtype=Float64, description="开盘价与成交量相关性"),
        Field(name="alpha007", dtype=Float64, description="成交量突破的价格变化"),
        Field(name="alpha008", dtype=Float64, description="开盘价与收益率乘积延迟"),
        Field(name="alpha009", dtype=Float64, description="价格变化方向性"),
        Field(name="alpha010", dtype=Float64, description="价格变化方向性排序"),
        Field(name="alpha011", dtype=Float64, description="VWAP偏离与成交量变化"),
        Field(name="alpha012", dtype=Float64, description="成交量变化符号与价格变化"),
        Field(name="alpha013", dtype=Float64, description="价格成交量协方差排序"),
        Field(name="alpha014", dtype=Float64, description="收益率变化与开盘成交量相关性"),
        Field(name="alpha015", dtype=Float64, description="高价成交量相关性求和"),
        Field(name="alpha016", dtype=Float64, description="高价成交量协方差排序"),
        Field(name="alpha017", dtype=Float64, description="价格排序与成交量比率"),
        Field(name="alpha018", dtype=Float64, description="开盘收盘差异与相关性"),
        Field(name="alpha019", dtype=Float64, description="价格变化符号与收益率求和"),
        Field(name="alpha020", dtype=Float64, description="开盘价与延迟价格差异"),
        Field(name="alpha021", dtype=Float64, description="移动平均与标准差条件"),
        Field(name="alpha022", dtype=Float64, description="高价成交量相关性变化"),
        Field(name="alpha023", dtype=Float64, description="高价移动平均比较"),
        Field(name="alpha024", dtype=Float64, description="移动平均变化率条件"),
        Field(name="alpha025", dtype=Float64, description="收益率成交量VWAP乘积"),
        
        # Alpha 026-050
        Field(name="alpha026", dtype=Float64, description="时序排序相关性最大值"),
        Field(name="alpha027", dtype=Float64, description="成交量VWAP相关性条件"),
        Field(name="alpha028", dtype=Float64, description="标准化的平均日成交量低价相关性"),
        Field(name="alpha029", dtype=Float64, description="复合排序对数求和"),
        Field(name="alpha030", dtype=Float64, description="价格符号与成交量比率"),
        Field(name="alpha031", dtype=Float64, description="衰减线性排序与相关性"),
        Field(name="alpha032", dtype=Float64, description="标准化移动平均与VWAP相关性"),
        Field(name="alpha033", dtype=Float64, description="开盘收盘比率排序"),
        Field(name="alpha034", dtype=Float64, description="收益率标准差比率排序"),
        Field(name="alpha035", dtype=Float64, description="成交量与价格排序乘积"),
        Field(name="alpha036", dtype=Float64, description="复合加权相关性组合"),
        Field(name="alpha037", dtype=Float64, description="延迟开盘收盘差与收盘价相关性"),
        Field(name="alpha038", dtype=Float64, description="收盘价排序与开盘收盘比率"),
        Field(name="alpha039", dtype=Float64, description="价格变化与衰减成交量比率"),
        Field(name="alpha040", dtype=Float64, description="高价标准差与相关性"),
        Field(name="alpha041", dtype=Float64, description="几何平均价格与VWAP差异"),
        Field(name="alpha042", dtype=Float64, description="VWAP收盘价差异比率"),
        Field(name="alpha043", dtype=Float64, description="成交量比率与价格变化排序"),
        Field(name="alpha044", dtype=Float64, description="高价与成交量排序相关性"),
        Field(name="alpha045", dtype=Float64, description="延迟收盘价与相关性乘积"),
        Field(name="alpha046", dtype=Float64, description="价格斜率差异条件"),
        Field(name="alpha047", dtype=Float64, description="复合价格成交量比率"),
        Field(name="alpha048", dtype=Float64, description="回归残差标准化"),
        Field(name="alpha049", dtype=Float64, description="价格斜率负向条件"),
        Field(name="alpha050", dtype=Float64, description="VWAP成交量相关性最大值"),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "basic_factors"},
)

# Alpha 101 高级因子特征视图 (051-101)
alpha101_advanced_features_fv = FeatureView(
    name="alpha101_advanced_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # Alpha 051-075
        Field(name="alpha051", dtype=Float64, description="价格趋势变化因子"),
        Field(name="alpha052", dtype=Float64, description="低价位与收益率关系"),
        Field(name="alpha053", dtype=Float64, description="价格位置变化"),
        Field(name="alpha054", dtype=Float64, description="开盘收盘价格幂函数关系"),
        Field(name="alpha055", dtype=Float64, description="随机指标与成交量相关性"),
        Field(name="alpha056", dtype=Float64, description="收益率比率与加权收益"),
        Field(name="alpha057", dtype=Float64, description="VWAP偏离与价格位置"),
        Field(name="alpha058", dtype=Float64, description="衰减相关性排序"),
        Field(name="alpha059", dtype=Float64, description="衰减相关性排序变体"),
        Field(name="alpha060", dtype=Float64, description="价格位置与成交量综合"),
        Field(name="alpha061", dtype=Float64, description="VWAP最小值比较"),
        Field(name="alpha062", dtype=Float64, description="复合条件因子"),
        Field(name="alpha063", dtype=Float64, description="加权价格衰减"),
        Field(name="alpha064", dtype=Float64, description="复合加权因子"),
        Field(name="alpha065", dtype=Float64, description="开盘价最小值比较"),
        Field(name="alpha066", dtype=Float64, description="VWAP衰减与低价关系"),
        Field(name="alpha067", dtype=Float64, description="高价最小值幂函数"),
        Field(name="alpha068", dtype=Float64, description="高价与平均日成交量关系"),
        Field(name="alpha069", dtype=Float64, description="VWAP最大值幂函数"),
        Field(name="alpha070", dtype=Float64, description="VWAP变化幂函数"),
        Field(name="alpha071", dtype=Float64, description="复合最大值因子"),
        Field(name="alpha072", dtype=Float64, description="中价与VWAP关系比率"),
        Field(name="alpha073", dtype=Float64, description="复合最大值衰减"),
        Field(name="alpha074", dtype=Float64, description="收盘价与高价VWAP关系比较"),
        Field(name="alpha075", dtype=Float64, description="VWAP成交量与低价关系"),
        
        # Alpha 076-101
        Field(name="alpha076", dtype=Float64, description="VWAP变化与低价相关性最大值"),
        Field(name="alpha077", dtype=Float64, description="价格差异与中价相关性最小值"),
        Field(name="alpha078", dtype=Float64, description="加权低价VWAP相关性幂函数"),
        Field(name="alpha079", dtype=Float64, description="加权收盘开盘价与VWAP关系比较"),
        Field(name="alpha080", dtype=Float64, description="加权开盘高价符号幂函数"),
        Field(name="alpha081", dtype=Float64, description="VWAP相关性对数比较"),
        Field(name="alpha082", dtype=Float64, description="开盘价变化与成交量相关性最小值"),
        Field(name="alpha083", dtype=Float64, description="高低价比率延迟与成交量关系"),
        Field(name="alpha084", dtype=Float64, description="VWAP排序幂函数"),
        Field(name="alpha085", dtype=Float64, description="加权高价收盘价相关性幂函数"),
        Field(name="alpha086", dtype=Float64, description="收盘价相关性与价格差异比较"),
        Field(name="alpha087", dtype=Float64, description="加权收盘VWAP相关性最大值"),
        Field(name="alpha088", dtype=Float64, description="开盘低价与高价收盘价排序最小值"),
        Field(name="alpha089", dtype=Float64, description="低价相关性与VWAP变化差异"),
        Field(name="alpha090", dtype=Float64, description="收盘价最大值相关性幂函数"),
        Field(name="alpha091", dtype=Float64, description="复合衰减相关性差异"),
        Field(name="alpha092", dtype=Float64, description="价格条件与低价相关性最小值"),
        Field(name="alpha093", dtype=Float64, description="VWAP相关性与加权价格变化比率"),
        Field(name="alpha094", dtype=Float64, description="VWAP最小值相关性幂函数"),
        Field(name="alpha095", dtype=Float64, description="开盘价最小值相关性幂函数比较"),
        Field(name="alpha096", dtype=Float64, description="VWAP成交量相关性最大值"),
        Field(name="alpha097", dtype=Float64, description="加权低价VWAP变化差异"),
        Field(name="alpha098", dtype=Float64, description="VWAP相关性差异"),
        Field(name="alpha099", dtype=Float64, description="中价相关性比较"),
        Field(name="alpha100", dtype=Float64, description="复合标准化因子"),
        Field(name="alpha101", dtype=Float64, description="收盘开盘价与高低价范围比率"),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "advanced_factors"},
)

# Alpha 101 组合因子特征视图
alpha101_composite_features_fv = FeatureView(
    name="alpha101_composite_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # 主要组合因子
        Field(name="momentum_alpha_composite", dtype=Float64, description="动量类Alpha因子组合"),
        Field(name="reversal_alpha_composite", dtype=Float64, description="反转类Alpha因子组合"),
        Field(name="volume_alpha_composite", dtype=Float64, description="成交量类Alpha因子组合"),
        Field(name="volatility_alpha_composite", dtype=Float64, description="波动率类Alpha因子组合"),
        Field(name="trend_alpha_composite", dtype=Float64, description="趋势类Alpha因子组合"),
        Field(name="pattern_alpha_composite", dtype=Float64, description="价格形态类Alpha因子组合"),
        
        # 策略组合因子
        Field(name="long_alpha_composite", dtype=Float64, description="多头Alpha因子组合"),
        Field(name="short_alpha_composite", dtype=Float64, description="空头Alpha因子组合"),
        Field(name="market_neutral_alpha", dtype=Float64, description="市场中性Alpha因子"),
        
        # 特殊用途因子
        Field(name="hft_alpha_composite", dtype=Float64, description="高频交易Alpha因子组合"),
        Field(name="low_freq_alpha_composite", dtype=Float64, description="低频交易Alpha因子组合"),
        Field(name="risk_parity_alpha", dtype=Float64, description="风险平价Alpha因子"),
        
        # 质量控制指标
        Field(name="total_valid_factors", dtype=Int64, description="有效因子总数"),
        Field(name="momentum_consistency", dtype=Int64, description="动量因子一致性"),
        Field(name="reversal_consistency", dtype=Int64, description="反转因子一致性"),
        Field(name="factor_strength", dtype=Float64, description="因子强度"),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "composite"},
)

# Alpha 101 实时因子推送源
alpha101_realtime_push_source = PushSource(
    name="alpha101_realtime_push_source",
    schema=[
        Field(name="symbol", dtype=String),
        
        # 核心实时Alpha因子
        Field(name="alpha001_rt", dtype=Float64, description="实时价格反转因子"),
        Field(name="alpha003_rt", dtype=Float64, description="实时价格成交量相关性"),
        Field(name="alpha006_rt", dtype=Float64, description="实时开盘成交量相关性"),
        Field(name="alpha012_rt", dtype=Float64, description="实时成交量价格变化"),
        Field(name="alpha041_rt", dtype=Float64, description="实时几何平均价格偏离"),
        Field(name="alpha101_rt", dtype=Float64, description="实时收盘开盘价比率"),
        
        # 实时组合因子
        Field(name="momentum_alpha_rt", dtype=Float64, description="实时动量Alpha组合"),
        Field(name="reversal_alpha_rt", dtype=Float64, description="实时反转Alpha组合"),
        Field(name="volume_alpha_rt", dtype=Float64, description="实时成交量Alpha组合"),
        
        # 实时市场状态
        Field(name="alpha_market_regime", dtype=String, description="Alpha因子市场状态"),
        Field(name="alpha_signal_strength", dtype=Float64, description="Alpha信号强度"),
    ],
)

# Alpha 101 实时因子特征视图
alpha101_realtime_features_fv = FeatureView(
    name="alpha101_realtime_factors",
    entities=[trading_pair],
    ttl=timedelta(minutes=30),
    schema=[
        # 实时核心因子
        Field(name="alpha001_rt", dtype=Float64),
        Field(name="alpha003_rt", dtype=Float64),
        Field(name="alpha006_rt", dtype=Float64),
        Field(name="alpha012_rt", dtype=Float64),
        Field(name="alpha041_rt", dtype=Float64),
        Field(name="alpha101_rt", dtype=Float64),
        
        # 实时组合因子
        Field(name="momentum_alpha_rt", dtype=Float64),
        Field(name="reversal_alpha_rt", dtype=Float64),
        Field(name="volume_alpha_rt", dtype=Float64),
        
        # 实时状态
        Field(name="alpha_market_regime", dtype=String),
        Field(name="alpha_signal_strength", dtype=Float64),
    ],
    source=alpha101_realtime_push_source,
    tags={"team": "quant", "type": "alpha101", "category": "realtime"},
)

# Alpha 101 因子选择特征视图 (精选高效因子)
alpha101_selected_features_fv = FeatureView(
    name="alpha101_selected_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # 精选的高效Alpha因子 (基于学术研究和实践验证)
        Field(name="alpha001", dtype=Float64, description="经典反转因子"),
        Field(name="alpha003", dtype=Float64, description="经典量价背离因子"),
        Field(name="alpha006", dtype=Float64, description="经典量价相关因子"),
        Field(name="alpha012", dtype=Float64, description="经典成交量动量因子"),
        Field(name="alpha022", dtype=Float64, description="高价成交量相关性动态因子"),
        Field(name="alpha028", dtype=Float64, description="标准化趋势因子"),
        Field(name="alpha032", dtype=Float64, description="VWAP相关性因子"),
        Field(name="alpha041", dtype=Float64, description="几何平均偏离因子"),
        Field(name="alpha053", dtype=Float64, description="价格位置动态因子"),
        Field(name="alpha101", dtype=Float64, description="经典日内动量因子"),
        
        # 组合因子
        Field(name="momentum_alpha_composite", dtype=Float64),
        Field(name="reversal_alpha_composite", dtype=Float64),
        Field(name="volume_alpha_composite", dtype=Float64),
        Field(name="market_neutral_alpha", dtype=Float64),
        
        # 质量指标
        Field(name="factor_strength", dtype=Float64),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "selected"},
)