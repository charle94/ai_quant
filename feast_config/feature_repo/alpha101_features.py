from datetime import timedelta
from feast import FeatureView, Field, PushSource
from feast.types import Float64, Int64, String
from entities import trading_pair
from data_sources import offline_features_source

# Alpha 101 因子特征视图
alpha101_features_fv = FeatureView(
    name="alpha101_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # 基础Alpha因子 (1-20)
        Field(name="alpha001", dtype=Float64),
        Field(name="alpha002", dtype=Float64),
        Field(name="alpha003", dtype=Float64),
        Field(name="alpha004", dtype=Float64),
        Field(name="alpha005", dtype=Float64),
        Field(name="alpha006", dtype=Float64),
        Field(name="alpha007", dtype=Float64),
        Field(name="alpha008", dtype=Float64),
        Field(name="alpha009", dtype=Float64),
        Field(name="alpha010", dtype=Float64),
        Field(name="alpha011", dtype=Float64),
        Field(name="alpha012", dtype=Float64),
        Field(name="alpha013", dtype=Float64),
        Field(name="alpha014", dtype=Float64),
        Field(name="alpha015", dtype=Float64),
        Field(name="alpha016", dtype=Float64),
        Field(name="alpha017", dtype=Float64),
        Field(name="alpha018", dtype=Float64),
        Field(name="alpha019", dtype=Float64),
        Field(name="alpha020", dtype=Float64),
        
        # 扩展Alpha因子 (21-50)
        Field(name="alpha021", dtype=Float64),
        Field(name="alpha022", dtype=Float64),
        Field(name="alpha023", dtype=Float64),
        Field(name="alpha024", dtype=Float64),
        Field(name="alpha025", dtype=Float64),
        Field(name="alpha026", dtype=Float64),
        Field(name="alpha027", dtype=Float64),
        Field(name="alpha028", dtype=Float64),
        Field(name="alpha029", dtype=Float64),
        Field(name="alpha030", dtype=Float64),
        Field(name="alpha031", dtype=Float64),
        Field(name="alpha032", dtype=Float64),
        Field(name="alpha033", dtype=Float64),
        Field(name="alpha034", dtype=Float64),
        Field(name="alpha035", dtype=Float64),
        Field(name="alpha036", dtype=Float64),
        Field(name="alpha037", dtype=Float64),
        Field(name="alpha038", dtype=Float64),
        Field(name="alpha039", dtype=Float64),
        Field(name="alpha040", dtype=Float64),
        Field(name="alpha041", dtype=Float64),
        Field(name="alpha042", dtype=Float64),
        Field(name="alpha043", dtype=Float64),
        Field(name="alpha044", dtype=Float64),
        Field(name="alpha045", dtype=Float64),
        Field(name="alpha046", dtype=Float64),
        Field(name="alpha047", dtype=Float64),
        Field(name="alpha048", dtype=Float64),
        Field(name="alpha049", dtype=Float64),
        Field(name="alpha050", dtype=Float64),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "factors"},
)

# 高级因子特征视图
alpha_advanced_features_fv = FeatureView(
    name="alpha_advanced_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # 动量类因子
        Field(name="momentum_reversal_norm", dtype=Float64),
        Field(name="short_momentum_norm", dtype=Float64),
        Field(name="medium_momentum_norm", dtype=Float64),
        Field(name="long_momentum_norm", dtype=Float64),
        Field(name="price_acceleration_norm", dtype=Float64),
        
        # 成交量价格关系因子
        Field(name="volume_price_divergence_norm", dtype=Float64),
        Field(name="volume_price_confirmation_norm", dtype=Float64),
        Field(name="volume_breakout_norm", dtype=Float64),
        Field(name="relative_volume_norm", dtype=Float64),
        
        # 波动率因子
        Field(name="volatility_rank_norm", dtype=Float64),
        Field(name="price_volatility_norm", dtype=Float64),
        Field(name="return_volatility_norm", dtype=Float64),
        Field(name="volatility_breakout_norm", dtype=Float64),
        
        # 趋势因子
        Field(name="ma_trend_norm", dtype=Float64),
        Field(name="multi_timeframe_trend_norm", dtype=Float64),
        Field(name="trend_strength_norm", dtype=Float64),
        
        # 均值回归因子
        Field(name="bollinger_position_norm", dtype=Float64),
        Field(name="price_deviation_norm", dtype=Float64),
        Field(name="rsi_like_factor_norm", dtype=Float64),
        
        # 技术分析因子
        Field(name="hl_position_norm", dtype=Float64),
        Field(name="hl_range_norm", dtype=Float64),
        Field(name="shadow_length_norm", dtype=Float64),
        Field(name="macd_like_norm", dtype=Float64),
        Field(name="stoch_like_norm", dtype=Float64),
        Field(name="williams_like_norm", dtype=Float64),
        
        # 市场微观结构因子
        Field(name="opening_gap_norm", dtype=Float64),
        Field(name="closing_strength_norm", dtype=Float64),
        Field(name="intraday_momentum_norm", dtype=Float64),
        Field(name="intraday_volatility_norm", dtype=Float64),
        
        # 跨期套利因子
        Field(name="term_structure_norm", dtype=Float64),
        Field(name="basis_like_norm", dtype=Float64),
        Field(name="convenience_yield_norm", dtype=Float64),
        
        # 情绪和行为因子
        Field(name="sentiment_volume_norm", dtype=Float64),
        Field(name="herding_effect_norm", dtype=Float64),
        Field(name="overreaction_norm", dtype=Float64),
        
        # 质量因子
        Field(name="price_quality_norm", dtype=Float64),
        Field(name="liquidity_norm", dtype=Float64),
        Field(name="efficiency_norm", dtype=Float64),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "advanced"},
)

# 因子组合特征视图
alpha_composite_features_fv = FeatureView(
    name="alpha_composite_factors",
    entities=[trading_pair],
    ttl=timedelta(days=1),
    schema=[
        # 组合因子
        Field(name="momentum_composite", dtype=Float64),
        Field(name="reversal_composite", dtype=Float64),
        Field(name="volume_composite", dtype=Float64),
        Field(name="volatility_composite", dtype=Float64),
        Field(name="trend_composite", dtype=Float64),
        
        # 风险调整因子
        Field(name="risk_adjusted_momentum", dtype=Float64),
        Field(name="risk_adjusted_reversal", dtype=Float64),
        
        # 市场状态因子
        Field(name="market_stress", dtype=Float64),
        Field(name="market_efficiency", dtype=Float64),
        
        # 元因子
        Field(name="factor_momentum_alpha001", dtype=Float64),
        Field(name="factor_momentum_volume", dtype=Float64),
        Field(name="factor_volatility_alpha003", dtype=Float64),
        Field(name="factor_volatility_momentum", dtype=Float64),
        Field(name="factor_corr_mom_rev", dtype=Float64),
        
        # 市场状态标识
        Field(name="market_regime", dtype=String),
        Field(name="volatility_regime", dtype=String),
        Field(name="volume_regime", dtype=String),
    ],
    source=offline_features_source,
    tags={"team": "quant", "type": "alpha101", "category": "composite"},
)

# Alpha因子实时推送源
alpha_realtime_push_source = PushSource(
    name="alpha_realtime_push_source",
    schema=[
        Field(name="symbol", dtype=String),
        
        # 核心Alpha因子 (实时计算的简化版本)
        Field(name="alpha001_rt", dtype=Float64),
        Field(name="alpha003_rt", dtype=Float64),
        Field(name="alpha006_rt", dtype=Float64),
        Field(name="alpha012_rt", dtype=Float64),
        
        # 实时组合因子
        Field(name="momentum_composite_rt", dtype=Float64),
        Field(name="reversal_composite_rt", dtype=Float64),
        Field(name="volume_composite_rt", dtype=Float64),
        
        # 实时市场状态
        Field(name="market_regime_rt", dtype=String),
        Field(name="volatility_regime_rt", dtype=String),
    ],
)

# Alpha因子实时特征视图
alpha_realtime_features_fv = FeatureView(
    name="alpha_realtime_factors",
    entities=[trading_pair],
    ttl=timedelta(minutes=30),
    schema=[
        # 实时Alpha因子
        Field(name="alpha001_rt", dtype=Float64),
        Field(name="alpha003_rt", dtype=Float64),
        Field(name="alpha006_rt", dtype=Float64),
        Field(name="alpha012_rt", dtype=Float64),
        
        # 实时组合因子
        Field(name="momentum_composite_rt", dtype=Float64),
        Field(name="reversal_composite_rt", dtype=Float64),
        Field(name="volume_composite_rt", dtype=Float64),
        
        # 实时市场状态
        Field(name="market_regime_rt", dtype=String),
        Field(name="volatility_regime_rt", dtype=String),
    ],
    source=alpha_realtime_push_source,
    tags={"team": "quant", "type": "alpha101", "category": "realtime"},
)