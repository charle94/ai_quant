"""
Alpha101 特征定义
包含完整的 101 个 Alpha 因子特征定义和详细注释
"""

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String
from datetime import timedelta
from entities import stock

# 定义数据源
alpha101_source = FileSource(
    path="/workspace/feast_alpha101/data/alpha101_complete.parquet",
    timestamp_field="timestamp",
    created_timestamp_column="timestamp",
)

# Alpha101 特征视图定义
alpha101_features = FeatureView(
    name="alpha101_features",
    entities=[stock],
    ttl=timedelta(days=30),  # 特征有效期 30 天
    schema=[
        # ===== 基础市场数据 =====
        Field(name="open", dtype=Float64, description="开盘价 - 当日交易开始时的价格"),
        Field(name="high", dtype=Float64, description="最高价 - 当日交易的最高价格"),
        Field(name="low", dtype=Float64, description="最低价 - 当日交易的最低价格"),
        Field(name="close", dtype=Float64, description="收盘价 - 当日交易结束时的价格"),
        Field(name="volume", dtype=Int64, description="成交量 - 当日总交易股数"),
        Field(name="vwap", dtype=Float64, description="成交量加权平均价格 - Volume Weighted Average Price"),
        Field(name="returns", dtype=Float64, description="日收益率 - 当日价格变化百分比"),

        # ===== Alpha 001-020: 基础动量和反转因子 =====
        Field(name="alpha001", dtype=Float64, description="Alpha001 - 收益率排序因子，基于风险调整后的收益率排序"),
        Field(name="alpha002", dtype=Float64, description="Alpha002 - 成交量变化排序因子，衡量成交量相对变化"),
        Field(name="alpha003", dtype=Float64, description="Alpha003 - 价格成交量比率，反映流动性特征"),
        Field(name="alpha004", dtype=Float64, description="Alpha004 - 成交量相对强度，当前成交量与平均成交量比率"),
        Field(name="alpha005", dtype=Float64, description="Alpha005 - 开盘价相对VWAP强度，衡量开盘价偏离程度"),
        Field(name="alpha006", dtype=Float64, description="Alpha006 - 开盘价成交量乘积，综合价格和成交量信息"),
        Field(name="alpha007", dtype=Float64, description="Alpha007 - 价格变化幅度，7日价格变化的绝对值"),
        Field(name="alpha008", dtype=Float64, description="Alpha008 - 开盘价动量，开盘价相对前日收盘价变化"),
        Field(name="alpha009", dtype=Float64, description="Alpha009 - 价格变化方向，价格上涨下跌的方向性指标"),
        Field(name="alpha010", dtype=Float64, description="Alpha010 - 收盘价变化，当日收盘价变化量"),
        Field(name="alpha011", dtype=Float64, description="Alpha011 - VWAP收盘价差异，反映价格偏离均值程度"),
        Field(name="alpha012", dtype=Float64, description="Alpha012 - 成交量价格关系，成交量和价格变化的协同性"),
        Field(name="alpha013", dtype=Float64, description="Alpha013 - 收盘价排序，当日收盘价在所有股票中的排名"),
        Field(name="alpha014", dtype=Float64, description="Alpha014 - 风险调整收益率，收益率除以波动率"),
        Field(name="alpha015", dtype=Float64, description="Alpha015 - 高价排序，最高价在所有股票中的排名"),
        Field(name="alpha016", dtype=Float64, description="Alpha016 - 高价负值，最高价的负数表示"),
        Field(name="alpha017", dtype=Float64, description="Alpha017 - VWAP相对强度，VWAP相对收盘价的偏离"),
        Field(name="alpha018", dtype=Float64, description="Alpha018 - 日内波动率，开盘收盘价差异相对收盘价比率"),
        Field(name="alpha019", dtype=Float64, description="Alpha019 - 中期价格动量，5日价格变化率"),
        Field(name="alpha020", dtype=Float64, description="Alpha020 - 开盘价复合强度，开盘价多重相对强度指标"),

        # ===== Alpha 021-040: 扩展技术指标 =====
        Field(name="alpha021", dtype=Float64, description="Alpha021 - 成交量相对强度，成交量与20日均量比率"),
        Field(name="alpha022", dtype=Float64, description="Alpha022 - 高价变化率，最高价相对前日收盘价变化"),
        Field(name="alpha023", dtype=Float64, description="Alpha023 - 低价相对强度，最低价相对前日收盘价的条件逻辑"),
        Field(name="alpha024", dtype=Float64, description="Alpha024 - 均线相对强度，5日均线相对20日均线比率"),
        Field(name="alpha025", dtype=Float64, description="Alpha025 - 成交量排序，当日成交量在所有股票中排名"),
        Field(name="alpha026", dtype=Float64, description="Alpha026 - VWAP动量，VWAP相对5日前值的变化率"),
        Field(name="alpha027", dtype=Float64, description="Alpha027 - 成交量变化率，当日成交量相对前日变化"),
        Field(name="alpha028", dtype=Float64, description="Alpha028 - 短期趋势强度，5日均线相对10日均线比率"),
        Field(name="alpha029", dtype=Float64, description="Alpha029 - 价格相对位置，收盘价在当日高低价区间位置"),
        Field(name="alpha030", dtype=Float64, description="Alpha030 - 多重信号组合，价格和成交量多重信号加总"),
        Field(name="alpha031", dtype=Float64, description="Alpha031 - 低价相关性，最低价相对收盘价比率"),
        Field(name="alpha032", dtype=Float64, description="Alpha032 - 中期价格动量，10日价格变化率"),
        Field(name="alpha033", dtype=Float64, description="Alpha033 - 开盘收盘比率，1减去开盘价除以收盘价"),
        Field(name="alpha034", dtype=Float64, description="Alpha034 - 波动率比率，20日波动率相对当日收益率"),
        Field(name="alpha035", dtype=Float64, description="Alpha035 - 成交量动量，成交量相对20日均量比率"),
        Field(name="alpha036", dtype=Float64, description="Alpha036 - VWAP成交量关系，VWAP偏离度与成交量乘积"),
        Field(name="alpha037", dtype=Float64, description="Alpha037 - 日内收益率，开盘收盘价差相对开盘价比率"),
        Field(name="alpha038", dtype=Float64, description="Alpha038 - 开盘收盘比，收盘价除以开盘价"),
        Field(name="alpha039", dtype=Float64, description="Alpha039 - 价格变化加权，7日价格变化与成交量权重乘积"),
        Field(name="alpha040", dtype=Float64, description="Alpha040 - 成交量标准化，成交量相对均值的标准化偏离"),

        # ===== Alpha 041-060: 高级技术因子 =====
        Field(name="alpha041", dtype=Float64, description="Alpha041 - VWAP相对强度，VWAP相对收盘价偏离度"),
        Field(name="alpha042", dtype=Float64, description="Alpha042 - VWAP排序比率，VWAP差异排序的比率关系"),
        Field(name="alpha043", dtype=Float64, description="Alpha043 - 成交量条件强度，成交量增长时的相对强度"),
        Field(name="alpha044", dtype=Float64, description="Alpha044 - 低价成交量关系，最低价与成交量乘积"),
        Field(name="alpha045", dtype=Float64, description="Alpha045 - 成交量加权价格，开盘收盘价的成交量加权平均"),
        Field(name="alpha046", dtype=Float64, description="Alpha046 - 中期价格动量，10日均线相对20日均线比率"),
        Field(name="alpha047", dtype=Float64, description="Alpha047 - 复合价格成交量关系，多重价格成交量指标组合"),
        Field(name="alpha048", dtype=Float64, description="Alpha048 - 标准化收益率，收益率除以历史波动率"),
        Field(name="alpha049", dtype=Float64, description="Alpha049 - 价格变化比率，短期与中期价格变化比较"),
        Field(name="alpha050", dtype=Float64, description="Alpha050 - 成交量相对强度，成交量相对20日均量比率"),
        Field(name="alpha051", dtype=Float64, description="Alpha051 - 价格变化逻辑，价格下跌时的条件逻辑"),
        Field(name="alpha052", dtype=Float64, description="Alpha052 - 短期价格动量，5日价格变化率"),
        Field(name="alpha053", dtype=Float64, description="Alpha053 - 随机指标样式，价格在高低价区间的相对位置"),
        Field(name="alpha054", dtype=Float64, description="Alpha054 - 开盘价复合强度，开盘价多重幂函数变换"),
        Field(name="alpha055", dtype=Float64, description="Alpha055 - 长期随机指标，12日高低价区间的价格位置"),
        Field(name="alpha056", dtype=Float64, description="Alpha056 - 收益率符号强度，收益率相对绝对值比率"),
        Field(name="alpha057", dtype=Float64, description="Alpha057 - VWAP均线关系，VWAP相对均线差异的比率"),
        Field(name="alpha058", dtype=Float64, description="Alpha058 - 收盘价排序，收盘价的百分位排名"),
        Field(name="alpha059", dtype=Float64, description="Alpha059 - 成交量排序，成交量的百分位排名"),
        Field(name="alpha060", dtype=Float64, description="Alpha060 - 价格成交量乘积，价格位置与成交量乘积"),

        # ===== Alpha 061-080: 复杂技术因子 =====
        Field(name="alpha061", dtype=Float64, description="Alpha061 - 成交量排序，成交量百分位排名"),
        Field(name="alpha062", dtype=Float64, description="Alpha062 - 高价成交量相关性，最高价与成交量5日相关性负值"),
        Field(name="alpha063", dtype=Float64, description="Alpha063 - 价格动量幂函数，价格变化率的符号幂函数"),
        Field(name="alpha064", dtype=Float64, description="Alpha064 - 成交量相对强度排序，成交量比率的排序"),
        Field(name="alpha065", dtype=Float64, description="Alpha065 - 价格相对强度排序，价格变化率的排序"),
        Field(name="alpha066", dtype=Float64, description="Alpha066 - 低价VWAP差异，低价VWAP差异的复合指标"),
        Field(name="alpha067", dtype=Float64, description="Alpha067 - 高价排序，最高价的百分位排名"),
        Field(name="alpha068", dtype=Float64, description="Alpha068 - 高价成交量乘积，最高价排序与成交量乘积"),
        Field(name="alpha069", dtype=Float64, description="Alpha069 - 价格变化平方，价格变化率的平方"),
        Field(name="alpha070", dtype=Float64, description="Alpha070 - 价格波动率，20日收盘价标准差"),
        Field(name="alpha071", dtype=Float64, description="Alpha071 - 价格均值回归，收盘价相对20日均价偏离"),
        Field(name="alpha072", dtype=Float64, description="Alpha072 - 成交量相对强度排序，成交量比率排序"),
        Field(name="alpha073", dtype=Float64, description="Alpha073 - 收盘价排序负值，收盘价排序的负数"),
        Field(name="alpha074", dtype=Float64, description="Alpha074 - 高低价排序和，最高价和最低价排序之和"),
        Field(name="alpha075", dtype=Float64, description="Alpha075 - VWAP成交量相关性，VWAP与成交量4日相关性"),
        Field(name="alpha076", dtype=Float64, description="Alpha076 - 成交量标准化，成交量比率的截面标准化"),
        Field(name="alpha077", dtype=Float64, description="Alpha077 - 高价排序，最高价百分位排名"),
        Field(name="alpha078", dtype=Float64, description="Alpha078 - 低价排序，最低价百分位排名"),
        Field(name="alpha079", dtype=Float64, description="Alpha079 - 价格变化符号，价格变化的符号函数"),
        Field(name="alpha080", dtype=Float64, description="Alpha080 - 成交量变化率，成交量相对前日变化率"),

        # ===== Alpha 081-101: 最终高级因子 =====
        Field(name="alpha081", dtype=Float64, description="Alpha081 - 成交量排序，成交量百分位排名"),
        Field(name="alpha082", dtype=Float64, description="Alpha082 - 收盘价排序，收盘价百分位排名"),
        Field(name="alpha083", dtype=Float64, description="Alpha083 - 高低价差异比率，日内价差相对5日均价比率"),
        Field(name="alpha084", dtype=Float64, description="Alpha084 - VWAP相对强度幂函数，VWAP偏离的符号幂函数"),
        Field(name="alpha085", dtype=Float64, description="Alpha085 - 成交量排序，成交量百分位排名"),
        Field(name="alpha086", dtype=Float64, description="Alpha086 - 价格趋势判断，基于20日均价与10日前价格比较"),
        Field(name="alpha087", dtype=Float64, description="Alpha087 - 收盘价排序，收盘价百分位排名"),
        Field(name="alpha088", dtype=Float64, description="Alpha088 - 长期价格动量，20日价格变化率"),
        Field(name="alpha089", dtype=Float64, description="Alpha089 - 短期价格趋势，5日价格变化率"),
        Field(name="alpha090", dtype=Float64, description="Alpha090 - 收盘价排序，收盘价百分位排名"),
        Field(name="alpha091", dtype=Float64, description="Alpha091 - 成交量排序，成交量百分位排名"),
        Field(name="alpha092", dtype=Float64, description="Alpha092 - 高低价和排序，最高价加最低价的排序"),
        Field(name="alpha093", dtype=Float64, description="Alpha093 - 成交量排序，成交量百分位排名"),
        Field(name="alpha094", dtype=Float64, description="Alpha094 - 长期价格成交量相关性，30日收盘价与成交量相关性"),
        Field(name="alpha095", dtype=Float64, description="Alpha095 - 成交量标准化，成交量的截面标准化"),
        Field(name="alpha096", dtype=Float64, description="Alpha096 - 收盘价排序，收盘价百分位排名"),
        Field(name="alpha097", dtype=Float64, description="Alpha097 - 成交量标准化，成交量的截面标准化"),
        Field(name="alpha098", dtype=Float64, description="Alpha098 - VWAP收盘价相关性，5日VWAP与收盘价相关性"),
        Field(name="alpha099", dtype=Float64, description="Alpha099 - 成交量排序，成交量百分位排名"),
        Field(name="alpha100", dtype=Float64, description="Alpha100 - 收盘价标准化，收盘价的截面标准化"),
        Field(name="alpha101", dtype=Float64, description="Alpha101 - 收盘价相对强度，收盘价相对日内价差的位置"),

        # ===== 组合因子 =====
        Field(name="momentum_composite", dtype=Float64, description="动量组合因子 - 多个动量类Alpha因子的加权平均"),
        Field(name="reversal_composite", dtype=Float64, description="反转组合因子 - 多个反转类Alpha因子的加权平均"),
        Field(name="volume_composite", dtype=Float64, description="成交量组合因子 - 多个成交量类Alpha因子的加权平均"),

        # ===== 统计指标 =====
        Field(name="total_valid_factors", dtype=Int64, description="有效因子总数 - 当日计算成功的Alpha因子数量"),
    ],
    source=alpha101_source,
    tags={
        "team": "quant_research",
        "data_source": "alpha101_pipeline", 
        "model_type": "alpha_factors",
        "update_frequency": "daily",
        "data_quality": "production",
        "version": "1.0.0"
    }
)