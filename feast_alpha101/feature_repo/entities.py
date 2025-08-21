"""
股票实体定义
定义了用于 Alpha101 特征的股票实体
"""

from feast import Entity, ValueType

# 股票实体定义
stock = Entity(
    name="symbol",
    value_type=ValueType.STRING,
    description="股票代码 - 用于标识不同的股票证券",
    tags={
        "entity_type": "stock",
        "data_source": "market_data",
        "created_by": "alpha101_pipeline"
    }
)