from feast import Entity, ValueType

# 定义实体
trading_pair = Entity(
    name="trading_pair",
    description="Trading pair entity (e.g., BTCUSDT, ETHUSDT)",
    value_type=ValueType.STRING,
)