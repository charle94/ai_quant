#!/usr/bin/env python3
"""
基础Feast功能测试
"""
import os
import sys
import pandas as pd
import duckdb
from datetime import datetime, timedelta

def test_feature_store_config():
    """测试特征存储配置"""
    print("=== 测试特征存储配置 ===")
    
    # 测试DuckDB连接
    conn = duckdb.connect('/workspace/data/quant_features.duckdb')
    
    # 读取特征数据
    df = conn.execute("""
        SELECT symbol, timestamp, price, ma_5, ma_20, rsi_14, volume_ratio, entity_id, event_timestamp
        FROM main.features_ohlc_technical 
        LIMIT 10
    """).fetchdf()
    
    print(f"特征数据样本 ({len(df)} 条记录):")
    print(df)
    
    # 测试Redis连接
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("\n✓ Redis连接成功")
        
        # 测试Redis存储
        test_key = "test:feature:BTCUSDT"
        test_value = {"price": 45100.0, "ma_5": 45175.0, "timestamp": "2024-01-01T09:00:00"}
        r.hset(test_key, mapping=test_value)
        
        # 读取测试
        stored_value = r.hgetall(test_key)
        print(f"Redis存储测试: {stored_value}")
        
        # 清理测试数据
        r.delete(test_key)
        
    except Exception as e:
        print(f"✗ Redis连接失败: {e}")
        return False
    
    conn.close()
    return True

def test_feature_preparation():
    """测试特征数据准备"""
    print("\n=== 测试特征数据准备 ===")
    
    conn = duckdb.connect('/workspace/data/quant_features.duckdb')
    
    # 准备Feast格式的特征数据
    df = conn.execute("""
        SELECT 
            entity_id,
            event_timestamp,
            price,
            ma_5,
            ma_20,
            rsi_14,
            volume_ratio,
            momentum_5d,
            volatility_20d
        FROM main.features_ohlc_technical
        ORDER BY event_timestamp
    """).fetchdf()
    
    print(f"准备用于Feast的特征数据: {len(df)} 条记录")
    print(df.head())
    
    # 保存为Parquet格式供Feast使用
    df.to_parquet('/workspace/data/feast_features.parquet')
    print(f"✓ 特征数据已保存到 /workspace/data/feast_features.parquet")
    
    conn.close()
    return df

def create_feast_feature_definition():
    """创建Feast特征定义文件"""
    print("\n=== 创建Feast特征定义 ===")
    
    feature_def = '''
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, String
from datetime import timedelta

# 定义实体
symbol_entity = Entity(
    name="symbol_entity",
    value_type=String,
    description="股票/加密货币交易对",
)

# 定义数据源
quant_features_source = FileSource(
    path="/workspace/data/feast_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="event_timestamp",
)

# 定义特征视图
quant_features_fv = FeatureView(
    name="quant_features",
    entities=[symbol_entity],
    ttl=timedelta(days=1),
    schema=[
        Field(name="price", dtype=Float64),
        Field(name="ma_5", dtype=Float64),
        Field(name="ma_20", dtype=Float64),
        Field(name="rsi_14", dtype=Float64),
        Field(name="volume_ratio", dtype=Float64),
        Field(name="momentum_5d", dtype=Float64),
        Field(name="volatility_20d", dtype=Float64),
    ],
    source=quant_features_source,
    tags={"team": "quant_team"},
)
'''
    
    with open('/workspace/feast_config/feature_repo/test_features.py', 'w') as f:
        f.write(feature_def)
    
    print("✓ Feast特征定义已创建")
    return True

def main():
    """主测试函数"""
    print("开始Feast基础功能测试...")
    
    # 测试1: 配置测试
    if not test_feature_store_config():
        print("✗ 特征存储配置测试失败")
        return False
    
    # 测试2: 特征数据准备
    df = test_feature_preparation()
    if df is None or len(df) == 0:
        print("✗ 特征数据准备失败")
        return False
    
    # 测试3: 创建Feast特征定义
    if not create_feast_feature_definition():
        print("✗ Feast特征定义创建失败")
        return False
    
    print("\n🎉 Feast基础功能测试完成!")
    print("✓ DuckDB特征数据读取正常")
    print("✓ Redis在线存储连接正常")
    print("✓ 特征数据格式准备完成")
    print("✓ Feast特征定义创建完成")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)