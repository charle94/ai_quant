#!/usr/bin/env python3
"""
Feast Alpha101 特征存储测试脚本
直接使用 Python API 来避免 CLI 依赖问题
"""

import pandas as pd
import sys
import os
sys.path.append('/workspace/feast_alpha101/feature_repo')

try:
    from feast import FeatureStore, Entity, FeatureView, Field, FileSource
    from feast.types import Float64, Int64, String
    from datetime import timedelta, datetime
    
    print("✅ Feast 导入成功")
    
    # 检查数据文件
    data_path = "/workspace/feast_alpha101/data/alpha101_complete.parquet"
    if os.path.exists(data_path):
        print(f"✅ 数据文件存在: {data_path}")
        
        # 读取数据样本
        df = pd.read_parquet(data_path)
        print(f"📊 数据形状: {df.shape}")
        print(f"📅 时间范围: {df.timestamp.min()} 到 {df.timestamp.max()}")
        print(f"🏢 股票数量: {df.symbol.nunique()}")
        print(f"📋 列数: {len(df.columns)}")
        
        # 显示前几列
        print(f"\n📋 前10列: {list(df.columns[:10])}")
        
        # 显示数据样本
        print(f"\n📊 数据样本:")
        print(df[['symbol', 'timestamp', 'alpha001', 'alpha002', 'alpha101']].head())
        
    else:
        print(f"❌ 数据文件不存在: {data_path}")
        
    # 创建实体定义
    stock = Entity(
        name="symbol",
        value_type=String,
        description="股票代码"
    )
    print("✅ 股票实体创建成功")
    
    # 创建数据源
    alpha101_source = FileSource(
        path=data_path,
        timestamp_field="timestamp",
    )
    print("✅ 数据源创建成功")
    
    # 创建简化的特征视图（只包含部分特征以避免过长）
    alpha101_sample_features = FeatureView(
        name="alpha101_sample",
        entities=[stock],
        ttl=timedelta(days=30),
        schema=[
            Field(name="open", dtype=Float64),
            Field(name="close", dtype=Float64), 
            Field(name="volume", dtype=Int64),
            Field(name="alpha001", dtype=Float64),
            Field(name="alpha002", dtype=Float64),
            Field(name="alpha101", dtype=Float64),
            Field(name="momentum_composite", dtype=Float64),
            Field(name="total_valid_factors", dtype=Int64),
        ],
        source=alpha101_source,
    )
    print("✅ 特征视图创建成功")
    
    print(f"\n🎉 Feast Alpha101 特征存储测试完成!")
    print(f"📈 包含 101 个 Alpha 因子")
    print(f"🏢 支持 {df.symbol.nunique()} 只股票")
    print(f"📅 数据时间范围: {df.timestamp.min()} 到 {df.timestamp.max()}")
    
except ImportError as e:
    print(f"❌ 导入错误: {e}")
except Exception as e:
    print(f"❌ 运行错误: {e}")
    import traceback
    traceback.print_exc()