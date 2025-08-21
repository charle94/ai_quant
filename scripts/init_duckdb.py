#!/usr/bin/env python3
"""
初始化DuckDB数据库和表结构
"""
import duckdb
import pandas as pd
import yaml
import os
from pathlib import Path

def load_config():
    """加载数据库配置"""
    config_path = Path(__file__).parent.parent / "config" / "database.yml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def init_offline_db(db_path):
    """初始化离线特征数据库"""
    print(f"初始化离线数据库: {db_path}")
    
    # 确保目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # 创建schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")
    
    # 创建原始数据表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ohlc_data (
            symbol VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        )
    """)
    
    # 创建索引
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_timestamp 
        ON raw.ohlc_data (symbol, timestamp)
    """)
    
    print("离线数据库初始化完成")
    conn.close()

def init_realtime_db(db_path):
    """初始化实时特征数据库"""
    print(f"初始化实时数据库: {db_path}")
    
    # 确保目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # 创建实时特征表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.realtime_features (
            symbol VARCHAR,
            timestamp TIMESTAMP,
            price DOUBLE,
            volume BIGINT,
            ma_5 DOUBLE,
            ma_10 DOUBLE,
            rsi_14 DOUBLE,
            volatility DOUBLE,
            volume_ratio DOUBLE,
            momentum_5d DOUBLE,
            entity_id VARCHAR,
            event_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 创建索引
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_realtime_symbol_timestamp 
        ON main.realtime_features (symbol, timestamp)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_realtime_entity_id 
        ON main.realtime_features (entity_id)
    """)
    
    print("实时数据库初始化完成")
    conn.close()

def create_sample_data(db_path):
    """创建示例数据用于测试"""
    print("创建示例数据...")
    
    conn = duckdb.connect(db_path)
    
    # 生成示例OHLC数据
    sample_data = """
        INSERT INTO raw.ohlc_data VALUES
        ('BTCUSDT', '2024-01-01 09:00:00', 45000, 45200, 44800, 45100, 1000000),
        ('BTCUSDT', '2024-01-01 09:01:00', 45100, 45300, 45000, 45250, 1100000),
        ('BTCUSDT', '2024-01-01 09:02:00', 45250, 45400, 45150, 45350, 1200000),
        ('BTCUSDT', '2024-01-01 09:03:00', 45350, 45500, 45200, 45400, 1050000),
        ('BTCUSDT', '2024-01-01 09:04:00', 45400, 45600, 45300, 45500, 1300000),
        ('ETHUSDT', '2024-01-01 09:00:00', 2500, 2520, 2480, 2510, 500000),
        ('ETHUSDT', '2024-01-01 09:01:00', 2510, 2530, 2500, 2525, 550000),
        ('ETHUSDT', '2024-01-01 09:02:00', 2525, 2540, 2515, 2535, 600000),
        ('ETHUSDT', '2024-01-01 09:03:00', 2535, 2550, 2520, 2545, 520000),
        ('ETHUSDT', '2024-01-01 09:04:00', 2545, 2560, 2535, 2555, 650000)
    """
    
    try:
        conn.execute(sample_data)
        print("示例数据创建完成")
    except Exception as e:
        print(f"创建示例数据时出错: {e}")
    
    conn.close()

def main():
    """主函数"""
    config = load_config()
    
    # 初始化离线数据库
    offline_db_path = config['duckdb']['offline_db_path']
    init_offline_db(offline_db_path)
    
    # 初始化实时数据库
    realtime_db_path = config['duckdb']['realtime_db_path']
    init_realtime_db(realtime_db_path)
    
    # 创建示例数据
    create_sample_data(offline_db_path)
    
    print("所有数据库初始化完成!")

if __name__ == "__main__":
    main()