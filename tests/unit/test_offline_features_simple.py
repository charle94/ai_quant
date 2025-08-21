#!/usr/bin/env python3
"""
离线特征衍生模块简单测试
"""
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
import sys
import os

def create_test_data(conn):
    """创建测试数据"""
    # 生成30天的OHLC数据
    dates = pd.date_range(start='2024-01-01', periods=30, freq='1D')
    
    test_data = []
    base_price = 45000
    
    for i, date in enumerate(dates):
        # 模拟价格走势
        price_change = np.random.normal(0, 0.02) * base_price
        base_price = max(base_price + price_change, base_price * 0.9)
        
        open_price = base_price
        high_price = base_price * (1 + abs(np.random.normal(0, 0.01)))
        low_price = base_price * (1 - abs(np.random.normal(0, 0.01)))
        close_price = base_price + np.random.normal(0, 0.005) * base_price
        volume = int(np.random.exponential(1000000))
        
        test_data.append({
            'symbol': 'BTCUSDT',
            'timestamp': date,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        })
    
    # 插入测试数据
    df = pd.DataFrame(test_data)
    
    # 创建表
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw
    """)
    
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
    
    # 清空并插入数据
    conn.execute("DELETE FROM raw.ohlc_data")
    conn.execute("INSERT INTO raw.ohlc_data SELECT * FROM df")
    
    return len(test_data)

def test_data_cleaning(conn):
    """测试数据清洗"""
    print("📊 测试数据清洗...")
    
    query = """
    SELECT 
        symbol,
        timestamp,
        open, high, low, close, volume,
        -- 数据验证
        CASE 
            WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 'INVALID'
            WHEN high < GREATEST(open, close, low) THEN 'INVALID'
            WHEN low > LEAST(open, close, high) THEN 'INVALID'
            ELSE 'VALID'
        END as data_quality
    FROM raw.ohlc_data
    """
    
    result = conn.execute(query).df()
    
    # 验证结果
    assert len(result) > 0, "应该有数据返回"
    
    invalid_count = len(result[result['data_quality'] == 'INVALID'])
    valid_count = len(result[result['data_quality'] == 'VALID'])
    
    print(f"   ✅ 数据清洗完成: {valid_count} 条有效，{invalid_count} 条无效")
    return True

def test_basic_features(conn):
    """测试基础特征计算"""
    print("📊 测试基础特征计算...")
    
    query = """
    SELECT 
        symbol,
        timestamp,
        close,
        -- 基础特征
        (high + low + close) / 3 as typical_price,
        (high - low) as daily_range,
        CASE WHEN open != 0 THEN (close - open) / open ELSE 0 END as daily_return,
        CASE WHEN close != 0 THEN volume / close ELSE 0 END as volume_price_ratio
    FROM raw.ohlc_data
    ORDER BY timestamp
    """
    
    result = conn.execute(query).df()
    
    # 验证基础特征
    assert len(result) > 0, "应该有特征数据"
    assert not result['typical_price'].isna().any(), "typical_price不应有空值"
    assert all(result['daily_range'] >= 0), "daily_range应该非负"
    
    print(f"   ✅ 基础特征计算完成: {len(result)} 条记录")
    print(f"   📈 平均日收益率: {result['daily_return'].mean():.4f}")
    print(f"   📊 平均波动范围: {result['daily_range'].mean():.2f}")
    return True

def test_moving_averages(conn):
    """测试移动平均线"""
    print("📊 测试移动平均线...")
    
    query = """
    SELECT 
        symbol,
        timestamp,
        close,
        -- 移动平均线
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as ma_5,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as ma_10,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as ma_20
    FROM raw.ohlc_data
    ORDER BY timestamp
    """
    
    result = conn.execute(query).df()
    
    # 验证移动平均线
    assert len(result) > 0, "应该有移动平均线数据"
    
    print(f"   ✅ 移动平均线计算完成")
    print(f"   📈 最新MA5: {result.iloc[-1]['ma_5']:.2f}")
    print(f"   📈 最新MA10: {result.iloc[-1]['ma_10']:.2f}")
    print(f"   📈 最新MA20: {result.iloc[-1]['ma_20']:.2f}")
    return True

def test_technical_indicators(conn):
    """测试技术指标"""
    print("📊 测试技术指标...")
    
    # 测试RSI计算
    query = """
    WITH price_changes AS (
        SELECT 
            symbol,
            timestamp,
            close,
            close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as price_change
        FROM raw.ohlc_data
    ),
    gains_losses AS (
        SELECT 
            *,
            CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
            CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
        FROM price_changes
        WHERE price_change IS NOT NULL
    ),
    rsi_calc AS (
        SELECT 
            *,
            AVG(gain) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) as avg_gain_14,
            AVG(loss) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) as avg_loss_14
        FROM gains_losses
    )
    SELECT 
        *,
        CASE 
            WHEN avg_loss_14 = 0 THEN 100
            WHEN avg_gain_14 = 0 THEN 0
            ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
        END as rsi_14
    FROM rsi_calc
    ORDER BY timestamp
    """
    
    result = conn.execute(query).df()
    
    if len(result) > 0:
        rsi_values = result['rsi_14'].dropna()
        if len(rsi_values) > 0:
            assert all(rsi_values >= 0), "RSI应该 >= 0"
            assert all(rsi_values <= 100), "RSI应该 <= 100"
            print(f"   ✅ RSI计算完成，范围: {rsi_values.min():.2f} - {rsi_values.max():.2f}")
            print(f"   📊 最新RSI: {result.iloc[-1]['rsi_14']:.2f}")
    
    return True

def test_volume_indicators(conn):
    """测试成交量指标"""
    print("📊 测试成交量指标...")
    
    query = """
    SELECT 
        symbol,
        timestamp,
        close,
        volume,
        -- 成交量移动平均
        AVG(volume) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as avg_volume_20d,
        -- 成交量比率
        volume / NULLIF(AVG(volume) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) as volume_ratio
    FROM raw.ohlc_data
    ORDER BY timestamp
    """
    
    result = conn.execute(query).df()
    
    valid_data = result.dropna()
    if len(valid_data) > 0:
        assert all(valid_data['avg_volume_20d'] > 0), "平均成交量应该为正"
        print(f"   ✅ 成交量指标计算完成")
        print(f"   📊 平均成交量比率: {valid_data['volume_ratio'].mean():.2f}")
    
    return True

def run_offline_features_test():
    """运行离线特征测试"""
    print("🧪 开始离线特征衍生模块测试...")
    
    # 创建内存数据库
    conn = duckdb.connect(":memory:")
    
    try:
        # 创建测试数据
        data_count = create_test_data(conn)
        print(f"📊 创建了 {data_count} 条测试数据")
        
        # 运行测试
        tests = [
            test_data_cleaning,
            test_basic_features,
            test_moving_averages,
            test_technical_indicators,
            test_volume_indicators
        ]
        
        passed = 0
        failed = 0
        
        for test_func in tests:
            try:
                test_func(conn)
                passed += 1
            except Exception as e:
                print(f"❌ 测试失败: {test_func.__name__} - {str(e)}")
                failed += 1
        
        print(f"\n📈 离线特征测试结果:")
        print(f"   ✅ 通过: {passed}")
        print(f"   ❌ 失败: {failed}")
        print(f"   📊 通过率: {passed/(passed+failed)*100:.1f}%")
        
        return failed == 0
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = run_offline_features_test()
    exit(0 if success else 1)