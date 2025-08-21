#!/usr/bin/env python3
"""
离线特征衍生模块单元测试
"""
import pytest
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
import sys
import os

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.init_duckdb import init_offline_db, create_sample_data

class TestOfflineFeatures:
    """离线特征测试类"""
    
    @classmethod
    def setup_class(cls):
        """测试类初始化"""
        cls.test_db_path = "/tmp/test_quant_features.duckdb"
        cls.conn = None
        
    def setup_method(self):
        """每个测试方法前的初始化"""
        # 删除测试数据库（如果存在）
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        
        # 初始化测试数据库
        init_offline_db(self.test_db_path)
        self.conn = duckdb.connect(self.test_db_path)
        
        # 创建更多测试数据
        self.create_test_data()
    
    def teardown_method(self):
        """每个测试方法后的清理"""
        if self.conn:
            self.conn.close()
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
    
    def create_test_data(self):
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
        self.conn.execute("DELETE FROM raw.ohlc_data")
        self.conn.execute("INSERT INTO raw.ohlc_data SELECT * FROM df")
    
    def test_data_cleaning(self):
        """测试数据清洗"""
        # 执行数据清洗查询
        query = """
        SELECT 
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            -- 数据验证
            CASE 
                WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 'INVALID'
                WHEN high < GREATEST(open, close, low) THEN 'INVALID'
                WHEN low > LEAST(open, close, high) THEN 'INVALID'
                ELSE 'VALID'
            END as data_quality
        FROM raw.ohlc_data
        """
        
        result = self.conn.execute(query).df()
        
        # 验证结果
        assert len(result) > 0, "应该有数据返回"
        assert all(result['data_quality'] == 'VALID'), "所有数据都应该是有效的"
        
        print(f"✅ 数据清洗测试通过，处理了 {len(result)} 条数据")
    
    def test_basic_features(self):
        """测试基础特征计算"""
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
        
        result = self.conn.execute(query).df()
        
        # 验证基础特征
        assert len(result) > 0, "应该有特征数据"
        assert not result['typical_price'].isna().any(), "typical_price不应有空值"
        assert not result['daily_range'].isna().any(), "daily_range不应有空值"
        assert all(result['daily_range'] >= 0), "daily_range应该非负"
        
        print(f"✅ 基础特征测试通过，计算了 {len(result)} 条记录")
        print(f"   平均日收益率: {result['daily_return'].mean():.4f}")
        print(f"   平均波动范围: {result['daily_range'].mean():.2f}")
    
    def test_moving_averages(self):
        """测试移动平均线"""
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
        
        result = self.conn.execute(query).df()
        
        # 验证移动平均线
        assert len(result) > 0, "应该有移动平均线数据"
        
        # 检查移动平均线的合理性
        for i in range(5, len(result)):
            ma5 = result.iloc[i]['ma_5']
            ma10 = result.iloc[i]['ma_10'] if i >= 10 else None
            close = result.iloc[i]['close']
            
            assert ma5 > 0, "MA5应该为正数"
            if ma10 is not None:
                # MA10应该比MA5更平滑
                pass
        
        print(f"✅ 移动平均线测试通过")
        print(f"   最新MA5: {result.iloc[-1]['ma_5']:.2f}")
        print(f"   最新MA10: {result.iloc[-1]['ma_10']:.2f}")
        print(f"   最新MA20: {result.iloc[-1]['ma_20']:.2f}")
    
    def test_rsi_calculation(self):
        """测试RSI计算"""
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
        
        result = self.conn.execute(query).df()
        
        # 验证RSI
        assert len(result) > 0, "应该有RSI数据"
        
        # RSI应该在0-100之间
        rsi_values = result['rsi_14'].dropna()
        assert all(rsi_values >= 0), "RSI应该 >= 0"
        assert all(rsi_values <= 100), "RSI应该 <= 100"
        
        print(f"✅ RSI计算测试通过")
        print(f"   RSI范围: {rsi_values.min():.2f} - {rsi_values.max():.2f}")
        print(f"   最新RSI: {result.iloc[-1]['rsi_14']:.2f}")
    
    def test_bollinger_bands(self):
        """测试布林带计算"""
        query = """
        WITH bb_calc AS (
            SELECT 
                symbol,
                timestamp,
                close,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as ma_20,
                STDDEV(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as std_20
            FROM raw.ohlc_data
        )
        SELECT 
            *,
            ma_20 + (2 * std_20) as bb_upper,
            ma_20 - (2 * std_20) as bb_lower,
            CASE 
                WHEN ma_20 + (2 * std_20) - (ma_20 - (2 * std_20)) != 0 
                THEN (close - (ma_20 - (2 * std_20))) / ((ma_20 + (2 * std_20)) - (ma_20 - (2 * std_20)))
                ELSE 0.5
            END as bb_position
        FROM bb_calc
        ORDER BY timestamp
        """
        
        result = self.conn.execute(query).df()
        
        # 验证布林带
        assert len(result) > 0, "应该有布林带数据"
        
        # 检查布林带的合理性
        valid_data = result.dropna()
        if len(valid_data) > 0:
            assert all(valid_data['bb_upper'] >= valid_data['ma_20']), "上轨应该 >= 中轨"
            assert all(valid_data['bb_lower'] <= valid_data['ma_20']), "下轨应该 <= 中轨"
            assert all(valid_data['bb_position'] >= 0), "BB位置应该 >= 0"
            assert all(valid_data['bb_position'] <= 1), "BB位置应该 <= 1"
        
        print(f"✅ 布林带计算测试通过")
        if len(valid_data) > 0:
            print(f"   最新上轨: {valid_data.iloc[-1]['bb_upper']:.2f}")
            print(f"   最新中轨: {valid_data.iloc[-1]['ma_20']:.2f}")
            print(f"   最新下轨: {valid_data.iloc[-1]['bb_lower']:.2f}")
    
    def test_volume_indicators(self):
        """测试成交量指标"""
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
        
        result = self.conn.execute(query).df()
        
        # 验证成交量指标
        assert len(result) > 0, "应该有成交量指标数据"
        
        valid_data = result.dropna()
        if len(valid_data) > 0:
            assert all(valid_data['avg_volume_20d'] > 0), "平均成交量应该为正"
            assert all(valid_data['volume_ratio'] > 0), "成交量比率应该为正"
        
        print(f"✅ 成交量指标测试通过")
        if len(valid_data) > 0:
            print(f"   平均成交量比率: {valid_data['volume_ratio'].mean():.2f}")
    
    def test_feature_completeness(self):
        """测试特征完整性"""
        # 执行完整的特征工程查询
        query = """
        WITH base_data AS (
            SELECT 
                symbol,
                timestamp,
                open, high, low, close, volume,
                (high + low + close) / 3 as typical_price,
                (high - low) as daily_range,
                CASE WHEN open != 0 THEN (close - open) / open ELSE 0 END as daily_return
            FROM raw.ohlc_data
        ),
        technical_indicators AS (
            SELECT 
                *,
                -- 移动平均
                AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5,
                AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma_10,
                AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20,
                -- 波动率
                STDDEV(daily_return) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as volatility_20d,
                -- 成交量
                AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_volume_20d
            FROM base_data
        )
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN ma_5 IS NOT NULL THEN 1 END) as ma_5_count,
            COUNT(CASE WHEN ma_20 IS NOT NULL THEN 1 END) as ma_20_count,
            COUNT(CASE WHEN volatility_20d IS NOT NULL THEN 1 END) as volatility_count,
            AVG(CASE WHEN ma_5 IS NOT NULL THEN 1.0 ELSE 0.0 END) as ma_5_completeness,
            AVG(CASE WHEN volatility_20d IS NOT NULL THEN 1.0 ELSE 0.0 END) as volatility_completeness
        FROM technical_indicators
        """
        
        result = self.conn.execute(query).df()
        
        # 验证特征完整性
        assert len(result) == 1, "应该返回一行统计结果"
        
        stats = result.iloc[0]
        assert stats['total_records'] > 0, "应该有总记录数"
        assert stats['ma_5_completeness'] > 0.8, "MA5完整性应该 > 80%"
        
        print(f"✅ 特征完整性测试通过")
        print(f"   总记录数: {stats['total_records']}")
        print(f"   MA5完整性: {stats['ma_5_completeness']:.2%}")
        print(f"   波动率完整性: {stats['volatility_completeness']:.2%}")


def run_offline_features_test():
    """运行离线特征测试"""
    print("🧪 开始离线特征衍生模块测试...")
    
    test_instance = TestOfflineFeatures()
    test_methods = [
        'test_data_cleaning',
        'test_basic_features', 
        'test_moving_averages',
        'test_rsi_calculation',
        'test_bollinger_bands',
        'test_volume_indicators',
        'test_feature_completeness'
    ]
    
    passed = 0
    failed = 0
    
    for method_name in test_methods:
        try:
            print(f"\n📊 运行测试: {method_name}")
            test_instance.setup_method()
            method = getattr(test_instance, method_name)
            method()
            test_instance.teardown_method()
            passed += 1
        except Exception as e:
            print(f"❌ 测试失败: {method_name} - {str(e)}")
            failed += 1
            test_instance.teardown_method()
    
    print(f"\n📈 测试结果:")
    print(f"   ✅ 通过: {passed}")
    print(f"   ❌ 失败: {failed}")
    print(f"   📊 通过率: {passed/(passed+failed)*100:.1f}%")
    
    return failed == 0


if __name__ == "__main__":
    success = run_offline_features_test()
    exit(0 if success else 1)