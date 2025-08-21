#!/usr/bin/env python3
"""
实时特征处理测试
"""
import sys
import os
import time
import threading
from datetime import datetime, timedelta
import json

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

def create_mock_data():
    """创建模拟数据"""
    symbols = ['BTCUSDT', 'ETHUSDT']
    mock_data = []
    
    base_prices = {'BTCUSDT': 45000, 'ETHUSDT': 2500}
    
    for symbol in symbols:
        base_price = base_prices[symbol]
        
        for i in range(10):  # 10个数据点
            timestamp = datetime.now() + timedelta(seconds=i)
            
            # 模拟价格波动
            import random
            price_change = random.gauss(0, 0.001) * base_price
            current_price = base_price + price_change
            
            high = current_price * (1 + abs(random.gauss(0, 0.0005)))
            low = current_price * (1 - abs(random.gauss(0, 0.0005)))
            volume = int(random.expovariate(1/1000000))
            
            mock_data.append({
                'symbol': symbol,
                'timestamp': timestamp,
                'open': base_price,
                'high': high,
                'low': low,
                'close': current_price,
                'volume': volume,
                'amount': current_price * volume,
                'count': random.randint(50, 200)
            })
            
            base_price = current_price
    
    return mock_data

def test_data_generation():
    """测试数据生成"""
    print("📊 测试实时数据生成...")
    
    mock_data = create_mock_data()
    
    assert len(mock_data) > 0, "应该生成模拟数据"
    
    # 验证数据结构
    for data in mock_data[:3]:  # 检查前3条
        required_fields = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        for field in required_fields:
            assert field in data, f"数据应包含 {field} 字段"
        
        # 验证OHLC逻辑
        assert data['high'] >= max(data['open'], data['close']), "最高价应该 >= max(开盘价, 收盘价)"
        assert data['low'] <= min(data['open'], data['close']), "最低价应该 <= min(开盘价, 收盘价)"
        assert data['volume'] > 0, "成交量应该为正"
    
    print(f"   ✅ 生成了 {len(mock_data)} 条模拟数据")
    return True

def test_feature_calculation():
    """测试特征计算"""
    print("📊 测试实时特征计算...")
    
    mock_data = create_mock_data()
    
    # 按交易对分组
    symbol_data = {}
    for data in mock_data:
        symbol = data['symbol']
        if symbol not in symbol_data:
            symbol_data[symbol] = []
        symbol_data[symbol].append(data)
    
    # 为每个交易对计算特征
    for symbol, data_list in symbol_data.items():
        if len(data_list) < 5:
            continue
            
        # 排序数据
        data_list.sort(key=lambda x: x['timestamp'])
        
        # 计算基础特征
        latest_data = data_list[-1]
        
        # 移动平均
        if len(data_list) >= 5:
            recent_prices = [d['close'] for d in data_list[-5:]]
            ma_5 = sum(recent_prices) / len(recent_prices)
            
            assert ma_5 > 0, "MA5应该为正数"
            print(f"   📈 {symbol} MA5: {ma_5:.2f}")
        
        # 价格变化
        if len(data_list) >= 2:
            prev_price = data_list[-2]['close']
            current_price = latest_data['close']
            price_change = (current_price - prev_price) / prev_price
            
            print(f"   📊 {symbol} 价格变化: {price_change:.4f}")
        
        # 成交量特征
        if len(data_list) >= 5:
            recent_volumes = [d['volume'] for d in data_list[-5:]]
            avg_volume = sum(recent_volumes) / len(recent_volumes)
            volume_ratio = latest_data['volume'] / avg_volume if avg_volume > 0 else 1.0
            
            assert volume_ratio > 0, "成交量比率应该为正"
            print(f"   📊 {symbol} 成交量比率: {volume_ratio:.2f}")
    
    print("   ✅ 实时特征计算完成")
    return True

def test_rsi_calculation():
    """测试RSI计算"""
    print("📊 测试RSI计算...")
    
    # 创建价格序列
    prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109, 108, 110, 112, 111, 113]
    
    def calculate_rsi(prices, period=14):
        if len(prices) < period + 1:
            return 50.0  # 默认值
        
        # 计算价格变化
        changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        
        # 分离涨跌
        gains = [max(change, 0) for change in changes]
        losses = [abs(min(change, 0)) for change in changes]
        
        # 计算平均涨跌幅
        if len(gains) >= period:
            avg_gain = sum(gains[-period:]) / period
            avg_loss = sum(losses[-period:]) / period
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        
        return 50.0
    
    rsi = calculate_rsi(prices)
    
    assert 0 <= rsi <= 100, f"RSI应该在0-100之间，实际值: {rsi}"
    
    print(f"   📊 计算RSI: {rsi:.2f}")
    print("   ✅ RSI计算测试通过")
    return True

def test_bollinger_bands():
    """测试布林带计算"""
    print("📊 测试布林带计算...")
    
    # 创建价格序列
    prices = [100 + i * 0.5 + (-1)**i * 2 for i in range(20)]
    
    def calculate_bollinger_bands(prices, period=20, std_dev=2):
        if len(prices) < period:
            return None, None, None
        
        # 移动平均
        recent_prices = prices[-period:]
        ma = sum(recent_prices) / period
        
        # 标准差
        variance = sum((p - ma) ** 2 for p in recent_prices) / period
        std = variance ** 0.5
        
        # 布林带
        upper = ma + std_dev * std
        lower = ma - std_dev * std
        
        return upper, ma, lower
    
    upper, middle, lower = calculate_bollinger_bands(prices)
    
    if upper is not None:
        assert upper > middle > lower, "布林带上轨 > 中轨 > 下轨"
        
        # 计算价格在布林带中的位置
        current_price = prices[-1]
        if upper != lower:
            bb_position = (current_price - lower) / (upper - lower)
            assert 0 <= bb_position <= 1 or abs(bb_position - 0.5) < 1, "布林带位置应该合理"
        
        print(f"   📊 布林带上轨: {upper:.2f}")
        print(f"   📊 布林带中轨: {middle:.2f}")
        print(f"   📊 布林带下轨: {lower:.2f}")
    
    print("   ✅ 布林带计算测试通过")
    return True

def test_momentum_indicators():
    """测试动量指标"""
    print("📊 测试动量指标...")
    
    # 创建价格序列（模拟上升趋势）
    prices = [100 + i * 0.5 + (-1)**i * 0.2 for i in range(15)]
    
    # 计算动量
    def calculate_momentum(prices, period=5):
        if len(prices) < period + 1:
            return 0.0
        
        current_price = prices[-1]
        past_price = prices[-(period + 1)]
        
        momentum = (current_price - past_price) / past_price
        return momentum
    
    momentum_5d = calculate_momentum(prices, 5)
    momentum_10d = calculate_momentum(prices, 10)
    
    print(f"   📊 5日动量: {momentum_5d:.4f}")
    print(f"   📊 10日动量: {momentum_10d:.4f}")
    
    # 验证动量合理性（上升趋势应该有正动量）
    assert momentum_5d > -0.5, "动量不应该过于负值"
    
    print("   ✅ 动量指标测试通过")
    return True

def test_feature_integration():
    """测试特征集成"""
    print("📊 测试特征集成...")
    
    mock_data = create_mock_data()
    
    # 模拟完整的特征计算流程
    features_results = []
    
    symbol_data = {}
    for data in mock_data:
        symbol = data['symbol']
        if symbol not in symbol_data:
            symbol_data[symbol] = []
        symbol_data[symbol].append(data)
    
    for symbol, data_list in symbol_data.items():
        if len(data_list) < 5:
            continue
        
        data_list.sort(key=lambda x: x['timestamp'])
        latest = data_list[-1]
        
        # 计算各种特征
        features = {
            'symbol': symbol,
            'timestamp': latest['timestamp'],
            'price': latest['close'],
            'volume': latest['volume']
        }
        
        # 移动平均
        if len(data_list) >= 5:
            features['ma_5'] = sum(d['close'] for d in data_list[-5:]) / 5
        
        if len(data_list) >= 10:
            features['ma_10'] = sum(d['close'] for d in data_list[-10:]) / min(10, len(data_list))
        
        # 成交量比率
        if len(data_list) >= 5:
            avg_vol = sum(d['volume'] for d in data_list[-5:]) / 5
            features['volume_ratio'] = latest['volume'] / avg_vol if avg_vol > 0 else 1.0
        
        # 价格变化
        if len(data_list) >= 2:
            prev_price = data_list[-2]['close']
            features['price_change'] = (latest['close'] - prev_price) / prev_price
        
        # 波动率
        if len(data_list) >= 5:
            prices = [d['close'] for d in data_list[-5:]]
            if len(prices) > 1:
                avg_price = sum(prices) / len(prices)
                variance = sum((p - avg_price) ** 2 for p in prices) / len(prices)
                features['volatility'] = variance ** 0.5 / avg_price  # 相对波动率
        
        features_results.append(features)
    
    # 验证特征完整性
    assert len(features_results) > 0, "应该生成特征结果"
    
    for features in features_results:
        assert 'symbol' in features, "特征应包含symbol"
        assert 'price' in features, "特征应包含price"
        assert features['price'] > 0, "价格应该为正"
        
        if 'volume_ratio' in features:
            assert features['volume_ratio'] > 0, "成交量比率应该为正"
    
    print(f"   ✅ 生成了 {len(features_results)} 个交易对的特征")
    
    # 显示特征示例
    for features in features_results[:2]:
        print(f"   📊 {features['symbol']} 特征:")
        for key, value in features.items():
            if key not in ['symbol', 'timestamp']:
                if isinstance(value, float):
                    print(f"      {key}: {value:.4f}")
                else:
                    print(f"      {key}: {value}")
    
    print("   ✅ 特征集成测试通过")
    return True

def run_realtime_features_test():
    """运行实时特征测试"""
    print("🧪 开始实时特征处理测试...")
    
    tests = [
        test_data_generation,
        test_feature_calculation,
        test_rsi_calculation,
        test_bollinger_bands,
        test_momentum_indicators,
        test_feature_integration
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            print(f"\n🔍 运行测试: {test_func.__name__}")
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ 测试失败: {test_func.__name__} - {str(e)}")
            failed += 1
    
    print(f"\n📈 实时特征处理测试结果:")
    print(f"   ✅ 通过: {passed}")
    print(f"   ❌ 失败: {failed}")
    print(f"   📊 通过率: {passed/(passed+failed)*100:.1f}%")
    
    return failed == 0

if __name__ == "__main__":
    success = run_realtime_features_test()
    exit(0 if success else 1)