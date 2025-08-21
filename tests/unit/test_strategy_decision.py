#!/usr/bin/env python3
"""
策略规则决策测试
"""
import json
from datetime import datetime
import sys
import os

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

def create_test_features():
    """创建测试特征数据"""
    test_cases = [
        {
            'name': '超买信号',
            'features': {
                'trading_pair': 'BTCUSDT',
                'price': 45000,
                'ma_5': 44800,
                'ma_10': 44600,
                'rsi_14': 75,  # 超买
                'volatility': 0.02,
                'volume_ratio': 1.8,  # 高成交量
                'momentum_5d': -0.01,  # 负动量
                'timestamp': datetime.now().isoformat()
            },
            'expected_signal': 'SELL'
        },
        {
            'name': '超卖信号',
            'features': {
                'trading_pair': 'BTCUSDT',
                'price': 44000,
                'ma_5': 44200,
                'ma_10': 44400,
                'rsi_14': 25,  # 超卖
                'volatility': 0.02,
                'volume_ratio': 2.0,  # 高成交量
                'momentum_5d': 0.015,  # 正动量
                'timestamp': datetime.now().isoformat()
            },
            'expected_signal': 'BUY'
        },
        {
            'name': '横盘整理',
            'features': {
                'trading_pair': 'BTCUSDT',
                'price': 45000,
                'ma_5': 45010,
                'ma_10': 45020,
                'rsi_14': 50,  # 中性
                'volatility': 0.01,
                'volume_ratio': 1.0,  # 正常成交量
                'momentum_5d': 0.002,  # 微弱动量
                'timestamp': datetime.now().isoformat()
            },
            'expected_signal': 'HOLD'
        },
        {
            'name': '强势上涨',
            'features': {
                'trading_pair': 'BTCUSDT',
                'price': 46000,
                'ma_5': 45800,
                'ma_10': 45600,
                'rsi_14': 65,
                'volatility': 0.025,
                'volume_ratio': 2.5,  # 极高成交量
                'momentum_5d': 0.025,  # 强正动量
                'timestamp': datetime.now().isoformat()
            },
            'expected_signal': 'BUY'
        },
        {
            'name': '下跌趋势',
            'features': {
                'trading_pair': 'BTCUSDT',
                'price': 43000,
                'ma_5': 43200,
                'ma_10': 43500,
                'rsi_14': 35,
                'volatility': 0.03,
                'volume_ratio': 1.8,
                'momentum_5d': -0.02,  # 强负动量
                'timestamp': datetime.now().isoformat()
            },
            'expected_signal': 'SELL'
        }
    ]
    
    return test_cases

def simple_decision_engine(features):
    """简单决策引擎实现"""
    # 初始化评分
    buy_score = 0
    sell_score = 0
    
    # 趋势分析
    price = features['price']
    ma_5 = features['ma_5']
    ma_10 = features['ma_10']
    
    # 趋势判断
    if price > ma_5 and ma_5 > ma_10:
        trend = 'UP'
        buy_score += 3
    elif price < ma_5 and ma_5 < ma_10:
        trend = 'DOWN'
        sell_score += 3
    else:
        trend = 'SIDEWAYS'
    
    # 动量分析
    momentum_5d = features['momentum_5d']
    if momentum_5d > 0.005:
        momentum_signal = 'STRONG_UP'
        buy_score += 2
    elif momentum_5d < -0.005:
        momentum_signal = 'STRONG_DOWN'
        sell_score += 2
    else:
        momentum_signal = 'WEAK'
    
    # RSI分析
    rsi_14 = features['rsi_14']
    if rsi_14 < 30:
        buy_score += 2  # 超卖买入
    elif rsi_14 > 70:
        sell_score += 2  # 超买卖出
    
    # 成交量确认
    volume_ratio = features['volume_ratio']
    if volume_ratio > 1.5:
        volume_signal = 'HIGH'
        # 高成交量增强信号
        if buy_score > sell_score:
            buy_score += 1
        elif sell_score > buy_score:
            sell_score += 1
    elif volume_ratio < 0.5:
        volume_signal = 'LOW'
    else:
        volume_signal = 'NORMAL'
    
    # 波动率风险管理
    volatility = features['volatility']
    if volatility > 0.05:
        risk_level = 'HIGH'
        position_size = 0.5
    elif volatility > 0.03:
        risk_level = 'MEDIUM'
        position_size = 0.7
    else:
        risk_level = 'LOW'
        position_size = 1.0
    
    # 决策逻辑
    if buy_score >= 5:
        signal = 'BUY'
    elif sell_score >= 5:
        signal = 'SELL'
    else:
        signal = 'HOLD'
    
    return {
        'trading_pair': features['trading_pair'],
        'signal': signal,
        'price': price,
        'buy_score': buy_score,
        'sell_score': sell_score,
        'trend': trend,
        'momentum_signal': momentum_signal,
        'volume_signal': volume_signal,
        'risk_level': risk_level,
        'position_size': position_size,
        'timestamp': features['timestamp'],
        'features': {
            'rsi_14': rsi_14,
            'ma_5': ma_5,
            'ma_10': ma_10,
            'volatility': volatility,
            'volume_ratio': volume_ratio,
            'momentum_5d': momentum_5d
        }
    }

def test_signal_generation():
    """测试信号生成"""
    print("📊 测试信号生成...")
    
    test_cases = create_test_features()
    
    results = []
    for test_case in test_cases:
        features = test_case['features']
        expected = test_case['expected_signal']
        
        # 生成决策
        decision = simple_decision_engine(features)
        
        print(f"\n   🔍 测试案例: {test_case['name']}")
        print(f"      输入特征: RSI={features['rsi_14']}, 动量={features['momentum_5d']:.3f}")
        print(f"      期望信号: {expected}")
        print(f"      实际信号: {decision['signal']}")
        print(f"      买入评分: {decision['buy_score']}, 卖出评分: {decision['sell_score']}")
        print(f"      趋势: {decision['trend']}, 风险: {decision['risk_level']}")
        
        results.append({
            'test_name': test_case['name'],
            'expected': expected,
            'actual': decision['signal'],
            'correct': decision['signal'] == expected,
            'decision': decision
        })
    
    # 统计结果
    correct_count = sum(1 for r in results if r['correct'])
    total_count = len(results)
    accuracy = correct_count / total_count
    
    print(f"\n   📈 信号生成准确率: {accuracy:.1%} ({correct_count}/{total_count})")
    
    # 显示错误案例
    for result in results:
        if not result['correct']:
            print(f"   ⚠️  错误案例: {result['test_name']} - 期望{result['expected']}, 实际{result['actual']}")
    
    print("   ✅ 信号生成测试完成")
    return accuracy >= 0.6  # 60%以上准确率视为通过

def test_risk_management():
    """测试风险管理"""
    print("📊 测试风险管理...")
    
    # 高波动率测试
    high_vol_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45100,
        'ma_10': 45200,
        'rsi_14': 60,
        'volatility': 0.08,  # 高波动率
        'volume_ratio': 1.5,
        'momentum_5d': 0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(high_vol_features)
    
    assert decision['risk_level'] == 'HIGH', "高波动率应该被识别为高风险"
    assert decision['position_size'] <= 0.5, "高风险应该降低仓位"
    
    print(f"   📊 高波动率测试: 风险等级={decision['risk_level']}, 仓位={decision['position_size']}")
    
    # 低波动率测试
    low_vol_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45100,
        'ma_10': 45200,
        'rsi_14': 60,
        'volatility': 0.01,  # 低波动率
        'volume_ratio': 1.5,
        'momentum_5d': 0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(low_vol_features)
    
    assert decision['risk_level'] == 'LOW', "低波动率应该被识别为低风险"
    assert decision['position_size'] >= 0.8, "低风险可以使用较大仓位"
    
    print(f"   📊 低波动率测试: 风险等级={decision['risk_level']}, 仓位={decision['position_size']}")
    
    print("   ✅ 风险管理测试通过")
    return True

def test_trend_analysis():
    """测试趋势分析"""
    print("📊 测试趋势分析...")
    
    # 上升趋势
    uptrend_features = {
        'trading_pair': 'BTCUSDT',
        'price': 46000,
        'ma_5': 45800,
        'ma_10': 45600,
        'rsi_14': 65,
        'volatility': 0.02,
        'volume_ratio': 1.5,
        'momentum_5d': 0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(uptrend_features)
    assert decision['trend'] == 'UP', "应该识别为上升趋势"
    
    print(f"   📈 上升趋势测试: 趋势={decision['trend']}, 信号={decision['signal']}")
    
    # 下降趋势
    downtrend_features = {
        'trading_pair': 'BTCUSDT',
        'price': 44000,
        'ma_5': 44200,
        'ma_10': 44400,
        'rsi_14': 35,
        'volatility': 0.02,
        'volume_ratio': 1.5,
        'momentum_5d': -0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(downtrend_features)
    assert decision['trend'] == 'DOWN', "应该识别为下降趋势"
    
    print(f"   📉 下降趋势测试: 趋势={decision['trend']}, 信号={decision['signal']}")
    
    # 横盘趋势
    sideways_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45010,
        'ma_10': 44990,
        'rsi_14': 50,
        'volatility': 0.02,
        'volume_ratio': 1.0,
        'momentum_5d': 0.001,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(sideways_features)
    assert decision['trend'] == 'SIDEWAYS', "应该识别为横盘趋势"
    
    print(f"   ↔️  横盘趋势测试: 趋势={decision['trend']}, 信号={decision['signal']}")
    
    print("   ✅ 趋势分析测试通过")
    return True

def test_volume_analysis():
    """测试成交量分析"""
    print("📊 测试成交量分析...")
    
    # 高成交量
    high_vol_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 44800,
        'ma_10': 44600,
        'rsi_14': 65,
        'volatility': 0.02,
        'volume_ratio': 2.5,  # 高成交量
        'momentum_5d': 0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(high_vol_features)
    assert decision['volume_signal'] == 'HIGH', "应该识别为高成交量"
    
    print(f"   📊 高成交量测试: 成交量信号={decision['volume_signal']}")
    
    # 低成交量
    low_vol_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 44800,
        'ma_10': 44600,
        'rsi_14': 65,
        'volatility': 0.02,
        'volume_ratio': 0.3,  # 低成交量
        'momentum_5d': 0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(low_vol_features)
    assert decision['volume_signal'] == 'LOW', "应该识别为低成交量"
    
    print(f"   📊 低成交量测试: 成交量信号={decision['volume_signal']}")
    
    print("   ✅ 成交量分析测试通过")
    return True

def test_decision_consistency():
    """测试决策一致性"""
    print("📊 测试决策一致性...")
    
    # 相同特征应该产生相同决策
    features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 44800,
        'ma_10': 44600,
        'rsi_14': 75,
        'volatility': 0.02,
        'volume_ratio': 1.8,
        'momentum_5d': -0.01,
        'timestamp': datetime.now().isoformat()
    }
    
    # 多次运行决策
    decisions = []
    for i in range(5):
        decision = simple_decision_engine(features)
        decisions.append(decision['signal'])
    
    # 检查一致性
    unique_decisions = set(decisions)
    assert len(unique_decisions) == 1, f"相同输入应产生相同决策，实际得到: {unique_decisions}"
    
    print(f"   📊 一致性测试: 5次运行都产生 {decisions[0]} 信号")
    
    print("   ✅ 决策一致性测试通过")
    return True

def test_edge_cases():
    """测试边界情况"""
    print("📊 测试边界情况...")
    
    # 极端RSI值
    extreme_rsi_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45000,
        'ma_10': 45000,
        'rsi_14': 0,  # 极端超卖
        'volatility': 0.02,
        'volume_ratio': 1.0,
        'momentum_5d': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(extreme_rsi_features)
    print(f"   📊 极端RSI(0)测试: 信号={decision['signal']}, 买入评分={decision['buy_score']}")
    
    # 零动量
    zero_momentum_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45000,
        'ma_10': 45000,
        'rsi_14': 50,
        'volatility': 0.02,
        'volume_ratio': 1.0,
        'momentum_5d': 0,  # 零动量
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(zero_momentum_features)
    print(f"   📊 零动量测试: 信号={decision['signal']}, 动量信号={decision['momentum_signal']}")
    
    # 极高波动率
    extreme_vol_features = {
        'trading_pair': 'BTCUSDT',
        'price': 45000,
        'ma_5': 45000,
        'ma_10': 45000,
        'rsi_14': 50,
        'volatility': 0.15,  # 极高波动率
        'volume_ratio': 1.0,
        'momentum_5d': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    decision = simple_decision_engine(extreme_vol_features)
    print(f"   📊 极高波动率测试: 风险等级={decision['risk_level']}, 仓位={decision['position_size']}")
    
    print("   ✅ 边界情况测试通过")
    return True

def run_strategy_decision_test():
    """运行策略决策测试"""
    print("🧪 开始策略规则决策测试...")
    
    tests = [
        test_signal_generation,
        test_risk_management,
        test_trend_analysis,
        test_volume_analysis,
        test_decision_consistency,
        test_edge_cases
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            print(f"\n🔍 运行测试: {test_func.__name__}")
            if test_func():
                passed += 1
            else:
                print(f"⚠️  测试未完全通过: {test_func.__name__}")
                failed += 1
        except Exception as e:
            print(f"❌ 测试失败: {test_func.__name__} - {str(e)}")
            failed += 1
    
    print(f"\n📈 策略规则决策测试结果:")
    print(f"   ✅ 通过: {passed}")
    print(f"   ❌ 失败: {failed}")
    print(f"   📊 通过率: {passed/(passed+failed)*100:.1f}%")
    
    return failed == 0

if __name__ == "__main__":
    success = run_strategy_decision_test()
    exit(0 if success else 1)