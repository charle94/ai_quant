#!/usr/bin/env python3
"""
Python版本的决策引擎 - 基于规则的交易信号生成
"""
import json
import time
import redis
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TradingSignal:
    """交易信号数据类"""
    trading_pair: str
    signal: str  # BUY, SELL, HOLD
    price: float
    buy_score: int
    sell_score: int
    trend: str
    momentum_signal: str
    volume_signal: str
    risk_level: str
    position_size: float
    timestamp: str
    features: Dict

class DecisionEngine:
    """Python决策引擎"""
    
    def __init__(self, redis_host='localhost', redis_port=6379):
        """初始化决策引擎"""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.trading_pairs = ['BTCUSDT', 'ETHUSDT']
        self.signals_history = []
        self.is_running = False
        
    def get_features_from_redis(self, symbol: str) -> Optional[Dict]:
        """从Redis获取特征数据"""
        try:
            # 尝试获取实时特征
            realtime_key = f"feast:realtime_features:{symbol}"
            features = self.redis_client.hgetall(realtime_key)
            
            if not features:
                # 如果没有实时特征，尝试获取离线特征
                offline_key = f"feast:features:{symbol}"
                features = self.redis_client.hgetall(offline_key)
            
            if not features:
                logger.warning(f"没有找到 {symbol} 的特征数据")
                return None
            
            # 转换字节数据为Python类型
            result = {}
            for key, value in features.items():
                key_str = key.decode('utf-8')
                value_str = value.decode('utf-8')
                
                # 转换数据类型
                if key_str in ['symbol', 'timestamp']:
                    result[key_str] = value_str
                elif key_str in ['price_above_ma5', 'price_above_ma10', 'high_volume']:
                    result[key_str] = int(float(value_str))
                else:
                    result[key_str] = float(value_str)
            
            return result
            
        except Exception as e:
            logger.error(f"获取 {symbol} 特征数据时出错: {e}")
            return None
    
    def apply_trading_rules(self, features: Dict) -> TradingSignal:
        """应用交易规则生成信号"""
        symbol = features.get('symbol', 'UNKNOWN')
        price = features.get('price', 0.0)
        
        # 初始化得分
        buy_score = 0
        sell_score = 0
        
        # 规则1: 趋势分析
        ma_5 = features.get('ma_5', price)
        ma_10 = features.get('ma_10', price)
        ma_20 = features.get('ma_20', price)
        
        trend = "NEUTRAL"
        if price > ma_5 > ma_10 > ma_20:
            trend = "STRONG_UP"
            buy_score += 30
        elif price > ma_5 > ma_10:
            trend = "UP"
            buy_score += 20
        elif price < ma_5 < ma_10 < ma_20:
            trend = "STRONG_DOWN"
            sell_score += 30
        elif price < ma_5 < ma_10:
            trend = "DOWN"
            sell_score += 20
        
        # 规则2: RSI超买超卖
        rsi = features.get('rsi_14', 50.0)
        momentum_signal = "NEUTRAL"
        
        if rsi > 70:
            momentum_signal = "OVERBOUGHT"
            sell_score += 25
        elif rsi < 30:
            momentum_signal = "OVERSOLD"
            buy_score += 25
        elif rsi > 60:
            momentum_signal = "BULLISH"
            buy_score += 10
        elif rsi < 40:
            momentum_signal = "BEARISH"
            sell_score += 10
        
        # 规则3: 成交量分析
        volume_ratio = features.get('volume_ratio', 1.0)
        high_volume = features.get('high_volume', 0)
        volume_signal = "NORMAL"
        
        if high_volume and volume_ratio > 1.5:
            volume_signal = "HIGH_VOLUME"
            if trend in ["UP", "STRONG_UP"]:
                buy_score += 15
            elif trend in ["DOWN", "STRONG_DOWN"]:
                sell_score += 15
        elif volume_ratio < 0.5:
            volume_signal = "LOW_VOLUME"
            # 低成交量时保持谨慎
            buy_score -= 10
            sell_score -= 10
        
        # 规则4: 动量分析
        momentum_5d = features.get('momentum_5d', 0.0)
        if momentum_5d > 0.02:  # 2%以上涨幅
            buy_score += 15
        elif momentum_5d < -0.02:  # 2%以上跌幅
            sell_score += 15
        
        # 规则5: 布林带位置
        bb_position = features.get('bb_position', 0.5)
        if bb_position > 0.8:
            sell_score += 10  # 接近上轨，可能回调
        elif bb_position < 0.2:
            buy_score += 10  # 接近下轨，可能反弹
        
        # 决定最终信号
        signal = "HOLD"
        if buy_score > sell_score and buy_score >= 40:
            signal = "BUY"
        elif sell_score > buy_score and sell_score >= 40:
            signal = "SELL"
        
        # 风险等级评估
        risk_level = "LOW"
        volatility = features.get('volatility_20d', 0.01)
        
        if volatility > 0.03:
            risk_level = "HIGH"
        elif volatility > 0.02:
            risk_level = "MEDIUM"
        
        # 仓位大小建议
        position_size = 0.0
        if signal == "BUY":
            if risk_level == "LOW":
                position_size = 0.3
            elif risk_level == "MEDIUM":
                position_size = 0.2
            else:
                position_size = 0.1
        elif signal == "SELL":
            position_size = 1.0  # 全部卖出
        
        return TradingSignal(
            trading_pair=symbol,
            signal=signal,
            price=price,
            buy_score=buy_score,
            sell_score=sell_score,
            trend=trend,
            momentum_signal=momentum_signal,
            volume_signal=volume_signal,
            risk_level=risk_level,
            position_size=position_size,
            timestamp=datetime.now().isoformat(),
            features=features
        )
    
    def generate_signals(self):
        """生成所有交易对的信号"""
        signals = []
        
        for symbol in self.trading_pairs:
            features = self.get_features_from_redis(symbol)
            if features:
                signal = self.apply_trading_rules(features)
                signals.append(signal)
                logger.info(f"{symbol}: {signal.signal} (买入:{signal.buy_score}, 卖出:{signal.sell_score}, 趋势:{signal.trend})")
            else:
                logger.warning(f"无法获取 {symbol} 的特征数据，跳过信号生成")
        
        return signals
    
    def save_signals_to_redis(self, signals: List[TradingSignal]):
        """保存信号到Redis"""
        try:
            for signal in signals:
                key = f"trading:signals:{signal.trading_pair}"
                signal_data = {
                    'signal': signal.signal,
                    'price': signal.price,
                    'buy_score': signal.buy_score,
                    'sell_score': signal.sell_score,
                    'trend': signal.trend,
                    'momentum_signal': signal.momentum_signal,
                    'volume_signal': signal.volume_signal,
                    'risk_level': signal.risk_level,
                    'position_size': signal.position_size,
                    'timestamp': signal.timestamp
                }
                
                self.redis_client.hset(key, mapping=signal_data)
                
                # 设置过期时间（1小时）
                self.redis_client.expire(key, 3600)
                
        except Exception as e:
            logger.error(f"保存信号到Redis时出错: {e}")
    
    def start_decision_loop(self, interval=10):
        """启动决策循环"""
        logger.info("启动决策引擎循环...")
        self.is_running = True
        
        while self.is_running:
            try:
                # 生成信号
                signals = self.generate_signals()
                
                if signals:
                    # 保存信号
                    self.save_signals_to_redis(signals)
                    self.signals_history.extend(signals)
                    
                    # 打印信号摘要
                    buy_signals = [s for s in signals if s.signal == 'BUY']
                    sell_signals = [s for s in signals if s.signal == 'SELL']
                    hold_signals = [s for s in signals if s.signal == 'HOLD']
                    
                    logger.info(f"信号摘要 - 买入:{len(buy_signals)}, 卖出:{len(sell_signals)}, 持有:{len(hold_signals)}")
                
                # 等待下一轮
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("收到中断信号，停止决策引擎")
                break
            except Exception as e:
                logger.error(f"决策循环出错: {e}")
                time.sleep(5)
        
        self.is_running = False
    
    def stop(self):
        """停止决策引擎"""
        logger.info("停止决策引擎...")
        self.is_running = False
    
    def get_latest_signals(self):
        """获取最新信号"""
        signals = {}
        
        for symbol in self.trading_pairs:
            try:
                key = f"trading:signals:{symbol}"
                signal_data = self.redis_client.hgetall(key)
                
                if signal_data:
                    signals[symbol] = {
                        key.decode('utf-8'): value.decode('utf-8') 
                        for key, value in signal_data.items()
                    }
                
            except Exception as e:
                logger.error(f"获取 {symbol} 最新信号时出错: {e}")
        
        return signals

def main():
    """主测试函数"""
    print("开始Python决策引擎测试...")
    
    engine = DecisionEngine()
    
    try:
        # 生成一次信号测试
        print("\n=== 单次信号生成测试 ===")
        signals = engine.generate_signals()
        
        if signals:
            engine.save_signals_to_redis(signals)
            print(f"生成了 {len(signals)} 个交易信号")
            
            for signal in signals:
                print(f"{signal.trading_pair}: {signal.signal} "
                      f"(价格:{signal.price:.2f}, 风险:{signal.risk_level}, "
                      f"仓位:{signal.position_size:.1%})")
        
        # 测试信号检索
        print("\n=== 信号检索测试 ===")
        latest_signals = engine.get_latest_signals()
        for symbol, signal in latest_signals.items():
            print(f"{symbol}: {signal}")
        
        print("\n🎉 Python决策引擎测试完成!")
        
    except Exception as e:
        print(f"测试过程中出错: {e}")

if __name__ == "__main__":
    main()