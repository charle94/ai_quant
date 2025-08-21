#!/usr/bin/env python3
"""
实时特征计算器 - 基于Arrow数据计算各种技术指标特征
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import talib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureCalculator:
    """实时特征计算器"""
    
    def __init__(self):
        self.feature_cache = {}
    
    def calculate_comprehensive_features(self, df: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """计算全面的技术指标特征"""
        try:
            if df.empty or len(df) < 5:
                logger.warning(f"{symbol} 数据不足，无法计算特征")
                return None
            
            # 确保数据按时间排序
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 提取价格数据
            closes = df['close'].values
            highs = df['high'].values
            lows = df['low'].values
            opens = df['open'].values
            volumes = df['volume'].values
            
            features = {
                'symbol': symbol,
                'timestamp': df.iloc[-1]['timestamp'],
                'event_timestamp': df.iloc[-1]['timestamp'],
            }
            
            # 基础价格特征
            features.update(self._calculate_price_features(closes, opens, highs, lows, volumes))
            
            # 趋势指标
            features.update(self._calculate_trend_indicators(closes, highs, lows))
            
            # 动量指标
            features.update(self._calculate_momentum_indicators(closes, highs, lows, volumes))
            
            # 波动率指标
            features.update(self._calculate_volatility_indicators(closes, highs, lows))
            
            # 成交量指标
            features.update(self._calculate_volume_indicators(closes, volumes))
            
            # 模式识别特征
            features.update(self._calculate_pattern_features(opens, highs, lows, closes))
            
            # 组合特征
            features.update(self._calculate_composite_features(features))
            
            return features
            
        except Exception as e:
            logger.error(f"计算 {symbol} 综合特征时出错: {e}")
            return None
    
    def _calculate_price_features(self, closes, opens, highs, lows, volumes):
        """计算基础价格特征"""
        features = {}
        
        try:
            # 当前价格
            features['price'] = float(closes[-1])
            features['volume'] = int(volumes[-1])
            
            # 日内收益率
            if len(closes) >= 2:
                features['daily_return'] = float((closes[-1] - closes[-2]) / closes[-2])
            else:
                features['daily_return'] = 0.0
            
            # 价格范围
            features['high_low_ratio'] = float((highs[-1] - lows[-1]) / closes[-1]) if closes[-1] != 0 else 0.0
            features['open_close_ratio'] = float((closes[-1] - opens[-1]) / opens[-1]) if opens[-1] != 0 else 0.0
            
            # 价格位置（在当日高低点中的位置）
            if highs[-1] != lows[-1]:
                features['price_position'] = float((closes[-1] - lows[-1]) / (highs[-1] - lows[-1]))
            else:
                features['price_position'] = 0.5
                
        except Exception as e:
            logger.error(f"计算价格特征时出错: {e}")
            features.update({
                'price': 0.0, 'volume': 0, 'daily_return': 0.0,
                'high_low_ratio': 0.0, 'open_close_ratio': 0.0, 'price_position': 0.5
            })
        
        return features
    
    def _calculate_trend_indicators(self, closes, highs, lows):
        """计算趋势指标"""
        features = {}
        
        try:
            # 移动平均线
            if len(closes) >= 5:
                features['ma_5'] = float(np.mean(closes[-5:]))
            else:
                features['ma_5'] = float(closes[-1])
            
            if len(closes) >= 10:
                features['ma_10'] = float(np.mean(closes[-10:]))
            else:
                features['ma_10'] = float(closes[-1])
            
            if len(closes) >= 20:
                features['ma_20'] = float(np.mean(closes[-20:]))
            else:
                features['ma_20'] = float(closes[-1])
            
            # 价格相对于移动平均线的位置
            current_price = closes[-1]
            features['price_above_ma5'] = int(current_price > features['ma_5'])
            features['price_above_ma10'] = int(current_price > features['ma_10'])
            features['price_above_ma20'] = int(current_price > features['ma_20'])
            
            # 移动平均线趋势
            features['ma5_above_ma10'] = int(features['ma_5'] > features['ma_10'])
            features['ma10_above_ma20'] = int(features['ma_10'] > features['ma_20'])
            
            # EMA指标
            if len(closes) >= 12:
                features['ema_12'] = float(self._calculate_ema(closes, 12)[-1])
            else:
                features['ema_12'] = float(closes[-1])
            
            if len(closes) >= 26:
                features['ema_26'] = float(self._calculate_ema(closes, 26)[-1])
            else:
                features['ema_26'] = float(closes[-1])
                
        except Exception as e:
            logger.error(f"计算趋势指标时出错: {e}")
            features.update({
                'ma_5': float(closes[-1]), 'ma_10': float(closes[-1]), 'ma_20': float(closes[-1]),
                'price_above_ma5': 0, 'price_above_ma10': 0, 'price_above_ma20': 0,
                'ma5_above_ma10': 0, 'ma10_above_ma20': 0,
                'ema_12': float(closes[-1]), 'ema_26': float(closes[-1])
            })
        
        return features
    
    def _calculate_momentum_indicators(self, closes, highs, lows, volumes):
        """计算动量指标"""
        features = {}
        
        try:
            # RSI
            if len(closes) >= 14:
                features['rsi_14'] = float(self._calculate_rsi(closes, 14))
            else:
                features['rsi_14'] = 50.0
            
            # 超买超卖信号
            features['rsi_overbought'] = int(features['rsi_14'] > 70)
            features['rsi_oversold'] = int(features['rsi_14'] < 30)
            
            # 随机指标 %K
            if len(closes) >= 14:
                features['stoch_k_14'] = float(self._calculate_stoch_k(closes, highs, lows, 14))
            else:
                features['stoch_k_14'] = 50.0
            
            features['stoch_overbought'] = int(features['stoch_k_14'] > 80)
            features['stoch_oversold'] = int(features['stoch_k_14'] < 20)
            
            # 动量
            if len(closes) >= 6:
                features['momentum_5d'] = float((closes[-1] - closes[-6]) / closes[-6])
            else:
                features['momentum_5d'] = 0.0
            
            if len(closes) >= 11:
                features['momentum_10d'] = float((closes[-1] - closes[-11]) / closes[-11])
            else:
                features['momentum_10d'] = 0.0
            
            # 动量方向
            features['momentum_5d_positive'] = int(features['momentum_5d'] > 0)
            features['momentum_10d_positive'] = int(features['momentum_10d'] > 0)
            
            # MACD
            if len(closes) >= 26:
                macd_line, macd_signal, macd_histogram = self._calculate_macd(closes)
                features['macd'] = float(macd_line[-1])
                features['macd_signal'] = float(macd_signal[-1])
                features['macd_histogram'] = float(macd_histogram[-1])
                features['macd_bullish'] = int(features['macd'] > features['macd_signal'])
            else:
                features.update({
                    'macd': 0.0, 'macd_signal': 0.0, 'macd_histogram': 0.0, 'macd_bullish': 0
                })
                
        except Exception as e:
            logger.error(f"计算动量指标时出错: {e}")
            features.update({
                'rsi_14': 50.0, 'rsi_overbought': 0, 'rsi_oversold': 0,
                'stoch_k_14': 50.0, 'stoch_overbought': 0, 'stoch_oversold': 0,
                'momentum_5d': 0.0, 'momentum_10d': 0.0,
                'momentum_5d_positive': 0, 'momentum_10d_positive': 0,
                'macd': 0.0, 'macd_signal': 0.0, 'macd_histogram': 0.0, 'macd_bullish': 0
            })
        
        return features
    
    def _calculate_volatility_indicators(self, closes, highs, lows):
        """计算波动率指标"""
        features = {}
        
        try:
            # 历史波动率
            if len(closes) >= 20:
                returns = np.diff(closes) / closes[:-1]
                features['volatility_20d'] = float(np.std(returns[-20:]))
            else:
                features['volatility_20d'] = 0.0
            
            # 布林带
            if len(closes) >= 20:
                ma_20 = np.mean(closes[-20:])
                std_20 = np.std(closes[-20:])
                features['bollinger_upper'] = float(ma_20 + 2 * std_20)
                features['bollinger_lower'] = float(ma_20 - 2 * std_20)
                features['bollinger_width'] = float(features['bollinger_upper'] - features['bollinger_lower'])
                
                # 价格在布林带中的位置
                if features['bollinger_width'] != 0:
                    features['bb_position'] = float((closes[-1] - features['bollinger_lower']) / features['bollinger_width'])
                else:
                    features['bb_position'] = 0.5
                
                features['price_above_bb_upper'] = int(closes[-1] > features['bollinger_upper'])
                features['price_below_bb_lower'] = int(closes[-1] < features['bollinger_lower'])
            else:
                price = closes[-1]
                features.update({
                    'bollinger_upper': float(price * 1.02),
                    'bollinger_lower': float(price * 0.98),
                    'bollinger_width': float(price * 0.04),
                    'bb_position': 0.5,
                    'price_above_bb_upper': 0,
                    'price_below_bb_lower': 0
                })
            
            # ATR (平均真实范围)
            if len(closes) >= 14:
                features['atr_14'] = float(self._calculate_atr(highs, lows, closes, 14))
            else:
                features['atr_14'] = float((highs[-1] - lows[-1]))
                
        except Exception as e:
            logger.error(f"计算波动率指标时出错: {e}")
            price = closes[-1]
            features.update({
                'volatility_20d': 0.0,
                'bollinger_upper': float(price * 1.02),
                'bollinger_lower': float(price * 0.98),
                'bollinger_width': float(price * 0.04),
                'bb_position': 0.5,
                'price_above_bb_upper': 0,
                'price_below_bb_lower': 0,
                'atr_14': float(highs[-1] - lows[-1]) if len(highs) > 0 else 0.0
            })
        
        return features
    
    def _calculate_volume_indicators(self, closes, volumes):
        """计算成交量指标"""
        features = {}
        
        try:
            # 成交量移动平均
            if len(volumes) >= 20:
                features['avg_volume_20d'] = float(np.mean(volumes[-20:]))
            else:
                features['avg_volume_20d'] = float(volumes[-1])
            
            # 成交量比率
            if features['avg_volume_20d'] != 0:
                features['volume_ratio'] = float(volumes[-1] / features['avg_volume_20d'])
            else:
                features['volume_ratio'] = 1.0
            
            # 高成交量标志
            features['high_volume'] = int(features['volume_ratio'] > 1.5)
            
            # 成交量价格趋势 (VPT)
            if len(closes) >= 2 and len(volumes) >= 2:
                price_change = (closes[-1] - closes[-2]) / closes[-2]
                features['vpt'] = float(volumes[-1] * price_change)
            else:
                features['vpt'] = 0.0
            
            # OBV (On Balance Volume) 简化版
            if len(closes) >= 5:
                obv = 0
                for i in range(1, min(5, len(closes))):
                    if closes[-i] > closes[-i-1]:
                        obv += volumes[-i]
                    elif closes[-i] < closes[-i-1]:
                        obv -= volumes[-i]
                features['obv_5'] = float(obv)
            else:
                features['obv_5'] = 0.0
                
        except Exception as e:
            logger.error(f"计算成交量指标时出错: {e}")
            features.update({
                'avg_volume_20d': float(volumes[-1]),
                'volume_ratio': 1.0,
                'high_volume': 0,
                'vpt': 0.0,
                'obv_5': 0.0
            })
        
        return features
    
    def _calculate_pattern_features(self, opens, highs, lows, closes):
        """计算价格模式特征"""
        features = {}
        
        try:
            # K线模式
            if len(closes) >= 1:
                # 十字星
                body_size = abs(closes[-1] - opens[-1])
                total_range = highs[-1] - lows[-1]
                features['doji'] = int(body_size / total_range < 0.1) if total_range != 0 else 0
                
                # 锤子线
                lower_shadow = min(opens[-1], closes[-1]) - lows[-1]
                upper_shadow = highs[-1] - max(opens[-1], closes[-1])
                features['hammer'] = int(lower_shadow > 2 * body_size and upper_shadow < body_size) if body_size != 0 else 0
                
                # 流星线
                features['shooting_star'] = int(upper_shadow > 2 * body_size and lower_shadow < body_size) if body_size != 0 else 0
            else:
                features.update({'doji': 0, 'hammer': 0, 'shooting_star': 0})
            
            # 价格缺口
            if len(closes) >= 2:
                gap_up = int(lows[-1] > highs[-2])
                gap_down = int(highs[-1] < lows[-2])
                features['gap_up'] = gap_up
                features['gap_down'] = gap_down
            else:
                features.update({'gap_up': 0, 'gap_down': 0})
                
        except Exception as e:
            logger.error(f"计算模式特征时出错: {e}")
            features.update({
                'doji': 0, 'hammer': 0, 'shooting_star': 0,
                'gap_up': 0, 'gap_down': 0
            })
        
        return features
    
    def _calculate_composite_features(self, features):
        """计算组合特征"""
        composite = {}
        
        try:
            # 多重超买超卖信号
            composite['double_overbought'] = int(
                features.get('rsi_overbought', 0) and features.get('stoch_overbought', 0)
            )
            composite['double_oversold'] = int(
                features.get('rsi_oversold', 0) and features.get('stoch_oversold', 0)
            )
            
            # 趋势强度
            trend_signals = [
                features.get('price_above_ma5', 0),
                features.get('ma5_above_ma10', 0),
                features.get('ma10_above_ma20', 0),
                features.get('momentum_5d_positive', 0)
            ]
            composite['trend_strength'] = sum(trend_signals)
            composite['strong_uptrend'] = int(composite['trend_strength'] >= 3)
            
            # 反转信号
            reversal_signals = [
                features.get('double_overbought', 0),
                features.get('double_oversold', 0),
                features.get('doji', 0),
                features.get('hammer', 0)
            ]
            composite['reversal_signal'] = int(sum(reversal_signals) >= 1)
            
            # 突破信号
            composite['breakout_signal'] = int(
                features.get('price_above_bb_upper', 0) or 
                features.get('high_volume', 0)
            )
            
        except Exception as e:
            logger.error(f"计算组合特征时出错: {e}")
            composite.update({
                'double_overbought': 0, 'double_oversold': 0,
                'trend_strength': 0, 'strong_uptrend': 0,
                'reversal_signal': 0, 'breakout_signal': 0
            })
        
        return composite
    
    # 辅助计算方法
    def _calculate_ema(self, values, period):
        """计算指数移动平均"""
        alpha = 2 / (period + 1)
        ema = [values[0]]
        
        for i in range(1, len(values)):
            ema.append(alpha * values[i] + (1 - alpha) * ema[-1])
        
        return np.array(ema)
    
    def _calculate_rsi(self, closes, period):
        """计算RSI"""
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.mean(gains[-period:])
        avg_losses = np.mean(losses[-period:])
        
        if avg_losses == 0:
            return 100.0
        
        rs = avg_gains / avg_losses
        return 100 - (100 / (1 + rs))
    
    def _calculate_stoch_k(self, closes, highs, lows, period):
        """计算随机指标%K"""
        period = min(period, len(closes))
        recent_high = np.max(highs[-period:])
        recent_low = np.min(lows[-period:])
        
        if recent_high == recent_low:
            return 50.0
        
        return 100 * (closes[-1] - recent_low) / (recent_high - recent_low)
    
    def _calculate_macd(self, closes):
        """计算MACD"""
        ema_12 = self._calculate_ema(closes, 12)
        ema_26 = self._calculate_ema(closes, 26)
        macd_line = ema_12 - ema_26
        macd_signal = self._calculate_ema(macd_line, 9)
        macd_histogram = macd_line - macd_signal
        
        return macd_line, macd_signal, macd_histogram
    
    def _calculate_atr(self, highs, lows, closes, period):
        """计算平均真实范围"""
        if len(closes) < 2:
            return highs[-1] - lows[-1]
        
        tr_list = []
        for i in range(1, len(closes)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_list.append(max(tr1, tr2, tr3))
        
        period = min(period, len(tr_list))
        return np.mean(tr_list[-period:])

def main():
    """主函数 - 测试特征计算器"""
    calculator = FeatureCalculator()
    
    # 创建测试数据
    dates = pd.date_range(start='2024-01-01', periods=50, freq='1min')
    
    # 模拟OHLC数据
    np.random.seed(42)
    base_price = 45000
    prices = []
    current_price = base_price
    
    for _ in range(50):
        change = np.random.normal(0, 0.001) * current_price
        current_price += change
        
        open_price = current_price
        high_price = current_price * (1 + abs(np.random.normal(0, 0.0005)))
        low_price = current_price * (1 - abs(np.random.normal(0, 0.0005)))
        close_price = current_price + np.random.normal(0, 0.0005) * current_price
        volume = int(np.random.exponential(1000000))
        
        prices.append({
            'timestamp': dates[len(prices)],
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        })
    
    df = pd.DataFrame(prices)
    
    # 计算特征
    features = calculator.calculate_comprehensive_features(df, 'BTCUSDT')
    
    if features:
        logger.info("计算的特征示例:")
        for key, value in list(features.items())[:20]:  # 显示前20个特征
            logger.info(f"  {key}: {value}")
    else:
        logger.error("特征计算失败")

if __name__ == "__main__":
    main()