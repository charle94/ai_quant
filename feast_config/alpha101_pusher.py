#!/usr/bin/env python3
"""
Alpha 101因子推送器 - 将Alpha因子推送到Feast
"""
import sys
import os
from datetime import datetime, timedelta
import logging
import json
from typing import Dict, List, Optional

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Alpha101FactorPusher:
    """Alpha 101因子推送器"""
    
    def __init__(self, repo_path="/workspace/feast_config/feature_repo"):
        self.repo_path = repo_path
        self.fs = None
        self._init_feast_store()
    
    def _init_feast_store(self):
        """初始化Feast存储"""
        try:
            from feast import FeatureStore
            self.fs = FeatureStore(repo_path=self.repo_path)
            logger.info("Feast存储初始化成功")
        except Exception as e:
            logger.error(f"初始化Feast存储时出错: {e}")
            self.fs = None
    
    def calculate_realtime_alpha_factors(self, market_data: Dict) -> Dict:
        """计算实时Alpha因子 (简化版本)"""
        try:
            symbol = market_data['symbol']
            prices = market_data.get('prices', [])  # 最近的价格序列
            volumes = market_data.get('volumes', [])  # 最近的成交量序列
            
            if len(prices) < 10:
                logger.warning(f"{symbol} 数据不足，无法计算Alpha因子")
                return {}
            
            # 计算实时Alpha因子
            factors = {}
            
            # Alpha001 简化版: 基于价格位置的反转因子
            recent_prices = prices[-5:]
            max_price_idx = recent_prices.index(max(recent_prices))
            alpha001_rt = (max_price_idx / (len(recent_prices) - 1)) - 0.5
            factors['alpha001_rt'] = alpha001_rt
            
            # Alpha003 简化版: 开盘价与成交量的负相关
            if len(prices) >= 10 and len(volumes) >= 10:
                # 简单相关性计算
                recent_volumes = volumes[-10:]
                recent_opens = prices[-10:]  # 假设prices包含开盘价信息
                
                mean_open = sum(recent_opens) / len(recent_opens)
                mean_vol = sum(recent_volumes) / len(recent_volumes)
                
                numerator = sum((recent_opens[i] - mean_open) * (recent_volumes[i] - mean_vol) 
                              for i in range(len(recent_opens)))
                
                open_var = sum((p - mean_open) ** 2 for p in recent_opens)
                vol_var = sum((v - mean_vol) ** 2 for v in recent_volumes)
                
                if open_var > 0 and vol_var > 0:
                    correlation = numerator / ((open_var * vol_var) ** 0.5)
                    alpha003_rt = -1 * correlation
                else:
                    alpha003_rt = 0.0
            else:
                alpha003_rt = 0.0
            
            factors['alpha003_rt'] = alpha003_rt
            
            # Alpha006 简化版: 开盘价与成交量的相关性
            alpha006_rt = -1 * alpha003_rt  # 与Alpha003相反
            factors['alpha006_rt'] = alpha006_rt
            
            # Alpha012 简化版: 成交量变化 * 价格变化
            if len(prices) >= 2 and len(volumes) >= 2:
                price_change = prices[-1] - prices[-2]
                volume_change = volumes[-1] - volumes[-2]
                volume_change_sign = 1 if volume_change > 0 else (-1 if volume_change < 0 else 0)
                alpha012_rt = volume_change_sign * (-1 * price_change)
            else:
                alpha012_rt = 0.0
            
            factors['alpha012_rt'] = alpha012_rt
            
            # 计算组合因子
            momentum_factors = [alpha001_rt]
            reversal_factors = [alpha003_rt]
            volume_factors = [alpha006_rt, alpha012_rt]
            
            factors['momentum_composite_rt'] = sum(momentum_factors) / len(momentum_factors)
            factors['reversal_composite_rt'] = sum(reversal_factors) / len(reversal_factors)
            factors['volume_composite_rt'] = sum(volume_factors) / len(volume_factors)
            
            # 市场状态判断
            if len(prices) >= 5:
                recent_returns = [(prices[i] - prices[i-1]) / prices[i-1] 
                                for i in range(-4, 0)]  # 最近4个收益率
                
                avg_return = sum(recent_returns) / len(recent_returns)
                return_std = (sum((r - avg_return) ** 2 for r in recent_returns) / len(recent_returns)) ** 0.5
                
                # 趋势判断
                if avg_return > 0.01:
                    factors['market_regime_rt'] = 'TRENDING_UP'
                elif avg_return < -0.01:
                    factors['market_regime_rt'] = 'TRENDING_DOWN'
                else:
                    factors['market_regime_rt'] = 'SIDEWAYS'
                
                # 波动率判断
                if return_std > 0.03:
                    factors['volatility_regime_rt'] = 'HIGH_VOL'
                elif return_std < 0.01:
                    factors['volatility_regime_rt'] = 'LOW_VOL'
                else:
                    factors['volatility_regime_rt'] = 'NORMAL_VOL'
            else:
                factors['market_regime_rt'] = 'UNKNOWN'
                factors['volatility_regime_rt'] = 'UNKNOWN'
            
            return factors
            
        except Exception as e:
            logger.error(f"计算实时Alpha因子时出错: {e}")
            return {}
    
    def push_alpha_factors(self, symbol: str, market_data: Dict) -> bool:
        """推送Alpha因子到Feast"""
        if not self.fs:
            logger.error("Feast存储未初始化")
            return False
        
        try:
            # 计算实时Alpha因子
            factors = self.calculate_realtime_alpha_factors(market_data)
            
            if not factors:
                logger.warning(f"没有计算出 {symbol} 的Alpha因子")
                return False
            
            # 准备推送数据
            import pandas as pd
            
            push_data = {
                'trading_pair': symbol,
                'event_timestamp': datetime.now(),
                **factors
            }
            
            df = pd.DataFrame([push_data])
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
            
            # 推送到Feast
            self.fs.push("alpha_realtime_push_source", df)
            
            logger.info(f"成功推送 {symbol} 的Alpha因子: {list(factors.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"推送Alpha因子时出错: {e}")
            return False
    
    def get_alpha_features_for_decision(self, trading_pairs: List[str], 
                                       feature_set: str = "basic") -> Dict:
        """为决策引擎获取Alpha因子"""
        if not self.fs:
            logger.error("Feast存储未初始化")
            return {}
        
        try:
            # 构建实体行
            entity_rows = [{"trading_pair": pair} for pair in trading_pairs]
            
            # 根据feature_set选择不同的特征
            if feature_set == "basic":
                feature_refs = [
                    "alpha_realtime_factors:alpha001_rt",
                    "alpha_realtime_factors:alpha003_rt",
                    "alpha_realtime_factors:alpha006_rt",
                    "alpha_realtime_factors:alpha012_rt",
                    "alpha_realtime_factors:momentum_composite_rt",
                    "alpha_realtime_factors:reversal_composite_rt",
                ]
            elif feature_set == "composite":
                feature_refs = [
                    "alpha_realtime_factors:momentum_composite_rt",
                    "alpha_realtime_factors:reversal_composite_rt",
                    "alpha_realtime_factors:volume_composite_rt",
                    "alpha_realtime_factors:market_regime_rt",
                    "alpha_realtime_factors:volatility_regime_rt",
                ]
            else:
                # 默认特征集
                feature_refs = [
                    "alpha_realtime_factors:alpha001_rt",
                    "alpha_realtime_factors:momentum_composite_rt",
                    "alpha_realtime_factors:market_regime_rt",
                ]
            
            # 获取在线特征
            feature_vector = self.fs.get_online_features(
                features=feature_refs,
                entity_rows=entity_rows
            )
            
            # 转换为字典格式
            result = feature_vector.to_dict()
            
            logger.info(f"成功获取 {len(trading_pairs)} 个交易对的Alpha因子")
            return result
            
        except Exception as e:
            logger.error(f"获取Alpha因子时出错: {e}")
            return {}
    
    def batch_calculate_and_push(self, market_data_batch: List[Dict]) -> int:
        """批量计算和推送Alpha因子"""
        success_count = 0
        
        for market_data in market_data_batch:
            symbol = market_data.get('symbol')
            if symbol and self.push_alpha_factors(symbol, market_data):
                success_count += 1
        
        logger.info(f"批量推送完成: {success_count}/{len(market_data_batch)} 成功")
        return success_count
    
    def export_alpha_factors_config(self) -> Dict:
        """导出Alpha因子配置信息"""
        config = {
            'alpha101_factors': {
                'basic_factors': [f'alpha{i:03d}' for i in range(1, 51)],
                'advanced_factors': [
                    'momentum_reversal_norm', 'short_momentum_norm', 'medium_momentum_norm',
                    'volume_price_divergence_norm', 'volatility_rank_norm', 'ma_trend_norm',
                    'bollinger_position_norm', 'opening_gap_norm', 'sentiment_volume_norm'
                ],
                'composite_factors': [
                    'momentum_composite', 'reversal_composite', 'volume_composite',
                    'volatility_composite', 'trend_composite'
                ],
                'realtime_factors': [
                    'alpha001_rt', 'alpha003_rt', 'alpha006_rt', 'alpha012_rt',
                    'momentum_composite_rt', 'reversal_composite_rt', 'volume_composite_rt'
                ]
            },
            'factor_categories': {
                'momentum': ['alpha001', 'alpha012', 'short_momentum_norm', 'momentum_composite'],
                'reversal': ['alpha003', 'alpha004', 'momentum_reversal_norm', 'reversal_composite'],
                'volume': ['alpha006', 'alpha025', 'volume_price_confirmation_norm', 'volume_composite'],
                'volatility': ['alpha022', 'alpha040', 'price_volatility_norm', 'volatility_composite'],
                'trend': ['alpha005', 'alpha028', 'ma_trend_norm', 'trend_composite']
            },
            'market_regimes': {
                'trending': ['TRENDING_UP', 'TRENDING_DOWN'],
                'mean_reverting': ['SIDEWAYS'],
                'volatility': ['HIGH_VOL', 'LOW_VOL', 'NORMAL_VOL'],
                'volume': ['HIGH_VOLUME', 'LOW_VOLUME', 'NORMAL_VOLUME']
            }
        }
        
        return config

def create_mock_market_data():
    """创建模拟市场数据"""
    import random
    
    symbols = ['BTCUSDT', 'ETHUSDT']
    market_data_batch = []
    
    for symbol in symbols:
        # 生成最近20期的价格和成交量数据
        base_price = 45000 if symbol == 'BTCUSDT' else 2500
        prices = []
        volumes = []
        
        for i in range(20):
            price_change = random.gauss(0, 0.01) * base_price
            base_price = max(base_price + price_change, base_price * 0.95)
            volume = int(random.expovariate(1/1000000))
            
            prices.append(base_price)
            volumes.append(volume)
        
        market_data_batch.append({
            'symbol': symbol,
            'prices': prices,
            'volumes': volumes,
            'timestamp': datetime.now()
        })
    
    return market_data_batch

def main():
    """测试Alpha 101因子推送器"""
    print("🧪 开始Alpha 101因子推送器测试...")
    
    # 创建推送器实例
    pusher = Alpha101FactorPusher()
    
    # 创建模拟市场数据
    market_data_batch = create_mock_market_data()
    print(f"📊 创建了 {len(market_data_batch)} 个交易对的市场数据")
    
    # 测试因子计算
    print("\n🔍 测试因子计算...")
    for market_data in market_data_batch:
        symbol = market_data['symbol']
        factors = pusher.calculate_realtime_alpha_factors(market_data)
        
        if factors:
            print(f"   📈 {symbol} Alpha因子:")
            for factor_name, value in factors.items():
                if isinstance(value, float):
                    print(f"      {factor_name}: {value:.6f}")
                else:
                    print(f"      {factor_name}: {value}")
        else:
            print(f"   ⚠️  {symbol} 未计算出Alpha因子")
    
    # 测试批量推送 (模拟)
    print("\n🚀 测试批量推送...")
    try:
        # 这里我们模拟推送过程，因为可能没有实际的Feast环境
        success_count = 0
        for market_data in market_data_batch:
            factors = pusher.calculate_realtime_alpha_factors(market_data)
            if factors:
                success_count += 1
                logger.info(f"模拟推送 {market_data['symbol']} 的Alpha因子成功")
        
        print(f"   📊 模拟推送结果: {success_count}/{len(market_data_batch)} 成功")
        
    except Exception as e:
        print(f"   ⚠️  推送测试出错: {e}")
    
    # 导出配置信息
    print("\n📋 导出Alpha因子配置...")
    config = pusher.export_alpha_factors_config()
    
    config_file = "/workspace/feast_config/alpha101_config.json"
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"   📄 配置已保存到: {config_file}")
    
    # 显示配置摘要
    print(f"\n📊 Alpha 101因子配置摘要:")
    print(f"   • 基础因子: {len(config['alpha101_factors']['basic_factors'])} 个")
    print(f"   • 高级因子: {len(config['alpha101_factors']['advanced_factors'])} 个")
    print(f"   • 组合因子: {len(config['alpha101_factors']['composite_factors'])} 个")
    print(f"   • 实时因子: {len(config['alpha101_factors']['realtime_factors'])} 个")
    print(f"   • 因子类别: {len(config['factor_categories'])} 种")
    
    print("\n✅ Alpha 101因子推送器测试完成!")
    
    return True

if __name__ == "__main__":
    main()