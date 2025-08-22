#!/usr/bin/env python3
"""
实时特征加工和推送测试
"""
import sys
import time
import threading
from datetime import datetime
import pandas as pd

# 导入自定义模块
from realtime_processing.miniqmt_connector import MiniQMTConnector
from realtime_processing.feature_calculator import FeatureCalculator
from feast_config.simple_push_features import SimpleFeastPusher

class RealtimeFeaturePipeline:
    """实时特征处理管道"""
    
    def __init__(self):
        self.data_connector = MiniQMTConnector()
        self.feature_calculator = FeatureCalculator()
        self.feast_pusher = SimpleFeastPusher()
        self.is_running = False
        
    def start_pipeline(self, symbols=['BTCUSDT', 'ETHUSDT']):
        """启动实时特征处理管道"""
        print("=== 启动实时特征处理管道 ===")
        
        # 启动数据采集
        self.data_connector.start_data_collection(symbols)
        self.is_running = True
        
        # 启动特征处理线程
        feature_thread = threading.Thread(
            target=self._feature_processing_worker,
            args=(symbols,),
            daemon=True
        )
        feature_thread.start()
        
        print(f"✓ 实时特征管道已启动，处理交易对: {symbols}")
        
    def stop_pipeline(self):
        """停止实时特征处理管道"""
        print("停止实时特征处理管道...")
        self.is_running = False
        self.data_connector.stop_data_collection()
        
    def _feature_processing_worker(self, symbols):
        """特征处理工作线程"""
        print("特征处理工作线程已启动")
        
        while self.is_running:
            try:
                for symbol in symbols:
                    # 获取最新数据
                    latest_data = self.data_connector.get_latest_data(symbol, limit=100)
                    
                    if not latest_data.empty:
                        # 计算实时特征
                        features = self.feature_calculator.calculate_comprehensive_features(
                            latest_data, symbol
                        )
                        
                        if features:
                            # 推送特征到Redis
                            self._push_features_to_redis(symbol, features)
                            print(f"✓ {symbol} 特征已更新: price={features.get('price', 0):.2f}, "
                                  f"rsi={features.get('rsi_14', 0):.2f}, "
                                  f"ma_5={features.get('ma_5', 0):.2f}")
                
                # 等待5秒再处理下一批
                time.sleep(5)
                
            except Exception as e:
                print(f"特征处理出错: {e}")
                time.sleep(1)
    
    def _push_features_to_redis(self, symbol, features):
        """推送特征到Redis"""
        try:
            feature_key = f"feast:realtime_features:{symbol}"
            
            # 准备要推送的特征数据
            feature_data = {
                'symbol': symbol,
                'timestamp': features['timestamp'].isoformat() if pd.notna(features['timestamp']) else datetime.now().isoformat(),
                'price': float(features.get('price', 0)),
                'ma_5': float(features.get('ma_5', 0)),
                'ma_10': float(features.get('ma_10', 0)),
                'ma_20': float(features.get('ma_20', 0)),
                'rsi_14': float(features.get('rsi_14', 50)),
                'volume_ratio': float(features.get('volume_ratio', 1)),
                'momentum_5d': float(features.get('momentum_5d', 0)),
                'volatility_20d': float(features.get('volatility_20d', 0)),
                'price_above_ma5': int(features.get('price_above_ma5', 0)),
                'price_above_ma10': int(features.get('price_above_ma10', 0)),
                'high_volume': int(features.get('high_volume', 0)),
                'bb_position': float(features.get('bb_position', 0.5)),
            }
            
            # 推送到Redis
            self.feast_pusher.redis_client.hset(feature_key, mapping=feature_data)
            
        except Exception as e:
            print(f"推送 {symbol} 特征到Redis时出错: {e}")
    
    def test_feature_serving(self):
        """测试特征服务"""
        print("\n=== 测试实时特征服务 ===")
        
        try:
            # 获取实时特征
            symbols = ['BTCUSDT', 'ETHUSDT']
            
            for symbol in symbols:
                feature_key = f"feast:realtime_features:{symbol}"
                features = self.feast_pusher.redis_client.hgetall(feature_key)
                
                if features:
                    print(f"\n{symbol} 实时特征:")
                    for key, value in features.items():
                        key_str = key.decode('utf-8')
                        value_str = value.decode('utf-8')
                        print(f"  {key_str}: {value_str}")
                else:
                    print(f"{symbol} 暂无实时特征数据")
                    
        except Exception as e:
            print(f"测试特征服务时出错: {e}")

def main():
    """主测试函数"""
    print("开始实时特征加工和推送测试...")
    
    pipeline = RealtimeFeaturePipeline()
    
    try:
        # 启动管道
        pipeline.start_pipeline(['BTCUSDT', 'ETHUSDT'])
        
        # 运行30秒
        print("运行30秒以收集和处理实时数据...")
        time.sleep(30)
        
        # 测试特征服务
        pipeline.test_feature_serving()
        
        print("\n🎉 实时特征加工和推送测试完成!")
        
    except KeyboardInterrupt:
        print("\n收到中断信号")
    finally:
        pipeline.stop_pipeline()

if __name__ == "__main__":
    main()