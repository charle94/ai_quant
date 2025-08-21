#!/usr/bin/env python3
"""
简化的特征推送脚本
使用Redis直接推送特征数据，模拟Feast推送功能
"""
import redis
import json
import pandas as pd
import duckdb
from datetime import datetime
from typing import Dict, Any

class SimpleFeastPusher:
    """简化的Feast特征推送器"""
    
    def __init__(self, redis_host='localhost', redis_port=6379):
        """初始化Redis连接"""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.feature_prefix = "feast:features"
        
    def push_features_from_duckdb(self):
        """从DuckDB读取特征并推送到Redis"""
        print("从DuckDB读取特征数据...")
        
        conn = duckdb.connect('/workspace/data/quant_features.duckdb')
        
        # 读取最新的特征数据
        df = conn.execute("""
            SELECT 
                symbol,
                entity_id,
                event_timestamp,
                price,
                ma_5,
                ma_20,
                rsi_14,
                volume_ratio,
                momentum_5d,
                volatility_20d,
                bb_position,
                price_above_ma5,
                price_above_ma10,
                high_volume
            FROM main.features_ohlc_technical
            ORDER BY event_timestamp DESC
        """).fetchdf()
        
        print(f"读取到 {len(df)} 条特征记录")
        
        # 推送特征到Redis
        pushed_count = 0
        for _, row in df.iterrows():
            feature_key = f"{self.feature_prefix}:{row['symbol']}"
            
            # 构建特征字典
            features = {
                'entity_id': row['entity_id'],
                'timestamp': row['event_timestamp'].isoformat(),
                'price': float(row['price']) if pd.notna(row['price']) else 0.0,
                'ma_5': float(row['ma_5']) if pd.notna(row['ma_5']) else 0.0,
                'ma_20': float(row['ma_20']) if pd.notna(row['ma_20']) else 0.0,
                'rsi_14': float(row['rsi_14']) if pd.notna(row['rsi_14']) else 0.0,
                'volume_ratio': float(row['volume_ratio']) if pd.notna(row['volume_ratio']) else 0.0,
                'momentum_5d': float(row['momentum_5d']) if pd.notna(row['momentum_5d']) else 0.0,
                'volatility_20d': float(row['volatility_20d']) if pd.notna(row['volatility_20d']) else 0.0,
                'bb_position': float(row['bb_position']) if pd.notna(row['bb_position']) else 0.0,
                'price_above_ma5': int(row['price_above_ma5']) if pd.notna(row['price_above_ma5']) else 0,
                'price_above_ma10': int(row['price_above_ma10']) if pd.notna(row['price_above_ma10']) else 0,
                'high_volume': int(row['high_volume']) if pd.notna(row['high_volume']) else 0,
            }
            
            # 推送到Redis
            self.redis_client.hset(feature_key, mapping=features)
            pushed_count += 1
        
        conn.close()
        print(f"✓ 成功推送 {pushed_count} 条特征到Redis")
        return pushed_count
    
    def get_features(self, symbol: str) -> Dict[str, Any]:
        """从Redis获取特征数据"""
        feature_key = f"{self.feature_prefix}:{symbol}"
        features = self.redis_client.hgetall(feature_key)
        
        if not features:
            return {}
        
        # 转换字节数据为Python类型
        result = {}
        for key, value in features.items():
            key_str = key.decode('utf-8')
            value_str = value.decode('utf-8')
            
            # 尝试转换数据类型
            if key_str in ['price_above_ma5', 'price_above_ma10', 'high_volume']:
                result[key_str] = int(value_str)
            elif key_str in ['timestamp', 'entity_id']:
                result[key_str] = value_str
            else:
                result[key_str] = float(value_str)
        
        return result
    
    def list_available_symbols(self):
        """列出所有可用的交易对"""
        pattern = f"{self.feature_prefix}:*"
        keys = self.redis_client.keys(pattern)
        symbols = [key.decode('utf-8').split(':')[-1] for key in keys]
        return symbols
    
    def test_feature_serving(self):
        """测试特征服务功能"""
        print("\n=== 测试特征服务功能 ===")
        
        # 获取可用交易对
        symbols = self.list_available_symbols()
        print(f"可用交易对: {symbols}")
        
        # 测试获取特征
        for symbol in symbols:
            features = self.get_features(symbol)
            print(f"\n{symbol} 的特征数据:")
            for key, value in features.items():
                print(f"  {key}: {value}")
        
        return True

def main():
    """主函数"""
    print("开始简化Feast特征推送测试...")
    
    # 初始化推送器
    pusher = SimpleFeastPusher()
    
    # 推送特征
    count = pusher.push_features_from_duckdb()
    if count == 0:
        print("✗ 特征推送失败")
        return False
    
    # 测试特征服务
    pusher.test_feature_serving()
    
    print("\n🎉 简化Feast特征推送测试完成!")
    return True

if __name__ == "__main__":
    main()