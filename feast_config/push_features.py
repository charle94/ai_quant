#!/usr/bin/env python3
"""
将特征推送到Feast的脚本
"""
import pandas as pd
from feast import FeatureStore
from datetime import datetime
import duckdb
import yaml
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeastFeaturePusher:
    def __init__(self, repo_path="/workspace/feast_config/feature_repo"):
        self.fs = FeatureStore(repo_path=repo_path)
        
    def push_offline_features(self, db_path="/workspace/data/quant_features.duckdb"):
        """推送离线特征到Feast"""
        logger.info("开始推送离线特征...")
        
        conn = duckdb.connect(db_path)
        
        # 查询特征数据
        query = """
            SELECT 
                symbol as trading_pair,
                price,
                daily_return,
                volatility_20d,
                ma_5,
                ma_10, 
                ma_20,
                rsi_14,
                stoch_k_14,
                momentum_5d,
                momentum_10d,
                volume_ratio,
                bb_position,
                price_above_ma5,
                price_above_ma10,
                price_above_ma20,
                rsi_overbought,
                rsi_oversold,
                high_volume,
                double_overbought,
                double_oversold,
                event_timestamp
            FROM main.features_ohlc_technical
            WHERE event_timestamp >= current_timestamp - interval '7 days'
        """
        
        try:
            df = conn.execute(query).df()
            logger.info(f"获取到 {len(df)} 条离线特征记录")
            
            if not df.empty:
                # 转换数据类型
                df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
                
                # 推送到Feast (这里主要是为了注册特征，实际的离线特征已经在DuckDB中)
                logger.info("离线特征数据已准备就绪")
            else:
                logger.warning("没有找到离线特征数据")
                
        except Exception as e:
            logger.error(f"推送离线特征时出错: {e}")
        finally:
            conn.close()
    
    def push_realtime_features(self, features_df):
        """推送实时特征到Feast"""
        logger.info("推送实时特征到Feast...")
        
        try:
            # 确保数据格式正确
            features_df['event_timestamp'] = pd.to_datetime(features_df['event_timestamp'])
            
            # 推送到Feast在线存储
            self.fs.push("realtime_features_push_source", features_df)
            logger.info(f"成功推送 {len(features_df)} 条实时特征")
            
        except Exception as e:
            logger.error(f"推送实时特征时出错: {e}")
    
    def get_online_features(self, entity_rows):
        """从Feast获取在线特征"""
        try:
            # 获取实时特征
            feature_vector = self.fs.get_online_features(
                features=[
                    "realtime_features:price",
                    "realtime_features:ma_5",
                    "realtime_features:ma_10", 
                    "realtime_features:rsi_14",
                    "realtime_features:volatility",
                    "realtime_features:volume_ratio",
                    "realtime_features:momentum_5d",
                ],
                entity_rows=entity_rows,
            )
            
            return feature_vector.to_dict()
            
        except Exception as e:
            logger.error(f"获取在线特征时出错: {e}")
            return None

def main():
    """主函数 - 测试特征推送"""
    pusher = FeastFeaturePusher()
    
    # 推送离线特征
    pusher.push_offline_features()
    
    # 创建示例实时特征数据
    sample_realtime_data = pd.DataFrame({
        'trading_pair': ['BTCUSDT', 'ETHUSDT'],
        'price': [45000.0, 2500.0],
        'volume': [1000000, 500000],
        'ma_5': [44950.0, 2495.0],
        'ma_10': [44900.0, 2490.0],
        'rsi_14': [65.5, 45.2],
        'volatility': [0.02, 0.03],
        'volume_ratio': [1.2, 1.1],
        'momentum_5d': [0.01, -0.005],
        'event_timestamp': [datetime.now(), datetime.now()]
    })
    
    # 推送实时特征
    pusher.push_realtime_features(sample_realtime_data)
    
    # 测试获取在线特征
    entity_rows = [
        {"trading_pair": "BTCUSDT"},
        {"trading_pair": "ETHUSDT"}
    ]
    
    features = pusher.get_online_features(entity_rows)
    if features:
        logger.info(f"获取到的特征: {features}")

if __name__ == "__main__":
    main()