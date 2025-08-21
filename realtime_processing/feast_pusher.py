#!/usr/bin/env python3
"""
Feast特征推送器 - 将实时特征推送到Feast
"""
import pandas as pd
from feast import FeatureStore
from datetime import datetime
import logging
import time
import threading
from typing import List, Dict
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeastPusher:
    """Feast实时特征推送器"""
    
    def __init__(self, repo_path="/workspace/feast_config/feature_repo"):
        self.repo_path = repo_path
        self.fs = None
        self.is_running = False
        self.push_queue = []
        self.push_lock = threading.Lock()
        self._init_feast_store()
    
    def _init_feast_store(self):
        """初始化Feast存储"""
        try:
            self.fs = FeatureStore(repo_path=self.repo_path)
            logger.info("Feast存储初始化成功")
        except Exception as e:
            logger.error(f"初始化Feast存储时出错: {e}")
            self.fs = None
    
    def push_single_feature(self, features: Dict) -> bool:
        """推送单个特征到Feast"""
        if not self.fs:
            logger.error("Feast存储未初始化")
            return False
        
        try:
            # 准备数据格式
            feature_df = self._prepare_feature_dataframe([features])
            
            if feature_df.empty:
                logger.warning("特征数据为空，跳过推送")
                return False
            
            # 推送到Feast
            self.fs.push("realtime_features_push_source", feature_df)
            
            logger.info(f"成功推送 {features['symbol']} 的实时特征")
            return True
            
        except Exception as e:
            logger.error(f"推送单个特征时出错: {e}")
            return False
    
    def push_batch_features(self, features_list: List[Dict]) -> int:
        """批量推送特征到Feast"""
        if not self.fs:
            logger.error("Feast存储未初始化")
            return 0
        
        if not features_list:
            logger.warning("特征列表为空")
            return 0
        
        try:
            # 准备数据格式
            feature_df = self._prepare_feature_dataframe(features_list)
            
            if feature_df.empty:
                logger.warning("特征数据为空，跳过推送")
                return 0
            
            # 推送到Feast
            self.fs.push("realtime_features_push_source", feature_df)
            
            success_count = len(feature_df)
            logger.info(f"成功批量推送 {success_count} 条实时特征")
            return success_count
            
        except Exception as e:
            logger.error(f"批量推送特征时出错: {e}")
            return 0
    
    def _prepare_feature_dataframe(self, features_list: List[Dict]) -> pd.DataFrame:
        """准备特征数据格式用于Feast推送"""
        try:
            if not features_list:
                return pd.DataFrame()
            
            # 定义Feast推送源需要的字段
            required_fields = [
                'symbol', 'price', 'volume', 'ma_5', 'ma_10', 
                'rsi_14', 'volatility_20d', 'volume_ratio', 'momentum_5d'
            ]
            
            processed_data = []
            
            for features in features_list:
                if not isinstance(features, dict):
                    continue
                
                # 提取必需字段
                row_data = {
                    'trading_pair': features.get('symbol', 'UNKNOWN'),
                    'event_timestamp': features.get('event_timestamp', datetime.now())
                }
                
                # 映射特征字段
                field_mapping = {
                    'price': 'price',
                    'volume': 'volume', 
                    'ma_5': 'ma_5',
                    'ma_10': 'ma_10',
                    'rsi_14': 'rsi_14',
                    'volatility_20d': 'volatility',
                    'volume_ratio': 'volume_ratio',
                    'momentum_5d': 'momentum_5d'
                }
                
                for feast_field, feature_key in field_mapping.items():
                    value = features.get(feature_key, 0.0)
                    
                    # 数据类型转换和验证
                    if feast_field == 'volume':
                        row_data[feast_field] = int(value) if value is not None else 0
                    else:
                        row_data[feast_field] = float(value) if value is not None else 0.0
                
                processed_data.append(row_data)
            
            if not processed_data:
                logger.warning("没有有效的特征数据")
                return pd.DataFrame()
            
            # 创建DataFrame
            df = pd.DataFrame(processed_data)
            
            # 确保事件时间戳格式正确
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
            
            # 数据验证
            df = df.dropna()  # 移除空值行
            
            logger.info(f"准备了 {len(df)} 条特征数据用于推送")
            return df
            
        except Exception as e:
            logger.error(f"准备特征数据时出错: {e}")
            return pd.DataFrame()
    
    def start_background_pusher(self, push_interval=10):
        """启动后台推送服务"""
        if self.is_running:
            logger.warning("后台推送服务已在运行")
            return
        
        self.is_running = True
        
        def background_worker():
            logger.info(f"后台推送服务已启动，推送间隔: {push_interval}秒")
            
            while self.is_running:
                try:
                    # 获取待推送的特征
                    with self.push_lock:
                        if self.push_queue:
                            features_to_push = self.push_queue.copy()
                            self.push_queue.clear()
                        else:
                            features_to_push = []
                    
                    # 推送特征
                    if features_to_push:
                        success_count = self.push_batch_features(features_to_push)
                        logger.info(f"后台推送了 {success_count} 条特征")
                    
                    time.sleep(push_interval)
                    
                except Exception as e:
                    logger.error(f"后台推送服务出错: {e}")
                    time.sleep(push_interval)
        
        # 启动后台线程
        self.background_thread = threading.Thread(target=background_worker, daemon=True)
        self.background_thread.start()
    
    def stop_background_pusher(self):
        """停止后台推送服务"""
        if self.is_running:
            logger.info("停止后台推送服务...")
            self.is_running = False
    
    def queue_feature_for_push(self, features: Dict):
        """将特征加入推送队列"""
        with self.push_lock:
            self.push_queue.append(features)
            
            # 防止队列过大
            if len(self.push_queue) > 1000:
                self.push_queue = self.push_queue[-500:]  # 保留最新的500条
                logger.warning("推送队列过大，清理了旧数据")
    
    def get_online_features_for_decision(self, trading_pairs: List[str]) -> Dict:
        """为决策引擎获取在线特征"""
        if not self.fs:
            logger.error("Feast存储未初始化")
            return {}
        
        try:
            # 构建实体行
            entity_rows = [{"trading_pair": pair} for pair in trading_pairs]
            
            # 定义要获取的特征
            feature_refs = [
                "realtime_features:price",
                "realtime_features:ma_5",
                "realtime_features:ma_10",
                "realtime_features:rsi_14",
                "realtime_features:volatility",
                "realtime_features:volume_ratio",
                "realtime_features:momentum_5d",
            ]
            
            # 获取在线特征
            feature_vector = self.fs.get_online_features(
                features=feature_refs,
                entity_rows=entity_rows
            )
            
            # 转换为字典格式
            result = feature_vector.to_dict()
            
            logger.info(f"成功获取 {len(trading_pairs)} 个交易对的在线特征")
            return result
            
        except Exception as e:
            logger.error(f"获取在线特征时出错: {e}")
            return {}
    
    def health_check(self) -> Dict:
        """健康检查"""
        status = {
            'feast_store_initialized': self.fs is not None,
            'background_pusher_running': self.is_running,
            'queue_size': len(self.push_queue) if hasattr(self, 'push_queue') else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 测试Feast连接
            if self.fs:
                # 尝试获取一个简单的特征
                test_entity = [{"trading_pair": "BTCUSDT"}]
                self.fs.get_online_features(
                    features=["realtime_features:price"],
                    entity_rows=test_entity
                )
                status['feast_connection'] = 'healthy'
            else:
                status['feast_connection'] = 'not_initialized'
                
        except Exception as e:
            status['feast_connection'] = f'error: {str(e)}'
        
        return status

def main():
    """主函数 - 测试Feast推送器"""
    pusher = FeastPusher()
    
    try:
        # 健康检查
        health = pusher.health_check()
        logger.info(f"健康检查结果: {json.dumps(health, indent=2)}")
        
        # 创建测试特征数据
        test_features = [
            {
                'symbol': 'BTCUSDT',
                'price': 45000.0,
                'volume': 1000000,
                'ma_5': 44950.0,
                'ma_10': 44900.0,
                'rsi_14': 65.5,
                'volatility_20d': 0.02,
                'volume_ratio': 1.2,
                'momentum_5d': 0.01,
                'event_timestamp': datetime.now()
            },
            {
                'symbol': 'ETHUSDT',
                'price': 2500.0,
                'volume': 500000,
                'ma_5': 2495.0,
                'ma_10': 2490.0,
                'rsi_14': 45.2,
                'volatility_20d': 0.03,
                'volume_ratio': 1.1,
                'momentum_5d': -0.005,
                'event_timestamp': datetime.now()
            }
        ]
        
        # 测试批量推送
        success_count = pusher.push_batch_features(test_features)
        logger.info(f"测试推送结果: {success_count} 条特征成功推送")
        
        # 测试获取在线特征
        time.sleep(2)  # 等待数据写入
        online_features = pusher.get_online_features_for_decision(['BTCUSDT', 'ETHUSDT'])
        
        if online_features:
            logger.info("获取到的在线特征:")
            for key, values in online_features.items():
                logger.info(f"  {key}: {values}")
        
        # 测试后台推送服务
        logger.info("测试后台推送服务...")
        pusher.start_background_pusher(push_interval=5)
        
        # 添加一些特征到队列
        for features in test_features:
            pusher.queue_feature_for_push(features)
        
        time.sleep(10)  # 等待后台推送
        pusher.stop_background_pusher()
        
    except Exception as e:
        logger.error(f"测试过程中出错: {e}")

if __name__ == "__main__":
    main()