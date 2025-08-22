#!/usr/bin/env python3
"""
Feast离线特征客户端
用于回测时获取历史特征数据
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import logging
from feast import FeatureStore
from feast.feature_logging import LoggingConfig, LoggingSource
import os

logger = logging.getLogger(__name__)

class FeastOfflineClient:
    """Feast离线特征客户端"""
    
    def __init__(self, feature_store_path: str = "/workspace/feast_config/feature_repo"):
        """
        初始化Feast离线客户端
        
        Args:
            feature_store_path: Feast特征仓库路径
        """
        self.feature_store_path = feature_store_path
        self.store = None
        self._initialize_store()
    
    def _initialize_store(self):
        """初始化特征仓库"""
        try:
            # 切换到特征仓库目录
            original_cwd = os.getcwd()
            os.chdir(self.feature_store_path)
            
            # 初始化特征仓库
            self.store = FeatureStore(repo_path=".")
            
            # 切换回原目录
            os.chdir(original_cwd)
            
            logger.info(f"成功初始化Feast特征仓库: {self.feature_store_path}")
            
        except Exception as e:
            logger.error(f"初始化Feast特征仓库失败: {e}")
            raise
    
    def get_historical_features(self, 
                              trading_pairs: List[str],
                              start_date: datetime,
                              end_date: datetime,
                              features: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取历史特征数据
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            features: 特征列表，如果为None则使用默认特征
            
        Returns:
            包含历史特征的DataFrame
        """
        if features is None:
            features = [
                "technical_indicators:price",
                "technical_indicators:ma_5", 
                "technical_indicators:ma_10",
                "technical_indicators:ma_20",
                "technical_indicators:rsi_14",
                "technical_indicators:volatility_20d",
                "technical_indicators:volume_ratio",
                "technical_indicators:momentum_5d",
                "technical_indicators:momentum_10d",
                "technical_indicators:bb_position",
                "technical_indicators:price_above_ma5",
                "technical_indicators:price_above_ma10",
                "technical_indicators:rsi_overbought",
                "technical_indicators:rsi_oversold",
            ]
        
        try:
            # 构建实体DataFrame
            entity_df = self._create_entity_df(trading_pairs, start_date, end_date)
            
            # 获取历史特征
            training_df = self.store.get_historical_features(
                entity_df=entity_df,
                features=features,
            ).to_df()
            
            logger.info(f"成功获取历史特征数据: {len(training_df)} 条记录")
            return training_df
            
        except Exception as e:
            logger.error(f"获取历史特征失败: {e}")
            raise
    
    def _create_entity_df(self, 
                         trading_pairs: List[str], 
                         start_date: datetime, 
                         end_date: datetime,
                         freq: str = "H") -> pd.DataFrame:
        """
        创建实体DataFrame
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期  
            end_date: 结束日期
            freq: 时间频率，默认为小时
            
        Returns:
            实体DataFrame
        """
        # 生成时间序列
        timestamps = pd.date_range(start=start_date, end=end_date, freq=freq)
        
        # 为每个交易对和时间戳创建记录
        entity_rows = []
        for trading_pair in trading_pairs:
            for timestamp in timestamps:
                entity_rows.append({
                    "trading_pair": trading_pair,
                    "event_timestamp": timestamp
                })
        
        entity_df = pd.DataFrame(entity_rows)
        return entity_df
    
    def get_features_for_backtest(self,
                                 trading_pairs: List[str],
                                 start_date: datetime,
                                 end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        为回测获取特征数据，按交易对分组
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            按交易对分组的特征数据字典
        """
        # 获取所有历史特征
        all_features_df = self.get_historical_features(
            trading_pairs=trading_pairs,
            start_date=start_date,
            end_date=end_date
        )
        
        # 按交易对分组
        features_by_pair = {}
        for trading_pair in trading_pairs:
            pair_features = all_features_df[
                all_features_df['trading_pair'] == trading_pair
            ].copy()
            
            # 按时间排序
            pair_features = pair_features.sort_values('event_timestamp')
            features_by_pair[trading_pair] = pair_features
            
        logger.info(f"为{len(trading_pairs)}个交易对准备回测特征数据")
        return features_by_pair
    
    def get_alpha101_features(self,
                            trading_pairs: List[str], 
                            start_date: datetime,
                            end_date: datetime) -> pd.DataFrame:
        """
        获取Alpha101因子特征
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Alpha101特征DataFrame
        """
        alpha101_features = [
            "alpha101_features:alpha001",
            "alpha101_features:alpha002", 
            "alpha101_features:alpha003",
            "alpha101_features:alpha004",
            "alpha101_features:alpha005",
            "alpha101_features:alpha006",
            "alpha101_features:alpha007",
            "alpha101_features:alpha008",
            "alpha101_features:alpha009",
            "alpha101_features:alpha010",
            # 可以根据需要添加更多Alpha因子
        ]
        
        try:
            return self.get_historical_features(
                trading_pairs=trading_pairs,
                start_date=start_date, 
                end_date=end_date,
                features=alpha101_features
            )
        except Exception as e:
            logger.warning(f"获取Alpha101特征失败，使用基础特征: {e}")
            # 如果Alpha101特征不可用，回退到基础特征
            return self.get_historical_features(
                trading_pairs=trading_pairs,
                start_date=start_date,
                end_date=end_date
            )
    
    def validate_features(self, features_df: pd.DataFrame) -> bool:
        """
        验证特征数据的完整性
        
        Args:
            features_df: 特征DataFrame
            
        Returns:
            验证是否通过
        """
        required_columns = ['trading_pair', 'event_timestamp']
        
        # 检查必需列
        missing_columns = [col for col in required_columns if col not in features_df.columns]
        if missing_columns:
            logger.error(f"缺少必需列: {missing_columns}")
            return False
        
        # 检查数据完整性
        if features_df.empty:
            logger.error("特征数据为空")
            return False
        
        # 检查缺失值比例
        missing_ratio = features_df.isnull().sum().sum() / (len(features_df) * len(features_df.columns))
        if missing_ratio > 0.3:  # 超过30%缺失值
            logger.warning(f"特征数据缺失值比例较高: {missing_ratio:.2%}")
        
        logger.info(f"特征数据验证通过: {len(features_df)} 条记录, {len(features_df.columns)} 个特征")
        return True
    
    def get_feature_names(self) -> List[str]:
        """获取可用的特征名称列表"""
        try:
            feature_views = self.store.list_feature_views()
            feature_names = []
            
            for fv in feature_views:
                for field in fv.schema:
                    feature_names.append(f"{fv.name}:{field.name}")
            
            return feature_names
            
        except Exception as e:
            logger.error(f"获取特征名称失败: {e}")
            return []