#!/usr/bin/env python3
"""
RuleGo回测适配器
连接Feast离线数据和RuleGo决策引擎进行回测
"""
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from feast_offline_client import FeastOfflineClient

logger = logging.getLogger(__name__)

class SignalType(Enum):
    """信号类型"""
    BUY = "BUY"
    SELL = "SELL" 
    HOLD = "HOLD"

@dataclass
class RuleGoMessage:
    """RuleGo消息格式"""
    trading_pair: str
    timestamp: str
    price: float
    ma_5: float
    ma_10: float
    ma_20: float
    rsi_14: float
    volatility: float
    volume_ratio: float
    momentum_5d: float
    momentum_10d: float
    bb_position: float
    price_above_ma5: int
    price_above_ma10: int
    rsi_overbought: int
    rsi_oversold: int
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return asdict(self)

@dataclass
class BacktestSignal:
    """回测信号"""
    timestamp: datetime
    trading_pair: str
    signal: SignalType
    price: float
    confidence: float
    features: Dict[str, Any]
    buy_score: int = 0
    sell_score: int = 0
    trend: str = "UNKNOWN"
    momentum_signal: str = "WEAK"
    volume_signal: str = "NORMAL"
    risk_level: str = "MEDIUM"

class RuleGoBacktestAdapter:
    """RuleGo回测适配器"""
    
    def __init__(self, 
                 feast_client: FeastOfflineClient,
                 rulego_endpoint: str = "http://localhost:8080",
                 rules_chain_id: str = "trading_strategy_chain"):
        """
        初始化适配器
        
        Args:
            feast_client: Feast离线客户端
            rulego_endpoint: RuleGo服务端点
            rules_chain_id: 规则链ID
        """
        self.feast_client = feast_client
        self.rulego_endpoint = rulego_endpoint
        self.rules_chain_id = rules_chain_id
        self.session = requests.Session()
        self.session.timeout = 30
        
    def prepare_backtest_data(self,
                            trading_pairs: List[str],
                            start_date: datetime,
                            end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        准备回测数据
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            按交易对分组的特征数据
        """
        logger.info(f"准备回测数据: {trading_pairs}, {start_date} - {end_date}")
        
        try:
            # 从Feast获取历史特征
            features_data = self.feast_client.get_features_for_backtest(
                trading_pairs=trading_pairs,
                start_date=start_date,
                end_date=end_date
            )
            
            # 验证数据
            for trading_pair, df in features_data.items():
                if not self.feast_client.validate_features(df):
                    logger.warning(f"交易对 {trading_pair} 的特征数据验证失败")
            
            return features_data
            
        except Exception as e:
            logger.error(f"准备回测数据失败: {e}")
            raise
    
    def convert_to_rulego_message(self, row: pd.Series) -> RuleGoMessage:
        """
        将特征数据转换为RuleGo消息格式
        
        Args:
            row: 特征数据行
            
        Returns:
            RuleGo消息对象
        """
        try:
            message = RuleGoMessage(
                trading_pair=row.get('trading_pair', ''),
                timestamp=row.get('event_timestamp', datetime.now()).isoformat(),
                price=float(row.get('technical_indicators__price', 0.0)),
                ma_5=float(row.get('technical_indicators__ma_5', 0.0)),
                ma_10=float(row.get('technical_indicators__ma_10', 0.0)),
                ma_20=float(row.get('technical_indicators__ma_20', 0.0)),
                rsi_14=float(row.get('technical_indicators__rsi_14', 50.0)),
                volatility=float(row.get('technical_indicators__volatility_20d', 0.0)),
                volume_ratio=float(row.get('technical_indicators__volume_ratio', 1.0)),
                momentum_5d=float(row.get('technical_indicators__momentum_5d', 0.0)),
                momentum_10d=float(row.get('technical_indicators__momentum_10d', 0.0)),
                bb_position=float(row.get('technical_indicators__bb_position', 0.5)),
                price_above_ma5=int(row.get('technical_indicators__price_above_ma5', 0)),
                price_above_ma10=int(row.get('technical_indicators__price_above_ma10', 0)),
                rsi_overbought=int(row.get('technical_indicators__rsi_overbought', 0)),
                rsi_oversold=int(row.get('technical_indicators__rsi_oversold', 0))
            )
            return message
            
        except Exception as e:
            logger.error(f"转换RuleGo消息失败: {e}")
            # 返回默认消息
            return RuleGoMessage(
                trading_pair=row.get('trading_pair', ''),
                timestamp=datetime.now().isoformat(),
                price=0.0, ma_5=0.0, ma_10=0.0, ma_20=0.0, rsi_14=50.0,
                volatility=0.0, volume_ratio=1.0, momentum_5d=0.0, momentum_10d=0.0,
                bb_position=0.5, price_above_ma5=0, price_above_ma10=0,
                rsi_overbought=0, rsi_oversold=0
            )
    
    def call_rulego_engine(self, message: RuleGoMessage) -> Optional[Dict[str, Any]]:
        """
        调用RuleGo引擎处理消息
        
        Args:
            message: RuleGo消息
            
        Returns:
            RuleGo处理结果
        """
        try:
            # 构建请求数据
            request_data = {
                "chainId": self.rules_chain_id,
                "msg": message.to_dict()
            }
            
            # 发送请求到RuleGo
            response = self.session.post(
                f"{self.rulego_endpoint}/api/v1/rule/chain/process",
                json=request_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                logger.error(f"RuleGo请求失败: {response.status_code}, {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"调用RuleGo引擎失败: {e}")
            return None
    
    def parse_rulego_result(self, 
                          rulego_result: Dict[str, Any], 
                          original_message: RuleGoMessage) -> BacktestSignal:
        """
        解析RuleGo结果为回测信号
        
        Args:
            rulego_result: RuleGo处理结果
            original_message: 原始消息
            
        Returns:
            回测信号
        """
        try:
            # 从RuleGo结果中提取信号信息
            msg_data = rulego_result.get('msg', {})
            
            # 确定信号类型
            signal_str = msg_data.get('signal', 'HOLD')
            if signal_str == 'BUY':
                signal = SignalType.BUY
            elif signal_str == 'SELL':
                signal = SignalType.SELL
            else:
                signal = SignalType.HOLD
            
            # 计算置信度
            buy_score = msg_data.get('buy_score', 0)
            sell_score = msg_data.get('sell_score', 0)
            confidence = max(buy_score, sell_score) / 100.0  # 假设分数在0-100之间
            
            # 创建回测信号
            backtest_signal = BacktestSignal(
                timestamp=datetime.fromisoformat(original_message.timestamp.replace('Z', '+00:00')),
                trading_pair=original_message.trading_pair,
                signal=signal,
                price=original_message.price,
                confidence=confidence,
                features=original_message.to_dict(),
                buy_score=buy_score,
                sell_score=sell_score,
                trend=msg_data.get('trend', 'UNKNOWN'),
                momentum_signal=msg_data.get('momentum_signal', 'WEAK'),
                volume_signal=msg_data.get('volume_signal', 'NORMAL'),
                risk_level=msg_data.get('risk_level', 'MEDIUM')
            )
            
            return backtest_signal
            
        except Exception as e:
            logger.error(f"解析RuleGo结果失败: {e}")
            # 返回默认HOLD信号
            return BacktestSignal(
                timestamp=datetime.now(),
                trading_pair=original_message.trading_pair,
                signal=SignalType.HOLD,
                price=original_message.price,
                confidence=0.0,
                features=original_message.to_dict()
            )
    
    def process_backtest_data(self, 
                            features_data: Dict[str, pd.DataFrame]) -> List[BacktestSignal]:
        """
        处理回测数据，生成交易信号
        
        Args:
            features_data: 按交易对分组的特征数据
            
        Returns:
            交易信号列表
        """
        signals = []
        
        for trading_pair, df in features_data.items():
            logger.info(f"处理交易对 {trading_pair}: {len(df)} 条记录")
            
            for idx, row in df.iterrows():
                try:
                    # 转换为RuleGo消息
                    rulego_message = self.convert_to_rulego_message(row)
                    
                    # 调用RuleGo引擎
                    rulego_result = self.call_rulego_engine(rulego_message)
                    
                    if rulego_result:
                        # 解析结果为交易信号
                        signal = self.parse_rulego_result(rulego_result, rulego_message)
                        signals.append(signal)
                    else:
                        # 如果RuleGo调用失败，生成默认HOLD信号
                        default_signal = BacktestSignal(
                            timestamp=pd.to_datetime(row['event_timestamp']),
                            trading_pair=trading_pair,
                            signal=SignalType.HOLD,
                            price=row.get('technical_indicators__price', 0.0),
                            confidence=0.0,
                            features={}
                        )
                        signals.append(default_signal)
                        
                except Exception as e:
                    logger.error(f"处理数据行失败: {e}")
                    continue
        
        logger.info(f"生成 {len(signals)} 个交易信号")
        return signals
    
    def run_offline_backtest(self,
                           trading_pairs: List[str],
                           start_date: datetime,
                           end_date: datetime) -> Tuple[List[BacktestSignal], Dict[str, pd.DataFrame]]:
        """
        运行离线回测
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易信号列表和原始特征数据
        """
        logger.info(f"开始离线回测: {start_date} - {end_date}")
        
        try:
            # 准备回测数据
            features_data = self.prepare_backtest_data(
                trading_pairs=trading_pairs,
                start_date=start_date,
                end_date=end_date
            )
            
            # 处理数据生成信号
            signals = self.process_backtest_data(features_data)
            
            logger.info(f"离线回测完成: 生成 {len(signals)} 个信号")
            return signals, features_data
            
        except Exception as e:
            logger.error(f"离线回测失败: {e}")
            raise
    
    def test_rulego_connection(self) -> bool:
        """
        测试RuleGo连接
        
        Returns:
            连接是否成功
        """
        try:
            response = self.session.get(f"{self.rulego_endpoint}/api/v1/health")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"RuleGo连接测试失败: {e}")
            return False