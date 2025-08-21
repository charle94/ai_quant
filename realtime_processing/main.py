#!/usr/bin/env python3
"""
实时处理主程序 - 整合所有实时处理组件
"""
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Thread, Event

from miniqmt_connector import MiniQMTConnector
from arrow_processor import ArrowProcessor
from feature_calculator import FeatureCalculator
from feast_pusher import FeastPusher

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealtimeProcessingEngine:
    """实时处理引擎"""
    
    def __init__(self):
        self.is_running = False
        self.stop_event = Event()
        
        # 初始化组件
        self.miniqmt_connector = MiniQMTConnector()
        self.arrow_processor = ArrowProcessor()
        self.feature_calculator = FeatureCalculator()
        self.feast_pusher = FeastPusher()
        
        # 配置参数
        self.trading_pairs = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT']
        self.processing_interval = 10  # 秒
        
    def start(self):
        """启动实时处理引擎"""
        logger.info("启动实时处理引擎...")
        self.is_running = True
        
        try:
            # 启动MiniQMT数据采集
            self.miniqmt_connector.start_data_collection(self.trading_pairs)
            
            # 启动Feast后台推送服务
            self.feast_pusher.start_background_pusher(push_interval=15)
            
            # 启动主处理循环
            self.run_processing_loop()
            
        except Exception as e:
            logger.error(f"启动实时处理引擎时出错: {e}")
            self.stop()
    
    def stop(self):
        """停止实时处理引擎"""
        logger.info("停止实时处理引擎...")
        self.is_running = False
        self.stop_event.set()
        
        # 停止各个组件
        if hasattr(self, 'miniqmt_connector'):
            self.miniqmt_connector.stop_data_collection()
        
        if hasattr(self, 'feast_pusher'):
            self.feast_pusher.stop_background_pusher()
        
        if hasattr(self, 'arrow_processor'):
            self.arrow_processor.close()
    
    def run_processing_loop(self):
        """运行主处理循环"""
        logger.info(f"开始处理循环，间隔: {self.processing_interval}秒")
        
        while self.is_running and not self.stop_event.is_set():
            try:
                start_time = time.time()
                
                # 执行一轮处理
                self.process_round()
                
                # 计算处理时间
                processing_time = time.time() - start_time
                logger.info(f"处理轮次完成，耗时: {processing_time:.2f}秒")
                
                # 等待下一轮处理
                if not self.stop_event.wait(self.processing_interval):
                    continue
                else:
                    break
                    
            except KeyboardInterrupt:
                logger.info("收到中断信号，停止处理...")
                break
            except Exception as e:
                logger.error(f"处理循环中出错: {e}")
                time.sleep(5)  # 出错后等待5秒再继续
    
    def process_round(self):
        """执行一轮完整的处理"""
        try:
            # 1. 处理所有交易对的Arrow数据
            all_features = self.arrow_processor.process_all_symbols()
            
            if not all_features:
                logger.warning("没有获取到任何特征数据")
                return
            
            logger.info(f"处理了 {len(all_features)} 个交易对的特征")
            
            # 2. 为每个特征计算更详细的技术指标
            enhanced_features = []
            
            for basic_features in all_features:
                try:
                    symbol = basic_features['symbol']
                    
                    # 获取该交易对的最新数据
                    recent_data = self.miniqmt_connector.get_latest_data(symbol, limit=50)
                    
                    if recent_data.empty:
                        logger.warning(f"没有获取到 {symbol} 的最新数据")
                        continue
                    
                    # 计算全面的技术指标
                    comprehensive_features = self.feature_calculator.calculate_comprehensive_features(
                        recent_data, symbol
                    )
                    
                    if comprehensive_features:
                        # 合并基础特征和全面特征
                        merged_features = {**basic_features, **comprehensive_features}
                        enhanced_features.append(merged_features)
                        
                        logger.debug(f"增强了 {symbol} 的特征数据")
                    
                except Exception as e:
                    logger.error(f"处理 {symbol} 特征时出错: {e}")
                    continue
            
            # 3. 推送特征到Feast
            if enhanced_features:
                # 批量推送到队列
                for features in enhanced_features:
                    self.feast_pusher.queue_feature_for_push(features)
                
                logger.info(f"将 {len(enhanced_features)} 个特征加入推送队列")
            
            # 4. 健康检查
            self.health_check()
            
        except Exception as e:
            logger.error(f"处理轮次时出错: {e}")
    
    def health_check(self):
        """健康检查"""
        try:
            # 检查Feast推送器状态
            feast_health = self.feast_pusher.health_check()
            
            if feast_health.get('feast_connection') != 'healthy':
                logger.warning(f"Feast连接状态异常: {feast_health.get('feast_connection')}")
            
            # 检查队列大小
            queue_size = feast_health.get('queue_size', 0)
            if queue_size > 500:
                logger.warning(f"推送队列过大: {queue_size}")
            
        except Exception as e:
            logger.error(f"健康检查时出错: {e}")
    
    def get_status(self):
        """获取系统状态"""
        try:
            return {
                'is_running': self.is_running,
                'trading_pairs': self.trading_pairs,
                'processing_interval': self.processing_interval,
                'feast_health': self.feast_pusher.health_check() if hasattr(self, 'feast_pusher') else {},
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取状态时出错: {e}")
            return {'error': str(e)}

# 全局变量
engine = None

def signal_handler(signum, frame):
    """信号处理器"""
    global engine
    logger.info(f"收到信号 {signum}，正在关闭系统...")
    
    if engine:
        engine.stop()
    
    sys.exit(0)

def main():
    """主函数"""
    global engine
    
    logger.info("启动量化分析实时处理系统...")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 创建并启动处理引擎
        engine = RealtimeProcessingEngine()
        engine.start()
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断，正在关闭...")
    except Exception as e:
        logger.error(f"主程序出错: {e}")
    finally:
        if engine:
            engine.stop()
        logger.info("系统已关闭")

if __name__ == "__main__":
    main()