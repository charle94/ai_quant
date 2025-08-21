#!/usr/bin/env python3
"""
MiniQMT连接器 - 用于实时数据采集和Arrow IPC存储
"""
import asyncio
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import os
import time
from pathlib import Path
import threading
import queue

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MiniQMTConnector:
    """MiniQMT实时数据连接器"""
    
    def __init__(self, config_path="/workspace/config/database.yml"):
        self.config = self._load_config(config_path)
        self.arrow_cache_path = Path(self.config['data_sources']['arrow_cache_path'])
        self.arrow_cache_path.mkdir(parents=True, exist_ok=True)
        
        # 数据缓存队列
        self.data_queue = queue.Queue(maxsize=10000)
        self.is_running = False
        
        # Arrow schema定义
        self.arrow_schema = pa.schema([
            pa.field("symbol", pa.string()),
            pa.field("timestamp", pa.timestamp('ms')),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("volume", pa.int64()),
            pa.field("amount", pa.float64()),
            pa.field("count", pa.int64()),
        ])
    
    def _load_config(self, config_path):
        """加载配置文件"""
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def start_data_collection(self, symbols=['BTCUSDT', 'ETHUSDT']):
        """启动数据采集"""
        logger.info(f"开始采集数据，交易对: {symbols}")
        self.is_running = True
        
        # 启动数据生成线程（模拟实时数据）
        for symbol in symbols:
            thread = threading.Thread(
                target=self._simulate_realtime_data, 
                args=(symbol,),
                daemon=True
            )
            thread.start()
        
        # 启动Arrow写入线程
        writer_thread = threading.Thread(
            target=self._arrow_writer_worker,
            daemon=True
        )
        writer_thread.start()
        
        logger.info("数据采集已启动")
    
    def stop_data_collection(self):
        """停止数据采集"""
        logger.info("停止数据采集...")
        self.is_running = False
    
    def _simulate_realtime_data(self, symbol):
        """模拟实时数据生成（在实际环境中，这里会连接到真实的数据源）"""
        base_price = 45000 if symbol == 'BTCUSDT' else 2500
        last_price = base_price
        
        while self.is_running:
            try:
                # 模拟价格波动
                price_change = np.random.normal(0, 0.001) * last_price
                new_price = max(last_price + price_change, last_price * 0.99)
                
                # 生成OHLC数据
                high = new_price * (1 + abs(np.random.normal(0, 0.0005)))
                low = new_price * (1 - abs(np.random.normal(0, 0.0005)))
                volume = int(np.random.exponential(1000000))
                amount = new_price * volume
                count = int(np.random.poisson(100))
                
                tick_data = {
                    'symbol': symbol,
                    'timestamp': datetime.now(),
                    'open': last_price,
                    'high': high,
                    'low': low,
                    'close': new_price,
                    'volume': volume,
                    'amount': amount,
                    'count': count
                }
                
                # 添加到队列
                if not self.data_queue.full():
                    self.data_queue.put(tick_data)
                else:
                    logger.warning(f"数据队列已满，丢弃 {symbol} 的数据")
                
                last_price = new_price
                time.sleep(0.1)  # 100ms间隔
                
            except Exception as e:
                logger.error(f"生成 {symbol} 数据时出错: {e}")
                time.sleep(1)
    
    def _arrow_writer_worker(self):
        """Arrow IPC写入工作线程"""
        batch_size = 100
        batch_data = []
        
        while self.is_running or not self.data_queue.empty():
            try:
                # 获取数据
                try:
                    data = self.data_queue.get(timeout=1)
                    batch_data.append(data)
                except queue.Empty:
                    if batch_data:
                        self._write_arrow_batch(batch_data)
                        batch_data = []
                    continue
                
                # 达到批次大小时写入
                if len(batch_data) >= batch_size:
                    self._write_arrow_batch(batch_data)
                    batch_data = []
                    
            except Exception as e:
                logger.error(f"Arrow写入工作线程出错: {e}")
        
        # 写入剩余数据
        if batch_data:
            self._write_arrow_batch(batch_data)
    
    def _write_arrow_batch(self, batch_data):
        """写入Arrow批次数据"""
        try:
            if not batch_data:
                return
            
            # 转换为DataFrame
            df = pd.DataFrame(batch_data)
            
            # 转换为Arrow Table
            table = pa.Table.from_pandas(df, schema=self.arrow_schema)
            
            # 生成文件名（按小时分区）
            current_hour = datetime.now().strftime("%Y%m%d_%H")
            file_path = self.arrow_cache_path / f"ohlc_{current_hour}.arrow"
            
            # 写入Arrow IPC文件（追加模式）
            if file_path.exists():
                # 读取现有数据
                with pa.ipc.open_file(file_path) as reader:
                    existing_table = reader.read_all()
                
                # 合并数据
                combined_table = pa.concat_tables([existing_table, table])
            else:
                combined_table = table
            
            # 写入文件
            with pa.ipc.new_file(file_path, combined_table.schema) as writer:
                writer.write_table(combined_table)
            
            logger.info(f"写入 {len(batch_data)} 条数据到 {file_path}")
            
        except Exception as e:
            logger.error(f"写入Arrow批次数据时出错: {e}")
    
    def read_arrow_data(self, hours_back=1):
        """读取Arrow数据"""
        try:
            # 获取最近几小时的文件
            current_time = datetime.now()
            files_to_read = []
            
            for i in range(hours_back):
                target_time = current_time - timedelta(hours=i)
                file_pattern = target_time.strftime("%Y%m%d_%H")
                file_path = self.arrow_cache_path / f"ohlc_{file_pattern}.arrow"
                
                if file_path.exists():
                    files_to_read.append(file_path)
            
            if not files_to_read:
                logger.warning("没有找到Arrow数据文件")
                return pd.DataFrame()
            
            # 读取并合并数据
            tables = []
            for file_path in files_to_read:
                with pa.ipc.open_file(file_path) as reader:
                    table = reader.read_all()
                    tables.append(table)
            
            if tables:
                combined_table = pa.concat_tables(tables)
                df = combined_table.to_pandas()
                logger.info(f"读取到 {len(df)} 条Arrow数据")
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"读取Arrow数据时出错: {e}")
            return pd.DataFrame()
    
    def get_latest_data(self, symbol, limit=100):
        """获取指定交易对的最新数据"""
        try:
            df = self.read_arrow_data(hours_back=1)
            if df.empty:
                return pd.DataFrame()
            
            # 筛选指定交易对
            symbol_data = df[df['symbol'] == symbol]
            
            # 按时间排序并取最新数据
            latest_data = symbol_data.sort_values('timestamp', ascending=False).head(limit)
            
            return latest_data.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"获取 {symbol} 最新数据时出错: {e}")
            return pd.DataFrame()

def main():
    """主函数 - 测试MiniQMT连接器"""
    connector = MiniQMTConnector()
    
    try:
        # 启动数据采集
        connector.start_data_collection(['BTCUSDT', 'ETHUSDT'])
        
        # 运行10秒
        time.sleep(10)
        
        # 读取数据测试
        df = connector.read_arrow_data()
        logger.info(f"读取到数据: {len(df)} 条")
        
        if not df.empty:
            logger.info(f"数据示例:\n{df.head()}")
        
        # 获取最新数据测试
        latest_btc = connector.get_latest_data('BTCUSDT', limit=5)
        logger.info(f"BTCUSDT最新数据: {len(latest_btc)} 条")
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，停止程序")
    finally:
        connector.stop_data_collection()

if __name__ == "__main__":
    main()