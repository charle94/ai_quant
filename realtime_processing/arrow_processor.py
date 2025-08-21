#!/usr/bin/env python3
"""
Arrow数据处理器 - 从Arrow IPC文件读取数据并进行处理
"""
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import duckdb
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArrowProcessor:
    """Arrow数据处理器"""
    
    def __init__(self, arrow_cache_path="/workspace/data/arrow_cache/"):
        self.arrow_cache_path = Path(arrow_cache_path)
        self.duckdb_conn = None
        self._init_duckdb()
    
    def _init_duckdb(self):
        """初始化DuckDB连接"""
        try:
            db_path = "/workspace/data/realtime_features.duckdb"
            self.duckdb_conn = duckdb.connect(db_path)
            
            # 创建实时数据表
            self.duckdb_conn.execute("""
                CREATE TABLE IF NOT EXISTS realtime_ohlc (
                    symbol VARCHAR,
                    timestamp TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    amount DOUBLE,
                    count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            logger.info("DuckDB连接初始化完成")
            
        except Exception as e:
            logger.error(f"初始化DuckDB时出错: {e}")
    
    def load_arrow_to_duckdb(self, hours_back=1):
        """将Arrow数据加载到DuckDB"""
        try:
            # 获取Arrow文件列表
            current_time = datetime.now()
            loaded_count = 0
            
            for i in range(hours_back):
                target_time = current_time - timedelta(hours=i)
                file_pattern = target_time.strftime("%Y%m%d_%H")
                file_path = self.arrow_cache_path / f"ohlc_{file_pattern}.arrow"
                
                if file_path.exists():
                    # 读取Arrow文件
                    with pa.ipc.open_file(file_path) as reader:
                        table = reader.read_all()
                        df = table.to_pandas()
                    
                    if not df.empty:
                        # 清理旧数据（保留最近1小时）
                        cutoff_time = current_time - timedelta(hours=1)
                        self.duckdb_conn.execute("""
                            DELETE FROM realtime_ohlc 
                            WHERE timestamp < ?
                        """, [cutoff_time])
                        
                        # 插入新数据（避免重复）
                        self.duckdb_conn.execute("""
                            INSERT INTO realtime_ohlc 
                            (symbol, timestamp, open, high, low, close, volume, amount, count)
                            SELECT * FROM df
                            WHERE NOT EXISTS (
                                SELECT 1 FROM realtime_ohlc r 
                                WHERE r.symbol = df.symbol 
                                AND r.timestamp = df.timestamp
                            )
                        """)
                        
                        loaded_count += len(df)
                        logger.info(f"加载了 {len(df)} 条数据从 {file_path}")
            
            logger.info(f"总共加载了 {loaded_count} 条Arrow数据到DuckDB")
            return loaded_count
            
        except Exception as e:
            logger.error(f"加载Arrow数据到DuckDB时出错: {e}")
            return 0
    
    def calculate_realtime_features(self, symbol, lookback_periods=20):
        """计算实时技术指标特征"""
        try:
            # 获取最近的数据
            query = f"""
                SELECT *
                FROM realtime_ohlc 
                WHERE symbol = '{symbol}'
                ORDER BY timestamp DESC
                LIMIT {lookback_periods * 2}
            """
            
            df = self.duckdb_conn.execute(query).df()
            
            if len(df) < 5:
                logger.warning(f"{symbol} 数据不足，无法计算特征")
                return None
            
            # 按时间正序排列
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 计算技术指标
            features = self._compute_technical_indicators(df)
            
            if features:
                # 添加元数据
                features.update({
                    'symbol': symbol,
                    'timestamp': df.iloc[-1]['timestamp'],
                    'entity_id': f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M')}",
                    'event_timestamp': df.iloc[-1]['timestamp'],
                    'created_at': datetime.now()
                })
                
                return features
            
        except Exception as e:
            logger.error(f"计算 {symbol} 实时特征时出错: {e}")
            return None
    
    def _compute_technical_indicators(self, df):
        """计算技术指标"""
        try:
            if len(df) < 5:
                return None
            
            features = {}
            
            # 基础价格特征
            features['price'] = float(df.iloc[-1]['close'])
            features['volume'] = int(df.iloc[-1]['volume'])
            
            # 计算收益率
            if len(df) >= 2:
                prev_close = df.iloc[-2]['close']
                if prev_close != 0:
                    features['daily_return'] = float((features['price'] - prev_close) / prev_close)
                else:
                    features['daily_return'] = 0.0
            else:
                features['daily_return'] = 0.0
            
            # 移动平均线
            if len(df) >= 5:
                features['ma_5'] = float(df.tail(5)['close'].mean())
            else:
                features['ma_5'] = features['price']
            
            if len(df) >= 10:
                features['ma_10'] = float(df.tail(10)['close'].mean())
            else:
                features['ma_10'] = features['price']
            
            # RSI计算
            if len(df) >= 14:
                features['rsi_14'] = self._calculate_rsi(df['close'].tail(14))
            else:
                features['rsi_14'] = 50.0
            
            # 波动率（标准差）
            if len(df) >= 10:
                returns = df['close'].pct_change().dropna()
                if len(returns) > 1:
                    features['volatility'] = float(returns.std())
                else:
                    features['volatility'] = 0.0
            else:
                features['volatility'] = 0.0
            
            # 成交量比率
            if len(df) >= 10:
                avg_volume = df.tail(10)['volume'].mean()
                if avg_volume != 0:
                    features['volume_ratio'] = float(features['volume'] / avg_volume)
                else:
                    features['volume_ratio'] = 1.0
            else:
                features['volume_ratio'] = 1.0
            
            # 动量指标
            if len(df) >= 6:
                price_5d_ago = df.iloc[-6]['close']
                if price_5d_ago != 0:
                    features['momentum_5d'] = float((features['price'] - price_5d_ago) / price_5d_ago)
                else:
                    features['momentum_5d'] = 0.0
            else:
                features['momentum_5d'] = 0.0
            
            return features
            
        except Exception as e:
            logger.error(f"计算技术指标时出错: {e}")
            return None
    
    def _calculate_rsi(self, prices):
        """计算RSI指标"""
        try:
            if len(prices) < 2:
                return 50.0
            
            # 计算价格变化
            deltas = prices.diff().dropna()
            
            # 分离涨跌
            gains = deltas.where(deltas > 0, 0)
            losses = -deltas.where(deltas < 0, 0)
            
            # 计算平均涨跌幅
            avg_gains = gains.rolling(window=14, min_periods=1).mean().iloc[-1]
            avg_losses = losses.rolling(window=14, min_periods=1).mean().iloc[-1]
            
            if avg_losses == 0:
                return 100.0
            
            rs = avg_gains / avg_losses
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi)
            
        except Exception as e:
            logger.error(f"计算RSI时出错: {e}")
            return 50.0
    
    def get_all_symbols(self):
        """获取所有交易对"""
        try:
            result = self.duckdb_conn.execute("""
                SELECT DISTINCT symbol 
                FROM realtime_ohlc 
                ORDER BY symbol
            """).fetchall()
            
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"获取交易对列表时出错: {e}")
            return []
    
    def process_all_symbols(self):
        """处理所有交易对的实时特征"""
        try:
            # 首先加载最新的Arrow数据
            self.load_arrow_to_duckdb()
            
            # 获取所有交易对
            symbols = self.get_all_symbols()
            
            if not symbols:
                logger.warning("没有找到交易对数据")
                return []
            
            # 计算每个交易对的特征
            all_features = []
            for symbol in symbols:
                features = self.calculate_realtime_features(symbol)
                if features:
                    all_features.append(features)
                    logger.info(f"计算了 {symbol} 的实时特征")
            
            logger.info(f"总共计算了 {len(all_features)} 个交易对的特征")
            return all_features
            
        except Exception as e:
            logger.error(f"处理所有交易对特征时出错: {e}")
            return []
    
    def close(self):
        """关闭连接"""
        if self.duckdb_conn:
            self.duckdb_conn.close()

def main():
    """主函数 - 测试Arrow处理器"""
    processor = ArrowProcessor()
    
    try:
        # 处理所有交易对
        features_list = processor.process_all_symbols()
        
        if features_list:
            logger.info("计算的特征示例:")
            for features in features_list[:2]:  # 显示前2个
                logger.info(f"  {features['symbol']}: 价格={features['price']}, RSI={features['rsi_14']:.2f}")
        else:
            logger.warning("没有计算出任何特征")
    
    except Exception as e:
        logger.error(f"主函数执行出错: {e}")
    
    finally:
        processor.close()

if __name__ == "__main__":
    main()