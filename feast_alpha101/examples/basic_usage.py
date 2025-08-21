#!/usr/bin/env python3
"""
Alpha101 特征存储基础使用示例
演示如何加载和使用 Alpha101 特征数据
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def load_alpha101_features():
    """加载 Alpha101 特征数据"""
    data_path = "/workspace/feast_alpha101/data/alpha101_complete.parquet"
    df = pd.read_parquet(data_path)
    return df

def get_features_for_symbols(df, symbols, date=None):
    """获取指定股票和日期的特征"""
    if date is None:
        date = df.timestamp.max()
    
    result = df[
        (df.symbol.isin(symbols)) & 
        (df.timestamp == date)
    ]
    return result

def get_historical_features(df, symbol, start_date, end_date):
    """获取指定股票的历史特征"""
    result = df[
        (df.symbol == symbol) & 
        (df.timestamp >= start_date) & 
        (df.timestamp <= end_date)
    ]
    return result.sort_values('timestamp')

def calculate_feature_statistics(df, feature_name):
    """计算特征统计信息"""
    feature_data = df[feature_name].dropna()
    
    stats = {
        'count': len(feature_data),
        'mean': feature_data.mean(),
        'std': feature_data.std(),
        'min': feature_data.min(),
        'max': feature_data.max(),
        'q25': feature_data.quantile(0.25),
        'q50': feature_data.quantile(0.50),
        'q75': feature_data.quantile(0.75)
    }
    
    return stats

def main():
    print("🎯 Alpha101 特征存储基础使用示例")
    print("=" * 50)
    
    # 1. 加载数据
    print("📊 加载 Alpha101 特征数据...")
    df = load_alpha101_features()
    print(f"   数据形状: {df.shape}")
    print(f"   时间范围: {df.timestamp.min()} 到 {df.timestamp.max()}")
    print(f"   股票数量: {df.symbol.nunique()}")
    
    # 2. 获取最新特征
    print(f"\n📈 获取最新交易日特征 (所有股票):")
    latest_features = get_features_for_symbols(df, ['AAPL', 'GOOGL', 'MSFT', 'TSLA'])
    
    selected_cols = ['symbol', 'timestamp', 'close', 'alpha001', 'alpha002', 'momentum_composite']
    print(latest_features[selected_cols].to_string(index=False))
    
    # 3. 获取单只股票历史数据
    print(f"\n📊 获取 AAPL 历史特征:")
    start_date = df.timestamp.min()
    end_date = df.timestamp.max()
    aapl_history = get_historical_features(df, 'AAPL', start_date, end_date)
    
    history_cols = ['timestamp', 'close', 'returns', 'alpha001', 'momentum_composite']
    print(aapl_history[history_cols].to_string(index=False))
    
    # 4. 特征统计分析
    print(f"\n📈 Alpha001 特征统计:")
    alpha001_stats = calculate_feature_statistics(df, 'alpha001')
    for key, value in alpha001_stats.items():
        print(f"   {key}: {value:.6f}")
    
    # 5. 特征相关性分析
    print(f"\n🔗 前5个Alpha因子相关性矩阵:")
    alpha_cols = ['alpha001', 'alpha002', 'alpha003', 'alpha004', 'alpha005']
    correlation_matrix = df[alpha_cols].corr()
    print(correlation_matrix.round(4))
    
    # 6. 特征趋势分析
    print(f"\n📈 AAPL 动量因子趋势:")
    aapl_momentum = aapl_history[['timestamp', 'momentum_composite']].copy()
    aapl_momentum['momentum_trend'] = aapl_momentum['momentum_composite'].diff()
    print(aapl_momentum.to_string(index=False))
    
    # 7. 特征筛选示例
    print(f"\n🎯 高动量股票筛选 (momentum_composite > 0.3):")
    high_momentum = latest_features[latest_features['momentum_composite'] > 0.3]
    if not high_momentum.empty:
        print(high_momentum[['symbol', 'momentum_composite', 'alpha001', 'alpha002']].to_string(index=False))
    else:
        print("   没有找到高动量股票")
    
    # 8. 特征组合示例
    print(f"\n🔄 创建自定义特征组合:")
    df_copy = df.copy()
    
    # 创建自定义组合特征
    df_copy['custom_momentum'] = (
        df_copy['alpha001'] * 0.3 + 
        df_copy['alpha002'] * 0.3 + 
        df_copy['momentum_composite'] * 0.4
    )
    
    # 创建风险调整收益特征
    df_copy['risk_adjusted_alpha'] = df_copy['alpha001'] / (df_copy['alpha002'] + 0.001)
    
    custom_features = df_copy[df_copy.timestamp == df_copy.timestamp.max()]
    custom_cols = ['symbol', 'custom_momentum', 'risk_adjusted_alpha']
    print(custom_features[custom_cols].to_string(index=False))
    
    print(f"\n✅ 示例完成!")

if __name__ == "__main__":
    main()