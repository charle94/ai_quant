#!/usr/bin/env python3
"""
Alpha101 特征展示脚本
展示所有特征定义和数据样本，不依赖完整的 Feast 安装
"""

import pandas as pd
import os

def main():
    print("🎯 Alpha101 特征存储项目展示")
    print("=" * 60)
    
    # 检查数据文件
    data_path = "/workspace/feast_alpha101/data/alpha101_complete.parquet"
    if not os.path.exists(data_path):
        print(f"❌ 数据文件不存在: {data_path}")
        return
    
    # 读取数据
    print("📊 正在加载 Alpha101 特征数据...")
    df = pd.read_parquet(data_path)
    
    print(f"\n📈 数据概览:")
    print(f"   • 数据形状: {df.shape[0]} 行 × {df.shape[1]} 列")
    print(f"   • 时间范围: {df.timestamp.min()} 到 {df.timestamp.max()}")
    print(f"   • 股票数量: {df.symbol.nunique()}")
    print(f"   • 股票列表: {sorted(df.symbol.unique())}")
    
    # Alpha 因子列表
    alpha_cols = [col for col in df.columns if col.startswith('alpha') and col[5:].isdigit()]
    print(f"\n🧮 Alpha 因子统计:")
    print(f"   • Alpha 因子总数: {len(alpha_cols)}")
    print(f"   • 平均有效因子数: {df.total_valid_factors.mean():.1f}")
    print(f"   • 有效因子数范围: {df.total_valid_factors.min()} - {df.total_valid_factors.max()}")
    
    # 特征分类
    print(f"\n🏷️  特征分类:")
    feature_categories = {
        "基础市场数据": ["open", "high", "low", "close", "volume", "vwap", "returns"],
        "Alpha 001-020": [f"alpha{i:03d}" for i in range(1, 21)],
        "Alpha 021-040": [f"alpha{i:03d}" for i in range(21, 41)],
        "Alpha 041-060": [f"alpha{i:03d}" for i in range(41, 61)],
        "Alpha 061-080": [f"alpha{i:03d}" for i in range(61, 81)],
        "Alpha 081-101": [f"alpha{i:03d}" for i in range(81, 102)],
        "组合因子": ["momentum_composite", "reversal_composite", "volume_composite"],
        "统计指标": ["total_valid_factors"]
    }
    
    for category, features in feature_categories.items():
        available_features = [f for f in features if f in df.columns]
        print(f"   • {category}: {len(available_features)} 个特征")
    
    # 显示详细的特征定义
    print(f"\n📋 详细特征定义:")
    
    feature_descriptions = {
        # 基础市场数据
        "open": "开盘价 - 当日交易开始时的价格",
        "high": "最高价 - 当日交易的最高价格", 
        "low": "最低价 - 当日交易的最低价格",
        "close": "收盘价 - 当日交易结束时的价格",
        "volume": "成交量 - 当日总交易股数",
        "vwap": "成交量加权平均价格 - Volume Weighted Average Price",
        "returns": "日收益率 - 当日价格变化百分比",
        
        # Alpha 因子示例（前10个）
        "alpha001": "收益率排序因子 - 基于风险调整后的收益率排序",
        "alpha002": "成交量变化排序因子 - 衡量成交量相对变化",
        "alpha003": "价格成交量比率 - 反映流动性特征",
        "alpha004": "成交量相对强度 - 当前成交量与平均成交量比率",
        "alpha005": "开盘价相对VWAP强度 - 衡量开盘价偏离程度",
        "alpha006": "开盘价成交量乘积 - 综合价格和成交量信息",
        "alpha007": "价格变化幅度 - 7日价格变化的绝对值",
        "alpha008": "开盘价动量 - 开盘价相对前日收盘价变化",
        "alpha009": "价格变化方向 - 价格上涨下跌的方向性指标",
        "alpha010": "收盘价变化 - 当日收盘价变化量",
        
        # 组合因子
        "momentum_composite": "动量组合因子 - 多个动量类Alpha因子的加权平均",
        "reversal_composite": "反转组合因子 - 多个反转类Alpha因子的加权平均", 
        "volume_composite": "成交量组合因子 - 多个成交量类Alpha因子的加权平均",
        
        # 统计指标
        "total_valid_factors": "有效因子总数 - 当日计算成功的Alpha因子数量"
    }
    
    # 显示前20个特征的定义
    print(f"\n📖 前20个特征定义:")
    for i, (feature, description) in enumerate(feature_descriptions.items(), 1):
        if feature in df.columns:
            print(f"   {i:2d}. {feature:20s} - {description}")
        if i >= 20:
            break
    
    # 数据样本
    print(f"\n📊 数据样本 (AAPL 最新3天):")
    sample_cols = ["symbol", "timestamp", "close", "volume", "alpha001", "alpha002", "alpha101", "momentum_composite"]
    aapl_sample = df[df.symbol == 'AAPL'].sort_values('timestamp').tail(3)
    
    for col in sample_cols:
        if col in df.columns:
            print(f"\n{col}:")
            for idx, row in aapl_sample.iterrows():
                value = row[col]
                if isinstance(value, (int, float)) and not pd.isna(value):
                    print(f"   {row['timestamp']}: {value:.6f}" if col not in ['symbol', 'timestamp'] else f"   {value}")
                else:
                    print(f"   {row['timestamp']}: {value}")
    
    # 统计信息
    print(f"\n📈 Alpha 因子统计 (前5个):")
    for alpha in alpha_cols[:5]:
        if alpha in df.columns:
            mean_val = df[alpha].mean()
            std_val = df[alpha].std()
            min_val = df[alpha].min()
            max_val = df[alpha].max()
            print(f"   {alpha}: 均值={mean_val:.6f}, 标准差={std_val:.6f}, 范围=[{min_val:.6f}, {max_val:.6f}]")
    
    # Feast 特征存储配置信息
    print(f"\n🏗️  Feast 特征存储配置:")
    print(f"   • 项目名称: alpha101_features")
    print(f"   • 数据源: Parquet 文件")
    print(f"   • 实体: 股票代码 (symbol)")
    print(f"   • TTL: 30 天")
    print(f"   • 特征总数: {len([col for col in df.columns if col not in ['symbol', 'timestamp']])}")
    
    print(f"\n🎉 Alpha101 特征存储展示完成!")
    print(f"✨ 数据已准备就绪，可用于机器学习和量化分析")

if __name__ == "__main__":
    main()