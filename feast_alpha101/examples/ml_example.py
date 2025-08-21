#!/usr/bin/env python3
"""
Alpha101 特征存储机器学习示例
演示如何使用 Alpha101 特征进行机器学习建模
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """加载并准备机器学习数据"""
    data_path = "/workspace/feast_alpha101/data/alpha101_complete.parquet"
    df = pd.read_parquet(data_path)
    
    # 按股票和时间排序
    df = df.sort_values(['symbol', 'timestamp'])
    
    # 创建目标变量：下一期收益率
    df['next_return'] = df.groupby('symbol')['returns'].shift(-1)
    
    # 移除最后一期数据（没有目标变量）
    df = df.dropna(subset=['next_return'])
    
    return df

def select_features(df):
    """选择用于建模的特征"""
    # Alpha 因子特征
    alpha_features = [col for col in df.columns if col.startswith('alpha') and col[5:].isdigit()]
    
    # 组合特征
    composite_features = ['momentum_composite', 'reversal_composite', 'volume_composite']
    
    # 基础特征
    basic_features = ['returns']  # 当期收益率
    
    # 组合所有特征
    all_features = alpha_features + composite_features + basic_features
    
    # 只保留存在的特征
    available_features = [f for f in all_features if f in df.columns]
    
    return available_features

def create_ml_dataset(df, features, target='next_return'):
    """创建机器学习数据集"""
    # 选择特征和目标变量
    X = df[features].copy()
    y = df[target].copy()
    
    # 处理缺失值
    X = X.fillna(X.median())
    
    # 移除异常值（基于分位数）
    for col in X.columns:
        if X[col].dtype in ['float64', 'int64']:
            q1 = X[col].quantile(0.05)
            q3 = X[col].quantile(0.95)
            X[col] = X[col].clip(q1, q3)
    
    return X, y

def train_model(X, y):
    """训练随机森林模型"""
    # 分割训练和测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, shuffle=True
    )
    
    # 标准化特征
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # 训练随机森林模型
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    
    model.fit(X_train_scaled, y_train)
    
    # 预测
    y_train_pred = model.predict(X_train_scaled)
    y_test_pred = model.predict(X_test_scaled)
    
    return model, scaler, (X_train, X_test, y_train, y_test), (y_train_pred, y_test_pred)

def evaluate_model(y_true, y_pred, dataset_name):
    """评估模型性能"""
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_true, y_pred)
    
    print(f"\n📊 {dataset_name} 性能:")
    print(f"   MSE: {mse:.6f}")
    print(f"   RMSE: {rmse:.6f}")
    print(f"   R²: {r2:.6f}")
    
    return {'mse': mse, 'rmse': rmse, 'r2': r2}

def analyze_feature_importance(model, feature_names, top_n=15):
    """分析特征重要性"""
    importances = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)
    
    print(f"\n🎯 Top {top_n} 重要特征:")
    for i, row in feature_importance.head(top_n).iterrows():
        print(f"   {row['feature']:20s}: {row['importance']:.4f}")
    
    return feature_importance

def predict_returns(model, scaler, df, features):
    """预测收益率"""
    # 获取最新数据
    latest_data = df.groupby('symbol').tail(1)
    
    # 准备特征
    X_latest = latest_data[features].fillna(latest_data[features].median())
    
    # 标准化
    X_latest_scaled = scaler.transform(X_latest)
    
    # 预测
    predictions = model.predict(X_latest_scaled)
    
    # 创建预测结果
    results = pd.DataFrame({
        'symbol': latest_data['symbol'].values,
        'current_price': latest_data['close'].values,
        'predicted_return': predictions,
        'momentum_composite': latest_data['momentum_composite'].values,
        'alpha001': latest_data['alpha001'].values
    })
    
    return results.sort_values('predicted_return', ascending=False)

def main():
    print("🤖 Alpha101 特征存储机器学习示例")
    print("=" * 50)
    
    # 1. 加载和准备数据
    print("📊 加载和准备数据...")
    df = load_and_prepare_data()
    print(f"   数据形状: {df.shape}")
    print(f"   目标变量范围: {df['next_return'].min():.6f} 到 {df['next_return'].max():.6f}")
    
    # 2. 选择特征
    features = select_features(df)
    print(f"\n🎯 选择 {len(features)} 个特征用于建模")
    print(f"   Alpha 因子数: {len([f for f in features if f.startswith('alpha')])}")
    print(f"   组合因子数: {len([f for f in features if 'composite' in f])}")
    
    # 3. 创建数据集
    X, y = create_ml_dataset(df, features)
    print(f"\n📈 创建机器学习数据集:")
    print(f"   特征矩阵形状: {X.shape}")
    print(f"   目标变量形状: {y.shape}")
    
    # 4. 训练模型
    print(f"\n🔧 训练随机森林模型...")
    model, scaler, (X_train, X_test, y_train, y_test), (y_train_pred, y_test_pred) = train_model(X, y)
    
    # 5. 评估模型
    train_metrics = evaluate_model(y_train, y_train_pred, "训练集")
    test_metrics = evaluate_model(y_test, y_test_pred, "测试集")
    
    # 6. 特征重要性分析
    feature_importance = analyze_feature_importance(model, features)
    
    # 7. 预测最新收益率
    print(f"\n🔮 预测下期收益率:")
    predictions = predict_returns(model, scaler, df, features)
    print(predictions.to_string(index=False))
    
    # 8. 模型解释
    print(f"\n🧠 模型洞察:")
    top_features = feature_importance.head(5)['feature'].tolist()
    print(f"   • 最重要的5个特征: {', '.join(top_features)}")
    
    # 计算预测的方向准确率（如果有足够数据）
    if len(y_test) > 0:
        direction_accuracy = np.mean(np.sign(y_test) == np.sign(y_test_pred))
        print(f"   • 方向预测准确率: {direction_accuracy:.2%}")
    
    # 9. 投资建议
    print(f"\n💡 基于模型的投资建议:")
    top_stock = predictions.iloc[0]
    print(f"   • 推荐股票: {top_stock['symbol']}")
    print(f"   • 预期收益率: {top_stock['predicted_return']:.2%}")
    print(f"   • 当前价格: ${top_stock['current_price']:.2f}")
    print(f"   • 动量因子: {top_stock['momentum_composite']:.4f}")
    
    print(f"\n✅ 机器学习示例完成!")
    print(f"📊 模型已训练完成，可用于实时预测")

if __name__ == "__main__":
    main()