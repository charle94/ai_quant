#!/usr/bin/env python3
"""
测试Feast Serving API
"""
import requests
import json
import time
import threading
from feast_serving_api import app

def start_api_server():
    """启动API服务器（后台线程）"""
    app.run(host='0.0.0.0', port=6566, debug=False)

def test_feast_serving_api():
    """测试Feast Serving API"""
    print("=== 测试Feast Serving API ===")
    
    base_url = "http://localhost:6566"
    
    # 等待服务器启动
    print("等待API服务器启动...")
    time.sleep(3)
    
    try:
        # 测试1: 健康检查
        print("\n1. 测试健康检查")
        response = requests.get(f"{base_url}/health")
        print(f"状态码: {response.status_code}")
        print(f"响应: {response.json()}")
        
        # 测试2: 列出可用特征
        print("\n2. 列出可用特征")
        response = requests.get(f"{base_url}/list-features")
        print(f"状态码: {response.status_code}")
        features_data = response.json()
        print(f"可用交易对: {features_data.get('available_symbols', [])}")
        
        # 测试3: 获取特征元数据
        print("\n3. 获取特征元数据")
        response = requests.get(f"{base_url}/feature-metadata/quant_features")
        print(f"状态码: {response.status_code}")
        metadata = response.json()
        print(f"特征数量: {len(metadata.get('features', []))}")
        
        # 测试4: 获取在线特征
        print("\n4. 获取在线特征")
        
        # 为BTCUSDT获取特征
        request_data = {
            "feature_service": "quant_features",
            "entities": {
                "symbol": "BTCUSDT"
            }
        }
        
        response = requests.post(
            f"{base_url}/get-online-features",
            json=request_data,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"状态码: {response.status_code}")
        features_response = response.json()
        
        if 'features' in features_response and 'BTCUSDT' in features_response['features']:
            btc_features = features_response['features']['BTCUSDT']
            print(f"BTCUSDT特征:")
            print(f"  价格: {btc_features.get('price', 'N/A')}")
            print(f"  5日均线: {btc_features.get('ma_5', 'N/A')}")
            print(f"  RSI: {btc_features.get('rsi_14', 'N/A')}")
            print(f"  成交量比率: {btc_features.get('volume_ratio', 'N/A')}")
        else:
            print("未获取到BTCUSDT特征数据")
        
        # 测试5: 批量获取多个交易对的特征
        print("\n5. 批量获取特征")
        
        for symbol in ['BTCUSDT', 'ETHUSDT']:
            request_data = {
                "feature_service": "quant_features",
                "entities": {
                    "symbol": symbol
                }
            }
            
            response = requests.post(
                f"{base_url}/get-online-features",
                json=request_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                features_response = response.json()
                if symbol in features_response.get('features', {}):
                    features = features_response['features'][symbol]
                    price = features.get('price', 0)
                    signal_score = features.get('rsi_14', 50)
                    print(f"{symbol}: 价格={price:.2f}, RSI={signal_score:.2f}")
                else:
                    print(f"{symbol}: 无特征数据")
            else:
                print(f"{symbol}: API请求失败 (状态码: {response.status_code})")
        
        print("\n🎉 Feast Serving API测试完成!")
        
    except requests.exceptions.ConnectionError:
        print("❌ 无法连接到API服务器，请确保服务器正在运行")
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")

def main():
    """主函数"""
    print("启动Feast Serving API测试...")
    
    # 启动API服务器（后台线程）
    server_thread = threading.Thread(target=start_api_server, daemon=True)
    server_thread.start()
    
    # 运行测试
    test_feast_serving_api()

if __name__ == "__main__":
    main()