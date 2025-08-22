#!/usr/bin/env python3
"""
Feast Serving API - 提供特征服务接口
"""
import redis
import json
from flask import Flask, jsonify, request
from datetime import datetime
from typing import Dict, List, Optional

app = Flask(__name__)

class FeastServingAPI:
    """Feast特征服务API"""
    
    def __init__(self, redis_host='localhost', redis_port=6379):
        """初始化Feast服务"""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        
    def get_online_features(self, feature_service: str, entities: Dict) -> Dict:
        """获取在线特征"""
        try:
            results = {}
            
            for entity_key, entity_value in entities.items():
                if entity_key == 'symbol':
                    # 获取实时特征
                    realtime_key = f"feast:realtime_features:{entity_value}"
                    realtime_features = self.redis_client.hgetall(realtime_key)
                    
                    # 获取离线特征
                    offline_key = f"feast:features:{entity_value}"
                    offline_features = self.redis_client.hgetall(offline_key)
                    
                    # 合并特征
                    features = {}
                    
                    # 优先使用实时特征
                    if realtime_features:
                        for key, value in realtime_features.items():
                            key_str = key.decode('utf-8')
                            value_str = value.decode('utf-8')
                            features[key_str] = self._convert_value(key_str, value_str)
                    
                    # 补充离线特征
                    if offline_features:
                        for key, value in offline_features.items():
                            key_str = key.decode('utf-8')
                            if key_str not in features:  # 只有当实时特征中没有时才使用离线特征
                                value_str = value.decode('utf-8')
                                features[key_str] = self._convert_value(key_str, value_str)
                    
                    results[entity_value] = features
            
            return {
                'features': results,
                'metadata': {
                    'feature_service': feature_service,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'success'
                }
            }
            
        except Exception as e:
            return {
                'features': {},
                'metadata': {
                    'feature_service': feature_service,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'error',
                    'error': str(e)
                }
            }
    
    def _convert_value(self, key: str, value: str):
        """转换值的数据类型"""
        try:
            # 字符串类型
            if key in ['symbol', 'timestamp', 'entity_id']:
                return value
            
            # 整数类型
            elif key in ['price_above_ma5', 'price_above_ma10', 'high_volume', 'volume']:
                return int(float(value))
            
            # 浮点数类型
            else:
                return float(value)
                
        except (ValueError, TypeError):
            return value
    
    def get_feature_metadata(self, feature_service: str) -> Dict:
        """获取特征元数据"""
        return {
            'feature_service': feature_service,
            'features': [
                {'name': 'price', 'type': 'float64', 'description': '当前价格'},
                {'name': 'ma_5', 'type': 'float64', 'description': '5日移动平均'},
                {'name': 'ma_10', 'type': 'float64', 'description': '10日移动平均'},
                {'name': 'ma_20', 'type': 'float64', 'description': '20日移动平均'},
                {'name': 'rsi_14', 'type': 'float64', 'description': '14日RSI指标'},
                {'name': 'volume_ratio', 'type': 'float64', 'description': '成交量比率'},
                {'name': 'momentum_5d', 'type': 'float64', 'description': '5日动量'},
                {'name': 'volatility_20d', 'type': 'float64', 'description': '20日波动率'},
                {'name': 'price_above_ma5', 'type': 'int64', 'description': '价格是否高于5日均线'},
                {'name': 'price_above_ma10', 'type': 'int64', 'description': '价格是否高于10日均线'},
                {'name': 'high_volume', 'type': 'int64', 'description': '是否高成交量'},
                {'name': 'bb_position', 'type': 'float64', 'description': '布林带位置'},
            ],
            'entities': [
                {'name': 'symbol', 'type': 'string', 'description': '交易对符号'}
            ]
        }

# 创建全局服务实例
feast_service = FeastServingAPI()

@app.route('/get-online-features', methods=['POST'])
def get_online_features():
    """获取在线特征的API端点"""
    try:
        data = request.get_json()
        feature_service = data.get('feature_service', 'quant_features')
        entities = data.get('entities', {})
        
        result = feast_service.get_online_features(feature_service, entities)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'features': {},
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e)
            }
        }), 500

@app.route('/feature-metadata/<feature_service>', methods=['GET'])
def get_feature_metadata(feature_service):
    """获取特征元数据的API端点"""
    try:
        result = feast_service.get_feature_metadata(feature_service)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    try:
        # 测试Redis连接
        feast_service.redis_client.ping()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'redis': 'connected'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500

@app.route('/list-features', methods=['GET'])
def list_features():
    """列出所有可用特征"""
    try:
        # 获取所有特征键
        realtime_keys = feast_service.redis_client.keys('feast:realtime_features:*')
        offline_keys = feast_service.redis_client.keys('feast:features:*')
        
        realtime_symbols = [key.decode('utf-8').split(':')[-1] for key in realtime_keys]
        offline_symbols = [key.decode('utf-8').split(':')[-1] for key in offline_keys]
        
        all_symbols = list(set(realtime_symbols + offline_symbols))
        
        return jsonify({
            'available_symbols': all_symbols,
            'realtime_features': realtime_symbols,
            'offline_features': offline_symbols,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

def main():
    """主函数 - 启动Feast Serving API"""
    print("启动Feast Serving API服务...")
    print("API端点:")
    print("  POST /get-online-features - 获取在线特征")
    print("  GET  /feature-metadata/<service> - 获取特征元数据")
    print("  GET  /health - 健康检查")
    print("  GET  /list-features - 列出可用特征")
    
    app.run(host='0.0.0.0', port=6566, debug=False)

if __name__ == "__main__":
    main()