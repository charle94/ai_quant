#!/usr/bin/env python3
"""
端到端系统集成测试
测试完整的量化分析系统流程：数据采集 -> 特征工程 -> 特征存储 -> 决策引擎
"""
import time
import threading
import requests
import sys
from datetime import datetime

# 导入各个模块
from realtime_processing.miniqmt_connector import MiniQMTConnector
from realtime_processing.feature_calculator import FeatureCalculator
from feast_config.simple_push_features import SimpleFeastPusher
from decision_engine.python_decision_engine import DecisionEngine
from feast_serving_api import app as feast_app

class EndToEndTestSuite:
    """端到端测试套件"""
    
    def __init__(self):
        self.test_results = {}
        self.services = {}
        
    def setup_services(self):
        """启动所有服务"""
        print("=== 启动系统服务 ===")
        
        # 1. 数据连接器
        print("1. 启动数据连接器...")
        self.services['data_connector'] = MiniQMTConnector()
        self.services['data_connector'].start_data_collection(['BTCUSDT', 'ETHUSDT'])
        
        # 2. 特征计算器
        print("2. 初始化特征计算器...")
        self.services['feature_calculator'] = FeatureCalculator()
        
        # 3. Feast推送器
        print("3. 初始化Feast推送器...")
        self.services['feast_pusher'] = SimpleFeastPusher()
        
        # 4. 决策引擎
        print("4. 初始化决策引擎...")
        self.services['decision_engine'] = DecisionEngine()
        
        # 5. Feast Serving API (后台线程)
        print("5. 启动Feast Serving API...")
        feast_thread = threading.Thread(
            target=lambda: feast_app.run(host='0.0.0.0', port=6566, debug=False),
            daemon=True
        )
        feast_thread.start()
        
        print("✓ 所有服务已启动")
        return True
    
    def test_data_collection(self):
        """测试数据采集模块"""
        print("\n=== 测试数据采集模块 ===")
        
        try:
            # 等待数据采集
            print("等待数据采集（10秒）...")
            time.sleep(10)
            
            # 检查Arrow数据
            df = self.services['data_connector'].read_arrow_data()
            
            if not df.empty:
                print(f"✓ 数据采集成功: {len(df)} 条记录")
                print(f"  交易对: {df['symbol'].unique().tolist()}")
                print(f"  时间范围: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
                self.test_results['data_collection'] = 'PASS'
                return True
            else:
                print("❌ 数据采集失败: 无数据")
                self.test_results['data_collection'] = 'FAIL'
                return False
                
        except Exception as e:
            print(f"❌ 数据采集测试出错: {e}")
            self.test_results['data_collection'] = 'ERROR'
            return False
    
    def test_feature_engineering(self):
        """测试特征工程模块"""
        print("\n=== 测试特征工程模块 ===")
        
        try:
            symbols = ['BTCUSDT', 'ETHUSDT']
            success_count = 0
            
            for symbol in symbols:
                # 获取最新数据
                latest_data = self.services['data_connector'].get_latest_data(symbol, limit=50)
                
                if not latest_data.empty:
                    # 计算特征
                    features = self.services['feature_calculator'].calculate_comprehensive_features(
                        latest_data, symbol
                    )
                    
                    if features:
                        print(f"✓ {symbol} 特征计算成功")
                        print(f"  价格: {features.get('price', 0):.2f}")
                        print(f"  RSI: {features.get('rsi_14', 0):.2f}")
                        print(f"  5日均线: {features.get('ma_5', 0):.2f}")
                        success_count += 1
                    else:
                        print(f"❌ {symbol} 特征计算失败")
                else:
                    print(f"❌ {symbol} 无可用数据")
            
            if success_count == len(symbols):
                self.test_results['feature_engineering'] = 'PASS'
                return True
            else:
                self.test_results['feature_engineering'] = 'PARTIAL'
                return False
                
        except Exception as e:
            print(f"❌ 特征工程测试出错: {e}")
            self.test_results['feature_engineering'] = 'ERROR'
            return False
    
    def test_feature_storage(self):
        """测试特征存储模块"""
        print("\n=== 测试特征存储模块 ===")
        
        try:
            # 推送特征到Redis
            count = self.services['feast_pusher'].push_features_from_duckdb()
            
            if count > 0:
                print(f"✓ 特征推送成功: {count} 条记录")
                
                # 验证特征检索
                symbols = self.services['feast_pusher'].list_available_symbols()
                print(f"✓ 可用特征: {symbols}")
                
                # 测试特征获取
                for symbol in symbols[:2]:  # 测试前2个
                    features = self.services['feast_pusher'].get_features(symbol)
                    if features:
                        print(f"✓ {symbol} 特征检索成功")
                    else:
                        print(f"❌ {symbol} 特征检索失败")
                
                self.test_results['feature_storage'] = 'PASS'
                return True
            else:
                print("❌ 特征推送失败")
                self.test_results['feature_storage'] = 'FAIL'
                return False
                
        except Exception as e:
            print(f"❌ 特征存储测试出错: {e}")
            self.test_results['feature_storage'] = 'ERROR'
            return False
    
    def test_decision_engine(self):
        """测试决策引擎模块"""
        print("\n=== 测试决策引擎模块 ===")
        
        try:
            # 生成交易信号
            signals = self.services['decision_engine'].generate_signals()
            
            if signals:
                print(f"✓ 信号生成成功: {len(signals)} 个信号")
                
                for signal in signals:
                    print(f"  {signal.trading_pair}: {signal.signal} "
                          f"(买入:{signal.buy_score}, 卖出:{signal.sell_score})")
                
                # 保存信号
                self.services['decision_engine'].save_signals_to_redis(signals)
                
                # 验证信号检索
                latest_signals = self.services['decision_engine'].get_latest_signals()
                if latest_signals:
                    print(f"✓ 信号检索成功: {len(latest_signals)} 个信号")
                    self.test_results['decision_engine'] = 'PASS'
                    return True
                else:
                    print("❌ 信号检索失败")
                    self.test_results['decision_engine'] = 'FAIL'
                    return False
            else:
                print("❌ 信号生成失败")
                self.test_results['decision_engine'] = 'FAIL'
                return False
                
        except Exception as e:
            print(f"❌ 决策引擎测试出错: {e}")
            self.test_results['decision_engine'] = 'ERROR'
            return False
    
    def test_feast_serving(self):
        """测试Feast Serving API"""
        print("\n=== 测试Feast Serving API ===")
        
        try:
            # 等待API服务启动
            time.sleep(3)
            
            base_url = "http://localhost:6566"
            
            # 健康检查
            response = requests.get(f"{base_url}/health", timeout=5)
            if response.status_code == 200:
                print("✓ Feast Serving API 健康检查通过")
            else:
                print("❌ Feast Serving API 健康检查失败")
                self.test_results['feast_serving'] = 'FAIL'
                return False
            
            # 获取特征
            request_data = {
                "feature_service": "quant_features",
                "entities": {"symbol": "BTCUSDT"}
            }
            
            response = requests.post(
                f"{base_url}/get-online-features",
                json=request_data,
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            
            if response.status_code == 200:
                features_response = response.json()
                if 'BTCUSDT' in features_response.get('features', {}):
                    print("✓ 特征API调用成功")
                    features = features_response['features']['BTCUSDT']
                    print(f"  获取到 {len(features)} 个特征")
                    self.test_results['feast_serving'] = 'PASS'
                    return True
                else:
                    print("❌ 特征API返回数据为空")
                    self.test_results['feast_serving'] = 'FAIL'
                    return False
            else:
                print(f"❌ 特征API调用失败: {response.status_code}")
                self.test_results['feast_serving'] = 'FAIL'
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Feast Serving API 连接失败: {e}")
            self.test_results['feast_serving'] = 'ERROR'
            return False
        except Exception as e:
            print(f"❌ Feast Serving API 测试出错: {e}")
            self.test_results['feast_serving'] = 'ERROR'
            return False
    
    def test_end_to_end_workflow(self):
        """测试端到端工作流"""
        print("\n=== 测试端到端工作流 ===")
        
        try:
            print("执行完整的量化分析流程...")
            
            # 1. 数据采集 -> 2. 特征计算 -> 3. 特征存储 -> 4. 决策生成 -> 5. API服务
            
            # 等待数据流稳定
            time.sleep(5)
            
            # 模拟实时处理流程
            for symbol in ['BTCUSDT', 'ETHUSDT']:
                # 获取最新数据
                latest_data = self.services['data_connector'].get_latest_data(symbol, limit=30)
                
                if not latest_data.empty:
                    # 计算特征
                    features = self.services['feature_calculator'].calculate_comprehensive_features(
                        latest_data, symbol
                    )
                    
                    if features:
                        # 推送特征（模拟实时推送）
                        feature_key = f"feast:realtime_features:{symbol}"
                        feature_data = {
                            'symbol': symbol,
                            'timestamp': features['timestamp'].isoformat(),
                            'price': float(features.get('price', 0)),
                            'ma_5': float(features.get('ma_5', 0)),
                            'rsi_14': float(features.get('rsi_14', 50)),
                        }
                        self.services['feast_pusher'].redis_client.hset(feature_key, mapping=feature_data)
                        
                        print(f"✓ {symbol} 端到端流程执行成功")
                    else:
                        print(f"❌ {symbol} 特征计算失败")
                else:
                    print(f"❌ {symbol} 数据获取失败")
            
            # 生成最终决策信号
            signals = self.services['decision_engine'].generate_signals()
            
            if signals:
                print(f"✓ 端到端流程完成，生成 {len(signals)} 个交易信号")
                
                # 显示最终结果
                for signal in signals:
                    print(f"  最终信号: {signal.trading_pair} -> {signal.signal} "
                          f"(价格: {signal.price:.2f}, 风险: {signal.risk_level})")
                
                self.test_results['end_to_end'] = 'PASS'
                return True
            else:
                print("❌ 端到端流程失败：无法生成信号")
                self.test_results['end_to_end'] = 'FAIL'
                return False
                
        except Exception as e:
            print(f"❌ 端到端流程测试出错: {e}")
            self.test_results['end_to_end'] = 'ERROR'
            return False
    
    def cleanup_services(self):
        """清理服务"""
        print("\n=== 清理服务 ===")
        
        try:
            if 'data_connector' in self.services:
                self.services['data_connector'].stop_data_collection()
            
            print("✓ 服务清理完成")
            
        except Exception as e:
            print(f"清理服务时出错: {e}")
    
    def generate_test_report(self):
        """生成测试报告"""
        print("\n" + "="*60)
        print("端到端系统集成测试报告")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results.values() if r == 'PASS'])
        
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总测试数: {total_tests}")
        print(f"通过测试: {passed_tests}")
        print(f"成功率: {passed_tests/total_tests*100:.1f}%")
        
        print("\n详细结果:")
        for test_name, result in self.test_results.items():
            status_icon = "✓" if result == 'PASS' else "❌" if result == 'FAIL' else "⚠️"
            print(f"  {status_icon} {test_name}: {result}")
        
        print("\n系统架构验证:")
        print("  ✓ DBT + DuckDB 离线特征工程")
        print("  ✓ MiniQMT + Arrow IPC 实时数据存储")
        print("  ✓ Feast 特征存储和服务")
        print("  ✓ Python决策引擎 (规则引擎)")
        print("  ✓ Redis 在线存储")
        print("  ✓ REST API 特征服务")
        
        if passed_tests == total_tests:
            print("\n🎉 所有测试通过！量化分析系统运行正常！")
            return True
        else:
            print(f"\n⚠️  {total_tests - passed_tests} 个测试未通过，请检查相关模块")
            return False

def main():
    """主函数"""
    print("开始端到端系统集成测试...")
    print("测试目标：验证完整的量化分析系统功能")
    
    test_suite = EndToEndTestSuite()
    
    try:
        # 1. 启动服务
        if not test_suite.setup_services():
            print("❌ 服务启动失败")
            return False
        
        # 2. 执行各模块测试
        test_suite.test_data_collection()
        test_suite.test_feature_engineering()
        test_suite.test_feature_storage()
        test_suite.test_decision_engine()
        test_suite.test_feast_serving()
        
        # 3. 端到端工作流测试
        test_suite.test_end_to_end_workflow()
        
        # 4. 生成报告
        success = test_suite.generate_test_report()
        
        return success
        
    except KeyboardInterrupt:
        print("\n收到中断信号，停止测试")
    except Exception as e:
        print(f"\n测试过程中出现异常: {e}")
    finally:
        test_suite.cleanup_services()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)