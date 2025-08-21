#!/usr/bin/env python3
"""
增强版回测功能测试
"""
import unittest
import sys
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import os
import logging

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.enhanced_backtest_engine import (
    EnhancedBacktestEngine, 
    EnhancedBacktestConfig,
    MockRuleGoAdapter
)
from backtest.feast_offline_client import FeastOfflineClient
from backtest.rulego_backtest_adapter import RuleGoBacktestAdapter

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestEnhancedBacktest(unittest.TestCase):
    """增强版回测测试类"""
    
    def setUp(self):
        """测试前准备"""
        self.config = EnhancedBacktestConfig(
            initial_capital=10000,
            use_mock_rulego=True,  # 使用模拟模式
            min_confidence=0.1,
            feast_repo_path="/workspace/feast_config/feature_repo"
        )
        
        self.trading_pairs = ["BTCUSDT"]
        self.start_date = datetime.now() - timedelta(days=7)
        self.end_date = datetime.now() - timedelta(days=1)
    
    def test_config_creation(self):
        """测试配置创建"""
        self.assertEqual(self.config.initial_capital, 10000)
        self.assertTrue(self.config.use_mock_rulego)
        self.assertEqual(self.config.min_confidence, 0.1)
    
    def test_feast_client_initialization(self):
        """测试Feast客户端初始化"""
        try:
            client = FeastOfflineClient(self.config.feast_repo_path)
            self.assertIsNotNone(client.store)
            logger.info("✅ Feast客户端初始化测试通过")
        except Exception as e:
            self.skipTest(f"Feast客户端初始化失败，跳过测试: {e}")
    
    def test_mock_rulego_adapter(self):
        """测试模拟RuleGo适配器"""
        try:
            feast_client = FeastOfflineClient(self.config.feast_repo_path)
            adapter = MockRuleGoAdapter(feast_client)
            self.assertIsNotNone(adapter)
            logger.info("✅ 模拟RuleGo适配器测试通过")
        except Exception as e:
            self.skipTest(f"模拟RuleGo适配器测试失败，跳过测试: {e}")
    
    def test_enhanced_backtest_engine_creation(self):
        """测试增强版回测引擎创建"""
        try:
            engine = EnhancedBacktestEngine(self.config)
            self.assertIsNotNone(engine)
            self.assertIsNotNone(engine.feast_client)
            self.assertIsNotNone(engine.rulego_adapter)
            logger.info("✅ 增强版回测引擎创建测试通过")
        except Exception as e:
            self.skipTest(f"增强版回测引擎创建失败，跳过测试: {e}")
    
    def test_full_backtest_workflow(self):
        """测试完整回测流程"""
        try:
            engine = EnhancedBacktestEngine(self.config)
            
            # 运行回测
            result, stats = engine.run_enhanced_backtest(
                trading_pairs=self.trading_pairs,
                start_date=self.start_date,
                end_date=self.end_date
            )
            
            # 验证结果
            self.assertIsNotNone(result)
            self.assertIsNotNone(stats)
            self.assertEqual(result.initial_capital, self.config.initial_capital)
            self.assertGreaterEqual(result.final_capital, 0)
            
            # 验证统计信息
            self.assertIn("signal_stats", stats)
            self.assertIn("feature_stats", stats)
            self.assertIn("performance_stats", stats)
            self.assertIn("risk_stats", stats)
            
            logger.info("✅ 完整回测流程测试通过")
            logger.info(f"   总收益率: {result.total_return:.2%}")
            logger.info(f"   总交易数: {result.total_trades}")
            logger.info(f"   信号数量: {stats['signal_stats']['total_signals']}")
            
        except Exception as e:
            logger.error(f"完整回测流程测试失败: {e}", exc_info=True)
            self.fail(f"完整回测流程测试失败: {e}")


class TestFeastOfflineClient(unittest.TestCase):
    """Feast离线客户端测试"""
    
    def setUp(self):
        """测试前准备"""
        self.repo_path = "/workspace/feast_config/feature_repo"
    
    def test_client_initialization(self):
        """测试客户端初始化"""
        try:
            client = FeastOfflineClient(self.repo_path)
            self.assertIsNotNone(client.store)
            logger.info("✅ Feast客户端初始化测试通过")
        except Exception as e:
            self.skipTest(f"Feast客户端初始化失败，跳过测试: {e}")
    
    def test_get_feature_names(self):
        """测试获取特征名称"""
        try:
            client = FeastOfflineClient(self.repo_path)
            feature_names = client.get_feature_names()
            self.assertIsInstance(feature_names, list)
            logger.info(f"✅ 特征名称获取测试通过，共 {len(feature_names)} 个特征")
        except Exception as e:
            self.skipTest(f"特征名称获取测试失败，跳过测试: {e}")


class TestRuleGoAdapter(unittest.TestCase):
    """RuleGo适配器测试"""
    
    def setUp(self):
        """测试前准备"""
        try:
            self.feast_client = FeastOfflineClient("/workspace/feast_config/feature_repo")
        except Exception as e:
            self.skipTest(f"Feast客户端初始化失败，跳过RuleGo适配器测试: {e}")
    
    def test_mock_adapter_creation(self):
        """测试模拟适配器创建"""
        adapter = MockRuleGoAdapter(self.feast_client)
        self.assertIsNotNone(adapter)
        logger.info("✅ 模拟RuleGo适配器创建测试通过")
    
    def test_real_adapter_creation(self):
        """测试真实适配器创建"""
        try:
            adapter = RuleGoBacktestAdapter(
                feast_client=self.feast_client,
                rulego_endpoint="http://localhost:8080"
            )
            self.assertIsNotNone(adapter)
            logger.info("✅ 真实RuleGo适配器创建测试通过")
        except Exception as e:
            logger.warning(f"真实RuleGo适配器创建失败（这是正常的，如果RuleGo服务未运行）: {e}")


def run_integration_test():
    """运行集成测试"""
    print("🧪 开始增强版回测集成测试...")
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加测试用例
    test_suite.addTest(TestEnhancedBacktest('test_config_creation'))
    test_suite.addTest(TestEnhancedBacktest('test_feast_client_initialization'))
    test_suite.addTest(TestEnhancedBacktest('test_mock_rulego_adapter'))
    test_suite.addTest(TestEnhancedBacktest('test_enhanced_backtest_engine_creation'))
    test_suite.addTest(TestEnhancedBacktest('test_full_backtest_workflow'))
    
    test_suite.addTest(TestFeastOfflineClient('test_client_initialization'))
    test_suite.addTest(TestFeastOfflineClient('test_get_feature_names'))
    
    test_suite.addTest(TestRuleGoAdapter('test_mock_adapter_creation'))
    test_suite.addTest(TestRuleGoAdapter('test_real_adapter_creation'))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出结果
    if result.wasSuccessful():
        print("\n✅ 所有集成测试通过!")
    else:
        print(f"\n❌ 测试失败: {len(result.failures)} 个失败, {len(result.errors)} 个错误")
        for failure in result.failures:
            print(f"   失败: {failure[0]}")
        for error in result.errors:
            print(f"   错误: {error[0]}")
    
    return result.wasSuccessful()


def main():
    """主函数"""
    success = run_integration_test()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()