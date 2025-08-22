#!/usr/bin/env python3
"""
增强版回测引擎
支持RuleGo决策引擎和Feast特征存储的离线回测
"""
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import logging

from backtest_engine import (
    BacktestEngine, BacktestResult, Position, Order, OrderStatus,
    SignalType, MarketData
)
from feast_offline_client import FeastOfflineClient
from rulego_backtest_adapter import RuleGoBacktestAdapter, BacktestSignal

logger = logging.getLogger(__name__)

@dataclass
class EnhancedBacktestConfig:
    """增强版回测配置"""
    initial_capital: float = 100000.0
    commission_rate: float = 0.001
    slippage_rate: float = 0.0005
    position_size: float = 0.1  # 每次交易使用的资金比例
    max_position_size: float = 0.5  # 最大持仓比例
    stop_loss_pct: float = 0.05  # 止损比例
    take_profit_pct: float = 0.1  # 止盈比例
    min_confidence: float = 0.3  # 最小信号置信度
    
    # Feast配置
    feast_repo_path: str = "/workspace/feast_config/feature_repo"
    
    # RuleGo配置  
    rulego_endpoint: str = "http://localhost:8080"
    rules_chain_id: str = "trading_strategy_chain"
    
    # 回测配置
    use_mock_rulego: bool = False  # 是否使用模拟RuleGo

class EnhancedBacktestEngine(BacktestEngine):
    """增强版回测引擎"""
    
    def __init__(self, config: EnhancedBacktestConfig):
        """
        初始化增强版回测引擎
        
        Args:
            config: 回测配置
        """
        super().__init__(
            initial_capital=config.initial_capital,
            commission_rate=config.commission_rate
        )
        
        self.config = config
        self.feast_client = None
        self.rulego_adapter = None
        self.slippage_rate = config.slippage_rate
        
        # 风控参数
        self.stop_loss_pct = config.stop_loss_pct
        self.take_profit_pct = config.take_profit_pct
        self.min_confidence = config.min_confidence
        
        # 初始化组件
        self._initialize_components()
    
    def _initialize_components(self):
        """初始化Feast和RuleGo组件"""
        try:
            # 初始化Feast客户端
            self.feast_client = FeastOfflineClient(
                feature_store_path=self.config.feast_repo_path
            )
            logger.info("Feast客户端初始化成功")
            
            # 初始化RuleGo适配器
            if not self.config.use_mock_rulego:
                self.rulego_adapter = RuleGoBacktestAdapter(
                    feast_client=self.feast_client,
                    rulego_endpoint=self.config.rulego_endpoint,
                    rules_chain_id=self.config.rules_chain_id
                )
                
                # 测试RuleGo连接
                if self.rulego_adapter.test_rulego_connection():
                    logger.info("RuleGo连接测试成功")
                else:
                    logger.warning("RuleGo连接失败，将使用模拟模式")
                    self.config.use_mock_rulego = True
            
            if self.config.use_mock_rulego:
                self.rulego_adapter = MockRuleGoAdapter(self.feast_client)
                logger.info("使用模拟RuleGo适配器")
                
        except Exception as e:
            logger.error(f"初始化组件失败: {e}")
            raise
    
    def run_enhanced_backtest(self,
                            trading_pairs: List[str],
                            start_date: datetime,
                            end_date: datetime) -> Tuple[BacktestResult, Dict[str, Any]]:
        """
        运行增强版回测
        
        Args:
            trading_pairs: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            回测结果和详细统计信息
        """
        logger.info(f"开始增强版回测: {trading_pairs}, {start_date} - {end_date}")
        
        try:
            # 重置引擎状态
            self.reset()
            
            # 获取交易信号
            signals, features_data = self.rulego_adapter.run_offline_backtest(
                trading_pairs=trading_pairs,
                start_date=start_date,
                end_date=end_date
            )
            
            # 处理信号并执行交易
            processed_signals = self._process_signals_with_risk_management(signals)
            
            # 模拟市场数据并执行交易
            market_data_list = self._create_market_data_from_signals(processed_signals)
            
            # 执行回测
            for i, (signal, market_data) in enumerate(zip(processed_signals, market_data_list)):
                self._process_signal_and_trade(signal, market_data)
                
                # 更新权益曲线
                total_equity = self.get_total_equity()
                self.equity_curve.append((signal.timestamp, total_equity))
                
                # 检查止损止盈
                self._check_stop_loss_take_profit(market_data)
            
            # 计算回测结果
            result = self._calculate_enhanced_result(start_date, end_date)
            
            # 生成详细统计
            stats = self._generate_detailed_stats(processed_signals, features_data)
            
            logger.info(f"增强版回测完成: 总收益率 {result.total_return:.2%}")
            return result, stats
            
        except Exception as e:
            logger.error(f"增强版回测失败: {e}")
            raise
    
    def _process_signals_with_risk_management(self, 
                                            signals: List[BacktestSignal]) -> List[BacktestSignal]:
        """
        应用风险管理规则处理信号
        
        Args:
            signals: 原始信号列表
            
        Returns:
            处理后的信号列表
        """
        processed_signals = []
        
        for signal in signals:
            # 过滤低置信度信号
            if signal.confidence < self.min_confidence:
                signal.signal = SignalType.HOLD
                
            # 检查持仓限制
            if signal.signal in [SignalType.BUY, SignalType.SELL]:
                current_position = self.get_position(signal.trading_pair)
                total_exposure = abs(current_position.quantity * signal.price)
                max_exposure = self.capital * self.config.max_position_size
                
                if total_exposure >= max_exposure:
                    signal.signal = SignalType.HOLD
            
            processed_signals.append(signal)
        
        return processed_signals
    
    def _create_market_data_from_signals(self, 
                                       signals: List[BacktestSignal]) -> List[MarketData]:
        """
        从信号创建市场数据
        
        Args:
            signals: 信号列表
            
        Returns:
            市场数据列表
        """
        market_data_list = []
        
        for signal in signals:
            # 添加滑点
            slippage = signal.price * self.slippage_rate
            if signal.signal == SignalType.BUY:
                execution_price = signal.price + slippage
            elif signal.signal == SignalType.SELL:
                execution_price = signal.price - slippage
            else:
                execution_price = signal.price
            
            market_data = MarketData(
                timestamp=signal.timestamp,
                symbol=signal.trading_pair,
                open=signal.price,
                high=signal.price * 1.001,
                low=signal.price * 0.999,
                close=execution_price,
                volume=1000000  # 假设流动性充足
            )
            
            market_data_list.append(market_data)
        
        return market_data_list
    
    def _process_signal_and_trade(self, signal: BacktestSignal, market_data: MarketData):
        """
        处理信号并执行交易
        
        Args:
            signal: 交易信号
            market_data: 市场数据
        """
        if signal.signal == SignalType.HOLD:
            self.update_unrealized_pnl(market_data)
            return
        
        # 计算订单数量
        position = self.get_position(signal.trading_pair)
        
        if signal.signal == SignalType.BUY:
            if position.quantity <= 0:  # 开多仓或平空仓
                order_value = self.capital * self.config.position_size
                quantity = order_value / market_data.close
            else:
                return  # 已有多仓，不重复开仓
                
        elif signal.signal == SignalType.SELL:
            if position.quantity >= 0:  # 开空仓或平多仓
                if position.quantity > 0:
                    quantity = position.quantity  # 平多仓
                else:
                    order_value = self.capital * self.config.position_size
                    quantity = order_value / market_data.close
            else:
                return  # 已有空仓，不重复开仓
        else:
            return
        
        # 创建并执行订单
        self.order_id += 1
        order = Order(
            id=f"order_{self.order_id}",
            timestamp=signal.timestamp,
            symbol=signal.trading_pair,
            side=signal.signal.value,
            quantity=quantity,
            price=market_data.close,
            status=OrderStatus.PENDING
        )
        
        self.execute_order(order, market_data)
        self.update_unrealized_pnl(market_data)
    
    def _check_stop_loss_take_profit(self, market_data: MarketData):
        """
        检查止损止盈
        
        Args:
            market_data: 市场数据
        """
        position = self.get_position(market_data.symbol)
        
        if position.quantity == 0:
            return
        
        current_price = market_data.close
        entry_price = position.avg_price
        
        # 计算盈亏比例
        if position.quantity > 0:  # 多仓
            pnl_ratio = (current_price - entry_price) / entry_price
        else:  # 空仓
            pnl_ratio = (entry_price - current_price) / entry_price
        
        should_close = False
        
        # 检查止损
        if pnl_ratio <= -self.stop_loss_pct:
            should_close = True
            logger.info(f"触发止损: {market_data.symbol}, 亏损 {pnl_ratio:.2%}")
        
        # 检查止盈
        elif pnl_ratio >= self.take_profit_pct:
            should_close = True
            logger.info(f"触发止盈: {market_data.symbol}, 盈利 {pnl_ratio:.2%}")
        
        if should_close:
            # 平仓
            self.order_id += 1
            side = "SELL" if position.quantity > 0 else "BUY"
            
            order = Order(
                id=f"order_{self.order_id}",
                timestamp=market_data.timestamp,
                symbol=market_data.symbol,
                side=side,
                quantity=abs(position.quantity),
                price=current_price,
                status=OrderStatus.PENDING
            )
            
            self.execute_order(order, market_data)
    
    def _calculate_enhanced_result(self, start_date: datetime, end_date: datetime) -> BacktestResult:
        """
        计算增强版回测结果
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            回测结果
        """
        # 使用父类方法计算基础结果
        final_capital = self.get_total_equity()
        total_return = (final_capital - self.initial_capital) / self.initial_capital
        
        # 统计交易
        filled_orders = [o for o in self.orders if o.status == OrderStatus.FILLED]
        total_trades = len(filled_orders)
        
        winning_trades = 0
        losing_trades = 0
        
        for order in filled_orders:
            position = self.get_position(order.symbol)
            if position.realized_pnl > 0:
                winning_trades += 1
            elif position.realized_pnl < 0:
                losing_trades += 1
        
        win_rate = winning_trades / max(total_trades, 1)
        max_drawdown = self.calculate_max_drawdown()
        
        # 计算夏普比率
        if len(self.equity_curve) > 1:
            returns = []
            for i in range(1, len(self.equity_curve)):
                prev_equity = self.equity_curve[i-1][1]
                curr_equity = self.equity_curve[i][1]
                if prev_equity > 0:
                    returns.append((curr_equity - prev_equity) / prev_equity)
            
            if returns:
                avg_return = sum(returns) / len(returns)
                return_std = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
                sharpe_ratio = avg_return / max(return_std, 1e-6) * (252 ** 0.5)
            else:
                sharpe_ratio = 0.0
        else:
            sharpe_ratio = 0.0
        
        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            initial_capital=self.initial_capital,
            final_capital=final_capital,
            total_return=total_return,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            positions=list(self.positions.values()),
            orders=filled_orders,
            equity_curve=self.equity_curve
        )
    
    def _generate_detailed_stats(self, 
                               signals: List[BacktestSignal],
                               features_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        生成详细统计信息
        
        Args:
            signals: 信号列表
            features_data: 特征数据
            
        Returns:
            详细统计信息
        """
        stats = {
            "signal_stats": self._analyze_signals(signals),
            "feature_stats": self._analyze_features(features_data),
            "performance_stats": self._analyze_performance(),
            "risk_stats": self._analyze_risk()
        }
        
        return stats
    
    def _analyze_signals(self, signals: List[BacktestSignal]) -> Dict[str, Any]:
        """分析信号统计"""
        total_signals = len(signals)
        buy_signals = sum(1 for s in signals if s.signal == SignalType.BUY)
        sell_signals = sum(1 for s in signals if s.signal == SignalType.SELL)
        hold_signals = total_signals - buy_signals - sell_signals
        
        confidence_scores = [s.confidence for s in signals if s.confidence > 0]
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        return {
            "total_signals": total_signals,
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "hold_signals": hold_signals,
            "buy_ratio": buy_signals / total_signals if total_signals > 0 else 0,
            "sell_ratio": sell_signals / total_signals if total_signals > 0 else 0,
            "average_confidence": avg_confidence
        }
    
    def _analyze_features(self, features_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """分析特征统计"""
        total_records = sum(len(df) for df in features_data.values())
        trading_pairs = list(features_data.keys())
        
        return {
            "total_records": total_records,
            "trading_pairs": trading_pairs,
            "pairs_count": len(trading_pairs)
        }
    
    def _analyze_performance(self) -> Dict[str, Any]:
        """分析性能统计"""
        if not self.equity_curve:
            return {}
        
        returns = []
        for i in range(1, len(self.equity_curve)):
            prev_equity = self.equity_curve[i-1][1]
            curr_equity = self.equity_curve[i][1]
            if prev_equity > 0:
                returns.append((curr_equity - prev_equity) / prev_equity)
        
        if not returns:
            return {}
        
        positive_returns = [r for r in returns if r > 0]
        negative_returns = [r for r in returns if r < 0]
        
        return {
            "total_periods": len(returns),
            "positive_periods": len(positive_returns),
            "negative_periods": len(negative_returns),
            "avg_positive_return": sum(positive_returns) / len(positive_returns) if positive_returns else 0,
            "avg_negative_return": sum(negative_returns) / len(negative_returns) if negative_returns else 0,
            "volatility": (sum((r - sum(returns)/len(returns)) ** 2 for r in returns) / len(returns)) ** 0.5
        }
    
    def _analyze_risk(self) -> Dict[str, Any]:
        """分析风险统计"""
        total_realized_pnl = sum(pos.realized_pnl for pos in self.positions.values())
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        
        return {
            "total_realized_pnl": total_realized_pnl,
            "total_unrealized_pnl": total_unrealized_pnl,
            "max_drawdown": self.calculate_max_drawdown(),
            "current_positions": len([pos for pos in self.positions.values() if pos.quantity != 0])
        }


class MockRuleGoAdapter:
    """模拟RuleGo适配器，用于测试"""
    
    def __init__(self, feast_client: FeastOfflineClient):
        self.feast_client = feast_client
    
    def run_offline_backtest(self,
                           trading_pairs: List[str],
                           start_date: datetime,
                           end_date: datetime) -> Tuple[List[BacktestSignal], Dict[str, pd.DataFrame]]:
        """运行模拟回测"""
        # 获取特征数据
        features_data = self.feast_client.get_features_for_backtest(
            trading_pairs=trading_pairs,
            start_date=start_date,
            end_date=end_date
        )
        
        # 生成模拟信号
        signals = []
        for trading_pair, df in features_data.items():
            for _, row in df.iterrows():
                # 简单的模拟策略：基于RSI
                rsi = row.get('technical_indicators__rsi_14', 50)
                price = row.get('technical_indicators__price', 0)
                
                if rsi < 30:
                    signal_type = SignalType.BUY
                    confidence = 0.8
                elif rsi > 70:
                    signal_type = SignalType.SELL
                    confidence = 0.8
                else:
                    signal_type = SignalType.HOLD
                    confidence = 0.3
                
                signal = BacktestSignal(
                    timestamp=pd.to_datetime(row['event_timestamp']),
                    trading_pair=trading_pair,
                    signal=signal_type,
                    price=price,
                    confidence=confidence,
                    features=row.to_dict()
                )
                signals.append(signal)
        
        return signals, features_data


def main():
    """测试增强版回测引擎"""
    print("🧪 开始增强版回测测试...")
    
    # 配置
    config = EnhancedBacktestConfig(
        initial_capital=100000,
        use_mock_rulego=True,  # 使用模拟模式进行测试
        min_confidence=0.5
    )
    
    # 创建引擎
    try:
        engine = EnhancedBacktestEngine(config)
        
        # 运行回测
        trading_pairs = ["BTCUSDT", "ETHUSDT"]
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)
        
        print(f"🚀 开始回测: {trading_pairs}, {start_date.date()} - {end_date.date()}")
        
        result, stats = engine.run_enhanced_backtest(
            trading_pairs=trading_pairs,
            start_date=start_date,
            end_date=end_date
        )
        
        # 输出结果
        print(f"\n📈 增强版回测结果:")
        print(f"   期间: {result.start_date.date()} - {result.end_date.date()}")
        print(f"   初始资金: ${result.initial_capital:,.2f}")
        print(f"   最终资金: ${result.final_capital:,.2f}")
        print(f"   总收益率: {result.total_return:.2%}")
        print(f"   总交易数: {result.total_trades}")
        print(f"   胜率: {result.win_rate:.2%}")
        print(f"   最大回撤: {result.max_drawdown:.2%}")
        print(f"   夏普比率: {result.sharpe_ratio:.2f}")
        
        # 输出统计信息
        print(f"\n📊 信号统计:")
        signal_stats = stats["signal_stats"]
        print(f"   总信号数: {signal_stats['total_signals']}")
        print(f"   买入信号: {signal_stats['buy_signals']} ({signal_stats['buy_ratio']:.1%})")
        print(f"   卖出信号: {signal_stats['sell_signals']} ({signal_stats['sell_ratio']:.1%})")
        print(f"   平均置信度: {signal_stats['average_confidence']:.2f}")
        
        print("\n✅ 增强版回测测试完成!")
        
    except Exception as e:
        print(f"❌ 回测失败: {e}")
        logger.error(f"回测失败: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()