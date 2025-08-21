#!/usr/bin/env python3
"""
回测系统演示脚本
展示RuleGo + Feast集成的回测功能（简化版本）
"""
import sys
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, List, Optional, Any

# 添加项目路径
sys.path.insert(0, '/workspace')

from backtest.backtest_engine import (
    BacktestEngine, BacktestResult, SignalType, OrderStatus,
    MarketData, TechnicalFeatures, TradingSignal
)

@dataclass 
class DemoConfig:
    """演示配置"""
    initial_capital: float = 100000.0
    commission_rate: float = 0.001
    position_size: float = 0.1
    min_confidence: float = 0.3
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.1

class DemoRuleGoAdapter:
    """演示用的RuleGo适配器"""
    
    def __init__(self, config: DemoConfig):
        self.config = config
    
    def generate_demo_signals(self, 
                            trading_pairs: List[str],
                            start_date: datetime,
                            end_date: datetime) -> List[TradingSignal]:
        """生成演示信号"""
        signals = []
        
        # 生成7天的演示数据
        current_date = start_date
        base_price = 45000.0
        
        while current_date <= end_date:
            for trading_pair in trading_pairs:
                # 模拟价格波动
                import random
                price_change = random.gauss(0, 0.02) * base_price
                current_price = max(base_price + price_change, base_price * 0.9)
                
                # 模拟技术指标
                rsi = random.uniform(20, 80)
                ma_5 = current_price * random.uniform(0.98, 1.02)
                ma_10 = current_price * random.uniform(0.97, 1.03)
                
                # 简单策略：基于RSI
                if rsi < 30:
                    signal_type = SignalType.BUY
                    confidence = 0.8
                elif rsi > 70:
                    signal_type = SignalType.SELL
                    confidence = 0.8
                else:
                    signal_type = SignalType.HOLD
                    confidence = 0.3
                
                # 创建信号
                signal = TradingSignal(
                    timestamp=current_date,
                    symbol=trading_pair,
                    signal=signal_type,
                    price=current_price,
                    confidence=confidence,
                    features={
                        'rsi_14': rsi,
                        'ma_5': ma_5,
                        'ma_10': ma_10,
                        'price': current_price
                    }
                )
                
                signals.append(signal)
                base_price = current_price  # 更新基准价格
            
            current_date += timedelta(hours=4)  # 4小时间隔
        
        return signals

class DemoBacktestEngine(BacktestEngine):
    """演示用的回测引擎"""
    
    def __init__(self, config: DemoConfig):
        super().__init__(
            initial_capital=config.initial_capital,
            commission_rate=config.commission_rate
        )
        self.config = config
        self.adapter = DemoRuleGoAdapter(config)
    
    def run_demo_backtest(self,
                         trading_pairs: List[str],
                         start_date: datetime,
                         end_date: datetime) -> Dict[str, Any]:
        """运行演示回测"""
        print(f"🚀 开始演示回测...")
        print(f"   交易对: {trading_pairs}")
        print(f"   时间范围: {start_date.date()} - {end_date.date()}")
        print(f"   初始资金: ${self.config.initial_capital:,.2f}")
        
        # 重置引擎
        self.reset()
        
        # 生成信号
        signals = self.adapter.generate_demo_signals(trading_pairs, start_date, end_date)
        print(f"   生成信号: {len(signals)} 个")
        
        # 处理信号
        processed_signals = 0
        buy_signals = 0
        sell_signals = 0
        hold_signals = 0
        
        for signal in signals:
            # 过滤低置信度信号
            if signal.confidence < self.config.min_confidence:
                signal.signal = SignalType.HOLD
            
            # 统计信号
            if signal.signal == SignalType.BUY:
                buy_signals += 1
            elif signal.signal == SignalType.SELL:
                sell_signals += 1
            else:
                hold_signals += 1
            
            # 创建市场数据
            market_data = MarketData(
                timestamp=signal.timestamp,
                symbol=signal.symbol,
                open=signal.price,
                high=signal.price * 1.001,
                low=signal.price * 0.999,
                close=signal.price,
                volume=1000000
            )
            
            # 处理交易
            if signal.signal != SignalType.HOLD:
                self._process_signal(signal, market_data)
            
            # 更新未实现盈亏
            self.update_unrealized_pnl(market_data)
            
            # 记录权益曲线
            equity = self.get_total_equity()
            self.equity_curve.append((signal.timestamp, equity))
            
            processed_signals += 1
        
        # 计算结果
        final_equity = self.get_total_equity()
        total_return = (final_equity - self.initial_capital) / self.initial_capital
        
        # 统计交易
        filled_orders = [o for o in self.orders if o.status == OrderStatus.FILLED]
        total_trades = len(filled_orders)
        
        # 计算胜率
        winning_trades = 0
        losing_trades = 0
        for order in filled_orders:
            position = self.get_position(order.symbol)
            if position.realized_pnl > 0:
                winning_trades += 1
            elif position.realized_pnl < 0:
                losing_trades += 1
        
        win_rate = winning_trades / max(total_trades, 1)
        
        # 计算最大回撤
        max_drawdown = self.calculate_max_drawdown()
        
        return {
            'backtest_result': {
                'start_date': start_date,
                'end_date': end_date,
                'initial_capital': self.initial_capital,
                'final_capital': final_equity,
                'total_return': total_return,
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'max_drawdown': max_drawdown
            },
            'signal_stats': {
                'total_signals': len(signals),
                'processed_signals': processed_signals,
                'buy_signals': buy_signals,
                'sell_signals': sell_signals,
                'hold_signals': hold_signals,
                'buy_ratio': buy_signals / len(signals) if signals else 0,
                'sell_ratio': sell_signals / len(signals) if signals else 0
            },
            'positions': [
                {
                    'symbol': pos.symbol,
                    'quantity': pos.quantity,
                    'avg_price': pos.avg_price,
                    'unrealized_pnl': pos.unrealized_pnl,
                    'realized_pnl': pos.realized_pnl
                }
                for pos in self.positions.values() if pos.quantity != 0
            ]
        }
    
    def _process_signal(self, signal: TradingSignal, market_data: MarketData):
        """处理信号并执行交易"""
        position = self.get_position(signal.symbol)
        
        # 计算订单数量
        if signal.signal == SignalType.BUY:
            if position.quantity <= 0:  # 开多仓或平空仓
                order_value = self.capital * self.config.position_size
                quantity = order_value / market_data.close
            else:
                return  # 已有多仓
                
        elif signal.signal == SignalType.SELL:
            if position.quantity >= 0:  # 开空仓或平多仓
                if position.quantity > 0:
                    quantity = position.quantity  # 平多仓
                else:
                    order_value = self.capital * self.config.position_size
                    quantity = order_value / market_data.close
            else:
                return  # 已有空仓
        else:
            return
        
        # 创建并执行订单
        from backtest.backtest_engine import Order
        self.order_id += 1
        order = Order(
            id=f"order_{self.order_id}",
            timestamp=signal.timestamp,
            symbol=signal.symbol,
            side=signal.signal.value,
            quantity=quantity,
            price=market_data.close,
            status=OrderStatus.PENDING
        )
        
        self.execute_order(order, market_data)

def main():
    """主演示函数"""
    print("🎯 RuleGo + Feast 回测系统演示")
    print("=" * 60)
    
    # 创建配置
    config = DemoConfig(
        initial_capital=100000,
        commission_rate=0.001,
        position_size=0.1,
        min_confidence=0.5
    )
    
    # 创建回测引擎
    engine = DemoBacktestEngine(config)
    
    # 设置回测参数
    trading_pairs = ["BTCUSDT", "ETHUSDT"]
    start_date = datetime.now() - timedelta(days=7)
    end_date = datetime.now() - timedelta(days=1)
    
    try:
        # 运行回测
        results = engine.run_demo_backtest(trading_pairs, start_date, end_date)
        
        # 输出结果
        result = results['backtest_result']
        signal_stats = results['signal_stats']
        
        print(f"\n📈 回测结果:")
        print(f"   期间: {result['start_date'].date()} - {result['end_date'].date()}")
        print(f"   初始资金: ${result['initial_capital']:,.2f}")
        print(f"   最终资金: ${result['final_capital']:,.2f}")
        print(f"   总收益率: {result['total_return']:.2%}")
        print(f"   总交易数: {result['total_trades']}")
        print(f"   胜率: {result['win_rate']:.2%}")
        print(f"   最大回撤: {result['max_drawdown']:.2%}")
        
        print(f"\n📊 信号统计:")
        print(f"   总信号数: {signal_stats['total_signals']}")
        print(f"   买入信号: {signal_stats['buy_signals']} ({signal_stats['buy_ratio']:.1%})")
        print(f"   卖出信号: {signal_stats['sell_signals']} ({signal_stats['sell_ratio']:.1%})")
        print(f"   持有信号: {signal_stats['hold_signals']}")
        
        # 输出持仓
        positions = results['positions']
        if positions:
            print(f"\n💼 最终持仓:")
            for pos in positions:
                total_pnl = pos['realized_pnl'] + pos['unrealized_pnl']
                print(f"   {pos['symbol']}: {pos['quantity']:.4f} @ ${pos['avg_price']:.2f}, PnL: ${total_pnl:.2f}")
        
        print(f"\n✅ 演示完成！")
        print(f"\n📋 系统特性:")
        print(f"   ✓ RuleGo决策引擎集成 (模拟模式)")
        print(f"   ✓ Feast特征存储支持")
        print(f"   ✓ 离线历史数据回测")
        print(f"   ✓ 风险管理和仓位控制")
        print(f"   ✓ 详细统计和分析")
        
        # 保存结果
        output_file = '/workspace/demo_backtest_results.json'
        with open(output_file, 'w') as f:
            # 转换datetime对象为字符串
            results_copy = results.copy()
            results_copy['backtest_result']['start_date'] = result['start_date'].isoformat()
            results_copy['backtest_result']['end_date'] = result['end_date'].isoformat()
            json.dump(results_copy, f, indent=2)
        
        print(f"\n📁 结果已保存到: {output_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)