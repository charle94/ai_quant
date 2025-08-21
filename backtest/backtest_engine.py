#!/usr/bin/env python3
"""
策略回测引擎
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SignalType(Enum):
    """信号类型"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class OrderStatus(Enum):
    """订单状态"""
    PENDING = "PENDING"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"

@dataclass
class MarketData:
    """市场数据"""
    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass
class TechnicalFeatures:
    """技术指标特征"""
    timestamp: datetime
    symbol: str
    price: float
    ma_5: float
    ma_10: float
    ma_20: float
    rsi_14: float
    bb_upper: float
    bb_lower: float
    volume_ratio: float
    momentum_5d: float
    volatility: float

@dataclass
class TradingSignal:
    """交易信号"""
    timestamp: datetime
    symbol: str
    signal: SignalType
    price: float
    confidence: float
    features: Dict

@dataclass
class Order:
    """订单"""
    id: str
    timestamp: datetime
    symbol: str
    side: str  # BUY or SELL
    quantity: float
    price: float
    status: OrderStatus
    filled_price: Optional[float] = None
    filled_quantity: Optional[float] = None
    commission: float = 0.0

@dataclass
class Position:
    """持仓"""
    symbol: str
    quantity: float
    avg_price: float
    unrealized_pnl: float
    realized_pnl: float

@dataclass
class BacktestResult:
    """回测结果"""
    start_date: datetime
    end_date: datetime
    initial_capital: float
    final_capital: float
    total_return: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float
    positions: List[Position]
    orders: List[Order]
    equity_curve: List[Tuple[datetime, float]]

class SimpleStrategy:
    """简单交易策略"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'ma_trend_threshold': 0.01,
            'position_size': 0.1,  # 10% of capital
            'stop_loss': 0.02,     # 2% stop loss
            'take_profit': 0.05    # 5% take profit
        }
    
    def generate_signal(self, features: TechnicalFeatures, current_position: Optional[Position] = None) -> TradingSignal:
        """生成交易信号"""
        signal = SignalType.HOLD
        confidence = 0.0
        
        # 趋势判断
        trend_up = features.ma_5 > features.ma_10 and features.ma_10 > features.ma_20
        trend_down = features.ma_5 < features.ma_10 and features.ma_10 < features.ma_20
        
        # RSI判断
        rsi_oversold = features.rsi_14 < self.config['rsi_oversold']
        rsi_overbought = features.rsi_14 > self.config['rsi_overbought']
        
        # 动量判断
        momentum_positive = features.momentum_5d > 0.01
        momentum_negative = features.momentum_5d < -0.01
        
        # 成交量确认
        high_volume = features.volume_ratio > 1.5
        
        # 买入信号
        if trend_up and rsi_oversold and momentum_positive:
            signal = SignalType.BUY
            confidence = 0.8
            if high_volume:
                confidence = 0.9
        
        # 卖出信号
        elif trend_down and rsi_overbought and momentum_negative:
            signal = SignalType.SELL
            confidence = 0.8
            if high_volume:
                confidence = 0.9
        
        # 止损/止盈检查
        if current_position and current_position.quantity != 0:
            current_return = (features.price - current_position.avg_price) / current_position.avg_price
            
            if current_position.quantity > 0:  # 多头持仓
                if current_return <= -self.config['stop_loss']:
                    signal = SignalType.SELL
                    confidence = 1.0  # 止损确定性高
                elif current_return >= self.config['take_profit']:
                    signal = SignalType.SELL
                    confidence = 0.9  # 止盈
            
            elif current_position.quantity < 0:  # 空头持仓
                if current_return >= self.config['stop_loss']:
                    signal = SignalType.BUY
                    confidence = 1.0  # 止损
                elif current_return <= -self.config['take_profit']:
                    signal = SignalType.BUY
                    confidence = 0.9  # 止盈
        
        return TradingSignal(
            timestamp=features.timestamp,
            symbol=features.symbol,
            signal=signal,
            price=features.price,
            confidence=confidence,
            features=asdict(features)
        )

class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital: float = 100000, commission_rate: float = 0.001):
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.orders: List[Order] = []
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.order_id = 0
    
    def reset(self):
        """重置回测状态"""
        self.capital = self.initial_capital
        self.positions.clear()
        self.orders.clear()
        self.equity_curve.clear()
        self.order_id = 0
    
    def get_position(self, symbol: str) -> Position:
        """获取持仓"""
        if symbol not in self.positions:
            self.positions[symbol] = Position(
                symbol=symbol,
                quantity=0,
                avg_price=0,
                unrealized_pnl=0,
                realized_pnl=0
            )
        return self.positions[symbol]
    
    def place_order(self, signal: TradingSignal, strategy_config: Dict) -> Optional[Order]:
        """下单"""
        position = self.get_position(signal.symbol)
        
        # 计算订单数量
        if signal.signal == SignalType.BUY:
            # 买入：使用固定比例资金
            order_value = self.capital * strategy_config.get('position_size', 0.1)
            quantity = order_value / signal.price
            side = "BUY"
            
        elif signal.signal == SignalType.SELL:
            # 卖出：平仓或开空仓
            if position.quantity > 0:
                quantity = position.quantity  # 平多仓
            else:
                # 开空仓
                order_value = self.capital * strategy_config.get('position_size', 0.1)
                quantity = order_value / signal.price
            side = "SELL"
            
        else:
            return None  # HOLD信号不下单
        
        # 创建订单
        self.order_id += 1
        order = Order(
            id=f"order_{self.order_id}",
            timestamp=signal.timestamp,
            symbol=signal.symbol,
            side=side,
            quantity=quantity,
            price=signal.price,
            status=OrderStatus.PENDING
        )
        
        return order
    
    def execute_order(self, order: Order, market_data: MarketData) -> bool:
        """执行订单"""
        # 简化执行：假设市价成交
        execution_price = market_data.close
        commission = order.quantity * execution_price * self.commission_rate
        
        order.status = OrderStatus.FILLED
        order.filled_price = execution_price
        order.filled_quantity = order.quantity
        order.commission = commission
        
        # 更新持仓
        position = self.get_position(order.symbol)
        
        if order.side == "BUY":
            if position.quantity < 0:
                # 平空仓
                if abs(position.quantity) >= order.quantity:
                    # 部分或全部平仓
                    realized_pnl = order.quantity * (position.avg_price - execution_price)
                    position.realized_pnl += realized_pnl
                    position.quantity += order.quantity
                    self.capital += realized_pnl - commission
                else:
                    # 平仓后反向开仓
                    realized_pnl = abs(position.quantity) * (position.avg_price - execution_price)
                    position.realized_pnl += realized_pnl
                    remaining_qty = order.quantity - abs(position.quantity)
                    position.quantity = remaining_qty
                    position.avg_price = execution_price
                    self.capital += realized_pnl - commission
            else:
                # 开多仓或加仓
                total_cost = position.quantity * position.avg_price + order.quantity * execution_price
                position.quantity += order.quantity
                position.avg_price = total_cost / position.quantity if position.quantity != 0 else execution_price
                self.capital -= order.quantity * execution_price + commission
        
        else:  # SELL
            if position.quantity > 0:
                # 平多仓
                if position.quantity >= order.quantity:
                    # 部分或全部平仓
                    realized_pnl = order.quantity * (execution_price - position.avg_price)
                    position.realized_pnl += realized_pnl
                    position.quantity -= order.quantity
                    self.capital += realized_pnl + order.quantity * execution_price - commission
                else:
                    # 平仓后反向开仓
                    realized_pnl = position.quantity * (execution_price - position.avg_price)
                    position.realized_pnl += realized_pnl
                    remaining_qty = order.quantity - position.quantity
                    position.quantity = -remaining_qty
                    position.avg_price = execution_price
                    self.capital += realized_pnl + position.quantity * execution_price - commission
            else:
                # 开空仓或加仓
                total_cost = abs(position.quantity) * position.avg_price + order.quantity * execution_price
                position.quantity -= order.quantity
                position.avg_price = total_cost / abs(position.quantity) if position.quantity != 0 else execution_price
                self.capital += order.quantity * execution_price - commission
        
        self.orders.append(order)
        return True
    
    def update_unrealized_pnl(self, market_data: MarketData):
        """更新未实现盈亏"""
        if market_data.symbol in self.positions:
            position = self.positions[market_data.symbol]
            if position.quantity != 0:
                if position.quantity > 0:
                    position.unrealized_pnl = position.quantity * (market_data.close - position.avg_price)
                else:
                    position.unrealized_pnl = abs(position.quantity) * (position.avg_price - market_data.close)
    
    def get_total_equity(self) -> float:
        """获取总权益"""
        total_unrealized = sum(pos.unrealized_pnl for pos in self.positions.values())
        return self.capital + total_unrealized
    
    def run_backtest(self, market_data_list: List[MarketData], features_list: List[TechnicalFeatures], 
                     strategy: SimpleStrategy) -> BacktestResult:
        """运行回测"""
        self.reset()
        
        start_date = market_data_list[0].timestamp if market_data_list else datetime.now()
        end_date = market_data_list[-1].timestamp if market_data_list else datetime.now()
        
        # 创建特征字典以便快速查找
        features_dict = {(f.timestamp, f.symbol): f for f in features_list}
        
        for market_data in market_data_list:
            # 获取对应的特征数据
            key = (market_data.timestamp, market_data.symbol)
            if key not in features_dict:
                continue
            
            features = features_dict[key]
            current_position = self.get_position(market_data.symbol)
            
            # 生成交易信号
            signal = strategy.generate_signal(features, current_position)
            
            # 如果有交易信号，下单并执行
            if signal.signal != SignalType.HOLD:
                order = self.place_order(signal, strategy.config)
                if order:
                    self.execute_order(order, market_data)
            
            # 更新未实现盈亏
            self.update_unrealized_pnl(market_data)
            
            # 记录权益曲线
            total_equity = self.get_total_equity()
            self.equity_curve.append((market_data.timestamp, total_equity))
        
        # 计算回测结果
        return self.calculate_results(start_date, end_date)
    
    def calculate_results(self, start_date: datetime, end_date: datetime) -> BacktestResult:
        """计算回测结果"""
        final_capital = self.get_total_equity()
        total_return = (final_capital - self.initial_capital) / self.initial_capital
        
        # 统计交易
        filled_orders = [o for o in self.orders if o.status == OrderStatus.FILLED]
        total_trades = len(filled_orders)
        
        # 计算盈亏交易
        winning_trades = 0
        losing_trades = 0
        
        for pos in self.positions.values():
            total_pnl = pos.realized_pnl + pos.unrealized_pnl
            if total_pnl > 0:
                winning_trades += 1
            elif total_pnl < 0:
                losing_trades += 1
        
        win_rate = winning_trades / max(winning_trades + losing_trades, 1)
        
        # 计算最大回撤
        max_drawdown = self.calculate_max_drawdown()
        
        # 计算夏普比率（简化版本）
        if len(self.equity_curve) > 1:
            returns = [(self.equity_curve[i][1] - self.equity_curve[i-1][1]) / self.equity_curve[i-1][1] 
                      for i in range(1, len(self.equity_curve))]
            if returns:
                avg_return = sum(returns) / len(returns)
                return_std = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
                sharpe_ratio = avg_return / max(return_std, 1e-6) * (252 ** 0.5)  # 年化
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
    
    def calculate_max_drawdown(self) -> float:
        """计算最大回撤"""
        if len(self.equity_curve) < 2:
            return 0.0
        
        peak = self.equity_curve[0][1]
        max_dd = 0.0
        
        for _, equity in self.equity_curve:
            if equity > peak:
                peak = equity
            else:
                drawdown = (peak - equity) / peak
                max_dd = max(max_dd, drawdown)
        
        return max_dd

def create_sample_data():
    """创建示例数据用于测试"""
    import random
    
    # 生成30天的市场数据
    market_data = []
    features_data = []
    
    base_date = datetime(2024, 1, 1)
    base_price = 45000
    
    for i in range(30):
        timestamp = base_date + timedelta(days=i)
        
        # 模拟价格变动
        price_change = random.gauss(0, 0.02) * base_price
        base_price = max(base_price + price_change, base_price * 0.9)
        
        open_price = base_price
        high_price = base_price * (1 + abs(random.gauss(0, 0.01)))
        low_price = base_price * (1 - abs(random.gauss(0, 0.01)))
        close_price = base_price + random.gauss(0, 0.005) * base_price
        volume = int(random.expovariate(1/1000000))
        
        market_data.append(MarketData(
            timestamp=timestamp,
            symbol='BTCUSDT',
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume
        ))
        
        # 模拟技术指标
        ma_5 = close_price * random.uniform(0.98, 1.02)
        ma_10 = close_price * random.uniform(0.97, 1.03)
        ma_20 = close_price * random.uniform(0.95, 1.05)
        rsi_14 = random.uniform(20, 80)
        bb_upper = close_price * 1.02
        bb_lower = close_price * 0.98
        volume_ratio = random.uniform(0.5, 2.0)
        momentum_5d = random.gauss(0, 0.02)
        volatility = random.uniform(0.01, 0.05)
        
        features_data.append(TechnicalFeatures(
            timestamp=timestamp,
            symbol='BTCUSDT',
            price=close_price,
            ma_5=ma_5,
            ma_10=ma_10,
            ma_20=ma_20,
            rsi_14=rsi_14,
            bb_upper=bb_upper,
            bb_lower=bb_lower,
            volume_ratio=volume_ratio,
            momentum_5d=momentum_5d,
            volatility=volatility
        ))
    
    return market_data, features_data

def main():
    """测试回测引擎"""
    print("🧪 开始策略回测测试...")
    
    # 创建示例数据
    market_data, features_data = create_sample_data()
    print(f"📊 创建了 {len(market_data)} 天的市场数据")
    
    # 创建策略和回测引擎
    strategy = SimpleStrategy()
    engine = BacktestEngine(initial_capital=100000)
    
    # 运行回测
    print("🚀 开始回测...")
    result = engine.run_backtest(market_data, features_data, strategy)
    
    # 输出结果
    print(f"\n📈 回测结果:")
    print(f"   期间: {result.start_date.date()} - {result.end_date.date()}")
    print(f"   初始资金: ${result.initial_capital:,.2f}")
    print(f"   最终资金: ${result.final_capital:,.2f}")
    print(f"   总收益率: {result.total_return:.2%}")
    print(f"   总交易数: {result.total_trades}")
    print(f"   胜率: {result.win_rate:.2%}")
    print(f"   最大回撤: {result.max_drawdown:.2%}")
    print(f"   夏普比率: {result.sharpe_ratio:.2f}")
    
    # 输出持仓信息
    if result.positions:
        print(f"\n💼 最终持仓:")
        for pos in result.positions:
            if pos.quantity != 0:
                total_pnl = pos.realized_pnl + pos.unrealized_pnl
                print(f"   {pos.symbol}: {pos.quantity:.4f} @ ${pos.avg_price:.2f}, PnL: ${total_pnl:.2f}")
    
    print("\n✅ 策略回测测试完成!")
    return result

if __name__ == "__main__":
    main()