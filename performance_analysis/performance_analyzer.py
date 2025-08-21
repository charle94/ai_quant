#!/usr/bin/env python3
"""
量化策略绩效分析模块
"""
import json
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Trade:
    """交易记录"""
    id: str
    timestamp: datetime
    symbol: str
    side: str  # BUY/SELL
    quantity: float
    price: float
    commission: float
    pnl: float = 0.0
    cumulative_pnl: float = 0.0

@dataclass
class DailyReturn:
    """日收益率"""
    date: datetime
    portfolio_value: float
    daily_return: float
    cumulative_return: float
    benchmark_return: float = 0.0
    excess_return: float = 0.0

@dataclass
class DrawdownPeriod:
    """回撤期间"""
    start_date: datetime
    end_date: datetime
    peak_value: float
    trough_value: float
    drawdown_pct: float
    recovery_date: Optional[datetime] = None
    duration_days: int = 0

@dataclass
class PerformanceMetrics:
    """绩效指标"""
    # 收益指标
    total_return: float
    annualized_return: float
    cumulative_return: float
    
    # 风险指标
    volatility: float
    max_drawdown: float
    var_95: float  # 95% VaR
    cvar_95: float  # 95% CVaR
    
    # 风险调整收益
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # 交易统计
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    
    # 其他指标
    beta: float
    alpha: float
    information_ratio: float
    tracking_error: float

class PerformanceAnalyzer:
    """绩效分析器"""
    
    def __init__(self, initial_capital: float = 100000):
        self.initial_capital = initial_capital
        self.trades: List[Trade] = []
        self.daily_returns: List[DailyReturn] = []
        self.benchmark_returns: List[float] = []
        
    def add_trade(self, trade: Trade):
        """添加交易记录"""
        self.trades.append(trade)
        
    def add_daily_return(self, daily_return: DailyReturn):
        """添加日收益率"""
        self.daily_returns.append(daily_return)
        
    def set_benchmark_returns(self, returns: List[float]):
        """设置基准收益率"""
        self.benchmark_returns = returns
        
    def calculate_performance_metrics(self) -> PerformanceMetrics:
        """计算绩效指标"""
        if not self.daily_returns:
            raise ValueError("没有日收益率数据")
        
        returns = [dr.daily_return for dr in self.daily_returns]
        portfolio_values = [dr.portfolio_value for dr in self.daily_returns]
        
        # 收益指标
        total_return = self._calculate_total_return(portfolio_values)
        annualized_return = self._calculate_annualized_return(returns)
        cumulative_return = self._calculate_cumulative_return(returns)
        
        # 风险指标
        volatility = self._calculate_volatility(returns)
        max_drawdown = self._calculate_max_drawdown(portfolio_values)
        var_95 = self._calculate_var(returns, 0.95)
        cvar_95 = self._calculate_cvar(returns, 0.95)
        
        # 风险调整收益
        sharpe_ratio = self._calculate_sharpe_ratio(returns, volatility)
        sortino_ratio = self._calculate_sortino_ratio(returns)
        calmar_ratio = self._calculate_calmar_ratio(annualized_return, max_drawdown)
        
        # 交易统计
        trade_stats = self._calculate_trade_statistics()
        
        # 基准相关指标
        beta, alpha, information_ratio, tracking_error = self._calculate_benchmark_metrics(returns)
        
        return PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            cumulative_return=cumulative_return,
            volatility=volatility,
            max_drawdown=max_drawdown,
            var_95=var_95,
            cvar_95=cvar_95,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            **trade_stats,
            beta=beta,
            alpha=alpha,
            information_ratio=information_ratio,
            tracking_error=tracking_error
        )
    
    def _calculate_total_return(self, portfolio_values: List[float]) -> float:
        """计算总收益率"""
        if not portfolio_values:
            return 0.0
        return (portfolio_values[-1] - self.initial_capital) / self.initial_capital
    
    def _calculate_annualized_return(self, returns: List[float]) -> float:
        """计算年化收益率"""
        if not returns:
            return 0.0
        
        days = len(returns)
        if days == 0:
            return 0.0
            
        cumulative_return = 1.0
        for r in returns:
            cumulative_return *= (1 + r)
        
        return (cumulative_return ** (252 / days)) - 1
    
    def _calculate_cumulative_return(self, returns: List[float]) -> float:
        """计算累计收益率"""
        cumulative_return = 1.0
        for r in returns:
            cumulative_return *= (1 + r)
        return cumulative_return - 1
    
    def _calculate_volatility(self, returns: List[float]) -> float:
        """计算波动率（年化）"""
        if len(returns) < 2:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        daily_vol = math.sqrt(variance)
        return daily_vol * math.sqrt(252)  # 年化
    
    def _calculate_max_drawdown(self, portfolio_values: List[float]) -> float:
        """计算最大回撤"""
        if not portfolio_values:
            return 0.0
        
        peak = portfolio_values[0]
        max_dd = 0.0
        
        for value in portfolio_values:
            if value > peak:
                peak = value
            else:
                drawdown = (peak - value) / peak
                max_dd = max(max_dd, drawdown)
        
        return max_dd
    
    def _calculate_var(self, returns: List[float], confidence: float) -> float:
        """计算风险价值（VaR）"""
        if not returns:
            return 0.0
        
        sorted_returns = sorted(returns)
        index = int((1 - confidence) * len(sorted_returns))
        return abs(sorted_returns[index]) if index < len(sorted_returns) else 0.0
    
    def _calculate_cvar(self, returns: List[float], confidence: float) -> float:
        """计算条件风险价值（CVaR）"""
        if not returns:
            return 0.0
        
        sorted_returns = sorted(returns)
        index = int((1 - confidence) * len(sorted_returns))
        
        if index == 0:
            return abs(sorted_returns[0])
        
        tail_returns = sorted_returns[:index]
        return abs(sum(tail_returns) / len(tail_returns)) if tail_returns else 0.0
    
    def _calculate_sharpe_ratio(self, returns: List[float], volatility: float, risk_free_rate: float = 0.02) -> float:
        """计算夏普比率"""
        if volatility == 0:
            return 0.0
        
        excess_returns = [r - risk_free_rate/252 for r in returns]  # 日化无风险利率
        avg_excess_return = sum(excess_returns) / len(excess_returns) if excess_returns else 0
        
        return (avg_excess_return * 252) / volatility  # 年化
    
    def _calculate_sortino_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """计算索提诺比率"""
        if not returns:
            return 0.0
        
        excess_returns = [r - risk_free_rate/252 for r in returns]
        negative_returns = [r for r in excess_returns if r < 0]
        
        if not negative_returns:
            return float('inf')
        
        downside_deviation = math.sqrt(sum(r ** 2 for r in negative_returns) / len(negative_returns))
        avg_excess_return = sum(excess_returns) / len(excess_returns)
        
        return (avg_excess_return * 252) / (downside_deviation * math.sqrt(252))
    
    def _calculate_calmar_ratio(self, annualized_return: float, max_drawdown: float) -> float:
        """计算卡尔玛比率"""
        if max_drawdown == 0:
            return float('inf') if annualized_return > 0 else 0.0
        return annualized_return / max_drawdown
    
    def _calculate_trade_statistics(self) -> Dict:
        """计算交易统计"""
        if not self.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'profit_factor': 0.0
            }
        
        total_trades = len(self.trades)
        winning_trades = len([t for t in self.trades if t.pnl > 0])
        losing_trades = len([t for t in self.trades if t.pnl < 0])
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
        
        wins = [t.pnl for t in self.trades if t.pnl > 0]
        losses = [abs(t.pnl) for t in self.trades if t.pnl < 0]
        
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        
        total_wins = sum(wins)
        total_losses = sum(losses)
        profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor
        }
    
    def _calculate_benchmark_metrics(self, returns: List[float]) -> Tuple[float, float, float, float]:
        """计算基准相关指标"""
        if not self.benchmark_returns or len(self.benchmark_returns) != len(returns):
            return 0.0, 0.0, 0.0, 0.0
        
        # Beta计算
        portfolio_var = self._calculate_variance(returns)
        benchmark_var = self._calculate_variance(self.benchmark_returns)
        covariance = self._calculate_covariance(returns, self.benchmark_returns)
        
        beta = covariance / benchmark_var if benchmark_var != 0 else 0.0
        
        # Alpha计算
        portfolio_return = sum(returns) / len(returns) * 252  # 年化
        benchmark_return = sum(self.benchmark_returns) / len(self.benchmark_returns) * 252  # 年化
        risk_free_rate = 0.02
        
        alpha = portfolio_return - (risk_free_rate + beta * (benchmark_return - risk_free_rate))
        
        # 跟踪误差
        tracking_errors = [returns[i] - self.benchmark_returns[i] for i in range(len(returns))]
        tracking_error = self._calculate_volatility(tracking_errors)
        
        # 信息比率
        avg_excess_return = sum(tracking_errors) / len(tracking_errors) * 252  # 年化
        information_ratio = avg_excess_return / tracking_error if tracking_error != 0 else 0.0
        
        return beta, alpha, information_ratio, tracking_error
    
    def _calculate_variance(self, returns: List[float]) -> float:
        """计算方差"""
        if len(returns) < 2:
            return 0.0
        
        mean_return = sum(returns) / len(returns)
        return sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
    
    def _calculate_covariance(self, returns1: List[float], returns2: List[float]) -> float:
        """计算协方差"""
        if len(returns1) != len(returns2) or len(returns1) < 2:
            return 0.0
        
        mean1 = sum(returns1) / len(returns1)
        mean2 = sum(returns2) / len(returns2)
        
        return sum((returns1[i] - mean1) * (returns2[i] - mean2) 
                  for i in range(len(returns1))) / (len(returns1) - 1)
    
    def calculate_drawdown_periods(self) -> List[DrawdownPeriod]:
        """计算回撤期间"""
        if not self.daily_returns:
            return []
        
        portfolio_values = [dr.portfolio_value for dr in self.daily_returns]
        dates = [dr.date for dr in self.daily_returns]
        
        drawdown_periods = []
        peak_value = portfolio_values[0]
        peak_date = dates[0]
        in_drawdown = False
        start_date = None
        trough_value = peak_value
        
        for i, (date, value) in enumerate(zip(dates, portfolio_values)):
            if value > peak_value:
                # 新高点
                if in_drawdown:
                    # 结束回撤期
                    drawdown_pct = (peak_value - trough_value) / peak_value
                    duration = (date - start_date).days
                    
                    drawdown_periods.append(DrawdownPeriod(
                        start_date=start_date,
                        end_date=date,
                        peak_value=peak_value,
                        trough_value=trough_value,
                        drawdown_pct=drawdown_pct,
                        recovery_date=date,
                        duration_days=duration
                    ))
                    
                    in_drawdown = False
                
                peak_value = value
                peak_date = date
            else:
                # 低于峰值
                if not in_drawdown:
                    # 开始回撤期
                    in_drawdown = True
                    start_date = peak_date
                    trough_value = value
                else:
                    # 继续回撤
                    if value < trough_value:
                        trough_value = value
        
        # 如果最后还在回撤中
        if in_drawdown:
            drawdown_pct = (peak_value - trough_value) / peak_value
            duration = (dates[-1] - start_date).days
            
            drawdown_periods.append(DrawdownPeriod(
                start_date=start_date,
                end_date=dates[-1],
                peak_value=peak_value,
                trough_value=trough_value,
                drawdown_pct=drawdown_pct,
                recovery_date=None,
                duration_days=duration
            ))
        
        return drawdown_periods
    
    def generate_performance_report(self) -> Dict:
        """生成绩效报告"""
        metrics = self.calculate_performance_metrics()
        drawdown_periods = self.calculate_drawdown_periods()
        
        # 按回撤大小排序
        top_drawdowns = sorted(drawdown_periods, key=lambda x: x.drawdown_pct, reverse=True)[:5]
        
        report = {
            'summary': {
                'analysis_period': {
                    'start_date': self.daily_returns[0].date.isoformat() if self.daily_returns else None,
                    'end_date': self.daily_returns[-1].date.isoformat() if self.daily_returns else None,
                    'total_days': len(self.daily_returns)
                },
                'initial_capital': self.initial_capital,
                'final_value': self.daily_returns[-1].portfolio_value if self.daily_returns else self.initial_capital,
                'total_return_pct': metrics.total_return * 100
            },
            'returns': {
                'total_return': metrics.total_return,
                'annualized_return': metrics.annualized_return,
                'cumulative_return': metrics.cumulative_return
            },
            'risk': {
                'volatility': metrics.volatility,
                'max_drawdown': metrics.max_drawdown,
                'var_95': metrics.var_95,
                'cvar_95': metrics.cvar_95
            },
            'risk_adjusted_returns': {
                'sharpe_ratio': metrics.sharpe_ratio,
                'sortino_ratio': metrics.sortino_ratio,
                'calmar_ratio': metrics.calmar_ratio
            },
            'trading': {
                'total_trades': metrics.total_trades,
                'win_rate': metrics.win_rate,
                'profit_factor': metrics.profit_factor,
                'avg_win': metrics.avg_win,
                'avg_loss': metrics.avg_loss
            },
            'benchmark_comparison': {
                'beta': metrics.beta,
                'alpha': metrics.alpha,
                'information_ratio': metrics.information_ratio,
                'tracking_error': metrics.tracking_error
            },
            'top_drawdowns': [
                {
                    'start_date': dd.start_date.isoformat(),
                    'end_date': dd.end_date.isoformat(),
                    'drawdown_pct': dd.drawdown_pct * 100,
                    'duration_days': dd.duration_days,
                    'recovered': dd.recovery_date is not None
                }
                for dd in top_drawdowns
            ]
        }
        
        return report
    
    def export_to_json(self, filename: str):
        """导出绩效报告为JSON"""
        report = self.generate_performance_report()
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"绩效报告已导出到: {filename}")

def create_sample_performance_data():
    """创建示例绩效数据"""
    import random
    
    analyzer = PerformanceAnalyzer(initial_capital=100000)
    
    # 生成60天的模拟数据
    base_date = datetime(2024, 1, 1)
    portfolio_value = 100000
    
    for i in range(60):
        date = base_date + timedelta(days=i)
        
        # 模拟日收益率（带趋势和波动）
        trend = 0.0005  # 轻微上升趋势
        volatility = 0.02
        daily_return = random.gauss(trend, volatility)
        
        # 更新组合价值
        portfolio_value *= (1 + daily_return)
        
        # 计算累计收益率
        cumulative_return = (portfolio_value - 100000) / 100000
        
        analyzer.add_daily_return(DailyReturn(
            date=date,
            portfolio_value=portfolio_value,
            daily_return=daily_return,
            cumulative_return=cumulative_return,
            benchmark_return=random.gauss(0.0003, 0.015),  # 基准收益率
            excess_return=daily_return - random.gauss(0.0003, 0.015)
        ))
    
    # 生成一些交易记录
    for i in range(20):
        trade_date = base_date + timedelta(days=random.randint(0, 59))
        pnl = random.gauss(500, 1000)  # 平均盈利500，标准差1000
        
        analyzer.add_trade(Trade(
            id=f"trade_{i+1}",
            timestamp=trade_date,
            symbol='BTCUSDT',
            side=random.choice(['BUY', 'SELL']),
            quantity=random.uniform(0.1, 1.0),
            price=random.uniform(40000, 50000),
            commission=random.uniform(10, 50),
            pnl=pnl,
            cumulative_pnl=sum(t.pnl for t in analyzer.trades) + pnl
        ))
    
    # 设置基准收益率
    benchmark_returns = [dr.benchmark_return for dr in analyzer.daily_returns]
    analyzer.set_benchmark_returns(benchmark_returns)
    
    return analyzer

def main():
    """测试绩效分析器"""
    print("🧪 开始绩效分析模块测试...")
    
    # 创建示例数据
    analyzer = create_sample_performance_data()
    print(f"📊 创建了 {len(analyzer.daily_returns)} 天的绩效数据")
    print(f"📊 创建了 {len(analyzer.trades)} 笔交易记录")
    
    # 计算绩效指标
    print("📈 计算绩效指标...")
    metrics = analyzer.calculate_performance_metrics()
    
    print(f"\n📊 绩效指标:")
    print(f"   总收益率: {metrics.total_return:.2%}")
    print(f"   年化收益率: {metrics.annualized_return:.2%}")
    print(f"   年化波动率: {metrics.volatility:.2%}")
    print(f"   最大回撤: {metrics.max_drawdown:.2%}")
    print(f"   夏普比率: {metrics.sharpe_ratio:.2f}")
    print(f"   索提诺比率: {metrics.sortino_ratio:.2f}")
    print(f"   卡尔玛比率: {metrics.calmar_ratio:.2f}")
    print(f"   胜率: {metrics.win_rate:.2%}")
    print(f"   盈亏比: {metrics.profit_factor:.2f}")
    
    # 计算回撤期间
    print("\n📉 回撤分析...")
    drawdowns = analyzer.calculate_drawdown_periods()
    print(f"   回撤期间数: {len(drawdowns)}")
    
    if drawdowns:
        max_dd = max(drawdowns, key=lambda x: x.drawdown_pct)
        print(f"   最大回撤期间: {max_dd.start_date.date()} - {max_dd.end_date.date()}")
        print(f"   最大回撤幅度: {max_dd.drawdown_pct:.2%}")
        print(f"   持续天数: {max_dd.duration_days} 天")
    
    # 生成完整报告
    print("\n📋 生成绩效报告...")
    report = analyzer.generate_performance_report()
    
    # 导出报告
    output_file = "/workspace/performance_analysis/sample_performance_report.json"
    analyzer.export_to_json(output_file)
    
    print(f"✅ 绩效分析测试完成!")
    print(f"📄 报告已保存到: {output_file}")
    
    return True

if __name__ == "__main__":
    main()