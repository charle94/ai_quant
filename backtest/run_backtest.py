#!/usr/bin/env python3
"""
回测运行工具
使用RuleGo和Feast进行离线回测
"""
import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.enhanced_backtest_engine import EnhancedBacktestEngine, EnhancedBacktestConfig

def setup_logging(log_level: str = "INFO"):
    """设置日志"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/workspace/logs/backtest.log', mode='a')
        ]
    )

def parse_date(date_str: str) -> datetime:
    """解析日期字符串"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(f"无效的日期格式: {date_str}, 请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")

def save_results(result, stats, output_file: str):
    """保存回测结果"""
    output_data = {
        "backtest_result": {
            "start_date": result.start_date.isoformat(),
            "end_date": result.end_date.isoformat(),
            "initial_capital": result.initial_capital,
            "final_capital": result.final_capital,
            "total_return": result.total_return,
            "total_trades": result.total_trades,
            "winning_trades": result.winning_trades,
            "losing_trades": result.losing_trades,
            "win_rate": result.win_rate,
            "max_drawdown": result.max_drawdown,
            "sharpe_ratio": result.sharpe_ratio
        },
        "detailed_stats": stats,
        "positions": [
            {
                "symbol": pos.symbol,
                "quantity": pos.quantity,
                "avg_price": pos.avg_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "realized_pnl": pos.realized_pnl
            }
            for pos in result.positions if pos.quantity != 0
        ],
        "equity_curve": [
            {"timestamp": ts.isoformat(), "equity": equity}
            for ts, equity in result.equity_curve
        ]
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"📁 回测结果已保存到: {output_file}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="RuleGo + Feast 离线回测工具")
    
    # 基本参数
    parser.add_argument("--trading-pairs", nargs="+", default=["BTCUSDT", "ETHUSDT"],
                       help="交易对列表")
    parser.add_argument("--start-date", required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="结束日期 (YYYY-MM-DD)")
    
    # 回测配置
    parser.add_argument("--initial-capital", type=float, default=100000,
                       help="初始资金")
    parser.add_argument("--commission-rate", type=float, default=0.001,
                       help="手续费率")
    parser.add_argument("--position-size", type=float, default=0.1,
                       help="单次交易资金比例")
    parser.add_argument("--min-confidence", type=float, default=0.3,
                       help="最小信号置信度")
    
    # 风控参数
    parser.add_argument("--stop-loss", type=float, default=0.05,
                       help="止损比例")
    parser.add_argument("--take-profit", type=float, default=0.1,
                       help="止盈比例")
    
    # 系统配置
    parser.add_argument("--feast-repo", default="/workspace/feast_config/feature_repo",
                       help="Feast特征仓库路径")
    parser.add_argument("--rulego-endpoint", default="http://localhost:8080",
                       help="RuleGo服务端点")
    parser.add_argument("--rules-chain-id", default="trading_strategy_chain",
                       help="规则链ID")
    parser.add_argument("--use-mock", action="store_true",
                       help="使用模拟RuleGo模式")
    
    # 输出配置
    parser.add_argument("--output", default="/workspace/backtest_results.json",
                       help="结果输出文件")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="日志级别")
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # 解析日期
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        if start_date >= end_date:
            raise ValueError("开始日期必须早于结束日期")
        
        # 创建配置
        config = EnhancedBacktestConfig(
            initial_capital=args.initial_capital,
            commission_rate=args.commission_rate,
            position_size=args.position_size,
            min_confidence=args.min_confidence,
            stop_loss_pct=args.stop_loss,
            take_profit_pct=args.take_profit,
            feast_repo_path=args.feast_repo,
            rulego_endpoint=args.rulego_endpoint,
            rules_chain_id=args.rules_chain_id,
            use_mock_rulego=args.use_mock
        )
        
        print("🚀 启动增强版回测引擎...")
        print(f"   交易对: {args.trading_pairs}")
        print(f"   时间范围: {start_date.date()} - {end_date.date()}")
        print(f"   初始资金: ${args.initial_capital:,.2f}")
        print(f"   RuleGo模式: {'模拟' if args.use_mock else '真实'}")
        
        # 创建回测引擎
        engine = EnhancedBacktestEngine(config)
        
        # 运行回测
        result, stats = engine.run_enhanced_backtest(
            trading_pairs=args.trading_pairs,
            start_date=start_date,
            end_date=end_date
        )
        
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
        
        # 输出信号统计
        signal_stats = stats["signal_stats"]
        print(f"\n📊 信号统计:")
        print(f"   总信号数: {signal_stats['total_signals']}")
        print(f"   买入信号: {signal_stats['buy_signals']} ({signal_stats['buy_ratio']:.1%})")
        print(f"   卖出信号: {signal_stats['sell_signals']} ({signal_stats['sell_ratio']:.1%})")
        print(f"   平均置信度: {signal_stats['average_confidence']:.2f}")
        
        # 输出持仓信息
        active_positions = [pos for pos in result.positions if pos.quantity != 0]
        if active_positions:
            print(f"\n💼 最终持仓:")
            for pos in active_positions:
                total_pnl = pos.realized_pnl + pos.unrealized_pnl
                print(f"   {pos.symbol}: {pos.quantity:.4f} @ ${pos.avg_price:.2f}, PnL: ${total_pnl:.2f}")
        
        # 保存结果
        save_results(result, stats, args.output)
        
        print(f"\n✅ 回测完成! 结果已保存到 {args.output}")
        
    except Exception as e:
        logger.error(f"回测失败: {e}", exc_info=True)
        print(f"❌ 回测失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()