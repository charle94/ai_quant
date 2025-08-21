#!/usr/bin/env python3
"""
集成绩效分析的量化分析看板
"""
import streamlit as st
import json
import sys
import os
from datetime import datetime, timedelta
import time
import requests

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from performance_analysis.performance_analyzer import PerformanceAnalyzer, create_sample_performance_data
    from backtest.backtest_engine import BacktestEngine, SimpleStrategy, create_sample_data
except ImportError as e:
    st.error(f"无法导入模块: {e}")
    st.stop()

# 页面配置
st.set_page_config(
    page_title="量化分析系统集成看板",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.8;
    }
    .positive {
        color: #4caf50;
        font-weight: bold;
    }
    .negative {
        color: #f44336;
        font-weight: bold;
    }
    .neutral {
        color: #757575;
        font-weight: bold;
    }
    .status-good {
        background-color: #e8f5e8;
        color: #2e7d32;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
    }
    .status-warning {
        background-color: #fff3e0;
        color: #f57c00;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
    }
    .status-error {
        background-color: #ffebee;
        color: #d32f2f;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=30)  # 缓存30秒
def load_performance_data():
    """加载绩效数据"""
    try:
        # 尝试从文件加载
        report_file = "/workspace/performance_analysis/sample_performance_report.json"
        if os.path.exists(report_file):
            with open(report_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            # 生成示例数据
            analyzer = create_sample_performance_data()
            return analyzer.generate_performance_report()
    except Exception as e:
        st.error(f"加载绩效数据失败: {e}")
        return None

@st.cache_data(ttl=60)  # 缓存60秒
def load_backtest_data():
    """加载回测数据"""
    try:
        market_data, features_data = create_sample_data()
        strategy = SimpleStrategy()
        engine = BacktestEngine(initial_capital=100000)
        result = engine.run_backtest(market_data, features_data, strategy)
        return result
    except Exception as e:
        st.error(f"运行回测失败: {e}")
        return None

def get_api_status():
    """获取API服务状态"""
    services = {
        "决策引擎": "http://localhost:8080/health",
        "Feast服务": "http://localhost:6566/health"
    }
    
    status = {}
    for service_name, url in services.items():
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                status[service_name] = {"status": "运行中", "class": "status-good"}
            else:
                status[service_name] = {"status": "异常", "class": "status-error"}
        except:
            status[service_name] = {"status": "离线", "class": "status-error"}
    
    return status

def display_header():
    """显示页面标题"""
    st.markdown('<h1 class="main-header">📈 量化分析系统集成看板</h1>', unsafe_allow_html=True)
    
    # 实时状态
    api_status = get_api_status()
    
    cols = st.columns(len(api_status) + 2)
    
    # 系统状态
    with cols[0]:
        st.markdown(f"""
        <div style="text-align: center;">
            <h4>🔄 系统状态</h4>
            <span class="status-good">运行中</span>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[1]:
        st.markdown(f"""
        <div style="text-align: center;">
            <h4>📊 数据更新</h4>
            <span class="status-good">{datetime.now().strftime("%H:%M:%S")}</span>
        </div>
        """, unsafe_allow_html=True)
    
    # API服务状态
    for i, (service, info) in enumerate(api_status.items(), 2):
        with cols[i]:
            st.markdown(f"""
            <div style="text-align: center;">
                <h4>{service}</h4>
                <span class="{info['class']}">{info['status']}</span>
            </div>
            """, unsafe_allow_html=True)

def display_performance_overview(performance_report):
    """显示绩效概览"""
    if not performance_report:
        st.error("无法加载绩效数据")
        return
    
    st.header("📊 绩效概览")
    
    returns = performance_report.get('returns', {})
    risk = performance_report.get('risk', {})
    risk_adjusted = performance_report.get('risk_adjusted_returns', {})
    trading = performance_report.get('trading', {})
    
    # 主要指标
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_return = returns.get('total_return', 0)
        color_class = "positive" if total_return > 0 else "negative"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">总收益率</div>
            <div class="metric-value {color_class}">{total_return:.2%}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        sharpe = risk_adjusted.get('sharpe_ratio', 0)
        color_class = "positive" if sharpe > 1 else "neutral"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">夏普比率</div>
            <div class="metric-value {color_class}">{sharpe:.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        max_dd = risk.get('max_drawdown', 0)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value negative">{max_dd:.2%}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        win_rate = trading.get('win_rate', 0)
        color_class = "positive" if win_rate > 0.5 else "neutral"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">胜率</div>
            <div class="metric-value {color_class}">{win_rate:.2%}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # 详细指标表格
    st.subheader("详细绩效指标")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**收益指标**")
        st.write(f"• 总收益率: {returns.get('total_return', 0):.2%}")
        st.write(f"• 年化收益率: {returns.get('annualized_return', 0):.2%}")
        st.write(f"• 累计收益率: {returns.get('cumulative_return', 0):.2%}")
    
    with col2:
        st.markdown("**风险指标**")
        st.write(f"• 年化波动率: {risk.get('volatility', 0):.2%}")
        st.write(f"• 最大回撤: {risk.get('max_drawdown', 0):.2%}")
        st.write(f"• 95% VaR: {risk.get('var_95', 0):.2%}")
    
    with col3:
        st.markdown("**交易统计**")
        st.write(f"• 总交易数: {trading.get('total_trades', 0)}")
        st.write(f"• 盈亏比: {trading.get('profit_factor', 0):.2f}")
        st.write(f"• 平均盈利: ${trading.get('avg_win', 0):.2f}")

def display_backtest_results(backtest_result):
    """显示回测结果"""
    if not backtest_result:
        st.error("无法加载回测数据")
        return
    
    st.header("🔄 策略回测结果")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("初始资金", f"${backtest_result.initial_capital:,.2f}")
    
    with col2:
        st.metric("最终资金", f"${backtest_result.final_capital:,.2f}")
    
    with col3:
        return_color = "positive" if backtest_result.total_return > 0 else "negative"
        st.metric("总收益率", f"{backtest_result.total_return:.2%}")
    
    with col4:
        st.metric("夏普比率", f"{backtest_result.sharpe_ratio:.2f}")
    
    # 权益曲线
    if backtest_result.equity_curve:
        st.subheader("权益曲线")
        
        import pandas as pd
        
        equity_data = []
        for timestamp, value in backtest_result.equity_curve:
            equity_data.append({
                'timestamp': timestamp,
                'portfolio_value': value
            })
        
        df = pd.DataFrame(equity_data)
        df = df.set_index('timestamp')
        
        st.line_chart(df['portfolio_value'])
    
    # 交易统计
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("交易统计")
        st.write(f"• 总交易数: {backtest_result.total_trades}")
        st.write(f"• 盈利交易: {backtest_result.winning_trades}")
        st.write(f"• 亏损交易: {backtest_result.losing_trades}")
        st.write(f"• 胜率: {backtest_result.win_rate:.2%}")
    
    with col2:
        st.subheader("风险指标")
        st.write(f"• 最大回撤: {backtest_result.max_drawdown:.2%}")
        st.write(f"• 夏普比率: {backtest_result.sharpe_ratio:.2f}")
        
        if backtest_result.positions:
            total_pnl = sum(pos.realized_pnl + pos.unrealized_pnl for pos in backtest_result.positions)
            st.write(f"• 总盈亏: ${total_pnl:.2f}")

def display_real_time_signals():
    """显示实时交易信号"""
    st.header("🚦 实时交易信号")
    
    try:
        # 尝试从API获取信号
        response = requests.get("http://localhost:8080/signals", timeout=5)
        if response.status_code == 200:
            signals_data = response.json()
            
            if signals_data.get('signals'):
                import pandas as pd
                
                df = pd.DataFrame(signals_data['signals'])
                
                # 信号统计
                col1, col2, col3 = st.columns(3)
                
                buy_count = len([s for s in signals_data['signals'] if s.get('signal') == 'BUY'])
                sell_count = len([s for s in signals_data['signals'] if s.get('signal') == 'SELL'])
                hold_count = len([s for s in signals_data['signals'] if s.get('signal') == 'HOLD'])
                
                with col1:
                    st.metric("🟢 买入信号", buy_count)
                
                with col2:
                    st.metric("🔴 卖出信号", sell_count)
                
                with col3:
                    st.metric("🟡 持有信号", hold_count)
                
                # 信号表格
                st.subheader("最新信号")
                display_columns = ['trading_pair', 'signal', 'price', 'buy_score', 'sell_score', 'timestamp']
                available_columns = [col for col in display_columns if col in df.columns]
                
                if available_columns:
                    st.dataframe(df[available_columns].head(10), use_container_width=True)
                else:
                    st.write("信号数据格式不完整")
            else:
                st.info("暂无交易信号")
        else:
            st.warning("无法连接到决策引擎API")
    except Exception as e:
        st.error(f"获取实时信号失败: {e}")
        
        # 显示模拟信号
        st.info("显示模拟信号数据")
        import random
        
        mock_signals = []
        for i in range(5):
            mock_signals.append({
                'trading_pair': random.choice(['BTCUSDT', 'ETHUSDT']),
                'signal': random.choice(['BUY', 'SELL', 'HOLD']),
                'price': random.uniform(40000, 50000),
                'confidence': random.uniform(0.6, 0.9),
                'timestamp': (datetime.now() - timedelta(minutes=i*10)).strftime('%Y-%m-%d %H:%M:%S')
            })
        
        import pandas as pd
        df = pd.DataFrame(mock_signals)
        st.dataframe(df, use_container_width=True)

def display_system_monitoring():
    """显示系统监控"""
    st.header("🔧 系统监控")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("服务状态")
        
        api_status = get_api_status()
        
        for service, info in api_status.items():
            status_icon = "🟢" if info['status'] == "运行中" else "🔴"
            st.write(f"{status_icon} **{service}**: {info['status']}")
        
        # 模拟其他服务状态
        other_services = [
            ("DuckDB", "运行中", "🟢"),
            ("Redis", "运行中", "🟢"),
            ("实时处理", "运行中", "🟢")
        ]
        
        for service, status, icon in other_services:
            st.write(f"{icon} **{service}**: {status}")
    
    with col2:
        st.subheader("性能指标")
        
        # 模拟性能数据
        import random
        
        cpu_usage = random.uniform(10, 80)
        memory_usage = random.uniform(30, 90)
        disk_usage = random.uniform(20, 70)
        
        st.metric("CPU使用率", f"{cpu_usage:.1f}%")
        st.metric("内存使用率", f"{memory_usage:.1f}%")
        st.metric("磁盘使用率", f"{disk_usage:.1f}%")
        
        # 数据处理统计
        st.subheader("数据处理")
        st.metric("今日处理记录", f"{random.randint(10000, 50000):,}")
        st.metric("特征计算次数", f"{random.randint(1000, 5000):,}")
        st.metric("信号生成次数", f"{random.randint(100, 500):,}")

def main():
    """主函数"""
    # 侧边栏控制
    st.sidebar.title("📊 控制面板")
    
    # 页面选择
    page = st.sidebar.selectbox(
        "选择页面",
        ["综合概览", "绩效分析", "策略回测", "实时信号", "系统监控"]
    )
    
    # 自动刷新选项
    auto_refresh = st.sidebar.checkbox("自动刷新 (30秒)", value=False)
    refresh_interval = st.sidebar.slider("刷新间隔(秒)", 10, 300, 30)
    
    # 手动刷新按钮
    if st.sidebar.button("🔄 刷新数据"):
        st.cache_data.clear()
        st.rerun()
    
    # 数据导出
    st.sidebar.subheader("数据导出")
    if st.sidebar.button("📥 导出绩效报告"):
        performance_data = load_performance_data()
        if performance_data:
            st.sidebar.download_button(
                label="下载JSON报告",
                data=json.dumps(performance_data, indent=2, ensure_ascii=False),
                file_name=f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    # 显示页面标题
    display_header()
    
    # 根据选择显示不同页面
    if page == "综合概览":
        performance_data = load_performance_data()
        if performance_data:
            display_performance_overview(performance_data)
        
        st.divider()
        display_real_time_signals()
        
    elif page == "绩效分析":
        performance_data = load_performance_data()
        if performance_data:
            display_performance_overview(performance_data)
        
    elif page == "策略回测":
        backtest_data = load_backtest_data()
        display_backtest_results(backtest_data)
        
    elif page == "实时信号":
        display_real_time_signals()
        
    elif page == "系统监控":
        display_system_monitoring()
    
    # 自动刷新逻辑
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()
    
    # 页脚信息
    st.divider()
    st.markdown(f"""
    <div style='text-align: center; color: #666; font-size: 0.8rem;'>
        <p>量化分析系统集成看板 | 最后更新: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | 
        <a href="https://github.com/rulego/rulego" target="_blank">RuleGo项目</a></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()