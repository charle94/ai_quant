#!/usr/bin/env python3
"""
量化分析看板 - 基于Streamlit的可视化界面
"""
import streamlit as st
import json
import sys
import os
from datetime import datetime, timedelta
import time

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# 页面配置
st.set_page_config(
    page_title="量化分析系统看板",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .positive {
        color: #2e7d32;
    }
    .negative {
        color: #d32f2f;
    }
    .neutral {
        color: #757575;
    }
</style>
""", unsafe_allow_html=True)

def create_sample_data():
    """创建示例数据"""
    import random
    import pandas as pd
    
    # 模拟交易信号数据
    signals_data = []
    base_date = datetime.now()
    
    for i in range(20):
        timestamp = base_date - timedelta(minutes=i*30)
        signal = random.choice(['BUY', 'SELL', 'HOLD'])
        
        signals_data.append({
            'timestamp': timestamp,
            'trading_pair': random.choice(['BTCUSDT', 'ETHUSDT', 'ADAUSDT']),
            'signal': signal,
            'price': random.uniform(40000, 50000) if 'BTC' in 'BTCUSDT' else random.uniform(2000, 3000),
            'confidence': random.uniform(0.6, 0.95),
            'rsi_14': random.uniform(20, 80),
            'ma_5': random.uniform(40000, 50000),
            'volume_ratio': random.uniform(0.5, 2.5)
        })
    
    # 模拟绩效数据
    performance_data = {
        'total_return': random.uniform(-0.1, 0.3),
        'annualized_return': random.uniform(-0.2, 0.5),
        'volatility': random.uniform(0.15, 0.4),
        'max_drawdown': random.uniform(0.05, 0.25),
        'sharpe_ratio': random.uniform(-0.5, 2.5),
        'win_rate': random.uniform(0.4, 0.7),
        'total_trades': random.randint(50, 200),
        'profit_factor': random.uniform(0.8, 2.5)
    }
    
    # 模拟权益曲线
    equity_curve = []
    initial_value = 100000
    current_value = initial_value
    
    for i in range(60):
        date = base_date - timedelta(days=59-i)
        daily_return = random.gauss(0.001, 0.02)
        current_value *= (1 + daily_return)
        equity_curve.append({
            'date': date,
            'portfolio_value': current_value,
            'daily_return': daily_return
        })
    
    return signals_data, performance_data, equity_curve

def display_header():
    """显示页面标题"""
    st.markdown('<h1 class="main-header">📈 量化分析系统看板</h1>', unsafe_allow_html=True)
    
    # 状态指示器
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🔄 系统状态", "运行中", "正常")
    
    with col2:
        st.metric("📊 数据源", "实时", "连接正常")
    
    with col3:
        st.metric("🤖 决策引擎", "活跃", "信号生成中")
    
    with col4:
        st.metric("💾 特征存储", "Feast", "在线")

def display_performance_overview(performance_data):
    """显示绩效概览"""
    st.header("📊 绩效概览")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        return_color = "positive" if performance_data['total_return'] > 0 else "negative"
        st.markdown(f"""
        <div class="metric-card">
            <h3>总收益率</h3>
            <h2 class="{return_color}">{performance_data['total_return']:.2%}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        sharpe_color = "positive" if performance_data['sharpe_ratio'] > 1 else "neutral"
        st.markdown(f"""
        <div class="metric-card">
            <h3>夏普比率</h3>
            <h2 class="{sharpe_color}">{performance_data['sharpe_ratio']:.2f}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>最大回撤</h3>
            <h2 class="negative">{performance_data['max_drawdown']:.2%}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        win_color = "positive" if performance_data['win_rate'] > 0.5 else "neutral"
        st.markdown(f"""
        <div class="metric-card">
            <h3>胜率</h3>
            <h2 class="{win_color}">{performance_data['win_rate']:.2%}</h2>
        </div>
        """, unsafe_allow_html=True)

def display_equity_curve(equity_curve):
    """显示权益曲线"""
    st.header("📈 权益曲线")
    
    import pandas as pd
    
    df = pd.DataFrame(equity_curve)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    # 计算基准线（初始资金）
    initial_value = df['portfolio_value'].iloc[0] / (1 + df['daily_return'].iloc[0])
    df['benchmark'] = initial_value
    
    # 绘制图表
    st.line_chart(df[['portfolio_value', 'benchmark']])
    
    # 显示统计信息
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("起始资金", f"${initial_value:,.2f}")
    
    with col2:
        current_value = df['portfolio_value'].iloc[-1]
        st.metric("当前资金", f"${current_value:,.2f}")
    
    with col3:
        total_return = (current_value - initial_value) / initial_value
        st.metric("总收益", f"{total_return:.2%}")

def display_recent_signals(signals_data):
    """显示最近的交易信号"""
    st.header("🚦 最近交易信号")
    
    import pandas as pd
    
    df = pd.DataFrame(signals_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp', ascending=False)
    
    # 信号统计
    col1, col2, col3 = st.columns(3)
    
    with col1:
        buy_signals = len(df[df['signal'] == 'BUY'])
        st.metric("🟢 买入信号", buy_signals)
    
    with col2:
        sell_signals = len(df[df['signal'] == 'SELL'])
        st.metric("🔴 卖出信号", sell_signals)
    
    with col3:
        hold_signals = len(df[df['signal'] == 'HOLD'])
        st.metric("🟡 持有信号", hold_signals)
    
    # 最近信号表格
    st.subheader("最近10条信号")
    
    display_df = df.head(10)[['timestamp', 'trading_pair', 'signal', 'price', 'confidence', 'rsi_14']].copy()
    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    display_df['price'] = display_df['price'].round(2)
    display_df['confidence'] = display_df['confidence'].round(3)
    display_df['rsi_14'] = display_df['rsi_14'].round(1)
    
    # 为信号添加颜色
    def color_signal(val):
        if val == 'BUY':
            return 'background-color: #c8e6c9'
        elif val == 'SELL':
            return 'background-color: #ffcdd2'
        else:
            return 'background-color: #fff3e0'
    
    styled_df = display_df.style.applymap(color_signal, subset=['signal'])
    st.dataframe(styled_df, use_container_width=True)

def display_trading_pairs_analysis(signals_data):
    """显示交易对分析"""
    st.header("💰 交易对分析")
    
    import pandas as pd
    
    df = pd.DataFrame(signals_data)
    
    # 按交易对统计信号
    pair_stats = df.groupby('trading_pair').agg({
        'signal': 'count',
        'confidence': 'mean',
        'rsi_14': 'mean',
        'volume_ratio': 'mean'
    }).round(2)
    
    pair_stats.columns = ['信号数量', '平均置信度', '平均RSI', '平均成交量比']
    
    st.dataframe(pair_stats, use_container_width=True)
    
    # 信号分布图
    signal_dist = df.groupby(['trading_pair', 'signal']).size().unstack(fill_value=0)
    st.bar_chart(signal_dist)

def display_risk_metrics(performance_data):
    """显示风险指标"""
    st.header("⚠️ 风险指标")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("风险指标")
        
        risk_metrics = {
            "年化波动率": f"{performance_data['volatility']:.2%}",
            "最大回撤": f"{performance_data['max_drawdown']:.2%}",
            "夏普比率": f"{performance_data['sharpe_ratio']:.2f}",
            "盈亏比": f"{performance_data['profit_factor']:.2f}"
        }
        
        for metric, value in risk_metrics.items():
            st.metric(metric, value)
    
    with col2:
        st.subheader("交易统计")
        
        trade_stats = {
            "总交易数": f"{performance_data['total_trades']}",
            "胜率": f"{performance_data['win_rate']:.2%}",
            "年化收益率": f"{performance_data['annualized_return']:.2%}",
            "总收益率": f"{performance_data['total_return']:.2%}"
        }
        
        for stat, value in trade_stats.items():
            st.metric(stat, value)

def display_system_status():
    """显示系统状态"""
    st.header("🔧 系统状态")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("服务状态")
        
        services = [
            ("决策引擎", "运行中", "🟢"),
            ("Feast服务", "运行中", "🟢"),
            ("Redis", "运行中", "🟢"),
            ("DuckDB", "运行中", "🟢"),
            ("实时处理", "运行中", "🟢")
        ]
        
        for service, status, indicator in services:
            st.write(f"{indicator} **{service}**: {status}")
    
    with col2:
        st.subheader("数据状态")
        
        data_status = [
            ("特征数据", "实时更新", "🟢"),
            ("市场数据", "连接正常", "🟢"),
            ("Arrow缓存", "正常", "🟢"),
            ("数据库", "健康", "🟢"),
            ("API接口", "可用", "🟢")
        ]
        
        for data, status, indicator in data_status:
            st.write(f"{indicator} **{data}**: {status}")

def main():
    """主函数"""
    # 侧边栏
    st.sidebar.title("📊 控制面板")
    
    # 页面选择
    page = st.sidebar.selectbox(
        "选择页面",
        ["概览", "绩效分析", "交易信号", "风险管理", "系统状态"]
    )
    
    # 自动刷新选项
    auto_refresh = st.sidebar.checkbox("自动刷新 (30秒)", value=False)
    
    if auto_refresh:
        time.sleep(30)
        st.experimental_rerun()
    
    # 手动刷新按钮
    if st.sidebar.button("🔄 刷新数据"):
        st.experimental_rerun()
    
    # 数据时间范围选择
    st.sidebar.subheader("数据设置")
    days_back = st.sidebar.slider("历史数据天数", 1, 90, 30)
    
    # 创建示例数据
    signals_data, performance_data, equity_curve = create_sample_data()
    
    # 显示页面标题
    display_header()
    
    # 根据选择显示不同页面
    if page == "概览":
        display_performance_overview(performance_data)
        st.divider()
        display_equity_curve(equity_curve[-days_back:])
        
    elif page == "绩效分析":
        display_performance_overview(performance_data)
        st.divider()
        display_equity_curve(equity_curve[-days_back:])
        st.divider()
        display_risk_metrics(performance_data)
        
    elif page == "交易信号":
        display_recent_signals(signals_data)
        st.divider()
        display_trading_pairs_analysis(signals_data)
        
    elif page == "风险管理":
        display_risk_metrics(performance_data)
        
    elif page == "系统状态":
        display_system_status()
    
    # 页脚
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>量化分析系统看板 | 最后更新: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

if __name__ == "__main__":
    main()