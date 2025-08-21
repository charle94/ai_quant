# Rill Data 量化分析看板

## 项目概述

本项目使用 Rill Data 为量化分析系统创建交互式数据看板，提供实时的绩效分析和可视化。

## 项目结构

```
rill_project/
├── rill.yaml              # Rill 配置文件
├── start_rill.sh          # 启动脚本
├── data/                  # 数据文件
│   ├── daily_returns.csv  # 日收益率数据
│   ├── trades.csv         # 交易记录
│   └── performance_metrics.csv  # 绩效指标
├── models/                # 数据模型
│   ├── daily_returns.sql  # 日收益率模型
│   ├── trades_analysis.sql # 交易分析模型
│   └── performance_metrics.sql # 绩效指标模型
└── dashboards/            # 看板配置
    ├── main_dashboard.yaml     # 主看板
    └── trading_dashboard.yaml  # 交易看板
```

## 快速开始

### 1. 安装 Rill Data

**macOS (使用 Homebrew):**
```bash
brew install rilldata/tap/rill
```

**Linux/macOS (使用安装脚本):**
```bash
curl -s https://cdn.rilldata.com/install.sh | bash
```

### 2. 启动看板

```bash
cd /workspace/rill_project
./start_rill.sh
```

### 3. 访问看板

打开浏览器访问: http://localhost:9009

## 可用看板

### 📈 量化策略绩效概览
- **URL**: http://localhost:9009/dashboard/quant-performance-overview
- **功能**: 
  - 组合价值趋势
  - 日收益率分析
  - 累计收益率跟踪
  - 滚动夏普比率

### 💰 交易分析看板
- **URL**: http://localhost:9009/dashboard/trading-analysis
- **功能**:
  - 交易盈亏分析
  - 累计盈亏趋势
  - 胜率统计
  - 交易对比较

## 数据模型

### daily_returns
- 日收益率数据和衍生指标
- 包含移动平均、滚动波动率等

### trades_analysis
- 交易记录和分析
- 包含累计统计和分类

### performance_metrics
- 绩效指标汇总
- 包含格式化显示和评级

## 自定义配置

### 修改数据源
编辑 `rill.yaml` 中的连接器配置:

```yaml
connectors:
  - name: performance_db
    type: duckdb
    config:
      dsn: path/to/your/database.db
```

### 添加新看板
1. 在 `dashboards/` 目录下创建新的 YAML 文件
2. 定义看板的措施和维度
3. 重启 Rill 服务

### 修改数据模型
1. 编辑 `models/` 目录下的 SQL 文件
2. 使用 DuckDB SQL 语法
3. 重启 Rill 服务以应用更改

## 故障排除

### 常见问题

1. **端口冲突**
   - 修改 `rill.yaml` 中的端口设置
   - 或停止占用 9009 端口的其他服务

2. **数据加载失败**
   - 检查 CSV 文件格式
   - 确认文件路径正确
   - 查看 Rill 日志输出

3. **看板显示异常**
   - 检查 YAML 配置语法
   - 确认模型中的字段名称
   - 重启服务

### 查看日志
```bash
rill start --verbose
```

## 更多资源

- [Rill Data 官方文档](https://docs.rilldata.com/)
- [DuckDB SQL 参考](https://duckdb.org/docs/sql/introduction)
- [YAML 语法指南](https://yaml.org/spec/1.2/spec.html)
