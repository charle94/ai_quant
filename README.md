# 量化分析系统

一个基于DBT+DuckDB离线特征工程、MiniQMT实时数据处理、Feast特征存储和RuleGo决策引擎的完整量化分析系统。

## 🏗️ 系统架构

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   历史OHLC数据   │────│  DBT + DuckDB    │────│  Feast离线存储   │
└─────────────────┘    │   离线特征工程    │    └─────────────────┘
                       └──────────────────┘
                       
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  实时数据流     │────│  MiniQMT         │────│  Arrow IPC      │
│  (WebSocket等)  │    │  数据采集        │    │  缓存存储       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                       ┌──────────────────┐    ┌─────────────────┐
                       │  DuckDB实时      │────│  Feast在线存储   │
                       │  特征计算        │    │  (Redis)        │
                       └──────────────────┘    └─────────────────┘
                                                        │
                       ┌──────────────────┐    ┌─────────────────┐
                       │  RuleGo          │────│  Feast Serving  │
                       │  决策引擎        │    │  特征获取       │
                       └──────────────────┘    └─────────────────┘
```

## ✨ 核心特性

- **离线特征工程**: 使用DBT+DuckDB处理历史OHLC数据，生成技术指标特征
- **实时数据处理**: MiniQMT采集实时数据，Arrow IPC高效存储
- **特征存储**: Feast管理离线和在线特征，支持特征服务
- **决策引擎**: RuleGo规则引擎，基于特征数据进行交易决策
- **一键部署**: 支持Mac和Linux系统的自动化部署脚本
- **容器化**: Docker Compose编排所有服务

## 🚀 快速开始

### 系统要求

- **操作系统**: macOS 10.15+ 或 Linux (Ubuntu 18.04+, CentOS 7+)
- **内存**: 8GB+ 推荐
- **存储**: 10GB+ 可用空间
- **网络**: 稳定的互联网连接

### 一键部署

#### macOS
```bash
# 克隆项目
git clone <repository-url>
cd quant-analysis-system

# 运行部署脚本
./deploy/setup-mac.sh
```

#### Linux
```bash
# 克隆项目
git clone <repository-url>
cd quant-analysis-system

# 运行部署脚本
./deploy/setup-linux.sh
```

### 手动部署

如果一键部署遇到问题，可以按以下步骤手动部署：

1. **安装依赖**
```bash
# Python环境
python3 -m venv venv
source venv/bin/activate
pip install -r deploy/requirements.txt

# Go环境 (安装Go 1.21+)
# Docker环境 (安装Docker和Docker Compose)
# Redis (用于Feast在线存储)
```

2. **初始化数据库**
```bash
python scripts/init_duckdb.py
```

3. **配置Feast**
```bash
cd feast_config/feature_repo
feast apply
cd ../..
```

4. **构建决策引擎**
```bash
cd decision_engine
go mod tidy
go build -o decision-engine .
cd ..
```

5. **启动服务**
```bash
cd deploy
docker-compose up -d
```

## 📊 系统组件

### 1. 离线特征工程 (DBT + DuckDB)

**位置**: `dbt_project/`

- **数据模型**: 
  - `stg_ohlc_data.sql`: 数据清洗和预处理
  - `mart_technical_indicators.sql`: 技术指标计算
  - `features_ohlc_technical.sql`: 特征工程

**运行DBT**:
```bash
cd dbt_project
dbt run
dbt test
```

### 2. 实时数据处理

**位置**: `realtime_processing/`

- **MiniQMT连接器**: 模拟实时数据采集
- **Arrow处理器**: 高效的数据存储和读取
- **特征计算器**: 实时技术指标计算
- **Feast推送器**: 特征数据推送到Feast

**启动实时处理**:
```bash
python realtime_processing/main.py
```

### 3. Feast特征存储

**位置**: `feast_config/`

- **特征定义**: 技术指标特征视图
- **数据源**: DuckDB离线存储 + Redis在线存储
- **推送源**: 实时特征推送

**Feast操作**:
```bash
cd feast_config/feature_repo

# 应用特征定义
feast apply

# 启动特征服务器
feast serve

# 物化特征到在线存储
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### 4. RuleGo决策引擎

**位置**: `decision_engine/`

- **规则定义**: `rules/trading_rules.json`
- **Feast客户端**: 获取在线特征
- **交易信号生成**: 基于规则的决策逻辑

**启动决策引擎**:
```bash
cd decision_engine
./decision-engine
```

## 🔧 配置说明

### 数据库配置
**文件**: `config/database.yml`
```yaml
duckdb:
  offline_db_path: "/workspace/data/quant_features.duckdb"
  realtime_db_path: "/workspace/data/realtime_features.duckdb"
```

### Feast配置
**文件**: `feast_config/feature_repo/feature_store.yaml`
```yaml
project: quant_analysis
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
offline_store:
  type: duckdb
  path: /workspace/data/quant_features.duckdb
```

### RuleGo配置
**文件**: `config/rulego.yml`
```yaml
server:
  port: "8080"
feast:
  base_url: "http://localhost:6566"
  trading_pairs:
    - "BTCUSDT"
    - "ETHUSDT"
trading:
  update_interval: 30
```

## 📊 分析看板

### Streamlit集成看板
**访问地址**: `http://localhost:8501`
- 综合绩效概览
- 实时交易信号
- 策略回测结果
- 系统监控面板

### Rill Data专业看板
**访问地址**: `http://localhost:9009`
- **量化策略绩效概览**: 组合价值趋势、收益率分析、滚动夏普比率
- **交易分析看板**: 交易盈亏分析、胜率统计、交易对比较
- **风险分析看板**: 风险指标汇总、绩效评级

## 🌐 API接口

### 决策引擎API

**基础URL**: `http://localhost:8080`

- `GET /health` - 健康检查
- `GET /signals` - 获取最新交易信号
- `GET /signals/{pair}` - 获取指定交易对信号
- `POST /trigger` - 手动触发决策

**示例请求**:
```bash
# 获取健康状态
curl http://localhost:8080/health

# 获取BTCUSDT信号
curl http://localhost:8080/signals/BTCUSDT

# 获取所有最新信号
curl http://localhost:8080/signals
```

### Feast Serving API

**基础URL**: `http://localhost:6566`

- `POST /get-online-features` - 获取在线特征

**示例请求**:
```bash
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{
    "features": [
      "realtime_features:price",
      "realtime_features:rsi_14"
    ],
    "entity_rows": [
      {"trading_pair": "BTCUSDT"}
    ]
  }'
```

## 📈 特征说明

### 技术指标特征

- **价格特征**: price, daily_return, volatility_20d
- **趋势指标**: ma_5, ma_10, ma_20, ema_12, ema_26
- **动量指标**: rsi_14, stoch_k_14, momentum_5d, momentum_10d, macd
- **波动率指标**: bollinger_upper, bollinger_lower, atr_14
- **成交量指标**: volume_ratio, avg_volume_20d, obv_5
- **模式识别**: doji, hammer, shooting_star, gap_up, gap_down
- **组合特征**: double_overbought, trend_strength, breakout_signal

### 交易信号

- **BUY**: 买入信号 (buy_score >= 5)
- **SELL**: 卖出信号 (sell_score >= 5)  
- **HOLD**: 持有信号 (默认)

## 🛠️ 运维管理

### 服务管理

```bash
# 查看服务状态
cd deploy && docker-compose ps

# 查看服务日志
cd deploy && docker-compose logs -f [service_name]

# 重启服务
cd deploy && docker-compose restart [service_name]

# 停止所有服务
cd deploy && docker-compose down

# 启动所有服务
cd deploy && docker-compose up -d
```

### 数据管理

```bash
# 清理旧的Arrow缓存文件
find data/arrow_cache -name "*.arrow" -mtime +7 -delete

# 备份DuckDB数据库
cp data/quant_features.duckdb backups/quant_features_$(date +%Y%m%d).duckdb

# 重新初始化数据库
python scripts/init_duckdb.py
```

### 监控和日志

- **系统日志**: 使用`docker-compose logs`查看
- **性能监控**: 检查容器资源使用情况
- **特征质量**: 定期检查特征数据的完整性和准确性

## 🧪 测试

### 单元测试
```bash
# 激活Python环境
source venv/bin/activate

# 运行Python测试
pytest tests/

# 测试特定组件
python realtime_processing/miniqmt_connector.py
python feast_config/push_features.py
```

### 集成测试
```bash
# 测试完整的数据流
./scripts/test_data_pipeline.sh

# 测试API接口
./scripts/test_apis.sh
```

## 🔍 故障排除

### 常见问题

1. **Docker服务无法启动**
   - 检查Docker是否正在运行
   - 确保端口没有被占用 (6379, 6566, 8080)

2. **Feast特征获取失败**
   - 检查Redis连接
   - 确认特征已正确推送

3. **决策引擎无响应**
   - 检查Go应用日志
   - 确认Feast服务可访问

4. **实时数据处理中断**
   - 检查Python进程状态
   - 查看错误日志

### 日志位置

- **Docker服务日志**: `docker-compose logs`
- **Python应用日志**: 控制台输出
- **Go应用日志**: 控制台输出
- **DuckDB日志**: 数据库文件同目录

## 🤝 贡献指南

1. Fork项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

## 📄 许可证

本项目采用MIT许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 🙏 致谢

- [DBT](https://www.getdbt.com/) - 数据转换工具
- [DuckDB](https://duckdb.org/) - 嵌入式分析数据库
- [Feast](https://feast.dev/) - 特征存储平台
- [RuleGo](https://github.com/rulego/rulego) - Go规则引擎
- [Apache Arrow](https://arrow.apache.org/) - 内存列式数据格式

## 📞 支持

如有问题或建议，请：
- 创建GitHub Issue
- 发送邮件至: [your-email@example.com]
- 加入讨论群: [群链接]

---

**注意**: 这是一个演示系统，实际生产环境使用前请进行充分的测试和验证。