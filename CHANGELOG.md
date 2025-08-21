# 更新日志

## [1.0.0] - 2024-01-15

### 🎉 首次发布

#### 新增功能
- **离线特征工程**: 基于DBT+DuckDB的OHLC数据处理和技术指标计算
- **实时数据处理**: MiniQMT实时数据采集和Arrow IPC存储
- **特征存储**: Feast特征存储，支持离线和在线特征管理
- **决策引擎**: RuleGo规则引擎，基于特征数据生成交易信号
- **一键部署**: Mac和Linux系统的自动化部署脚本
- **容器化**: Docker Compose编排所有服务组件

#### 核心组件
- **DBT项目**: 完整的数据转换管道
  - 数据清洗和验证
  - 技术指标计算 (MA, RSI, 布林带等)
  - 特征工程和标准化
  
- **实时处理系统**: 
  - MiniQMT数据连接器
  - Arrow数据处理器
  - 全面的特征计算器
  - Feast特征推送器

- **Feast配置**:
  - 实体和特征视图定义
  - DuckDB离线存储集成
  - Redis在线存储配置
  - 推送源支持

- **RuleGo决策引擎**:
  - 灵活的规则配置
  - Feast客户端集成
  - RESTful API接口
  - 实时决策生成

#### 技术指标
- **趋势指标**: SMA, EMA, 布林带
- **动量指标**: RSI, 随机指标, MACD
- **波动率指标**: ATR, 历史波动率
- **成交量指标**: OBV, 成交量比率
- **模式识别**: K线形态识别
- **组合特征**: 多重信号组合

#### API接口
- **决策引擎API**:
  - `GET /health` - 健康检查
  - `GET /signals` - 获取交易信号
  - `GET /signals/{pair}` - 获取指定交易对信号
  - `POST /trigger` - 手动触发决策

- **Feast Serving API**:
  - `POST /get-online-features` - 获取在线特征
  - `GET /health` - 服务健康检查

#### 部署支持
- **macOS**: 支持Intel和Apple Silicon (M1/M2)
- **Linux**: 支持Ubuntu, CentOS, Debian, RHEL
- **Docker**: 完整的容器化部署方案
- **自动化**: 一键部署脚本，自动依赖安装

#### 文档
- **README**: 完整的项目介绍和快速开始指南
- **部署指南**: 详细的部署说明和故障排除
- **API文档**: 完整的API接口文档和使用示例
- **架构文档**: 系统架构设计和组件说明

### 🔧 技术栈
- **数据处理**: Python 3.11, DBT, DuckDB, Pandas, PyArrow
- **特征存储**: Feast, Redis
- **决策引擎**: Go 1.21, RuleGo
- **容器化**: Docker, Docker Compose
- **数据格式**: Arrow IPC, Parquet, JSON

### 📊 支持的交易对
- BTCUSDT (比特币/USDT)
- ETHUSDT (以太坊/USDT)
- ADAUSDT (卡尔达诺/USDT)
- DOTUSDT (波卡/USDT)

### 🎯 交易信号类型
- **BUY**: 买入信号
- **SELL**: 卖出信号  
- **HOLD**: 持有信号

### 🔒 风险管理
- 动态仓位管理
- 波动率调整
- 多重确认机制

---

## 未来计划

### [1.1.0] - 计划中
- [ ] 增加更多技术指标
- [ ] 支持更多交易对
- [ ] 增加回测功能
- [ ] 优化决策规则
- [ ] 添加Web界面

### [1.2.0] - 计划中
- [ ] 机器学习模型集成
- [ ] 实时监控面板
- [ ] 告警系统
- [ ] 性能优化
- [ ] 多时间周期支持

---

## 贡献者

- **主要开发者**: Claude (Anthropic AI)
- **项目发起人**: 量化分析师团队

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

---

**注意**: 这是一个演示系统，用于教育和研究目的。实际交易前请进行充分的测试和验证。