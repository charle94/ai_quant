# 量化分析系统架构

## 项目结构
```
quant-analysis-system/
├── deploy/                    # 部署脚本
│   ├── docker-compose.yml
│   ├── setup-mac.sh
│   ├── setup-linux.sh
│   └── requirements.txt
├── dbt_project/              # DBT离线特征工程
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── features/
│   └── profiles.yml
├── feast_config/             # Feast配置
│   ├── feature_repo/
│   │   ├── feature_store.yaml
│   │   ├── features.py
│   │   └── data_sources.py
│   └── push_features.py
├── realtime_processing/      # 实时数据处理
│   ├── miniqmt_connector.py
│   ├── arrow_processor.py
│   ├── feature_calculator.py
│   └── feast_pusher.py
├── decision_engine/          # RuleGo决策引擎
│   ├── main.go
│   ├── rules/
│   ├── feast_client.go
│   └── go.mod
├── data/                     # 数据目录
│   ├── raw/
│   ├── processed/
│   └── arrow_cache/
└── config/                   # 配置文件
    ├── database.yml
    ├── feast.yml
    └── rulego.yml
```

## 技术栈
- **数据处理**: DBT + DuckDB
- **特征存储**: Feast
- **实时数据**: MiniQMT + Apache Arrow
- **决策引擎**: RuleGo (Go)
- **容器化**: Docker + Docker Compose