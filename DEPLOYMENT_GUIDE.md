# 量化分析系统部署指南

## 📋 部署前准备

### 系统要求

| 组件 | 最低配置 | 推荐配置 |
|------|----------|----------|
| CPU | 2核心 | 4核心+ |
| 内存 | 4GB | 8GB+ |
| 存储 | 5GB | 20GB+ |
| 网络 | 10Mbps | 100Mbps+ |

### 支持的操作系统

- **macOS**: 10.15 (Catalina) 及以上
- **Ubuntu**: 18.04 LTS 及以上
- **CentOS**: 7.x 及以上
- **Debian**: 10 及以上
- **RHEL**: 7.x 及以上

## 🚀 一键部署

### macOS部署

```bash
# 1. 克隆项目
git clone <repository-url>
cd quant-analysis-system

# 2. 运行部署脚本
./deploy/setup-mac.sh

# 3. 等待部署完成（约10-20分钟）
```

**macOS特殊说明**:
- 自动检测Apple Silicon (M1/M2) 或Intel处理器
- 自动安装Homebrew（如未安装）
- 自动配置Docker Desktop
- 创建便捷启动脚本

### Linux部署

```bash
# 1. 克隆项目
git clone <repository-url>
cd quant-analysis-system

# 2. 运行部署脚本
sudo ./deploy/setup-linux.sh

# 3. 等待部署完成（约15-30分钟）
```

**Linux特殊说明**:
- 支持Ubuntu/Debian和CentOS/RHEL
- 自动安装Docker和Docker Compose
- 配置用户权限和防火墙
- 设置系统服务

## 🔧 手动部署

如果一键部署失败，可以按以下步骤手动部署：

### 步骤1: 安装基础依赖

#### Python环境
```bash
# 安装Python 3.11+
python3 --version  # 确认版本

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装Python依赖
pip install -r deploy/requirements.txt
```

#### Go环境
```bash
# 下载并安装Go 1.21+
wget https://golang.org/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz

# 设置环境变量
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# 验证安装
go version
```

#### Docker环境
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose

# CentOS/RHEL
sudo yum install docker docker-compose

# 启动Docker服务
sudo systemctl start docker
sudo systemctl enable docker

# 添加用户到docker组
sudo usermod -aG docker $USER
```

### 步骤2: 配置系统

#### 初始化数据库
```bash
python scripts/init_duckdb.py
```

#### 配置Feast
```bash
cd feast_config/feature_repo
feast apply
cd ../..
```

#### 构建Go应用
```bash
cd decision_engine
go mod tidy
go build -o decision-engine .
cd ..
```

### 步骤3: 启动服务

#### 使用Docker Compose
```bash
cd deploy
docker-compose up -d
```

#### 手动启动各服务

1. **启动Redis**
```bash
redis-server --daemonize yes
```

2. **启动Feast服务**
```bash
cd feast_config/feature_repo
feast serve --host 0.0.0.0 --port 6566 &
cd ../..
```

3. **启动实时处理**
```bash
source venv/bin/activate
python realtime_processing/main.py &
```

4. **启动决策引擎**
```bash
cd decision_engine
./decision-engine &
cd ..
```

## ✅ 部署验证

### 检查服务状态
```bash
# 检查Docker容器
docker-compose ps

# 检查端口占用
netstat -tlnp | grep -E '6379|6566|8080'

# 检查进程
ps aux | grep -E 'redis|feast|decision-engine'
```

### API健康检查
```bash
# 决策引擎健康检查
curl -f http://localhost:8080/health

# Feast服务健康检查
curl -f http://localhost:6566/health

# Redis连接检查
redis-cli ping
```

### 功能测试
```bash
# 激活Python环境
source venv/bin/activate

# 运行测试脚本
python scripts/test_system.py

# 测试特征推送
python feast_config/push_features.py

# 测试决策引擎
curl http://localhost:8080/signals
```

## 🔄 配置自定义

### 修改交易对
编辑 `config/rulego.yml`:
```yaml
feast:
  trading_pairs:
    - "BTCUSDT"
    - "ETHUSDT"
    - "ADAUSDT"  # 添加新的交易对
```

### 调整更新频率
编辑 `config/rulego.yml`:
```yaml
trading:
  update_interval: 30  # 决策更新间隔（秒）
```

### 修改特征定义
编辑 `feast_config/feature_repo/features.py`:
```python
# 添加新的特征字段
schema=[
    Field(name="new_feature", dtype=Float64),
    # ... 其他特征
]
```

### 自定义交易规则
编辑 `decision_engine/rules/trading_rules.json`:
```json
{
  "configuration": {
    "jsScript": "// 自定义交易逻辑"
  }
}
```

## 🐳 Docker部署详解

### 服务架构
```yaml
services:
  redis:          # 特征在线存储
  feast-server:   # 特征服务器
  decision-engine: # 决策引擎
  realtime-processor: # 实时数据处理
  db-init:        # 数据库初始化
```

### 自定义Docker配置

#### 修改端口映射
编辑 `deploy/docker-compose.yml`:
```yaml
services:
  decision-engine:
    ports:
      - "8081:8080"  # 改为8081端口
```

#### 调整资源限制
```yaml
services:
  decision-engine:
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: '0.5'
```

#### 添加环境变量
```yaml
services:
  decision-engine:
    environment:
      - LOG_LEVEL=debug
      - MAX_CONNECTIONS=100
```

## 📊 性能优化

### DuckDB优化
编辑 `config/database.yml`:
```yaml
duckdb:
  config:
    threads: 8        # 增加线程数
    memory_limit: "4GB"  # 增加内存限制
    max_memory: "8GB"
```

### Redis优化
```bash
# 编辑redis配置
redis-cli CONFIG SET maxmemory 2gb
redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

### Go应用优化
编辑 `config/rulego.yml`:
```yaml
server:
  read_timeout: 60
  write_timeout: 60
```

## 🔒 安全配置

### 防火墙设置
```bash
# Ubuntu/Debian
sudo ufw allow 8080/tcp
sudo ufw allow 6566/tcp

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --permanent --add-port=6566/tcp
sudo firewall-cmd --reload
```

### Redis安全
```bash
# 设置密码
redis-cli CONFIG SET requirepass "your-password"

# 绑定特定IP
redis-cli CONFIG SET bind "127.0.0.1"
```

### 访问控制
编辑 `deploy/docker-compose.yml`:
```yaml
services:
  decision-engine:
    environment:
      - ALLOWED_HOSTS=localhost,127.0.0.1
```

## 🚨 故障排除

### 常见部署问题

#### 1. Docker无法启动
**问题**: `Cannot connect to the Docker daemon`
**解决**:
```bash
sudo systemctl start docker
sudo usermod -aG docker $USER
# 重新登录
```

#### 2. 端口占用
**问题**: `Port already in use`
**解决**:
```bash
# 查找占用进程
sudo lsof -i :8080

# 杀死进程
sudo kill -9 <PID>
```

#### 3. Python依赖安装失败
**问题**: `Failed building wheel`
**解决**:
```bash
# 安装编译工具
sudo apt-get install build-essential python3-dev

# 或使用conda
conda install -c conda-forge <package-name>
```

#### 4. Go模块下载失败
**问题**: `go mod download failed`
**解决**:
```bash
# 设置Go代理
go env -w GOPROXY=https://goproxy.cn,direct
go env -w GOSUMDB=sum.golang.google.cn
```

#### 5. Feast服务无法启动
**问题**: `feast serve failed`
**解决**:
```bash
# 检查配置文件
feast validate

# 重新应用特征定义
feast apply

# 检查依赖版本
pip list | grep feast
```

### 日志分析

#### Docker服务日志
```bash
# 查看所有服务日志
docker-compose logs

# 查看特定服务日志
docker-compose logs decision-engine

# 实时查看日志
docker-compose logs -f realtime-processor
```

#### 应用级日志
```bash
# Python应用日志
tail -f logs/realtime_processing.log

# Go应用日志
tail -f logs/decision_engine.log
```

## 📈 监控和维护

### 系统监控
```bash
# 资源使用情况
docker stats

# 磁盘使用
df -h

# 内存使用
free -h

# CPU使用
top
```

### 数据维护
```bash
# 清理旧数据
find data/arrow_cache -name "*.arrow" -mtime +7 -delete

# 备份数据库
cp data/quant_features.duckdb backups/

# 压缩日志
gzip logs/*.log
```

### 定期任务
创建crontab任务:
```bash
# 编辑crontab
crontab -e

# 添加清理任务
0 2 * * * /path/to/cleanup.sh

# 添加备份任务
0 1 * * 0 /path/to/backup.sh
```

## 🔄 升级和迁移

### 系统升级
```bash
# 停止服务
docker-compose down

# 拉取最新代码
git pull origin main

# 重新构建
docker-compose build

# 启动服务
docker-compose up -d
```

### 数据迁移
```bash
# 导出现有数据
python scripts/export_data.py

# 升级数据库结构
python scripts/migrate_database.py

# 导入数据
python scripts/import_data.py
```

## 📞 获取帮助

如果遇到部署问题，请：

1. 查看本文档的故障排除部分
2. 检查GitHub Issues
3. 提交新的Issue并包含：
   - 操作系统版本
   - 错误日志
   - 部署步骤
   - 系统配置信息

---

**注意**: 部署过程中如有任何问题，请保存错误日志以便排查。