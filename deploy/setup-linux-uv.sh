#!/bin/bash

# 量化分析系统 - Linux一键部署脚本 (使用uv)
# 支持 Ubuntu/Debian/CentOS/RHEL

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

log_success() {
    echo -e "${PURPLE}[SUCCESS]${NC} $1"
}

# 显示横幅
show_banner() {
    echo -e "${CYAN}"
    cat << "EOF"
    ╔══════════════════════════════════════════════════════════════╗
    ║                  量化分析系统部署工具                        ║
    ║          DBT + DuckDB + Feast + RuleGo + uv                 ║
    ║                     Linux Edition                            ║
    ╚══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

# 检查是否为root用户
check_root() {
    if [[ $EUID -eq 0 ]]; then
        log_warn "检测到root用户，建议使用普通用户运行"
        read -p "是否继续? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# 检测操作系统
detect_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    else
        log_error "无法检测操作系统"
        exit 1
    fi
    
    log_info "检测到操作系统: $OS $VER"
    
    # 检查架构
    ARCH=$(uname -m)
    log_info "系统架构: $ARCH"
}

# 安装系统依赖
install_system_dependencies() {
    log_step "安装系统依赖..."
    
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        # Ubuntu/Debian
        sudo apt-get update
        sudo apt-get install -y \
            curl \
            wget \
            git \
            build-essential \
            ca-certificates \
            gnupg \
            lsb-release \
            software-properties-common \
            apt-transport-https
            
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]] || [[ "$OS" == *"Rocky"* ]]; then
        # CentOS/RHEL/Rocky
        sudo yum update -y
        sudo yum groupinstall -y "Development Tools"
        sudo yum install -y \
            curl \
            wget \
            git \
            ca-certificates
    else
        log_error "不支持的操作系统: $OS"
        exit 1
    fi
    
    log_success "系统依赖安装完成"
}

# 安装uv
install_uv() {
    log_step "安装uv Python包管理器..."
    
    if command -v uv &> /dev/null; then
        log_info "uv已安装，版本: $(uv --version)"
        return
    fi
    
    log_info "开始安装uv..."
    
    # 使用官方安装脚本
    curl -LsSf https://astral.sh/uv/install.sh | sh
    
    # 添加uv到PATH
    export PATH="$HOME/.cargo/bin:$PATH"
    echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.bashrc
    
    # 验证安装
    if command -v uv &> /dev/null; then
        log_success "uv安装成功，版本: $(uv --version)"
    else
        log_error "uv安装失败"
        exit 1
    fi
}

# 安装Python
install_python() {
    log_step "安装Python 3.11..."
    
    # 使用uv安装Python
    if ! uv python list | grep -q "3.11"; then
        log_info "使用uv安装Python 3.11..."
        uv python install 3.11
    else
        log_info "Python 3.11已安装"
    fi
    
    # 验证Python安装
    PYTHON_VERSION=$(uv python list | grep "3.11" | head -n1 | awk '{print $1}')
    log_success "Python安装完成: $PYTHON_VERSION"
}

# 创建虚拟环境
create_virtual_environment() {
    log_step "创建Python虚拟环境..."
    
    # 使用uv创建虚拟环境
    if [[ ! -d ".venv" ]]; then
        log_info "创建虚拟环境..."
        uv venv --python 3.11
        log_success "虚拟环境创建完成"
    else
        log_info "虚拟环境已存在"
    fi
    
    # 激活虚拟环境
    source .venv/bin/activate
    
    # 验证虚拟环境
    log_info "当前Python版本: $(python --version)"
    log_info "虚拟环境路径: $VIRTUAL_ENV"
}

# 安装Python依赖
install_python_dependencies() {
    log_step "安装Python依赖包..."
    
    # 确保在虚拟环境中
    source .venv/bin/activate
    
    # 使用uv安装依赖
    if [[ -f "pyproject.toml" ]]; then
        log_info "从pyproject.toml安装依赖..."
        uv pip install -e .
    elif [[ -f "deploy/requirements.txt" ]]; then
        log_info "从requirements.txt安装依赖..."
        uv pip install -r deploy/requirements.txt
    else
        log_error "未找到依赖配置文件"
        exit 1
    fi
    
    # 安装开发依赖
    if [[ -f "pyproject.toml" ]]; then
        log_info "安装开发依赖..."
        uv pip install -e ".[dev,test]"
    fi
    
    log_success "Python依赖安装完成"
}

# 安装Docker
install_docker() {
    log_step "安装Docker..."
    
    if command -v docker &> /dev/null; then
        log_info "Docker已安装，版本: $(docker --version)"
        return
    fi
    
    log_info "开始安装Docker..."
    
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        # Ubuntu/Debian
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
        
        echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
            $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
        
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]] || [[ "$OS" == *"Rocky"* ]]; then
        # CentOS/RHEL/Rocky
        sudo yum install -y yum-utils
        sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
        sudo systemctl start docker
        sudo systemctl enable docker
    fi
    
    # 添加用户到docker组
    sudo usermod -aG docker $USER
    
    log_success "Docker安装完成"
    log_warn "请重新登录以使docker组生效，或运行: newgrp docker"
}

# 安装Go
install_go() {
    log_step "安装Go语言环境..."
    
    if command -v go &> /dev/null; then
        log_info "Go已安装，版本: $(go version)"
        return
    fi
    
    log_info "开始安装Go..."
    
    GO_VERSION="1.21.5"
    GO_ARCH="amd64"
    
    if [[ "$ARCH" == "aarch64" ]]; then
        GO_ARCH="arm64"
    fi
    
    curl -L "https://golang.org/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" -o go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go.tar.gz
    rm go.tar.gz
    
    # 设置环境变量
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    echo 'export GOPATH=$HOME/go' >> ~/.bashrc
    echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bashrc
    
    export PATH=$PATH:/usr/local/go/bin
    export GOPATH=$HOME/go
    export PATH=$PATH:$GOPATH/bin
    
    log_success "Go安装完成，版本: $(go version)"
}

# 安装Redis
install_redis() {
    log_step "安装Redis..."
    
    if command -v redis-server &> /dev/null; then
        log_info "Redis已安装"
        return
    fi
    
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        sudo apt-get install -y redis-server
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]] || [[ "$OS" == *"Rocky"* ]]; then
        sudo yum install -y redis
        sudo systemctl start redis
        sudo systemctl enable redis
    fi
    
    log_success "Redis安装完成"
}

# 初始化数据库
init_database() {
    log_step "初始化数据库..."
    
    source .venv/bin/activate
    
    # 运行数据库初始化脚本
    python scripts/init_duckdb.py
    
    log_success "数据库初始化完成"
}

# 初始化Feast
init_feast() {
    log_step "初始化Feast特征存储..."
    
    source .venv/bin/activate
    
    # 进入Feast配置目录
    cd feast_config/feature_repo
    
    # 应用特征定义
    feast apply
    
    # 返回项目根目录
    cd ../..
    
    log_success "Feast特征存储初始化完成"
}

# 构建Go应用
build_go_app() {
    log_step "构建Go决策引擎..."
    
    cd decision_engine
    
    # 设置Go代理（中国用户）
    if [[ "${LANG}" == *"zh"* ]] || [[ "${LC_ALL}" == *"zh"* ]]; then
        export GOPROXY=https://goproxy.cn,direct
        export GOSUMDB=sum.golang.google.cn
    fi
    
    go mod tidy
    go build -o decision-engine .
    cd ..
    
    log_success "Go决策引擎构建完成"
}

# 启动服务
start_services() {
    log_step "启动系统服务..."
    
    # 确保Docker正在运行
    if ! sudo systemctl is-active --quiet docker; then
        log_info "启动Docker服务..."
        sudo systemctl start docker
    fi
    
    # 使用Docker Compose启动服务
    cd deploy
    
    # 构建并启动服务
    docker compose up -d --build
    
    cd ..
    
    log_success "系统服务启动完成"
    
    # 等待服务启动
    log_info "等待服务启动..."
    sleep 30
    
    # 检查服务状态
    check_services_status
}

# 检查服务状态
check_services_status() {
    log_step "检查服务状态..."
    
    services=(
        "Redis:6379"
        "Feast:6566"
        "决策引擎:8080"
    )
    
    for service in "${services[@]}"; do
        name=$(echo $service | cut -d: -f1)
        port=$(echo $service | cut -d: -f2)
        
        if nc -z localhost $port 2>/dev/null; then
            log_success "✓ $name 服务正常 (端口 $port)"
        else
            log_warn "✗ $name 服务异常 (端口 $port)"
        fi
    done
}

# 运行测试
run_tests() {
    log_step "运行系统测试..."
    
    source .venv/bin/activate
    
    # 运行单元测试
    if command -v pytest &> /dev/null; then
        log_info "运行pytest测试..."
        pytest tests/ -v --tb=short
    else
        log_info "运行自定义测试..."
        # 运行我们的测试脚本
        python tests/unit/test_offline_features_simple.py
        python tests/unit/test_realtime_features.py
        python tests/unit/test_strategy_decision.py
    fi
    
    # 测试绩效分析
    python performance_analysis/performance_analyzer.py
    
    # 测试回测引擎
    python backtest/backtest_engine.py
    
    log_success "系统测试完成"
}

# 创建启动脚本
create_startup_scripts() {
    log_step "创建启动脚本..."
    
    mkdir -p scripts/startup
    
    # 创建系统启动脚本
    cat > scripts/startup/start_system.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "🚀 启动量化分析系统..."

# 激活虚拟环境
source .venv/bin/activate

# 启动Docker服务
cd deploy
docker compose up -d
cd ..

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 15

# 检查服务状态
echo "🔍 检查服务状态..."
curl -f http://localhost:8080/health && echo "✅ 决策引擎正常"
curl -f http://localhost:6566/health && echo "✅ Feast服务正常"

echo "🎉 系统启动完成！"
echo "📊 访问看板: http://localhost:8501"
echo "🤖 决策引擎: http://localhost:8080"
echo "🍽️ Feast服务: http://localhost:6566"
EOF

    # 创建看板启动脚本
    cat > scripts/startup/start_dashboard.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "📊 启动量化分析看板..."

# 激活虚拟环境
source .venv/bin/activate

# 启动Streamlit看板
streamlit run dashboard/integrated_dashboard.py --server.port 8501 --server.address 0.0.0.0
EOF

    # 创建停止脚本
    cat > scripts/startup/stop_system.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "🛑 停止量化分析系统..."

# 停止Docker服务
cd deploy
docker compose down
cd ..

echo "✅ 系统已停止"
EOF

    # 设置执行权限
    chmod +x scripts/startup/*.sh
    
    log_success "启动脚本创建完成"
}

# 显示使用说明
show_usage() {
    log_step "部署完成！"
    
    echo -e "${GREEN}"
    cat << "EOF"
    ╔══════════════════════════════════════════════════════════════╗
    ║                        部署成功！                            ║
    ╚══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    
    echo -e "${CYAN}🌐 系统访问地址:${NC}"
    echo "  📊 集成看板: http://localhost:8501"
    echo "  🤖 决策引擎: http://localhost:8080"
    echo "  🍽️ Feast服务: http://localhost:6566"
    echo "  🔴 Redis: localhost:6379"
    echo ""
    
    echo -e "${CYAN}🚀 快速启动命令:${NC}"
    echo "  ./scripts/startup/start_system.sh     # 启动系统"
    echo "  ./scripts/startup/start_dashboard.sh  # 启动看板"
    echo "  ./scripts/startup/stop_system.sh      # 停止系统"
    echo ""
    
    echo -e "${CYAN}📊 Docker管理:${NC}"
    echo "  cd deploy && docker compose ps        # 查看服务状态"
    echo "  cd deploy && docker compose logs -f   # 查看日志"
    echo "  cd deploy && docker compose restart   # 重启服务"
    echo ""
    
    echo -e "${CYAN}🐍 Python环境:${NC}"
    echo "  source .venv/bin/activate             # 激活环境"
    echo "  uv pip list                           # 查看包列表"
    echo "  uv pip install <package>              # 安装新包"
    echo ""
    
    echo -e "${CYAN}🧪 测试命令:${NC}"
    echo "  pytest tests/                        # 运行所有测试"
    echo "  python performance_analysis/performance_analyzer.py  # 绩效分析"
    echo "  python backtest/backtest_engine.py   # 策略回测"
    echo ""
    
    echo -e "${YELLOW}⚠️  注意事项:${NC}"
    echo "  • 首次安装Docker后请重新登录或运行: newgrp docker"
    echo "  • 确保防火墙允许相应端口访问"
    echo "  • 生产环境使用前请修改默认配置"
    echo ""
    
    echo -e "${PURPLE}🎉 感谢使用量化分析系统！${NC}"
}

# 错误处理
error_handler() {
    local line_no=$1
    local error_code=$2
    log_error "脚本在第 $line_no 行发生错误，退出码: $error_code"
    log_error "请检查日志并重试"
    exit $error_code
}

# 主函数
main() {
    # 设置错误处理
    trap 'error_handler ${LINENO} $?' ERR
    
    # 显示横幅
    show_banner
    
    log_info "开始量化分析系统部署（使用uv）..."
    
    # 检查权限
    check_root
    
    # 检测系统
    detect_os
    
    # 安装系统依赖
    install_system_dependencies
    
    # 安装uv
    install_uv
    
    # 安装Python
    install_python
    
    # 创建虚拟环境
    create_virtual_environment
    
    # 安装Python依赖
    install_python_dependencies
    
    # 安装其他依赖
    install_docker
    install_go
    install_redis
    
    # 初始化系统
    init_database
    init_feast
    
    # 构建应用
    build_go_app
    
    # 创建启动脚本
    create_startup_scripts
    
    # 启动服务
    start_services
    
    # 运行测试
    run_tests
    
    # 显示使用说明
    show_usage
    
    log_success "量化分析系统部署完成！"
}

# 执行主函数
main "$@"