#!/bin/bash

# 量化分析系统 - macOS一键部署脚本 (使用uv)

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
    ║       DBT + DuckDB + Feast + RuleGo + Rill Data + uv       ║
    ║                     macOS Edition                            ║
    ╚══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

# 检查macOS版本
check_macos() {
    if [[ "$OSTYPE" != "darwin"* ]]; then
        log_error "此脚本仅适用于macOS系统"
        exit 1
    fi
    
    MACOS_VERSION=$(sw_vers -productVersion)
    log_info "检测到macOS版本: $MACOS_VERSION"
    
    # 检查是否为Apple Silicon
    if [[ $(uname -m) == "arm64" ]]; then
        log_info "检测到Apple Silicon (M1/M2/M3) 处理器"
        ARCH="arm64"
    else
        log_info "检测到Intel处理器"
        ARCH="x86_64"
    fi
}

# 安装Homebrew
install_homebrew() {
    log_step "检查并安装Homebrew..."
    
    if command -v brew &> /dev/null; then
        log_info "Homebrew已安装，版本: $(brew --version | head -n1)"
        brew update
    else
        log_info "开始安装Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        
        # 添加Homebrew到PATH
        if [[ $ARCH == "arm64" ]]; then
            echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
            eval "$(/opt/homebrew/bin/brew shellenv)"
        else
            echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zprofile
            eval "$(/usr/local/bin/brew shellenv)"
        fi
        
        log_success "Homebrew安装完成"
    fi
}

# 安装基础工具
install_basic_tools() {
    log_step "安装基础工具..."
    
    # 安装必要的工具
    brew install \
        git \
        curl \
        wget \
        jq \
        tree \
        htop
    
    log_success "基础工具安装完成"
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
    echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zprofile
    
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

# 安装Docker
install_docker() {
    log_step "安装Docker..."
    
    if command -v docker &> /dev/null; then
        log_info "Docker已安装，版本: $(docker --version)"
    else
        log_info "开始安装Docker Desktop..."
        
        # 使用Homebrew安装Docker Desktop
        brew install --cask docker
        
        # 启动Docker Desktop
        open /Applications/Docker.app
        
        log_info "Docker Desktop安装完成，请等待Docker启动..."
        
        # 等待Docker启动
        while ! docker system info &> /dev/null; do
            log_info "等待Docker启动..."
            sleep 5
        done
    fi
    
    log_success "Docker环境准备完成"
}

# 安装Go
install_go() {
    log_step "安装Go语言环境..."
    
    if command -v go &> /dev/null; then
        log_info "Go已安装，版本: $(go version)"
    else
        log_info "安装Go..."
        brew install go
        
        # 设置Go环境变量
        echo 'export GOPATH=$HOME/go' >> ~/.zprofile
        echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.zprofile
        export GOPATH=$HOME/go
        export PATH=$PATH:$GOPATH/bin
        
        log_success "Go安装完成，版本: $(go version)"
    fi
}

# 安装Redis
install_redis() {
    log_step "安装Redis..."
    
    if command -v redis-server &> /dev/null; then
        log_info "Redis已安装"
    else
        log_info "安装Redis..."
        brew install redis
    fi
    
    # 启动Redis服务
    brew services start redis
    log_success "Redis服务已启动"
}

# 安装Rill Data
install_rill_data() {
    log_step "安装Rill Data..."
    
    if command -v rill &> /dev/null; then
        log_info "Rill Data已安装，版本: $(rill --version)"
    else
        log_info "安装Rill Data..."
        
        # 尝试使用Homebrew安装
        if brew tap rilldata/tap 2>/dev/null && brew install rill 2>/dev/null; then
            log_success "通过Homebrew安装Rill Data成功"
        else
            # 使用官方安装脚本
            log_info "使用官方脚本安装Rill Data..."
            curl -s https://cdn.rilldata.com/install.sh | bash
            
            # 添加到PATH
            export PATH="$HOME/.local/bin:$PATH"
            echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zprofile
        fi
        
        if command -v rill &> /dev/null; then
            log_success "Rill Data安装成功，版本: $(rill --version)"
        else
            log_warn "Rill Data安装可能失败，将在后续步骤中重试"
        fi
    fi
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
    
    # 安装看板依赖
    log_info "安装看板依赖..."
    uv pip install streamlit plotly
    
    log_success "Python依赖安装完成"
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
    
    go mod tidy
    
    # 对于Apple Silicon，确保正确的架构
    if [[ $ARCH == "arm64" ]]; then
        GOOS=darwin GOARCH=arm64 go build -o decision-engine .
    else
        GOOS=darwin GOARCH=amd64 go build -o decision-engine .
    fi
    
    cd ..
    
    log_success "Go决策引擎构建完成"
}

# 设置Rill Data项目
setup_rill_data() {
    log_step "设置Rill Data看板项目..."
    
    source .venv/bin/activate
    
    # 运行Rill集成脚本
    python performance_analysis/rill_integration_simple.py
    
    log_success "Rill Data项目设置完成"
}

# 启动服务
start_services() {
    log_step "启动系统服务..."
    
    # 确保Docker正在运行
    if ! docker system info &> /dev/null; then
        log_error "Docker未运行，请启动Docker Desktop"
        exit 1
    fi
    
    # 使用Docker Compose启动服务
    cd deploy
    docker-compose up -d --build
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
        pytest tests/ -v --tb=short || true
    else
        log_info "运行自定义测试..."
        # 运行我们的测试脚本
        python tests/unit/test_realtime_features.py || true
        python tests/unit/test_strategy_decision.py || true
    fi
    
    # 测试绩效分析
    python performance_analysis/performance_analyzer.py || true
    
    # 测试回测引擎
    python backtest/backtest_engine.py || true
    
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
docker-compose up -d
cd ..

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 15

# 检查服务状态
echo "🔍 检查服务状态..."
curl -f http://localhost:8080/health && echo "✅ 决策引擎正常"
curl -f http://localhost:6566/health && echo "✅ Feast服务正常"

echo "🎉 系统启动完成！"
echo "📊 Streamlit看板: http://localhost:8501"
echo "🤖 决策引擎: http://localhost:8080"
echo "🍽️ Feast服务: http://localhost:6566"
EOF

    # 创建Streamlit看板启动脚本
    cat > scripts/startup/start_streamlit.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "📊 启动Streamlit看板..."

# 激活虚拟环境
source .venv/bin/activate

# 启动Streamlit看板
streamlit run dashboard/integrated_dashboard.py --server.port 8501 --server.address 0.0.0.0
EOF

    # 创建Rill Data看板启动脚本
    cat > scripts/startup/start_rill.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "📈 启动Rill Data看板..."

# 检查Rill项目是否存在
if [[ ! -d "rill_project" ]]; then
    echo "⚠️  Rill项目不存在，正在创建..."
    source .venv/bin/activate
    python performance_analysis/rill_integration_simple.py
fi

# 启动Rill看板
cd rill_project
./start_rill.sh
EOF

    # 创建停止脚本
    cat > scripts/startup/stop_system.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "🛑 停止量化分析系统..."

# 停止Docker服务
cd deploy
docker-compose down
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
    echo "  📊 Streamlit看板: http://localhost:8501"
    echo "  📈 Rill Data看板: http://localhost:9009"
    echo "  🤖 决策引擎: http://localhost:8080"
    echo "  🍽️ Feast服务: http://localhost:6566"
    echo "  🔴 Redis: localhost:6379"
    echo ""
    
    echo -e "${CYAN}🚀 快速启动命令:${NC}"
    echo "  ./scripts/startup/start_system.sh      # 启动核心系统"
    echo "  ./scripts/startup/start_streamlit.sh   # 启动Streamlit看板"
    echo "  ./scripts/startup/start_rill.sh        # 启动Rill Data看板"
    echo "  ./scripts/startup/stop_system.sh       # 停止系统"
    echo ""
    
    echo -e "${CYAN}📊 Docker管理:${NC}"
    echo "  cd deploy && docker-compose ps         # 查看服务状态"
    echo "  cd deploy && docker-compose logs -f    # 查看日志"
    echo "  cd deploy && docker-compose restart    # 重启服务"
    echo ""
    
    echo -e "${CYAN}🐍 Python环境 (uv):${NC}"
    echo "  source .venv/bin/activate              # 激活环境"
    echo "  uv pip list                            # 查看包列表"
    echo "  uv pip install <package>               # 安装新包"
    echo "  uv pip sync requirements.txt           # 同步依赖"
    echo ""
    
    echo -e "${CYAN}📈 Rill Data看板:${NC}"
    echo "  cd rill_project                        # 进入Rill项目"
    echo "  ./start_rill.sh                        # 启动Rill看板"
    echo "  rill start --verbose                   # 手动启动（调试模式）"
    echo ""
    
    echo -e "${CYAN}🧪 测试命令:${NC}"
    echo "  pytest tests/                          # 运行所有测试"
    echo "  python performance_analysis/performance_analyzer.py  # 绩效分析"
    echo "  python backtest/backtest_engine.py     # 策略回测"
    echo ""
    
    echo -e "${YELLOW}⚠️  注意事项:${NC}"
    echo "  • 确保Docker Desktop正在运行"
    echo "  • Apple Silicon用户已自动配置对应架构"
    echo "  • 如遇权限问题，可能需要在系统偏好设置中授权"
    echo "  • Rill Data看板需要单独启动"
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
    
    log_info "开始量化分析系统macOS部署（使用uv）..."
    
    # 检查macOS环境
    check_macos
    
    # 安装依赖
    install_homebrew
    install_basic_tools
    install_uv
    install_python
    install_docker
    install_go
    install_redis
    install_rill_data
    
    # 设置环境
    create_virtual_environment
    install_python_dependencies
    
    # 初始化系统
    init_database
    init_feast
    
    # 构建应用
    build_go_app
    
    # 设置Rill Data
    setup_rill_data
    
    # 创建启动脚本
    create_startup_scripts
    
    # 启动服务
    start_services
    
    # 运行测试
    run_tests
    
    # 显示使用说明
    show_usage
    
    log_success "量化分析系统macOS部署完成！"
}

# 执行主函数
main "$@"