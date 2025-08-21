#!/bin/bash

# 量化分析系统 - macOS一键部署脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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
        log_info "检测到Apple Silicon (M1/M2) 处理器"
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
        
        log_info "Homebrew安装完成"
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
        tree
    
    log_info "基础工具安装完成"
}

# 安装Docker
install_docker() {
    log_step "安装Docker..."
    
    if command -v docker &> /dev/null; then
        log_info "Docker已安装，版本: $(docker --version)"
    else
        log_info "开始安装Docker Desktop..."
        
        if [[ $ARCH == "arm64" ]]; then
            # Apple Silicon
            curl -L "https://desktop.docker.com/mac/main/arm64/Docker.dmg" -o Docker.dmg
        else
            # Intel
            curl -L "https://desktop.docker.com/mac/main/amd64/Docker.dmg" -o Docker.dmg
        fi
        
        # 挂载DMG并安装
        hdiutil attach Docker.dmg
        cp -R "/Volumes/Docker/Docker.app" /Applications/
        hdiutil detach "/Volumes/Docker"
        rm Docker.dmg
        
        # 启动Docker Desktop
        open /Applications/Docker.app
        
        log_info "Docker Desktop安装完成，请等待Docker启动..."
        
        # 等待Docker启动
        while ! docker system info &> /dev/null; do
            log_info "等待Docker启动..."
            sleep 5
        done
    fi
    
    # 安装Docker Compose (通常随Docker Desktop一起安装)
    if ! command -v docker-compose &> /dev/null; then
        log_info "安装Docker Compose..."
        brew install docker-compose
    fi
    
    log_info "Docker环境准备完成"
}

# 安装Python
install_python() {
    log_step "安装Python环境..."
    
    # 安装Python 3.11
    if command -v python3.11 &> /dev/null; then
        log_info "Python 3.11已安装"
    else
        log_info "安装Python 3.11..."
        brew install python@3.11
    fi
    
    # 创建符号链接
    ln -sf $(brew --prefix)/bin/python3.11 /usr/local/bin/python3
    ln -sf $(brew --prefix)/bin/pip3.11 /usr/local/bin/pip3
    
    # 升级pip
    python3 -m pip install --upgrade pip
    
    log_info "Python环境安装完成，版本: $(python3 --version)"
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
        
        log_info "Go安装完成，版本: $(go version)"
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
    log_info "Redis服务已启动"
}

# 创建Python虚拟环境
setup_python_env() {
    log_step "设置Python虚拟环境..."
    
    # 创建虚拟环境
    if [[ ! -d "venv" ]]; then
        python3 -m venv venv
        log_info "Python虚拟环境创建完成"
    fi
    
    # 激活虚拟环境并安装依赖
    source venv/bin/activate
    
    # 对于Apple Silicon，可能需要特殊处理某些包
    if [[ $ARCH == "arm64" ]]; then
        # 设置环境变量以支持某些包的编译
        export ARCHFLAGS="-arch arm64"
    fi
    
    pip install -r deploy/requirements.txt
    
    log_info "Python依赖包安装完成"
}

# 初始化数据库
init_database() {
    log_step "初始化数据库..."
    
    # 激活虚拟环境
    source venv/bin/activate
    
    # 运行数据库初始化脚本
    python scripts/init_duckdb.py
    
    log_info "数据库初始化完成"
}

# 初始化Feast
init_feast() {
    log_step "初始化Feast特征存储..."
    
    # 激活虚拟环境
    source venv/bin/activate
    
    # 进入Feast配置目录
    cd feast_config/feature_repo
    
    # 应用特征定义
    feast apply
    
    # 返回项目根目录
    cd ../..
    
    log_info "Feast特征存储初始化完成"
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
    
    log_info "Go决策引擎构建完成"
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
    docker-compose up -d
    cd ..
    
    log_info "系统服务启动完成"
    
    # 等待服务启动
    log_info "等待服务启动..."
    sleep 30
    
    # 检查服务状态
    check_services_status
}

# 检查服务状态
check_services_status() {
    log_step "检查服务状态..."
    
    # 检查Redis
    if redis-cli ping &> /dev/null; then
        log_info "✓ Redis服务正常"
    else
        log_warn "✗ Redis服务异常"
    fi
    
    # 检查Feast服务
    if curl -f http://localhost:6566/health &> /dev/null; then
        log_info "✓ Feast服务正常"
    else
        log_warn "✗ Feast服务异常"
    fi
    
    # 检查决策引擎
    if curl -f http://localhost:8080/health &> /dev/null; then
        log_info "✓ 决策引擎服务正常"
    else
        log_warn "✗ 决策引擎服务异常"
    fi
}

# 运行测试
run_tests() {
    log_step "运行系统测试..."
    
    # 激活虚拟环境
    source venv/bin/activate
    
    # 测试DBT
    cd dbt_project
    dbt debug
    cd ..
    
    # 测试特征推送
    python feast_config/push_features.py
    
    # 测试实时处理
    python -c "
from realtime_processing.miniqmt_connector import MiniQMTConnector
connector = MiniQMTConnector()
print('MiniQMT连接器测试通过')
"
    
    log_info "系统测试完成"
}

# 创建启动脚本
create_launch_scripts() {
    log_step "创建启动脚本..."
    
    # 创建启动脚本目录
    mkdir -p scripts/launch
    
    # 创建服务启动脚本
    cat > scripts/launch/start_services.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."
source venv/bin/activate

echo "启动量化分析系统服务..."
cd deploy
docker-compose up -d
cd ..

echo "等待服务启动..."
sleep 15

echo "检查服务状态..."
curl -f http://localhost:8080/health && echo "✓ 决策引擎正常"
curl -f http://localhost:6566/health && echo "✓ Feast服务正常"
redis-cli ping && echo "✓ Redis正常"

echo "系统启动完成！"
EOF

    # 创建服务停止脚本
    cat > scripts/launch/stop_services.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."

echo "停止量化分析系统服务..."
cd deploy
docker-compose down
cd ..

echo "系统已停止"
EOF

    # 创建开发模式启动脚本
    cat > scripts/launch/dev_mode.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/../.."
source venv/bin/activate

echo "启动开发模式..."

# 启动基础服务
cd deploy
docker-compose up -d redis feast-server
cd ..

# 启动实时处理器
python realtime_processing/miniqmt_connector.py &
MINIQMT_PID=$!

# 启动决策引擎
cd decision_engine
./decision-engine &
ENGINE_PID=$!
cd ..

echo "开发模式启动完成"
echo "MiniQMT PID: $MINIQMT_PID"
echo "决策引擎 PID: $ENGINE_PID"

# 等待中断信号
trap "kill $MINIQMT_PID $ENGINE_PID; exit" INT TERM

wait
EOF

    # 设置执行权限
    chmod +x scripts/launch/*.sh
    
    log_info "启动脚本创建完成"
}

# 显示使用说明
show_usage() {
    log_step "部署完成！"
    
    echo -e "${GREEN}系统访问地址:${NC}"
    echo "  - 决策引擎API: http://localhost:8080"
    echo "  - Feast服务: http://localhost:6566"
    echo "  - Redis: localhost:6379"
    echo ""
    echo -e "${GREEN}常用命令:${NC}"
    echo "  - 启动服务: ./scripts/launch/start_services.sh"
    echo "  - 停止服务: ./scripts/launch/stop_services.sh"
    echo "  - 开发模式: ./scripts/launch/dev_mode.sh"
    echo "  - 查看服务状态: cd deploy && docker-compose ps"
    echo "  - 查看服务日志: cd deploy && docker-compose logs -f"
    echo ""
    echo -e "${GREEN}激活Python环境:${NC}"
    echo "  source venv/bin/activate"
    echo ""
    echo -e "${GREEN}运行DBT:${NC}"
    echo "  cd dbt_project && dbt run"
    echo ""
    echo -e "${GREEN}macOS特别说明:${NC}"
    echo "  - 确保Docker Desktop正在运行"
    echo "  - 如遇权限问题，可能需要在系统偏好设置中授权"
    echo "  - Apple Silicon用户已自动配置对应架构"
}

# 主函数
main() {
    log_info "开始量化分析系统macOS部署..."
    
    # 检查macOS环境
    check_macos
    
    # 安装依赖
    install_homebrew
    install_basic_tools
    install_docker
    install_python
    install_go
    install_redis
    
    # 设置环境
    setup_python_env
    
    # 初始化系统
    init_database
    init_feast
    
    # 构建应用
    build_go_app
    
    # 创建启动脚本
    create_launch_scripts
    
    # 启动服务
    start_services
    
    # 运行测试
    run_tests
    
    # 显示使用说明
    show_usage
    
    log_info "量化分析系统macOS部署完成！"
}

# 错误处理
trap 'log_error "部署过程中发生错误，退出码: $?"' ERR

# 执行主函数
main "$@"