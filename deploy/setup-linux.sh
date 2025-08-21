#!/bin/bash

# 量化分析系统 - Linux一键部署脚本
# 支持 Ubuntu/Debian/CentOS/RHEL

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
}

# 安装Docker和Docker Compose
install_docker() {
    log_step "安装Docker和Docker Compose..."
    
    # 检查Docker是否已安装
    if command -v docker &> /dev/null; then
        log_info "Docker已安装，跳过安装步骤"
    else
        log_info "开始安装Docker..."
        
        # 根据不同系统安装Docker
        if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
            # Ubuntu/Debian
            sudo apt-get update
            sudo apt-get install -y \
                apt-transport-https \
                ca-certificates \
                curl \
                gnupg \
                lsb-release
            
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
            
            echo \
                "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
                $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            
            sudo apt-get update
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io
            
        elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
            # CentOS/RHEL
            sudo yum install -y yum-utils
            sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            sudo yum install -y docker-ce docker-ce-cli containerd.io
            sudo systemctl start docker
            sudo systemctl enable docker
        else
            log_error "不支持的操作系统: $OS"
            exit 1
        fi
        
        # 添加用户到docker组
        sudo usermod -aG docker $USER
        log_info "Docker安装完成"
    fi
    
    # 检查Docker Compose是否已安装
    if command -v docker-compose &> /dev/null; then
        log_info "Docker Compose已安装，跳过安装步骤"
    else
        log_info "开始安装Docker Compose..."
        sudo curl -L "https://github.com/docker/compose/releases/download/v2.21.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
        log_info "Docker Compose安装完成"
    fi
}

# 安装Python和相关工具
install_python() {
    log_step "安装Python和相关工具..."
    
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        sudo apt-get update
        sudo apt-get install -y \
            python3 \
            python3-pip \
            python3-venv \
            build-essential \
            git
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
        sudo yum groupinstall -y "Development Tools"
        sudo yum install -y python3 python3-pip git
    fi
    
    # 升级pip
    python3 -m pip install --upgrade pip
    
    log_info "Python环境安装完成"
}

# 安装Go
install_go() {
    log_step "安装Go语言环境..."
    
    if command -v go &> /dev/null; then
        log_info "Go已安装，版本: $(go version)"
    else
        log_info "开始安装Go..."
        
        GO_VERSION="1.21.5"
        curl -L "https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o go.tar.gz
        sudo rm -rf /usr/local/go
        sudo tar -C /usr/local -xzf go.tar.gz
        rm go.tar.gz
        
        # 设置环境变量
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
        export PATH=$PATH:/usr/local/go/bin
        
        log_info "Go安装完成，版本: $(go version)"
    fi
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
    go build -o decision-engine .
    cd ..
    
    log_info "Go决策引擎构建完成"
}

# 启动服务
start_services() {
    log_step "启动系统服务..."
    
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
    if curl -f http://localhost:6379 &> /dev/null; then
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

# 显示使用说明
show_usage() {
    log_step "部署完成！"
    
    echo -e "${GREEN}系统访问地址:${NC}"
    echo "  - 决策引擎API: http://localhost:8080"
    echo "  - Feast服务: http://localhost:6566"
    echo "  - Redis: localhost:6379"
    echo ""
    echo -e "${GREEN}常用命令:${NC}"
    echo "  - 查看服务状态: cd deploy && docker-compose ps"
    echo "  - 查看服务日志: cd deploy && docker-compose logs -f"
    echo "  - 停止服务: cd deploy && docker-compose down"
    echo "  - 重启服务: cd deploy && docker-compose restart"
    echo ""
    echo -e "${GREEN}激活Python环境:${NC}"
    echo "  source venv/bin/activate"
    echo ""
    echo -e "${GREEN}运行DBT:${NC}"
    echo "  cd dbt_project && dbt run"
    echo ""
    echo -e "${YELLOW}注意: 如果这是首次安装Docker，请重新登录以使用户组生效${NC}"
}

# 主函数
main() {
    log_info "开始量化分析系统部署..."
    
    # 检查root权限
    check_root
    
    # 检测操作系统
    detect_os
    
    # 安装依赖
    install_docker
    install_python
    install_go
    
    # 设置环境
    setup_python_env
    
    # 初始化系统
    init_database
    init_feast
    
    # 构建应用
    build_go_app
    
    # 启动服务
    start_services
    
    # 运行测试
    run_tests
    
    # 显示使用说明
    show_usage
    
    log_info "量化分析系统部署完成！"
}

# 错误处理
trap 'log_error "部署过程中发生错误，退出码: $?"' ERR

# 执行主函数
main "$@"