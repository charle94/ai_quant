#!/usr/bin/env python3
"""
快速启动和测试脚本
"""
import subprocess
import time
import requests
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_command(cmd, cwd=None):
    """运行命令"""
    try:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr
    except Exception as e:
        return False, str(e)

def check_service(url, service_name):
    """检查服务状态"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            logger.info(f"✅ {service_name} 服务正常")
            return True
        else:
            logger.error(f"❌ {service_name} 服务异常: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ {service_name} 服务无法访问: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 量化分析系统快速启动...")
    
    project_root = Path(__file__).parent.parent
    
    # 1. 检查Python虚拟环境
    logger.info("📦 检查Python环境...")
    venv_path = project_root / "venv"
    if not venv_path.exists():
        logger.error("❌ Python虚拟环境不存在，请先运行部署脚本")
        return False
    
    # 2. 初始化数据库
    logger.info("🗄️ 初始化数据库...")
    success, output = run_command(
        f"source {venv_path}/bin/activate && python scripts/init_duckdb.py",
        cwd=project_root
    )
    if success:
        logger.info("✅ 数据库初始化完成")
    else:
        logger.warning(f"⚠️ 数据库初始化可能有问题: {output}")
    
    # 3. 启动Docker服务
    logger.info("🐳 启动Docker服务...")
    deploy_path = project_root / "deploy"
    success, output = run_command("docker-compose up -d", cwd=deploy_path)
    if success:
        logger.info("✅ Docker服务启动中...")
    else:
        logger.error(f"❌ Docker服务启动失败: {output}")
        return False
    
    # 4. 等待服务启动
    logger.info("⏳ 等待服务启动 (30秒)...")
    time.sleep(30)
    
    # 5. 检查服务状态
    logger.info("🔍 检查服务状态...")
    services = [
        ("http://localhost:6379", "Redis"),
        ("http://localhost:6566/health", "Feast"),
        ("http://localhost:8080/health", "决策引擎")
    ]
    
    all_healthy = True
    for url, name in services:
        if not check_service(url, name):
            all_healthy = False
    
    if not all_healthy:
        logger.error("❌ 部分服务未正常启动，请检查日志")
        return False
    
    # 6. 运行快速测试
    logger.info("🧪 运行快速测试...")
    
    # 测试决策引擎API
    try:
        response = requests.get("http://localhost:8080/signals", timeout=10)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ 决策引擎API测试通过，获取到 {data.get('count', 0)} 个信号")
        else:
            logger.warning(f"⚠️ 决策引擎API返回异常状态: {response.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ 决策引擎API测试失败: {e}")
    
    # 测试特征推送
    try:
        success, output = run_command(
            f"source {venv_path}/bin/activate && python feast_config/push_features.py",
            cwd=project_root
        )
        if success:
            logger.info("✅ 特征推送测试通过")
        else:
            logger.warning(f"⚠️ 特征推送测试失败: {output}")
    except Exception as e:
        logger.warning(f"⚠️ 特征推送测试异常: {e}")
    
    # 7. 显示系统信息
    logger.info("📊 系统信息:")
    logger.info("  - 决策引擎API: http://localhost:8080")
    logger.info("  - Feast服务: http://localhost:6566") 
    logger.info("  - Redis: localhost:6379")
    logger.info("")
    logger.info("🔧 常用命令:")
    logger.info("  - 查看服务状态: cd deploy && docker-compose ps")
    logger.info("  - 查看日志: cd deploy && docker-compose logs -f")
    logger.info("  - 停止服务: cd deploy && docker-compose down")
    logger.info("")
    logger.info("🎉 系统启动完成！")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)