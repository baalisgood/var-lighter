"""
套利执行系统 V3 - 入口文件

功能：
- 完整的套利执行系统（决策、执行、风险控制）
- 自动化开仓/平仓
- 多交易所套利

使用方法：
    python3 run_arbitrage_execution_v3.py

可选参数：
    --unified-config    统一配置文件路径（套利决策、执行、风险控制）
    --monitor-config    监控配置文件路径（交易对、交易所等）
    --debug             启用基础Debug模式
    --debug-detail      启用详细Debug模式
"""

# 🔥 加载环境变量（必须在其他导入之前）
from dotenv import load_dotenv
from pathlib import Path
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

import asyncio
import argparse

from core.services.arbitrage_monitor_v2.core.arbitrage_orchestrator_v3 import ArbitrageOrchestratorV3
from core.services.arbitrage_monitor_v2.config.debug_config import DebugConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="套利执行系统 V3")
    
    parser.add_argument(
        '--unified-config',
        type=str,
        default='config/arbitrage/arbitrage_unified.yaml',
        help='统一配置文件路径（套利决策、执行、风险控制）'
    )
    
    parser.add_argument(
        '--monitor-config',
        type=str,
        default='config/arbitrage/monitor_v2.yaml',
        help='监控配置文件路径（交易对、交易所等）'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用基础Debug模式'
    )
    
    parser.add_argument(
        '--debug-detail',
        action='store_true',
        help='启用详细Debug模式'
    )
    
    return parser.parse_args()


async def main():
    """主函数"""
    # 解析参数
    args = parse_args()
    
    # 创建Debug配置
    if args.debug_detail:
        debug_config = DebugConfig.create_detailed(set())
        print("🐛 详细Debug模式已启用")
    elif args.debug:
        debug_config = DebugConfig.create_basic()
        print("🐛 基础Debug模式已启用")
    else:
        debug_config = DebugConfig.create_production()
    
    # 配置文件路径
    unified_config_path = Path(args.unified_config)
    monitor_config_path = Path(args.monitor_config)
    
    # 检查配置文件是否存在
    if not unified_config_path.exists():
        print(f"⚠️  统一配置文件不存在: {unified_config_path}")
        print("使用默认配置")
        unified_config_path = None
    
    if not monitor_config_path.exists():
        print(f"⚠️  监控配置文件不存在: {monitor_config_path}")
        print("使用默认配置")
        monitor_config_path = None
    
    # 创建调度器
    orchestrator = ArbitrageOrchestratorV3(
        unified_config_path=unified_config_path,
        monitor_config_path=monitor_config_path,
        debug_config=debug_config
    )
    
    try:
        # 启动系统
        print("=" * 70)
        print("🚀 套利执行系统 V3 启动中...")
        print("=" * 70)
        print(f"📋 统一配置: {unified_config_path or '默认配置'}")
        print(f"📋 监控配置: {monitor_config_path or '默认配置'}")
        print()
        
        await orchestrator.start()
        
        print("\n✅ 系统运行中，按 Ctrl+C 停止\n")
        print("⚠️  警告：此系统将执行实际交易，请确保已正确配置风险控制参数！")
        print()
        
        # 持续运行
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n收到停止信号 (Ctrl+C)...")
    except Exception as e:
        print(f"\n❌ 系统错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 停止系统
        print("\n🛑 正在停止系统...")
        await orchestrator.stop()
        print("✅ 系统已停止")
        print("\n👋 再见！")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

