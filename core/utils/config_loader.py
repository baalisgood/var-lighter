"""
统一配置加载工具
优先级：环境变量 > YAML配置文件 > 默认值
支持所有交易所的 API 密钥安全加载
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class AuthConfig:
    """认证配置数据类"""
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    private_key: str = ""
    wallet_address: str = ""
    # GRVT 特殊字段
    sub_account_id: str = ""
    # Paradex 特殊字段
    jwt_token: str = ""
    l2_address: str = ""
    # Lighter 特殊字段
    api_key_private_key: str = ""
    account_index: int = 0
    api_key_index: int = 0
    # EdgeX 特殊字段
    account_id: str = ""
    stark_private_key: str = ""


class ExchangeConfigLoader:
    """交易所配置加载器 - 统一管理所有交易所的配置加载"""
    
    # 环境变量命名规范
    ENV_KEY_PATTERNS = {
        'api_key': '{EXCHANGE}_API_KEY',
        'api_secret': '{EXCHANGE}_API_SECRET',
        'api_passphrase': '{EXCHANGE}_API_PASSPHRASE',
        'private_key': '{EXCHANGE}_PRIVATE_KEY',
        'wallet_address': '{EXCHANGE}_WALLET_ADDRESS',
        'sub_account_id': '{EXCHANGE}_SUB_ACCOUNT_ID',
        'jwt_token': '{EXCHANGE}_JWT_TOKEN',
        'l2_address': '{EXCHANGE}_L2_ADDRESS',
        'api_key_private_key': '{EXCHANGE}_API_KEY_PRIVATE_KEY',
        'account_index': '{EXCHANGE}_ACCOUNT_INDEX',
        'api_key_index': '{EXCHANGE}_API_KEY_INDEX',
        'account_id': '{EXCHANGE}_ACCOUNT_ID',
        'stark_private_key': '{EXCHANGE}_STARK_PRIVATE_KEY',
    }
    
    def __init__(self, config_dir: str = "config/exchanges"):
        self.config_dir = Path(config_dir)
    
    def load_auth_config(
        self, 
        exchange_name: str, 
        use_env: bool = True,
        config_file: Optional[str] = None
    ) -> AuthConfig:
        """
        加载交易所认证配置
        
        Args:
            exchange_name: 交易所名称 (如 'binance', 'hyperliquid', 'paradex')
            use_env: 是否优先使用环境变量 (默认 True)
            config_file: 自定义配置文件路径 (可选)
        
        Returns:
            AuthConfig: 认证配置对象
        
        优先级：
            1. 环境变量 (如果 use_env=True)
            2. YAML 配置文件
            3. 空字符串/默认值
        
        使用示例:
            >>> loader = ExchangeConfigLoader()
            >>> auth = loader.load_auth_config('paradex')
            >>> print(auth.api_key)  # 自动从环境变量或配置文件读取
        """
        exchange_upper = exchange_name.upper()
        auth_config = AuthConfig()
        
        # 1️⃣ 优先从环境变量读取
        if use_env:
            auth_config.api_key = os.getenv(f"{exchange_upper}_API_KEY", "")
            auth_config.api_secret = os.getenv(f"{exchange_upper}_API_SECRET", "")
            auth_config.api_passphrase = os.getenv(f"{exchange_upper}_API_PASSPHRASE", "")
            auth_config.private_key = os.getenv(f"{exchange_upper}_PRIVATE_KEY", "")
            auth_config.wallet_address = os.getenv(f"{exchange_upper}_WALLET_ADDRESS", "")
            auth_config.sub_account_id = os.getenv(f"{exchange_upper}_SUB_ACCOUNT_ID", "")
            auth_config.jwt_token = os.getenv(f"{exchange_upper}_JWT_TOKEN", "")
            auth_config.l2_address = os.getenv(f"{exchange_upper}_L2_ADDRESS", "")
            auth_config.api_key_private_key = os.getenv(f"{exchange_upper}_API_KEY_PRIVATE_KEY", "")
            auth_config.account_id = os.getenv(f"{exchange_upper}_ACCOUNT_ID", "")
            auth_config.stark_private_key = os.getenv(f"{exchange_upper}_STARK_PRIVATE_KEY", "")
            
            # 整数类型需要转换
            try:
                auth_config.account_index = int(os.getenv(f"{exchange_upper}_ACCOUNT_INDEX", "0"))
            except ValueError:
                auth_config.account_index = 0
            try:
                auth_config.api_key_index = int(os.getenv(f"{exchange_upper}_API_KEY_INDEX", "0"))
            except ValueError:
                auth_config.api_key_index = 0
        
        # 2️⃣ 如果环境变量没有值，从 YAML 配置文件读取
        if not auth_config.api_key and not auth_config.private_key and not auth_config.sub_account_id:
            yaml_config = self._load_yaml_config(exchange_name, config_file)
            self._merge_yaml_to_auth(auth_config, yaml_config, exchange_name)
        
        return auth_config
    
    def _load_yaml_config(
        self, 
        exchange_name: str, 
        config_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """从 YAML 文件加载配置"""
        if config_file:
            config_path = Path(config_file)
        else:
            config_path = self.config_dir / f"{exchange_name}_config.yaml"
        
        if not config_path.exists():
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            
            # 返回交易所特定的配置块
            return data.get(exchange_name, data)
        except Exception as e:
            print(f"⚠️ 加载配置文件失败 {config_path}: {e}")
            return {}
    
    def _merge_yaml_to_auth(
        self, 
        auth_config: AuthConfig, 
        yaml_config: Dict[str, Any],
        exchange_name: str
    ):
        """将 YAML 配置合并到 AuthConfig"""
        # 🔥 支持多种配置格式
        auth_block = yaml_config.get('authentication', {})
        api_block = yaml_config.get('api', {})
        api_config_block = yaml_config.get('api_config', {})
        auth_in_api = api_config_block.get('auth', {})
        extra_params = yaml_config.get('extra_params', {})
        
        # 标准字段
        auth_config.api_key = (
            auth_config.api_key or
            yaml_config.get('api_key') or
            auth_block.get('api_key') or
            ""
        )
        
        auth_config.api_secret = (
            auth_config.api_secret or
            yaml_config.get('api_secret') or
            auth_block.get('api_secret') or
            auth_block.get('private_key') or  # Backpack 使用 private_key 作为 secret
            ""
        )
        
        auth_config.api_passphrase = (
            auth_config.api_passphrase or
            yaml_config.get('api_passphrase') or
            auth_block.get('api_passphrase') or
            ""
        )
        
        auth_config.private_key = (
            auth_config.private_key or
            yaml_config.get('private_key') or
            auth_block.get('private_key') or
            ""
        )
        
        auth_config.wallet_address = (
            auth_config.wallet_address or
            yaml_config.get('wallet_address') or
            auth_block.get('wallet_address') or
            ""
        )

        # GRVT 特殊字段
        auth_config.sub_account_id = (
            auth_config.sub_account_id or
            str(extra_params.get('sub_account_id') or yaml_config.get('sub_account_id') or "").strip()
        )
        
        # Paradex 特殊字段
        auth_config.jwt_token = (
            auth_config.jwt_token or
            extra_params.get('jwt_token') or
            ""
        )
        
        auth_config.l2_address = (
            auth_config.l2_address or
            extra_params.get('l2_address') or
            ""
        )
        
        # Lighter 特殊字段
        auth_config.api_key_private_key = (
            auth_config.api_key_private_key or
            auth_in_api.get('api_key_private_key') or
            ""
        )
        
        if not auth_config.account_index:
            auth_config.account_index = auth_in_api.get('account_index', 0)
        
        if not auth_config.api_key_index:
            auth_config.api_key_index = auth_in_api.get('api_key_index', 0)
        
        # EdgeX 特殊字段
        auth_config.account_id = (
            auth_config.account_id or
            auth_block.get('account_id') or
            ""
        )
        
        auth_config.stark_private_key = (
            auth_config.stark_private_key or
            auth_block.get('stark_private_key') or
            ""
        )
        
        # 🔥 交易所特殊处理
        if exchange_name == 'hyperliquid':
            # Hyperliquid 的 api_key 和 api_secret 都使用 private_key
            if auth_config.private_key and not auth_config.api_key:
                auth_config.api_key = auth_config.private_key
            if auth_config.private_key and not auth_config.api_secret:
                auth_config.api_secret = auth_config.private_key
        
        elif exchange_name == 'lighter':
            # Lighter 的 api_key 和 api_secret 都使用 api_key_private_key
            if auth_config.api_key_private_key and not auth_config.api_key:
                auth_config.api_key = auth_config.api_key_private_key
            if auth_config.api_key_private_key and not auth_config.api_secret:
                auth_config.api_secret = auth_config.api_key_private_key
        
        elif exchange_name == 'edgex':
            # EdgeX 使用 account_id 和 stark_private_key
            if auth_config.stark_private_key and not auth_config.api_key:
                auth_config.api_key = auth_config.stark_private_key
            if auth_config.account_id and not auth_config.api_secret:
                auth_config.api_secret = auth_config.account_id
    
    @classmethod
    def get_env_var_name(cls, exchange_name: str, field_name: str) -> str:
        """
        获取指定字段的环境变量名称
        
        Args:
            exchange_name: 交易所名称
            field_name: 字段名 (如 'api_key', 'api_secret')
        
        Returns:
            环境变量名称 (如 'BINANCE_API_KEY')
        
        示例:
            >>> ExchangeConfigLoader.get_env_var_name('paradex', 'api_key')
            'PARADEX_API_KEY'
        """
        pattern = cls.ENV_KEY_PATTERNS.get(field_name)
        if not pattern:
            return f"{exchange_name.upper()}_{field_name.upper()}"
        return pattern.format(EXCHANGE=exchange_name.upper())
    
    @classmethod
    def print_env_template(cls, exchange_name: str):
        """
        打印指定交易所的环境变量模板
        
        Args:
            exchange_name: 交易所名称
        
        示例:
            >>> ExchangeConfigLoader.print_env_template('paradex')
            # Paradex 配置
            PARADEX_API_KEY=""
            PARADEX_API_SECRET=""
            ...
        """
        print(f"# {exchange_name.upper()} 配置")
        for field, pattern in cls.ENV_KEY_PATTERNS.items():
            env_name = pattern.format(EXCHANGE=exchange_name.upper())
            print(f'{env_name}=""')


# ============================================
# 便捷函数
# ============================================

def load_exchange_auth(
    exchange_name: str,
    use_env: bool = True,
    config_file: Optional[str] = None
) -> AuthConfig:
    """
    便捷函数：加载交易所认证配置
    
    Args:
        exchange_name: 交易所名称
        use_env: 是否优先使用环境变量
        config_file: 自定义配置文件路径
    
    Returns:
        AuthConfig 对象
    
    示例:
        >>> from core.utils.config_loader import load_exchange_auth
        >>> auth = load_exchange_auth('paradex')
        >>> print(auth.api_key)
    """
    loader = ExchangeConfigLoader()
    return loader.load_auth_config(exchange_name, use_env, config_file)


def print_env_template(exchange_name: str):
    """
    便捷函数：打印环境变量模板
    
    Args:
        exchange_name: 交易所名称
    
    示例:
        >>> from core.utils.config_loader import print_env_template
        >>> print_env_template('paradex')
    """
    ExchangeConfigLoader.print_env_template(exchange_name)


if __name__ == "__main__":
    # 测试示例
    print("=" * 80)
    print("交易所配置加载工具 - 测试")
    print("=" * 80)
    
    # 示例1：加载 Paradex 配置
    print("\n【示例1】加载 Paradex 配置:")
    print("-" * 80)
    loader = ExchangeConfigLoader()
    paradex_auth = loader.load_auth_config('paradex')
    print(f"API Key: {paradex_auth.api_key[:20] if paradex_auth.api_key else '(未配置)'}...")
    print(f"JWT Token: {paradex_auth.jwt_token[:20] if paradex_auth.jwt_token else '(未配置)'}...")
    print(f"L2 Address: {paradex_auth.l2_address[:20] if paradex_auth.l2_address else '(未配置)'}...")
    
    # 示例2：打印环境变量模板
    print("\n【示例2】Paradex 环境变量模板:")
    print("-" * 80)
    ExchangeConfigLoader.print_env_template('paradex')
    
    print("\n【示例3】Binance 环境变量模板:")
    print("-" * 80)
    ExchangeConfigLoader.print_env_template('binance')

