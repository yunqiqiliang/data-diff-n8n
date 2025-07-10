"""
配置管理器
负责管理整个 N8N 集成的配置
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from .database_registry import database_registry
import yaml


class ConfigManager:
    """
    配置管理器
    统一管理 N8N data-diff 集成的各种配置
    """

    def __init__(self, config_file: Optional[str] = None, config_dict: Optional[dict] = None):
        self.logger = logging.getLogger(__name__)
        self.config_dict = config_dict
        if config_dict is not None:
            self.config = config_dict
            self.config_file = None
            return
        if config_file is not None and not isinstance(config_file, str):
            self.logger.warning(f"ConfigManager: config_file 参数类型错误，期望 str 或 None，实际为 {type(config_file)}，将忽略并使用默认路径。")
            config_file = None
        self.config_file = config_file or os.getenv(
            'N8N_DATA_DIFF_CONFIG',
            '/etc/n8n-data-diff/config.yaml'
        )
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件

        Returns:
            配置字典
        """
        # 默认配置
        default_config = {
            "connections": {
                "max_pool_size": 10,
                "connection_timeout": 30,
                "idle_timeout": 300,
                "retry_attempts": 3,
                "retry_delay": 5
            },
            "comparison": {
                "default_algorithm": "joindiff",
                "max_table_size": 10000000,  # 1000万行
                "sample_size": 100000,  # 10万行采样
                "chunk_size": 50000,  # 5万行分块
                "parallel_workers": 4,
                "timeout": 3600  # 1小时超时
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": "/var/log/n8n-data-diff/app.log",
                "max_size": "10MB",
                "backup_count": 5
            },
            "security": {
                "encrypt_credentials": True,
                "credential_timeout": 3600,
                "allowed_ips": [],
                "rate_limit": {
                    "requests_per_minute": 100,
                    "requests_per_hour": 1000
                }
            },
            "monitoring": {
                "enabled": True,
                "metrics_endpoint": "/metrics",
                "health_check_endpoint": "/health",
                "alert_thresholds": {
                    "error_rate": 0.05,
                    "response_time": 30.0,
                    "memory_usage": 0.8
                }
            },
            "cache": {
                "enabled": True,
                "type": "redis",
                "ttl": 3600,
                "max_size": "1GB"
            }
        }

        # 从数据库注册表动态生成数据库配置
        database_configs = self._generate_database_configs()
        default_config.update(database_configs)

        # 尝试加载配置文件
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                        file_config = yaml.safe_load(f)
                    else:
                        file_config = json.load(f)

                # 合并配置
                self._merge_config(default_config, file_config)
                self.logger.info(f"Configuration loaded from {self.config_file}")

            except Exception as e:
                self.logger.warning(f"Failed to load config file {self.config_file}: {e}")
                self.logger.info("Using default configuration")
        else:
            self.logger.info("Config file not found, using default configuration")

        return default_config

    def _merge_config(self, default_config: Dict[str, Any], file_config: Dict[str, Any]):
        """
        合并配置字典

        Args:
            default_config: 默认配置
            file_config: 文件配置
        """
        for key, value in file_config.items():
            if key in default_config and isinstance(default_config[key], dict) and isinstance(value, dict):
                self._merge_config(default_config[key], value)
            else:
                default_config[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值

        Args:
            key: 配置键，支持点分割格式如 'database.connection_timeout'
            default: 默认值

        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any):
        """
        设置配置值

        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

    def get_database_config(self, db_type: str) -> Dict[str, Any]:
        """
        获取特定数据库类型的配置

        Args:
            db_type: 数据库类型

        Returns:
            数据库配置
        """
        return self.get(db_type, {})

    def get_connection_config(self) -> Dict[str, Any]:
        """
        获取连接配置

        Returns:
            连接配置
        """
        return self.get('connections', {})

    def get_comparison_config(self) -> Dict[str, Any]:
        """
        获取比对配置

        Returns:
            比对配置
        """
        return self.get('comparison', {})

    def get_logging_config(self) -> Dict[str, Any]:
        """
        获取日志配置

        Returns:
            日志配置
        """
        return self.get('logging', {})

    def get_security_config(self) -> Dict[str, Any]:
        """
        获取安全配置

        Returns:
            安全配置
        """
        return self.get('security', {})

    def get_monitoring_config(self) -> Dict[str, Any]:
        """
        获取监控配置

        Returns:
            监控配置
        """
        return self.get('monitoring', {})

    def save_config(self, file_path: Optional[str] = None):
        """
        保存配置到文件

        Args:
            file_path: 保存路径，默认使用当前配置文件路径
        """
        save_path = file_path or self.config_file

        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            with open(save_path, 'w', encoding='utf-8') as f:
                if save_path.endswith('.yaml') or save_path.endswith('.yml'):
                    yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
                else:
                    json.dump(self.config, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Configuration saved to {save_path}")

        except Exception as e:
            self.logger.error(f"Failed to save configuration: {e}")
            raise

    def validate_config(self) -> List[str]:
        """
        验证配置的有效性

        Returns:
            验证错误列表
        """
        errors = []

        # 验证必需的配置项
        required_sections = ['connections', 'comparison', 'logging']
        for section in required_sections:
            if section not in self.config:
                errors.append(f"Missing required configuration section: {section}")

        # 验证连接配置
        if 'connections' in self.config:
            conn_config = self.config['connections']
            if conn_config.get('max_pool_size', 0) <= 0:
                errors.append("connections.max_pool_size must be greater than 0")
            if conn_config.get('connection_timeout', 0) <= 0:
                errors.append("connections.connection_timeout must be greater than 0")

        # 验证比对配置
        if 'comparison' in self.config:
            comp_config = self.config['comparison']
            valid_algorithms = ['joindiff', 'hashdiff']
            if comp_config.get('default_algorithm') not in valid_algorithms:
                errors.append(f"comparison.default_algorithm must be one of {valid_algorithms}")
            if comp_config.get('max_table_size', 0) <= 0:
                errors.append("comparison.max_table_size must be greater than 0")

        # 验证日志配置
        if 'logging' in self.config:
            log_config = self.config['logging']
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if log_config.get('level') not in valid_levels:
                errors.append(f"logging.level must be one of {valid_levels}")

        # 验证数据库配置与注册表的一致性
        supported_databases = self.get_supported_databases()
        for db_type in supported_databases:
            if db_type in self.config:
                db_config = self.config[db_type]
                registry_config = self.get_database_registry_config(db_type)

                # 检查必需的配置项
                if 'connect_uri_help' not in db_config:
                    errors.append(f"{db_type}.connect_uri_help is required")
                elif db_config['connect_uri_help'] != registry_config['connect_uri_help']:
                    errors.append(f"{db_type}.connect_uri_help does not match registry")

                if 'connect_uri_params' not in db_config:
                    errors.append(f"{db_type}.connect_uri_params is required")
                elif db_config['connect_uri_params'] != registry_config['connect_uri_params']:
                    errors.append(f"{db_type}.connect_uri_params does not match registry")

        return errors

    def reload_config(self):
        """
        重新加载配置
        """
        self.config = self._load_config()
        self.logger.info("Configuration reloaded")

    def get_environment_overrides(self) -> Dict[str, Any]:
        """
        获取环境变量覆盖的配置

        Returns:
            环境变量配置
        """
        env_overrides = {}

        # 基础配置的环境变量映射
        base_env_mappings = {
            'N8N_DATA_DIFF_LOG_LEVEL': 'logging.level',
            'N8N_DATA_DIFF_MAX_POOL_SIZE': 'connections.max_pool_size',
            'N8N_DATA_DIFF_CONNECTION_TIMEOUT': 'connections.connection_timeout',
            'N8N_DATA_DIFF_DEFAULT_ALGORITHM': 'comparison.default_algorithm',
            'N8N_DATA_DIFF_MAX_TABLE_SIZE': 'comparison.max_table_size'
        }

        # 动态添加数据库端口的环境变量映射
        for db_type in self.get_supported_databases():
            db_config = database_registry.DATABASES[db_type]
            if db_config.default_port:
                env_var = f'{db_type.upper()}_DEFAULT_PORT'
                config_key = f'{db_type}.default_port'
                base_env_mappings[env_var] = config_key

        for env_var, config_key in base_env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]

                # 尝试转换类型
                if config_key.endswith('.port') or config_key.endswith('.size') or config_key.endswith('.timeout'):
                    try:
                        value = int(value)
                    except ValueError:
                        self.logger.warning(f"Invalid integer value for {env_var}: {value}")
                        continue

                # 设置配置值
                keys = config_key.split('.')
                current = env_overrides
                for key in keys[:-1]:
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                current[keys[-1]] = value

        return env_overrides

    def apply_environment_overrides(self):
        """
        应用环境变量覆盖
        """
        env_overrides = self.get_environment_overrides()
        if env_overrides:
            self._merge_config(self.config, env_overrides)
            self.logger.info("Applied environment variable overrides")

    def get_config(self, key: str = None, default: Any = None, *args, **kwargs) -> Any:
        """
        获取完整配置或指定 key 的配置，支持多级 key（如 'scheduler.storage_path'）
        """
        if key is None:
            return self.config
        # 支持多级 key 访问
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    async def initialize(self):
        """
        异步初始化方法（占位），兼容 FastAPI 生命周期钩子
        """
        pass

    def _generate_database_configs(self) -> Dict[str, Any]:
        """
        从数据库注册表动态生成数据库配置

        Returns:
            数据库配置字典
        """
        database_configs = {}

        for db_name, db_config in database_registry.DATABASES.items():
            config = {
                "connect_uri_help": db_config.connect_uri_help,
                "connect_uri_params": db_config.connect_uri_params,
                "supports_unique_constraint": db_config.supports_unique_constraint,
                "supports_alphanums": db_config.supports_alphanums,
                "threading_model": db_config.threading_model
            }

            # 添加可选参数
            if db_config.connect_uri_kwparams:
                config["connect_uri_kwparams"] = db_config.connect_uri_kwparams

            if db_config.default_port:
                config["default_port"] = db_config.default_port

            if db_config.default_schema:
                config["default_schema"] = db_config.default_schema

            # 添加额外的特定于数据库的参数
            if db_config.extra_params:
                config.update(db_config.extra_params)

            database_configs[db_name] = config

        return database_configs

    def get_supported_databases(self) -> List[str]:
        """
        获取支持的数据库类型列表

        Returns:
            支持的数据库类型
        """
        return list(database_registry.DATABASES.keys())

    def is_database_supported(self, db_type: str) -> bool:
        """
        检查数据库类型是否受支持

        Args:
            db_type: 数据库类型

        Returns:
            是否支持
        """
        return db_type in database_registry.DATABASES

    def get_database_registry_config(self, db_type: str) -> Optional[Dict[str, Any]]:
        """
        从注册表获取数据库配置

        Args:
            db_type: 数据库类型

        Returns:
            数据库配置，如果不支持则返回 None
        """
        if db_type not in database_registry.DATABASES:
            return None

        db_config = database_registry.DATABASES[db_type]
        return {
            "connect_uri_help": db_config.connect_uri_help,
            "connect_uri_params": db_config.connect_uri_params,
            "connect_uri_kwparams": db_config.connect_uri_kwparams,
            "default_port": db_config.default_port,
            "default_schema": db_config.default_schema,
            "supports_unique_constraint": db_config.supports_unique_constraint,
            "supports_alphanums": db_config.supports_alphanums,
            "threading_model": db_config.threading_model,
            "extra_params": db_config.extra_params
        }


# 全局配置管理器实例
config_manager = ConfigManager()
