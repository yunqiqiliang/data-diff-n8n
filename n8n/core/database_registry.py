"""
数据库注册表
统一管理所有支持的数据库类型及其配置，确保与 data_diff/databases 保持一致
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """数据库配置类"""
    name: str
    connect_uri_help: str
    connect_uri_params: List[str]
    connect_uri_kwparams: Optional[List[str]] = None
    default_port: Optional[int] = None
    default_schema: Optional[str] = None
    supports_unique_constraint: bool = False
    supports_alphanums: bool = True
    threading_model: str = "threaded"  # "threaded" or "direct"
    extra_params: Optional[Dict[str, Any]] = None


class DatabaseRegistry:
    """
    数据库注册表
    管理所有支持的数据库类型及其配置
    基于 data_diff/databases 中的实际实现
    """

    # 数据库配置注册表 - 与 data_diff/databases 中的实际实现保持一致
    DATABASES = {
        "clickzetta": DatabaseConfig(
            name="clickzetta",
            connect_uri_help="clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>",
            connect_uri_params=["virtualcluster", "schema"],
            default_schema="public",
            threading_model="threaded",
            extra_params={
                "default_workspace": "default"
            }
        ),

        "postgresql": DatabaseConfig(
            name="postgresql",
            connect_uri_help="postgresql://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=5432,
            default_schema="public",
            supports_unique_constraint=True,
            threading_model="threaded",
            extra_params={
                "ssl_mode": "prefer",
                "application_name": "n8n-data-diff"
            }
        ),

        "mysql": DatabaseConfig(
            name="mysql",
            connect_uri_help="mysql://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=3306,
            supports_alphanums=False,
            supports_unique_constraint=True,
            threading_model="threaded",
            extra_params={
                "charset": "utf8",
                "use_unicode": True
            }
        ),

        "clickhouse": DatabaseConfig(
            name="clickhouse",
            connect_uri_help="clickhouse://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=8123,
            threading_model="threaded",
            extra_params={
                "compression": "lz4"
            }
        ),

        "snowflake": DatabaseConfig(
            name="snowflake",
            connect_uri_help="snowflake://<user>:<password>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>",
            connect_uri_params=["database", "schema"],
            connect_uri_kwparams=["warehouse"],
            threading_model="direct",
            extra_params={
                "default_warehouse": "COMPUTE_WH",
                "default_role": "PUBLIC"
            }
        ),

        "bigquery": DatabaseConfig(
            name="bigquery",
            connect_uri_help="bigquery://<project>/<dataset>",
            connect_uri_params=[],
            threading_model="direct",
            extra_params={
                "project_required": True
            }
        ),

        "redshift": DatabaseConfig(
            name="redshift",
            connect_uri_help="redshift://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=5439,
            threading_model="threaded"
        ),

        "oracle": DatabaseConfig(
            name="oracle",
            connect_uri_help="oracle://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=1521,
            threading_model="threaded",
            extra_params={
                "default_service_name": "XE"
            }
        ),

        "mssql": DatabaseConfig(
            name="mssql",
            connect_uri_help="mssql://<user>:<password>@<host>/<database>/<schema>",
            connect_uri_params=["database", "schema"],
            default_port=1433,
            threading_model="threaded"
        ),

        "duckdb": DatabaseConfig(
            name="duckdb",
            connect_uri_help="duckdb://<dbname>@<filepath>",
            connect_uri_params=["database", "dbpath"],
            default_schema="main",
            supports_unique_constraint=False,  # Temporary, until implemented
            threading_model="direct",
            extra_params={
                "memory_mode": "duckdb://:memory:"
            }
        ),

        "databricks": DatabaseConfig(
            name="databricks",
            connect_uri_help="databricks://:<access_token>@<server_hostname>/<http_path>",
            connect_uri_params=["catalog", "schema"],
            default_schema="default",
            threading_model="threaded",
            extra_params={
                "default_catalog": "hive_metastore"
            }
        ),

        "trino": DatabaseConfig(
            name="trino",
            connect_uri_help="trino://<user>@<host>/<catalog>/<schema>",
            connect_uri_params=["catalog", "schema"],
            default_port=8080,
            threading_model="direct"
        ),

        "presto": DatabaseConfig(
            name="presto",
            connect_uri_help="presto://<user>@<host>/<catalog>/<schema>",
            connect_uri_params=["catalog", "schema"],
            default_port=8080,
            default_schema="public",
            threading_model="direct"
        ),

        "vertica": DatabaseConfig(
            name="vertica",
            connect_uri_help="vertica://<user>:<password>@<host>/<database>",
            connect_uri_params=["database?"],
            default_port=5433,
            threading_model="threaded"
        )
    }

    @classmethod
    def get_supported_databases(cls) -> List[str]:
        """获取支持的数据库类型列表"""
        return list(cls.DATABASES.keys())

    @classmethod
    def get_database_config(cls, db_type: str) -> Optional[DatabaseConfig]:
        """获取指定数据库类型的配置"""
        return cls.DATABASES.get(db_type)

    @classmethod
    def is_supported(cls, db_type: str) -> bool:
        """检查数据库类型是否支持"""
        return db_type in cls.DATABASES

    @classmethod
    def get_connect_uri_help(cls, db_type: str) -> str:
        """获取连接URI帮助信息"""
        config = cls.get_database_config(db_type)
        return config.connect_uri_help if config else ""

    @classmethod
    def get_default_port(cls, db_type: str) -> Optional[int]:
        """获取默认端口"""
        config = cls.get_database_config(db_type)
        return config.default_port if config else None

    @classmethod
    def get_default_schema(cls, db_type: str) -> Optional[str]:
        """获取默认模式"""
        config = cls.get_database_config(db_type)
        return config.default_schema if config else None

    @classmethod
    def supports_unique_constraint(cls, db_type: str) -> bool:
        """检查是否支持唯一约束"""
        config = cls.get_database_config(db_type)
        return config.supports_unique_constraint if config else False

    @classmethod
    def supports_alphanums(cls, db_type: str) -> bool:
        """检查是否支持字母数字"""
        config = cls.get_database_config(db_type)
        return config.supports_alphanums if config else True

    @classmethod
    def get_threading_model(cls, db_type: str) -> str:
        """获取线程模型"""
        config = cls.get_database_config(db_type)
        return config.threading_model if config else "threaded"

    @classmethod
    def get_extra_params(cls, db_type: str) -> Dict[str, Any]:
        """获取额外参数"""
        config = cls.get_database_config(db_type)
        return config.extra_params or {} if config else {}

    @classmethod
    def build_connection_string(cls, db_type: str, config: Dict[str, Any]) -> str:
        """
        构建连接字符串 - 与 data_diff/databases 中的实际实现保持一致
        """
        if not cls.is_supported(db_type):
            raise ValueError(f"Unsupported database type: {db_type}")

        # 根据数据库类型构建连接字符串
        if db_type == "clickzetta":
            # clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>
            instance = config.get("instance", "")
            service = config.get("service", "")
            workspace = config.get("workspace", "")
            username = config.get("username", "")
            password = config.get("password", "")
            return f"clickzetta://{username}:{password}@{instance}.{service}/{workspace}"

        # 对于其他数据库，需要 host 字段
        host = config["host"]
        port = config.get("port")
        database = config.get("database", "")
        username = config.get("username", "")
        password = config.get("password", "")

        if db_type == "postgresql":
            # postgresql://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"

        elif db_type == "mysql":
            # mysql://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"mysql://{username}:{password}@{host}:{port}/{database}"

        elif db_type == "clickhouse":
            # clickhouse://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"clickhouse://{username}:{password}@{host}:{port}/{database}"

        elif db_type == "snowflake":
            # snowflake://<user>:<password>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>
            account = config.get("account", host)
            schema = config.get("schema", "PUBLIC")
            warehouse = config.get("warehouse", "COMPUTE_WH")
            return f"snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"

        elif db_type == "bigquery":
            # bigquery://<project>/<dataset>
            project = config.get("project", host)
            dataset = config.get("dataset", database)
            return f"bigquery://{project}/{dataset}"

        elif db_type == "redshift":
            # redshift://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"redshift://{username}:{password}@{host}:{port}/{database}"

        elif db_type == "oracle":
            # oracle://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"oracle://{username}:{password}@{host}:{port}/{database}"

        elif db_type == "mssql":
            # mssql://<user>:<password>@<host>/<database>/<schema>
            port = port or cls.get_default_port(db_type)
            schema = config.get("schema", "dbo")
            return f"mssql://{username}:{password}@{host}:{port}/{database}/{schema}"

        elif db_type == "duckdb":
            # duckdb://<dbname>@<filepath> or duckdb://:memory:
            filepath = config.get("filepath", database)
            if not filepath or filepath == ":memory:":
                return "duckdb://:memory:"
            return f"duckdb:///{filepath}"

        elif db_type == "databricks":
            # databricks://:<access_token>@<server_hostname>/<http_path>
            access_token = config.get("access_token", password)
            server_hostname = config.get("server_hostname", host)
            http_path = config.get("http_path", database)
            return f"databricks://:{access_token}@{server_hostname}/{http_path}"

        elif db_type == "trino":
            # trino://<user>@<host>/<catalog>/<schema>
            port = port or cls.get_default_port(db_type)
            catalog = config.get("catalog", database)
            schema = config.get("schema", "default")
            return f"trino://{username}@{host}:{port}/{catalog}/{schema}"

        elif db_type == "presto":
            # presto://<user>@<host>/<catalog>/<schema>
            port = port or cls.get_default_port(db_type)
            catalog = config.get("catalog", database)
            schema = config.get("schema", cls.get_default_schema(db_type))
            return f"presto://{username}@{host}:{port}/{catalog}/{schema}"

        elif db_type == "vertica":
            # vertica://<user>:<password>@<host>/<database>
            port = port or cls.get_default_port(db_type)
            return f"vertica://{username}:{password}@{host}:{port}/{database}"

        else:
            raise ValueError(f"Unsupported database type for connection string: {db_type}")

    @classmethod
    def validate_config(cls, db_type: str, config: Dict[str, Any]) -> List[str]:
        """
        验证数据库配置

        Args:
            db_type: 数据库类型
            config: 配置字典

        Returns:
            错误列表
        """
        errors = []

        if not cls.is_supported(db_type):
            errors.append(f"Unsupported database type: {db_type}")
            return errors

        db_config = cls.get_database_config(db_type)

        # 验证必需字段
        required_fields = []

        # 根据数据库类型添加特定的必需字段
        if db_type in ["clickzetta"]:
            required_fields.extend(["username", "password", "instance", "service", "workspace"])
        elif db_type in ["postgresql", "mysql", "clickhouse", "redshift", "oracle", "mssql", "vertica"]:
            required_fields.extend(["host", "username", "password", "database"])
        elif db_type == "snowflake":
            required_fields.extend(["username", "password", "account", "database", "schema"])
        elif db_type == "bigquery":
            required_fields.extend(["project"])
        elif db_type == "databricks":
            required_fields.extend(["access_token", "server_hostname", "http_path"])
        elif db_type in ["trino", "presto"]:
            required_fields.extend(["host", "username", "catalog", "schema"])
        elif db_type == "duckdb":
            # DuckDB 只需要 filepath 或者使用内存模式
            pass
        else:
            # 对于其他数据库，默认需要 host
            required_fields.append("host")

        # 检查必需字段
        for field in required_fields:
            if field not in config or not config[field]:
                errors.append(f"Missing required field for {db_type}: {field}")

        # 验证端口号
        if "port" in config and config["port"]:
            try:
                port = int(config["port"])
                if port <= 0 or port > 65535:
                    errors.append("Port must be between 1 and 65535")
            except (ValueError, TypeError):
                errors.append("Port must be a valid integer")

        return errors


# 创建全局实例
database_registry = DatabaseRegistry()
