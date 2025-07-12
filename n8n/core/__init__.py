"""
Data-Diff 核心集成层
提供与 data-diff 库的集成功能
"""

from .connection_manager import ConnectionManager
from .comparison_engine import ComparisonEngine
from .result_processor import ResultProcessor
from .clickzetta_adapter import ClickzettaAdapter
from .config_manager import ConfigManager, config_manager
from .error_handler import ErrorHandler, error_handler, DataDiffError

__all__ = [
    "ConnectionManager",
    "ComparisonEngine",
    "ResultProcessor",
    "ClickzettaAdapter",
    "ConfigManager",
    "config_manager",
    "ErrorHandler",
    "error_handler",
    "DataDiffError"
]
