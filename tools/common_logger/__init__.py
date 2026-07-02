"""
Common Logger Module

提供统一的日志记录功能，支持多进程安全、彩色输出、文件轮转等特性。

使用示例：
    from tools.common_logger import get_logger, LoggerConfig
    
    # 方式1：使用配置对象
    config = LoggerConfig(name='my_app', log_dir='./logs')
    logger = get_logger(config)
    
    # 方式2：使用便捷函数（推荐）
    from tools.common_logger.logger_common import get_default_logger
    logger = get_default_logger(name='my_app')
"""

from .logger_common import (
    LoggerConfig,
    get_logger,
    get_default_logger,
)

__all__ = [
    "LoggerConfig",
    "get_logger",
    "get_default_logger",
]
