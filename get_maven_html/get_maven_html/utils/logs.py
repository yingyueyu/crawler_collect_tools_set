from loguru import logger
import os
import sys
from datetime import datetime
import inspect
from typing import Optional, Dict

# 存储已初始化模块的配置
_MODULE_CONFIGS: Dict[str, dict] = {}
# 存储模块专用的logger实例
_MODULE_LOGGERS = {}
# 控制台handler是否已添加
_CONSOLE_HANDLER_ADDED = False


def get_caller_info():
    """获取真正的调用者信息（跳过日志封装层）"""
    stack = inspect.stack()
    for frame in stack[2:]:  # 跳过前两层
        module = inspect.getmodule(frame.frame)
        if module and module.__file__ != __file__:
            return {
                "caller_function": frame.function,
                "caller_lineno": frame.lineno,
                "caller_file": os.path.basename(module.__file__)
            }
    return {
        "caller_function": "unknown",
        "caller_lineno": -1,
        "caller_file": "unknown"
    }


class EnhancedLogger:
    def __init__(
            self,
            module_name: str,
            log_dir: str = "logs",
            level: str = "DEBUG",
            rotation: str = "100 MB",
            retention: str = "30 days",
            backtrace: bool = True,
            diagnose: bool = True,
            enqueue: bool = True,
    ):
        self.module_name = module_name
        self.log_dir = os.path.join(log_dir, module_name)  # 模块专属目录
        self.level = level
        self.rotation = rotation
        self.retention = retention
        self.backtrace = backtrace
        self.diagnose = diagnose
        self.enqueue = enqueue

        # 创建模块专属日志目录
        os.makedirs(self.log_dir, exist_ok=True)

        # 设置文件handler
        self._setup_handlers()

    def _setup_handlers(self):
        """为当前模块设置各级别日志handler，确保精确级别过滤"""
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in levels:
            log_path = os.path.join(
                self.log_dir,
                f"{self.module_name}_{level.lower()}.log"
            )

            # 添加精确级别过滤的handler
            logger.add(
                log_path,
                level=level,  # 设置最低记录级别
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[module]} | {file}:{function}:{line} - {message}",
                rotation=self.rotation,
                retention=self.retention,
                backtrace=self.backtrace,
                diagnose=self.diagnose,
                enqueue=self.enqueue,
                # 关键：精确过滤当前模块和当前级别
                filter=lambda record, lvl=level:
                record["extra"].get("module") == self.module_name
                and record["level"].name == lvl
            )

    @staticmethod
    def get_logger(module_name: str = "default", **kwargs) -> logger:
        """获取模块专属logger实例"""
        global _CONSOLE_HANDLER_ADDED

        # 添加全局控制台handler（只添加一次）
        if not _CONSOLE_HANDLER_ADDED:
            logger.add(
                sys.stderr,
                level="INFO",
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[module]}</cyan> | <cyan>{file}:{function}:{line}</cyan> - <level>{message}</level>",
                backtrace=True,
                diagnose=True,
                enqueue=True,
            )
            _CONSOLE_HANDLER_ADDED = True

        # 返回已存在的logger
        if module_name in _MODULE_LOGGERS:
            return _MODULE_LOGGERS[module_name]

        # 首次创建配置
        if module_name not in _MODULE_CONFIGS:
            _MODULE_CONFIGS[module_name] = kwargs or {}

        # 创建新logger实例
        module_logger = logger.bind(module=module_name)

        # 初始化配置
        EnhancedLogger(module_name, **_MODULE_CONFIGS[module_name])

        _MODULE_LOGGERS[module_name] = module_logger
        return module_logger

    @staticmethod
    def log(
            message: str,
            level: str = "INFO",
            extra: Optional[dict] = None,
            context: Optional[dict] = None
    ):
        """通用日志方法"""
        caller_info = get_caller_info()
        bound_logger = logger.bind(**caller_info)

        if extra:
            bound_logger = bound_logger.bind(**extra)
        if context:
            bound_logger = bound_logger.bind(**context)

        try:
            log_method = getattr(bound_logger, level.lower())
            log_method(message)
        except Exception as e:
            print(f"[LOGGER ERROR] Failed to log: {e}\nMessage: {message}")