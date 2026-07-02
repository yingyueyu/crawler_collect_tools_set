from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import Optional


_LOCK = RLock()
_CONFIGURED_KEYS: set[str] = set()


def _to_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    s = str(level).strip().upper()
    if not s:
        return logging.INFO
    return logging._nameToLevel.get(s, logging.INFO)


def _stream_supports_color(stream) -> bool:
    try:
        return hasattr(stream, "isatty") and stream.isatty()
    except Exception:
        return False


def _enable_windows_ansi() -> None:
    """
    Try to enable ANSI color on Windows.
    - If colorama exists, initialize it (no hard dependency).
    - Otherwise rely on Windows 10+ terminal ANSI support.
    """
    if os.name != "nt":
        return
    try:
        import colorama  # type: ignore

        colorama.just_fix_windows_console()
    except Exception:
        # Best-effort: modern terminals usually support ANSI already
        return


def _sanitize_task_name(task_name: str) -> str:
    raw = (task_name or "").strip()
    if not raw:
        return "task"
    safe = re.sub(r"[^0-9A-Za-z_.-]+", "_", raw)
    safe = safe.strip("._-")
    return safe or "task"


class _Ansi:
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"
    WHITE = "\x1b[37m"
    BLUE = "\x1b[34m"
    BRIGHT_CYAN = "\x1b[96m"
    CYAN = "\x1b[36m"
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    RED = "\x1b[31m"
    MAGENTA = "\x1b[35m"


class ColorFormatter(logging.Formatter):
    """
    Colorize console logs by level. File logs should use a non-colored formatter.
    """

    LEVEL_COLOR = {
        logging.DEBUG: _Ansi.WHITE,
        logging.INFO: _Ansi.WHITE,
        logging.WARNING: _Ansi.YELLOW,
        logging.ERROR: _Ansi.RED,
        logging.CRITICAL: _Ansi.RED,
    }

    def __init__(self, fmt: str, datefmt: Optional[str] = None, *, enable_color: bool = True) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._enable_color = enable_color

    def format(self, record: logging.LogRecord) -> str:
        # Field-by-field composition keeps columns strictly aligned.
        record.message = record.getMessage()
        time_block = f"{self.formatTime(record, self.datefmt):<19.19}"
        level_block = f"{record.levelname:<8.8}"
        name_block = f"{record.name:<24.24}"
        loc_block = f"{record.filename}:{record.lineno}"

        if not self._enable_color:
            out = f"{time_block} | {level_block} | {name_block} | {loc_block} | {record.message}"
            if record.exc_info:
                out = f"{out}\n{self.formatException(record.exc_info)}"
            if record.stack_info:
                out = f"{out}\n{self.formatStack(record.stack_info)}"
            return out

        level_color = self.LEVEL_COLOR.get(record.levelno, _Ansi.WHITE)
        time_block = f"{_Ansi.GREEN}{time_block}{_Ansi.RESET}"
        level_block = f"{_Ansi.BOLD}{level_color}{level_block}{_Ansi.RESET}"
        name_block = f"{_Ansi.WHITE}{name_block}{_Ansi.RESET}"
        loc_block = f"{_Ansi.BRIGHT_CYAN}{loc_block}{_Ansi.RESET}"
        msg_block = f"{_Ansi.BOLD}{level_color}{record.message}{_Ansi.RESET}"
        out = f"{time_block} | {level_block} | {name_block} | {loc_block} | {msg_block}"
        if record.exc_info:
            out = f"{out}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            out = f"{out}\n{self.formatStack(record.stack_info)}"
        return out


class DateIndexedSizeFileHandler(logging.Handler):
    """
    Single-log stream to files named: {logger_name}_{YYYYMMDD}_{index}.log

    - Max size per file: max_bytes (default 50MB)
    - If current day's file exceeds size, index += 1
    - When date changes, index resets to 1
    """

    def __init__(self, *, log_dir: str, logger_name: str, max_bytes: int = 50 * 1024 * 1024, encoding: str = "utf-8") -> None:
        super().__init__()
        self._log_dir = os.path.abspath(log_dir)
        self._logger_name = logger_name or "app"
        self._max_bytes = max(1, int(max_bytes))
        self._encoding = encoding

        self._current_date: Optional[str] = None
        self._index: int = 1
        self._fh: Optional[logging.FileHandler] = None
        self._fh_path: Optional[str] = None
        self._lock = RLock()

        os.makedirs(self._log_dir, exist_ok=True)

    def _today(self) -> str:
        return datetime.now().strftime("%Y%m%d")

    def _build_path(self, date_str: str, idx: int) -> str:
        filename = f"{self._logger_name}_{date_str}_{idx}.log"
        return os.path.join(self._log_dir, filename)

    def _pick_existing_index(self, date_str: str) -> int:
        """
        Find last usable index for today.
        If the last file is already >= max_bytes, return next index.
        """
        prefix = f"{self._logger_name}_{date_str}_"
        best = 0
        try:
            for fn in os.listdir(self._log_dir):
                if not (fn.startswith(prefix) and fn.endswith(".log")):
                    continue
                mid = fn[len(prefix) : -4]
                if mid.isdigit():
                    best = max(best, int(mid))
        except Exception:
            return 1
        if best <= 0:
            return 1
        path = self._build_path(date_str, best)
        try:
            if os.path.exists(path) and os.path.getsize(path) >= self._max_bytes:
                return best + 1
        except Exception:
            return best + 1
        return best

    def _ensure_open_for_emit(self) -> None:
        date_str = self._today()
        if self._current_date != date_str:
            self._close_inner()
            self._current_date = date_str
            self._index = self._pick_existing_index(date_str)

        path = self._build_path(self._current_date, self._index)
        try:
            if os.path.exists(path) and os.path.getsize(path) >= self._max_bytes:
                self._close_inner()
                self._index += 1
                path = self._build_path(self._current_date, self._index)
        except Exception:
            # size check failed -> still try writing to current path
            pass

        if self._fh is None or self._fh_path != path:
            self._close_inner()
            self._fh = logging.FileHandler(path, encoding=self._encoding)
            self._fh_path = path
            # keep handler level/formatter consistent with this wrapper
            self._fh.setLevel(self.level)
            if self.formatter:
                self._fh.setFormatter(self.formatter)

    def emit(self, record: logging.LogRecord) -> None:
        with self._lock:
            self._ensure_open_for_emit()
            if self._fh is None:
                return
            self._fh.emit(record)

    def flush(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.flush()

    def _close_inner(self) -> None:
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass
        self._fh = None
        self._fh_path = None

    def close(self) -> None:
        with self._lock:
            self._close_inner()
        super().close()


@dataclass(frozen=True)
class LoggerConfig:
    name: str = "app"  # logger 名称
    log_dir: str = "logs" # 日志目录
    level: int | str = "INFO" # 日志级别
    console_level: int | str | None = None # 控制台日志级别
    file_level: int | str | None = None # 文件日志级别
    max_file_mb: int = 50  # 单个 log 文件最大 50MB（同一天超出则编号+1）
    encoding: str = "utf-8" # 日志文件编码
    force_color: Optional[bool] = True # 是否强制颜色（IDE 里 isatty 常为 False，所以默认开启）


def get_logger(config: LoggerConfig) -> logging.Logger:
    """
    获取 logger，如果 logger 已经存在，则返回已存在的 logger
    :param config: logger 配置
    :return: logger 对象
    """
    _enable_windows_ansi()

    name = config.name or "app"
    log_dir = config.log_dir or "logs"

    logger = logging.getLogger(name)
    logger.setLevel(_to_level(config.level))
    logger.propagate = False

    console_level = _to_level(config.console_level) if config.console_level is not None else logger.level
    file_level = _to_level(config.file_level) if config.file_level is not None else logger.level

    key = f"{name}|{os.path.abspath(log_dir)}|{console_level}|{file_level}|{int(config.max_file_mb)}|{config.encoding}"

    with _LOCK:
        if key in _CONFIGURED_KEYS:
            return logger

        os.makedirs(os.path.abspath(log_dir), exist_ok=True)

        # Avoid double handlers if logger already has some from previous configs.
        # We only remove handlers that we created (identified by attribute).
        for h in list(logger.handlers):
            if getattr(h, "_logger_common", False):
                logger.removeHandler(h)

        # Formatters
        # Keep key columns aligned like loguru style:
        # asctime(19) | levelname(8) | name(24)
        base_fmt = "%(asctime)s | %(levelname)-8s | %(name)-24.24s | %(filename)s:%(lineno)d | %(message)s"
        date_fmt = "%Y-%m-%d %H:%M:%S"

        stream = sys.stderr
        enable_color = config.force_color if config.force_color is not None else _stream_supports_color(stream)
        console_formatter = ColorFormatter(base_fmt, datefmt=date_fmt, enable_color=enable_color)
        file_formatter = logging.Formatter(base_fmt, datefmt=date_fmt)

        # Console handler (colored)
        sh = logging.StreamHandler(stream)
        sh.setLevel(console_level)
        sh.setFormatter(console_formatter)
        sh._logger_common = True  # type: ignore[attr-defined]
        logger.addHandler(sh)

        # Single file handler (date + index + size split)
        fh = DateIndexedSizeFileHandler(
            log_dir=log_dir,
            logger_name=name,
            max_bytes=max(1, int(config.max_file_mb)) * 1024 * 1024,
            encoding=config.encoding,
        )
        fh.setLevel(file_level)
        fh.setFormatter(file_formatter)
        fh._logger_common = True  # type: ignore[attr-defined]
        logger.addHandler(fh)

        _CONFIGURED_KEYS.add(key)

    return logger


def get_default_logger(
    name: str = "app",
    *,
    log_dir: str = "./logs",
    level: int | str = "INFO",
    max_file_mb: int = 50,
    force_color: Optional[bool] = True,
) -> logging.Logger:
    """
    获取默认 logger
    :param name: logger 名称
    :param log_dir: 日志目录
    :param level: 日志级别
    :param max_file_mb: 单文件最大 MB
    :param force_color: 是否强制颜色
    :return: logger 对象
    """
    return get_logger(
        LoggerConfig(
            name=name,
            log_dir=log_dir,
            level=level,
            max_file_mb=max_file_mb,
            force_color=force_color,
        )
    )


def get_task_logger(
    task_name: str,
    *,
    log_dir: str = "./logs",
    level: int | str = "INFO",
    max_file_mb: int = 50,
    force_color: Optional[bool] = True,
) -> logging.Logger:
    """
    任务维度快捷封装：
    - logger 名称使用清洗后的 task_name
    - 文件前缀也对应 task_name，命名规则保持 {task_name}_{YYYYMMDD}_{index}.log
    """
    logger_name = _sanitize_task_name(task_name)
    return get_default_logger(
        name=logger_name,
        log_dir=log_dir,
        level=level,
        max_file_mb=max_file_mb,
        force_color=force_color,
    )


def reset_logger(name: str) -> None:
    """
    重置 logger
    :param name: logger 名称
    """
    with _LOCK:
        logger = logging.getLogger(name)
        for h in list(logger.handlers):
            if getattr(h, "_logger_common", False):
                logger.removeHandler(h)
                try:
                    h.close()
                except Exception:
                    pass

        # Clear configured keys for this logger name
        for k in list(_CONFIGURED_KEYS):
            if k.startswith(f"{name}|"):
                _CONFIGURED_KEYS.remove(k)

