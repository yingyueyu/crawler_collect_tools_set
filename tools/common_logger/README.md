# common_logger

统一日志组件：控制台彩色输出、按日期与大小滚动的文件日志、多进程/多任务场景下的 logger 隔离。

## 特性

- 控制台与文件**分级**输出（`console_level` / `file_level`）
- Windows 终端 ANSI 颜色（可选 `colorama`）
- 文件命名：`{logger_name}_{YYYYMMDD}_{index}.log`，单文件超过 `max_file_mb` 自动切分
- 同名 logger 重复配置时复用已有 handler，避免重复打日志

## 安装依赖

标准库 `logging` 即可；Windows 彩色可选：

```bash
pip install colorama
```

## 快速使用

```python
from tools.common_logger import get_default_logger

logger = get_default_logger(
    name="my_batch_job",
    log_dir="./logs",
    level="INFO",
    max_file_mb=50,
    force_color=True,
)

logger.info("任务开始")
logger.warning("可重试异常")
logger.error("失败", exc_info=True)
```

## 进阶配置

```python
from tools.common_logger import LoggerConfig, get_logger

logger = get_logger(
    LoggerConfig(
        name="api_data",
        log_dir="./logs",
        level="DEBUG",
        console_level="INFO",
        file_level="DEBUG",
        max_file_mb=100,
        encoding="utf-8",
        force_color=None,  # None=自动检测 TTY
    )
)
```

## 按任务名创建 logger

```python
from tools.common_logger.logger_common import get_task_logger

# 名称会清洗为安全文件名，日志文件前缀与之一致
logger = get_task_logger("npm-tag-classify-worker-3", log_dir="./logs")
```

## 主要 API

| 函数 | 说明 |
|------|------|
| `get_default_logger(name, log_dir, level, max_file_mb, force_color)` | 常用入口 |
| `get_logger(LoggerConfig)` | 完整配置 |
| `get_task_logger(task_name, ...)` | 任务维度快捷封装 |
| `reset_logger(name)` | 移除本模块创建的 handler，便于测试或重启 |

## 日志格式示例

```
2026-05-29 10:00:00 | INFO     | tag_classify_npm       | batch_runner.py:120 | 待处理仓库总数: 12345
```

## 使用建议

- 批处理、CLI 入口统一使用 `get_default_logger` 或 `get_task_logger`，与 `tools/component_tag_classify` 保持一致。
- 长任务将 `log_dir` 指向固定目录（如 `./logs`），便于与失败 CSV 对照排查。
- 单元测试或重复初始化时可调用 `reset_logger(name)` 清理 handler。

## 相关模块

- `tools/component_tag_classify`：批处理默认 logger
- `tools/utils/log_utils.py`：旧版简易日志，新代码优先用本模块
