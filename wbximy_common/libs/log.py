# encoding=utf8

import sys
import inspect
import logging.config
from loguru import logger
from wbximy_common.libs.env import get_proj_dir

# https://github.com/Delgan/loguru?tab=readme-ov-file#entirely-compatible-with-standard-logging
# https://loguru.readthedocs.io/en/stable/_modules/loguru/_logger.html#Logger.add


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        # level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logger(
        use_file_log=False,  # 使用标准输出作为日志 此时后续选项无效
        app_name='main',  # 应用名称
        backup_count=3,  # 备份数
        rotate_mode=None,  # 按X备份 H D
        process_safe=False,  # similar as thread_safe 是否进程安全
        debug=False,
):
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    fmt_str = '<c>{level:7s} {thread.name:12s} {time:YYYY-MM-DD HH:mm:ss} {file:14s}:{line:03d}</c> - {message}'
    level = 'INFO' if not debug else 'DEBUG'

    logger_kwargs = dict(format=fmt_str, enqueue=process_safe, level=level)
    stdout_logger_kwargs = dict(format=fmt_str, level=level, sink=sys.stdout)
    if not use_file_log:
        logger_kwargs['sink'] = sys.stdout
        handlers = [logger_kwargs, ]
    else:
        handlers = [logger_kwargs, stdout_logger_kwargs]
        sink = f'{get_proj_dir()}/logs/{app_name}.log'
        rotation = None
        if rotate_mode == 'D':
            sink += '.{time:YYYY_MM_DD}'
            rotation = '00:00'
        elif rotate_mode == 'H':
            sink += '.{time:YYYY_MM_DD_HH}'
            rotation = '1h'
        elif rotate_mode == 'SIZE':
            # 这时retention可能不合适，待测试
            sink += '.{time:YYYY_MM_DD_HH}'
            rotation = '1 GB'
        logger_kwargs.update(sink=sink)
        logger_kwargs.update(encoding='utf8')
        logger_kwargs.update(rotation=rotation)
        logger_kwargs.update(retention=f'{backup_count} days')

    # logger.add(**logger_kwargs)
    logger.configure(handlers=handlers)
    return logger
