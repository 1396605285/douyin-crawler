import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


class Logger:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if Logger._initialized:
            return
        Logger._initialized = True
        self._system_logger = None
        self._user_loggers = {}
        self._init_system_logger()

    def _init_system_logger(self):
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)
        
        self._system_logger = logging.getLogger('system')
        self._system_logger.setLevel(logging.DEBUG)
        self._system_logger.handlers.clear()
        
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'system.log'),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        ))
        
        self._system_logger.addHandler(file_handler)
        self._system_logger.addHandler(console_handler)

    def get_user_logger(self, sec_uid: str) -> logging.Logger:
        if sec_uid in self._user_loggers:
            return self._user_loggers[sec_uid]
        
        log_path = os.path.join('data', sec_uid, 'history.log')
        
        logger = logging.getLogger(f'user_{sec_uid}')
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=2 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        logger.addHandler(file_handler)
        self._user_loggers[sec_uid] = logger
        return logger

    def info(self, msg: str):
        self._system_logger.info(msg)

    def debug(self, msg: str):
        self._system_logger.debug(msg)

    def warning(self, msg: str):
        self._system_logger.warning(msg)

    def error(self, msg: str):
        self._system_logger.error(msg)

    def log_user_action(self, sec_uid: str, action: str, stats: dict = None):
        user_logger = self.get_user_logger(sec_uid)
        
        if stats:
            flat_items = []
            for k, v in stats.items():
                if isinstance(v, dict):
                    for sk, sv in v.items():
                        flat_items.append(f"{k}.{sk}:{sv}")
                else:
                    flat_items.append(f"{k}:{v}")
            msg = f"[{action}] " + " | ".join(flat_items)
        else:
            msg = f"[{action}]"
        
        user_logger.info(msg)
        self.info(f"用户 {sec_uid[:20]}... {msg}")


logger = Logger()
