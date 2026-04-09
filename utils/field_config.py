import yaml
import os
from typing import List, Dict
from core.logger import logger


class UserManager:
    _instance = None
    _initialized = False

    def __new__(cls, config_path: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_path: str = None):
        if UserManager._initialized and config_path is None:
            return
        
        self.config_path = config_path or 'config.yaml'
        self._config = self._load_config()
        self._cookie_config = self._load_cookie_config()
        UserManager._initialized = True

    def _load_config(self) -> Dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _load_cookie_config(self) -> Dict:
        cookie_path = os.path.join(os.path.dirname(self.config_path), 'cookie.yaml')
        try:
            if os.path.exists(cookie_path):
                with open(cookie_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载cookie配置文件失败: {e}")
        return {}

    def get_active_users(self) -> List[Dict]:
        users = []
        for user in self._config.get('users', []):
            sec_uid = user.get('sec_uid', '')
            if sec_uid.startswith('#'):
                continue
            if not user.get('enabled', True):
                continue
            users.append(user)
        return users

    def get_all_users(self) -> List[Dict]:
        return self._config.get('users', [])

    def get_user_tasks(self, user: Dict) -> Dict[str, bool]:
        return {
            'videos': user.get('collect_videos', True),
            'comments': user.get('collect_comments', True),
            'replies': user.get('collect_replies', True)
        }

    def get_crawler_config(self) -> Dict:
        return self._config.get('crawler', {
            'page_size': 30,
            'max_retries': 3,
            'request_delay': 1.0,
            'download_threads': 5,
            'timeout': 60
        })

    def get_cookie(self) -> str:
        return self._cookie_config.get('cookie', '')

    def get_storage_config(self) -> Dict:
        default = {
            'video': {'database': True, 'csv': True},
            'comment': {'database': True, 'csv': True},
            'reply': {'database': True, 'csv': True}
        }
        return self._config.get('storage', default)

    def get_storage_for_type(self, data_type: str) -> Dict:
        storage = self.get_storage_config()
        return storage.get(data_type, {'database': True, 'csv': True})

    def get_media_download_config(self) -> Dict:
        default = {
            'video': {'images': True, 'videos': True, 'thumbs': True},
            'comment': {'images': True, 'avatars': True, 'stickers': True},
            'reply': {'images': True, 'avatars': True, 'stickers': True}
        }
        return self._config.get('media_download', default)

    def get_media_download_for_type(self, data_type: str) -> Dict:
        config = self.get_media_download_config()
        return config.get(data_type, {})

    def should_download_media(self, data_type: str, media_type: str) -> bool:
        type_config = self.get_media_download_for_type(data_type)
        return type_config.get(media_type, False)

    def get_fields(self, data_type: str) -> List[str]:
        fields = self._config.get('fields', {}).get(data_type, [])
        return [f for f in fields if f and not str(f).startswith('#')]

    def filter_data(self, data: Dict, data_type: str) -> Dict:
        export_fields = self.get_fields(data_type)
        return {k: v for k, v in data.items() if k in export_fields}

    def filter_batch(self, data_list: List[Dict], data_type: str) -> List[Dict]:
        return [self.filter_data(d, data_type) for d in data_list]
