import os
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime

from core.api import DouyinAPI
from core.downloader import MediaDownloader
from core.logger import logger
from services.storage import StorageManager
from utils.helpers import safe_str, safe_int
from utils.field_config import UserManager


class BaseService(ABC):
    data_type: str = ""
    id_field: str = ""
    table_name: str = ""
    csv_filename: str = ""
    media_fields: Dict[str, str] = {}
    has_avatar_sticker: bool = False
    
    def __init__(self, sec_uid: str, cookie: str = ""):
        self.sec_uid = sec_uid
        self.api = DouyinAPI(cookie)
        self.user_manager = UserManager()
        self.storage = StorageManager(
            sec_uid=sec_uid,
            data_type=self.data_type,
            table_name=self.table_name,
            id_field=self.id_field,
            csv_filename=self.csv_filename
        )
        self.downloader: Optional[MediaDownloader] = None
        self.data_dir = os.path.join('data', sec_uid)
        os.makedirs(self.data_dir, exist_ok=True)
    
    def init_downloader(self):
        if self.downloader is None:
            config = self.user_manager.get_crawler_config()
            self.downloader = MediaDownloader(
                sec_uid=self.sec_uid,
                max_workers=config.get('download_threads', 5)
            )
    
    def _extract_user_info(self, raw_item: Dict) -> Dict:
        user = raw_item.get('user', {})
        return {
            'user_nickname': safe_str(user.get('nickname')),
            'user_unique_id': safe_str(user.get('unique_id')),
            'user_avatar': safe_str(user.get('avatar_thumb', {}).get('url_list', [None])[0]),
        }
    
    def _extract_image_urls(self, raw_item: Dict) -> Optional[str]:
        image_list = raw_item.get('image_list', [])
        if image_list and len(image_list) > 0:
            origin_url = image_list[0].get('origin_url', {})
            url_list = origin_url.get('url_list', [])
            if url_list:
                return str(url_list)
        return None
    
    def _extract_sticker_url(self, raw_item: Dict) -> Optional[str]:
        sticker = raw_item.get('sticker', {})
        static_url = sticker.get('static_url', {})
        url_list = static_url.get('url_list', [None])
        return safe_str(url_list[0] if url_list else None)
    
    def _merge_updates(self, base: Dict, new: Dict) -> None:
        for item_id, updates in new.items():
            if item_id not in base:
                base[item_id] = {}
            base[item_id].update(updates)
    
    def _merge_stats(self, total: Dict, new: Dict) -> None:
        for key, value in new.items():
            if isinstance(value, (int, float)):
                total[key] = total.get(key, 0) + value
    
    @abstractmethod
    async def fetch(self, **kwargs) -> List[Dict]:
        pass
    
    @abstractmethod
    def process(self, raw_items: List[Dict], **kwargs) -> List[Dict]:
        pass
    
    @abstractmethod
    async def run(self, **kwargs) -> Dict:
        pass
    
    async def download_media(self, items: List[Dict], aweme_id: str = None,
                             update_urls: bool = True, video_timestamp: int = None) -> Dict:
        self.init_downloader()
        
        media_config = self.user_manager.get_media_download_for_type(self.data_type)
        
        def batch_update_callback(updates: Dict):
            if updates and update_urls:
                self.storage.update_urls(updates, aweme_id, video_timestamp=video_timestamp)
        
        result = await self.downloader.download_items_media(
            items, self.id_field, self.media_fields, media_config,
            update_callback=batch_update_callback,
            video_timestamp=video_timestamp
        )
        
        if self.has_avatar_sticker:
            avatar_result = await self.downloader.download_avatars_stickers(
                items, self.id_field, media_config,
                update_callback=batch_update_callback,
                video_timestamp=video_timestamp
            )
            self._merge_updates(result['updates'], avatar_result['updates'])
            self._merge_stats(result['stats'], avatar_result['stats'])
        
        if result['updates'] and update_urls:
            self.storage.update_urls(result['updates'], aweme_id, video_timestamp=video_timestamp)
        
        result['stats']['updated'] = len(result['updates'])
        return result['stats']
    
    async def run_download_only(self, source: str = None, aweme_id: str = None, 
                                 video_timestamp: int = None, quiet: bool = False) -> Dict:
        source_name = 'CSV' if source == 'csv' else '数据库'
        if not quiet:
            logger.info(f"从{source_name}下载用户 {self.sec_uid[:20]}... 的{self.data_type}媒体文件")
        
        self.init_downloader()
        
        items = self.storage.load(source, aweme_id, video_timestamp)
        if not items:
            if not quiet:
                logger.info("没有找到本地数据")
            return self._empty_stats()
        
        fields = list(self.media_fields.keys()) + ['user_avatar', 'sticker']
        items_to_download = [item for item in items if self.downloader.has_url(item, fields)]
        
        if not items_to_download:
            if not quiet:
                logger.info("没有需要下载的媒体文件")
            return self._empty_stats()
        
        if not quiet:
            logger.info(f"找到 {len(items_to_download)} 条数据需要下载媒体")
        return await self.download_media(items_to_download, aweme_id, update_urls=True, video_timestamp=video_timestamp)
    
    async def run_download_only_multi(self, source: str = None) -> Dict:
        aweme_ids = self.storage.get_video_ids(source) if source else self.storage.get_video_ids('db')
        video_timestamps = self.storage.get_video_timestamps(source)
        
        source_name = 'CSV' if source == 'csv' else '数据库'
        logger.info(f"从{source_name}下载用户 {self.sec_uid[:20]}... 的{self.data_type}媒体文件 (共{len(aweme_ids)}个视频)")
        
        total_stats = self._empty_stats()
        videos_with_media = 0
        videos_without_media = 0
        
        for aweme_id in aweme_ids:
            video_ts = video_timestamps.get(aweme_id)
            stats = await self.run_download_only(source, aweme_id, video_timestamp=video_ts, quiet=True)
            self._merge_stats(total_stats, stats)
            if stats.get('updated', 0) > 0:
                videos_with_media += 1
            else:
                videos_without_media += 1
        
        if videos_without_media > 0:
            logger.info(f"完成: {videos_with_media}个视频有媒体需要下载, {videos_without_media}个视频无需下载")
        
        return total_stats
    
    def _empty_stats(self) -> Dict:
        stats = {'updated': 0}
        for field in self.media_fields.keys():
            stats[field] = 0
        if self.has_avatar_sticker:
            stats['avatars'] = 0
            stats['stickers'] = 0
        return stats
