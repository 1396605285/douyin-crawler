import asyncio
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

from services.base_service import BaseService
from core.logger import logger
from utils.helpers import safe_str, safe_int


class VideoService(BaseService):
    data_type = "video"
    id_field = "aweme_id"
    table_name = "videos"
    csv_filename = "videos.csv"
    media_fields = {"images": "images", "video": "videos", "thumb": "thumbs"}
    has_avatar_sticker = False
    
    def __init__(self, sec_uid: str, cookie: str = "", category_id: int = 2):
        super().__init__(sec_uid, cookie)
        self.category_id = category_id
        self.init_downloader()
    
    async def fetch(self, page_size: int = 30, delay: float = 1.0,
                    limit: int = 0, **kwargs) -> List[Dict]:
        all_videos = []
        max_cursor = 0
        has_more = True
        retries = 0
        max_retries = 3
        
        pbar = tqdm(desc="采集作品", unit="条")
        
        while has_more and retries < max_retries:
            if limit > 0 and len(all_videos) >= limit:
                break
            
            try:
                response = await self.api.fetch_videos(self.sec_uid, max_cursor, page_size)
                aweme_list = response.get("aweme_list", [])
                
                if aweme_list:
                    if limit > 0:
                        remaining = limit - len(all_videos)
                        aweme_list = aweme_list[:remaining]
                    
                    all_videos.extend(aweme_list)
                    pbar.update(len(aweme_list))
                    max_cursor = response.get("max_cursor", 0)
                    has_more = response.get("has_more", False)
                    retries = 0
                else:
                    retries += 1
                    await asyncio.sleep(delay * retries)
                
                if has_more:
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"获取视频失败: {e}")
                retries += 1
                await asyncio.sleep(delay * retries)
        
        pbar.close()
        return all_videos
    
    def process(self, raw_videos: List[Dict], **kwargs) -> List[Dict]:
        processed = []
        
        for video in raw_videos:
            aweme_id = video.get("aweme_id", "")
            if not aweme_id:
                continue
            
            images = []
            raw_images = video.get("images") or []
            for img in raw_images:
                url_list = img.get("url_list", [])
                if url_list:
                    images.append(url_list)
            
            video_urls = []
            video_obj = video.get("video") or {}
            play_addr = video_obj.get("play_addr") or {}
            if play_addr.get("url_list"):
                video_urls = play_addr["url_list"]
            
            thumb_urls = []
            origin_cover = video_obj.get("origin_cover") or {}
            if origin_cover.get("url_list"):
                thumb_urls = origin_cover["url_list"]
            
            processed.append({
                "aweme_id": aweme_id,
                "desc": safe_str(video.get("desc")),
                "create_time": safe_int(video.get("create_time")),
                "images": str(images) if images else None,
                "video": str(video_urls) if video_urls else None,
                "thumb": str(thumb_urls) if thumb_urls else None,
                "sec_uid": self.sec_uid,
                "category_id": self.category_id
            })
        
        return processed
    
    async def run(self, page_size: int = 30, delay: float = 1.0, limit: int = 0, **kwargs) -> Dict:
        logger.info(f"开始采集用户 {self.sec_uid[:20]}... 的作品")
        
        start_time = datetime.now()
        raw_videos = await self.fetch(page_size, delay, limit)
        processed = self.process(raw_videos)
        
        save_result = self.storage.save(processed)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            'total': len(raw_videos),
            'new': max(save_result['csv'], save_result['db']),
            'new_csv': save_result['csv'],
            'new_db': save_result['db'],
            'duration': f"{duration:.1f}秒"
        }
        
        logger.log_user_action(self.sec_uid, '作品采集完成', result)
        return result
