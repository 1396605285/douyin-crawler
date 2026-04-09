import asyncio
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

from services.base_service import BaseService
from core.logger import logger
from utils.helpers import safe_str, safe_int


class CommentService(BaseService):
    data_type = "comment"
    id_field = "cid"
    table_name = "comments"
    csv_filename = "comments.csv"
    media_fields = {"image_list": "images"}
    has_avatar_sticker = True
    
    async def fetch(self, aweme_id: str, page_size: int = 50,
                    delay: float = 0.8, **kwargs) -> List[Dict]:
        all_comments = []
        cursor = 0
        has_more = 1
        
        while has_more:
            try:
                response = await self.api.fetch_comments(aweme_id, cursor, page_size)
                comments = response.get("comments", [])
                
                if isinstance(comments, list):
                    all_comments.extend(comments)
                
                has_more = response.get("has_more", 0)
                cursor = response.get("cursor", 0)
                
                if has_more:
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"获取评论失败 {aweme_id}: {e}")
                break
        
        return all_comments
    
    def process(self, raw_comments: List[Dict], aweme_id: str = None, **kwargs) -> List[Dict]:
        processed = []
        
        for c in raw_comments:
            cid = c.get('cid')
            if not cid:
                continue
            
            user_info = self._extract_user_info(c)
            
            processed.append({
                "aweme_id": aweme_id,
                "cid": str(cid),
                "text": safe_str(c.get('text')),
                "image_list": self._extract_image_urls(c),
                "digg_count": safe_int(c.get('digg_count')),
                "create_time": safe_int(c.get('create_time')),
                "user_nickname": user_info['user_nickname'],
                "user_unique_id": user_info['user_unique_id'],
                "user_avatar": user_info['user_avatar'],
                "sticker": self._extract_sticker_url(c),
                "reply_comment_total": safe_int(c.get('reply_comment_total')),
                "ip_label": safe_str(c.get('ip_label'))
            })
        
        return processed
    
    async def run(self, source: str = 'db', aweme_ids: List[str] = None,
                  page_size: int = 50, delay: float = 0.8, limit: int = 0,
                  skip_existing: bool = False, **kwargs) -> Dict:
        if aweme_ids is None:
            aweme_ids = self.storage.get_video_ids(source, limit)
        
        if skip_existing:
            existing = self.storage.get_videos_with_comments(source)
            aweme_ids = [vid for vid in aweme_ids if vid not in existing]
            if aweme_ids:
                logger.info(f"跳过已采集的视频，剩余 {len(aweme_ids)} 个")
        
        if not aweme_ids:
            logger.warning(f"用户 {self.sec_uid[:20]}... 没有需要采集的视频")
            return {'total': 0, 'new': 0, 'videos': 0}
        
        logger.info(f"开始采集用户 {self.sec_uid[:20]}... 的评论，共 {len(aweme_ids)} 个视频")
        
        start_time = datetime.now()
        stats = {'total': 0, 'new': 0, 'new_csv': 0, 'new_db': 0, 'videos': len(aweme_ids)}
        
        video_timestamps = self.storage.get_video_timestamps(source)
        
        for aweme_id in tqdm(aweme_ids, desc="采集评论", unit="视频"):
            raw_comments = await self.fetch(aweme_id, page_size, delay)
            processed = self.process(raw_comments, aweme_id=aweme_id)
            
            if processed:
                stats['total'] += len(processed)
                video_ts = video_timestamps.get(aweme_id)
                save_result = self.storage.save(processed, aweme_id, video_timestamp=video_ts)
                stats['new'] += max(save_result['csv'], save_result['db'])
                stats['new_csv'] += save_result['csv']
                stats['new_db'] += save_result['db']
            
            await asyncio.sleep(delay)
        
        stats['duration'] = f"{(datetime.now() - start_time).total_seconds():.1f}秒"
        
        logger.log_user_action(self.sec_uid, '评论采集完成', stats)
        return stats
    
    async def run_download_only(self, source: str = None, aweme_id: str = None, 
                                 video_timestamp: int = None, quiet: bool = False) -> Dict:
        if aweme_id:
            return await super().run_download_only(source, aweme_id, video_timestamp, quiet=quiet)
        return await self.run_download_only_multi(source)
