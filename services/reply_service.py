import asyncio
from datetime import datetime
from typing import List, Dict
from tqdm import tqdm

from services.base_service import BaseService
from core.logger import logger
from utils.helpers import safe_str, safe_int


class ReplyService(BaseService):
    data_type = "reply"
    id_field = "cid"
    table_name = "replies"
    csv_filename = "replies.csv"
    media_fields = {"image_list": "images"}
    has_avatar_sticker = True
    
    async def fetch(self, aweme_id: str, comment_id: str, page_size: int = 50,
                    delay: float = 0.2, **kwargs) -> List[Dict]:
        all_replies = []
        cursor = 0
        has_more = 1
        
        while has_more:
            try:
                response = await self.api.fetch_replies(aweme_id, comment_id, cursor, page_size)
                replies = response.get("comments", [])
                
                if isinstance(replies, list):
                    all_replies.extend(replies)
                
                has_more = response.get("has_more", 0)
                cursor = response.get("cursor", 0)
                
                if has_more:
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"获取回复失败 {comment_id}: {e}")
                break
        
        return all_replies
    
    def process(self, raw_replies: List[Dict], aweme_id: str = None,
                comment_id: str = None, **kwargs) -> List[Dict]:
        processed = []
        
        for r in raw_replies:
            cid = r.get('cid')
            if not cid:
                continue
            
            user_info = self._extract_user_info(r)
            
            processed.append({
                "aweme_id": aweme_id,
                "cid": str(cid),
                "reply_id": safe_str(r.get('reply_id')),
                "reply_to_reply_id": safe_str(r.get('reply_to_reply_id', '0')),
                "text": safe_str(r.get('text')),
                "image_list": self._extract_image_urls(r),
                "digg_count": safe_int(r.get('digg_count')),
                "create_time": safe_int(r.get('create_time')),
                "user_nickname": user_info['user_nickname'],
                "user_unique_id": user_info['user_unique_id'],
                "user_avatar": user_info['user_avatar'],
                "sticker": self._extract_sticker_url(r),
                "reply_to_username": safe_str(r.get('reply_to_username')),
                "ip_label": safe_str(r.get('ip_label'))
            })
        
        return processed
    
    async def _fetch_comment_reply(self, aweme_id: str, comment: Dict, 
                                   page_size: int, delay: float,
                                   semaphore: asyncio.Semaphore) -> List[Dict]:
        async with semaphore:
            comment_id = comment.get('cid')
            if not comment_id:
                return []
            
            raw_replies = await self.fetch(aweme_id, comment_id, page_size, delay)
            processed = self.process(raw_replies, aweme_id=aweme_id, comment_id=comment_id)
            
            await asyncio.sleep(delay)
            return processed
    
    async def run(self, source: str = 'db', page_size: int = 50, delay: float = 0.2,
                  limit: int = 0, skip_existing: bool = False, concurrency: int = 3, **kwargs) -> Dict:
        comments = self.storage.get_comment_ids(source, video_limit=limit)
        
        if skip_existing:
            existing = self.storage.get_comments_with_replies(source)
            comments = [c for c in comments if c['cid'] not in existing]
            if comments:
                logger.info(f"跳过已采集的评论，剩余 {len(comments)} 条")
        
        if not comments:
            logger.warning(f"用户 {self.sec_uid[:20]}... 没有需要采集回复的评论")
            return {'total': 0, 'new': 0, 'comments': 0}
        
        from collections import defaultdict
        video_comments = defaultdict(list)
        for c in comments:
            video_comments[c['aweme_id']].append(c)
        
        logger.info(f"开始采集用户 {self.sec_uid[:20]}... 的回复，共 {len(comments)} 条评论，{len(video_comments)} 个视频 (并发数: {concurrency})")
        
        start_time = datetime.now()
        stats = {'total': 0, 'new': 0, 'new_csv': 0, 'new_db': 0, 'comments': len(comments), 'videos': len(video_comments)}
        
        semaphore = asyncio.Semaphore(concurrency)
        
        video_timestamps = self.storage.get_video_timestamps(source)
        
        for aweme_id, video_comment_list in tqdm(video_comments.items(), desc="采集回复", unit="视频"):
            tasks = [
                self._fetch_comment_reply(aweme_id, comment, page_size, delay, semaphore)
                for comment in video_comment_list
            ]
            
            results = await asyncio.gather(*tasks)
            all_replies = [reply for result in results for reply in result]
            
            if all_replies:
                stats['total'] += len(all_replies)
                video_ts = video_timestamps.get(aweme_id)
                save_result = self.storage.save(all_replies, aweme_id, video_timestamp=video_ts)
                stats['new'] += max(save_result['csv'], save_result['db'])
                stats['new_csv'] += save_result['csv']
                stats['new_db'] += save_result['db']
        
        stats['duration'] = f"{(datetime.now() - start_time).total_seconds():.1f}秒"
        
        logger.log_user_action(self.sec_uid, '回复采集完成', stats)
        return stats
    
    async def run_download_only(self, source: str = None, aweme_id: str = None, 
                                 video_timestamp: int = None, quiet: bool = False) -> Dict:
        if aweme_id:
            return await super().run_download_only(source, aweme_id, video_timestamp, quiet=quiet)
        return await self.run_download_only_multi(source)
