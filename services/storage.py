import os
import csv
import threading
from typing import List, Dict, Optional, Callable, TypeVar, Set, Union
from core.database import get_database
from core.logger import logger
from utils.field_config import UserManager
from core.downloader import timestamp_to_year_month

T = TypeVar('T')


class StorageManager:
    _global_lock = threading.Lock()
    
    def __init__(self, sec_uid: str, data_type: str, table_name: str,
                 id_field: str, csv_filename: str):
        self.sec_uid = sec_uid
        self.data_type = data_type
        self.table_name = table_name
        self.id_field = id_field
        self.csv_filename = csv_filename
        self.db = get_database(sec_uid=sec_uid)
        self.user_manager = UserManager()
        self.data_dir = os.path.join('data', sec_uid)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self._csv_cache = {}
        self._db_cache = None
        self._placeholder = self.db._get_placeholder()
    
    def _get_existing_ids_from_csv(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> set:
        cache_key = aweme_id or '__root__'
        if cache_key not in self._csv_cache:
            filepath = self._get_csv_path(aweme_id, video_timestamp)
            if os.path.exists(filepath):
                with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                    self._csv_cache[cache_key] = {row.get(self.id_field) for row in csv.DictReader(f)}
            else:
                self._csv_cache[cache_key] = set()
        return self._csv_cache[cache_key]
    
    def _get_existing_ids_from_db(self) -> set:
        if self._db_cache is None:
            if self.table_name == 'videos':
                rows = self.db.query(
                    f"SELECT {self.id_field} FROM {self.table_name} WHERE sec_uid = {self._placeholder}",
                    (self.sec_uid,)
                )
            else:
                rows = self.db.query(
                    f"SELECT t.{self.id_field} FROM {self.table_name} t JOIN videos v ON t.aweme_id = v.aweme_id WHERE v.sec_uid = {self._placeholder}",
                    (self.sec_uid,)
                )
            self._db_cache = {row[self.id_field] for row in rows}
        return self._db_cache

    def _normalize_id(self, value, sample_type: type):
        if value is None:
            return None
        if sample_type == str:
            return str(value)
        elif sample_type == int:
            if isinstance(value, str):
                return int(value) if value.isdigit() else value
            return int(value) if isinstance(value, (int, float)) else value
        return value

    def _normalize_existing_ids(self, existing_ids: set, sample_item: Dict) -> set:
        if not existing_ids or not sample_item:
            return existing_ids
        
        sample_value = sample_item.get(self.id_field)
        if sample_value is None:
            return existing_ids
        
        sample_type = type(sample_value)
        if sample_type not in (str, int):
            return existing_ids
        
        return {self._normalize_id(vid, sample_type) for vid in existing_ids}
    
    def _add_to_cache(self, item_id: str, aweme_id: str = None, update_db: bool = True):
        cache_key = aweme_id or '__root__'
        if cache_key in self._csv_cache:
            self._csv_cache[cache_key].add(item_id)
        if update_db and self._db_cache is not None:
            self._db_cache.add(item_id)
    
    def _get_csv_path(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> str:
        if aweme_id:
            year_month = timestamp_to_year_month(video_timestamp)
            return os.path.join(self.data_dir, year_month, str(aweme_id), self.csv_filename)
        return os.path.join(self.data_dir, self.csv_filename)
    
    def _find_csv_path(self, aweme_id: str, filename: str) -> Optional[str]:
        for entry in os.listdir(self.data_dir):
            year_month_path = os.path.join(self.data_dir, entry)
            if os.path.isdir(year_month_path) and '-' in entry:
                filepath = os.path.join(year_month_path, aweme_id, filename)
                if os.path.exists(filepath):
                    return filepath
        return None
    
    def _from_source(self, source: str, csv_func: Callable[[], T], db_func: Callable[[], T]) -> T:
        return csv_func() if source == 'csv' else db_func()
    
    def save(self, items: List[Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> Dict:
        if not items:
            return {'csv': 0, 'db': 0}
        
        with self._global_lock:
            storage = self.user_manager.get_storage_for_type(self.data_type)
            result = {'csv': 0, 'db': 0}
            
            if storage.get('csv', True):
                result['csv'] = self._save_to_csv(items, aweme_id, video_timestamp)
            
            if storage.get('database', True):
                result['db'] = self._save_to_db(items)
            
            return result
    
    def _save_to_csv(self, items: List[Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        filepath = self._get_csv_path(aweme_id, video_timestamp)
        if aweme_id:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        fields = self.user_manager.get_fields(self.data_type)
        sample_item = items[0] if items else None
        existing_ids = self._normalize_existing_ids(
            self._get_existing_ids_from_csv(aweme_id, video_timestamp), sample_item
        )
        
        new_items = [item for item in items if item.get(self.id_field) not in existing_ids]
        if not new_items:
            return 0
        
        file_exists = os.path.exists(filepath)
        with open(filepath, 'a' if file_exists else 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_items)
        
        for item in new_items:
            self._add_to_cache(item.get(self.id_field), aweme_id, update_db=False)
        
        logger.info(f"保存 {len(new_items)} 条新数据到 {filepath}")
        return len(new_items)
    
    def _save_to_db(self, items: List[Dict]) -> int:
        sample_item = items[0] if items else None
        existing_ids = self._normalize_existing_ids(
            self._get_existing_ids_from_db(), sample_item
        )
        new_items = [item for item in items if item[self.id_field] not in existing_ids]
        
        if new_items:
            fields = self.user_manager.get_fields(self.data_type)
            if self.data_type == 'video':
                fields = fields + ['sec_uid']
            filtered_items = [{k: v for k, v in item.items() if k in fields} for item in new_items]
            
            count = self.db.insert_many(self.table_name, filtered_items)
            for item in new_items:
                self._add_to_cache(item[self.id_field])
            logger.info(f"保存 {count} 条新数据到数据库表 {self.table_name}")
            return count
        return 0
    
    def load(self, source: str = None, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> List[Dict]:
        if source == 'csv':
            return self._load_from_csv(aweme_id, video_timestamp)
        elif source == 'db':
            return self._load_from_db()
        
        storage = self.user_manager.get_storage_for_type(self.data_type)
        if storage.get('csv', True):
            return self._load_from_csv(aweme_id, video_timestamp)
        return self._load_from_db()
    
    def _load_from_csv(self, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> List[Dict]:
        filepath = self._get_csv_path(aweme_id, video_timestamp)
        if not os.path.exists(filepath):
            return []
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            return list(csv.DictReader(f))
    
    def _load_from_db(self) -> List[Dict]:
        if self.table_name == 'videos':
            return self.db.query(
                f"SELECT * FROM {self.table_name} WHERE sec_uid = {self._placeholder}",
                (self.sec_uid,)
            )
        else:
            return self.db.query(
                f"SELECT t.* FROM {self.table_name} t JOIN videos v ON t.aweme_id = v.aweme_id WHERE v.sec_uid = {self._placeholder}",
                (self.sec_uid,)
            )
    
    def update_urls(self, updates: Dict[str, Dict], aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        if not updates:
            return 0
        
        storage = self.user_manager.get_storage_for_type(self.data_type)
        total_updated = 0
        
        if storage.get('csv', True):
            total_updated += self._update_csv_urls(updates, aweme_id, video_timestamp)
        
        if storage.get('database', True):
            total_updated += self._update_db_urls(updates)
        
        return total_updated
    
    def _update_csv_urls(self, updates: Dict, aweme_id: str = None, video_timestamp: Union[int, float, None] = None) -> int:
        filepath = self._get_csv_path(aweme_id, video_timestamp)
        if not os.path.exists(filepath):
            logger.warning(f"CSV文件不存在: {filepath}")
            return 0
        
        try:
            rows = []
            updated_count = 0
            
            with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    item_id = row.get(self.id_field)
                    if item_id in updates:
                        for field, new_value in updates[item_id].items():
                            if field in row and new_value:
                                row[field] = new_value
                        updated_count += 1
                    rows.append(row)
            
            if updated_count > 0:
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(rows)
                logger.info(f"更新CSV成功: {updated_count} 条记录")
            
            return updated_count
        except Exception as e:
            logger.error(f"更新CSV失败: {e}")
            return 0
    
    def _update_db_urls(self, updates: Dict) -> int:
        if not updates:
            return 0
        
        updated_count = 0
        errors = []
        
        with self.db.transaction() as cur:
            for item_id, update in updates.items():
                set_clauses = []
                values = []
                for field, new_value in update.items():
                    if new_value:
                        set_clauses.append(f"{field} = {self._placeholder}")
                        values.append(new_value)
                
                if set_clauses:
                    values.append(item_id)
                    sql = f"UPDATE {self.table_name} SET {', '.join(set_clauses)} WHERE {self.id_field} = {self._placeholder}"
                    try:
                        cur.execute(sql, tuple(values))
                        updated_count += 1
                    except Exception as e:
                        errors.append((item_id, str(e)))
            
            if errors:
                logger.warning(f"批量更新中有 {len(errors)} 条失败")
                for item_id, err in errors[:5]:
                    logger.error(f"  [{item_id}]: {err}")
        
        if updated_count > 0:
            logger.info(f"更新数据库成功: {updated_count} 条记录")
        return updated_count
    
    def get_video_ids(self, source: str, limit: int = 0) -> List[str]:
        ids = self._from_source(source, self._get_video_ids_from_csv, self._get_video_ids_from_db)
        return ids[-limit:] if limit > 0 else ids
    
    def _get_video_ids_from_csv(self) -> List[str]:
        filepath = os.path.join(self.data_dir, 'videos.csv')
        if not os.path.exists(filepath):
            return []
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            return [row['aweme_id'] for row in csv.DictReader(f) if row.get('aweme_id')]
    
    def _get_video_ids_from_db(self) -> List[str]:
        rows = self.db.query(
            f"SELECT DISTINCT aweme_id FROM videos WHERE sec_uid = {self._placeholder}",
            (self.sec_uid,)
        )
        return [row['aweme_id'] for row in rows if row['aweme_id']]
    
    def get_video_timestamps(self, source: str) -> Dict[str, int]:
        return self._from_source(source, 
            self._get_video_timestamps_from_csv, 
            self._get_video_timestamps_from_db)
    
    def _get_video_timestamps_from_csv(self) -> Dict[str, int]:
        filepath = os.path.join(self.data_dir, 'videos.csv')
        if not os.path.exists(filepath):
            return {}
        timestamps = {}
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                aweme_id = row.get('aweme_id')
                create_time = row.get('create_time', '0') or '0'
                if aweme_id:
                    timestamps[aweme_id] = int(create_time)
        return timestamps
    
    def _get_video_timestamps_from_db(self) -> Dict[str, int]:
        rows = self.db.query(
            f"SELECT aweme_id, create_time FROM videos WHERE sec_uid = {self._placeholder}",
            (self.sec_uid,)
        )
        return {row['aweme_id']: row['create_time'] or 0 for row in rows if row['aweme_id']}
    
    def get_comment_ids(self, source: str, limit: int = 0, video_limit: int = 0) -> List[Dict]:
        if video_limit > 0:
            video_ids = self.get_video_ids(source, video_limit)
            comments = self._from_source(source, self._get_comment_ids_from_csv, self._get_comment_ids_from_db)
            comments = [c for c in comments if c['aweme_id'] in video_ids]
        else:
            comments = self._from_source(source, self._get_comment_ids_from_csv, self._get_comment_ids_from_db)
        return comments[-limit:] if limit > 0 else comments
    
    def _get_comment_ids_from_csv(self) -> List[Dict]:
        comments = []
        for aweme_id in self._get_video_ids_from_csv():
            filepath = self._find_csv_path(aweme_id, 'comments.csv')
            if not filepath:
                continue
            try:
                with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                    for row in csv.DictReader(f):
                        reply_comment_total = row.get('reply_comment_total', '0') or '0'
                        if row.get('cid') and int(reply_comment_total) > 0:
                            comments.append({'cid': row['cid'], 'aweme_id': aweme_id})
            except (ValueError, IOError) as e:
                logger.warning(f"读取评论CSV失败 {filepath}: {e}")
        return comments
    
    def _get_comment_ids_from_db(self) -> List[Dict]:
        rows = self.db.query(
            f"SELECT DISTINCT c.cid, c.aweme_id FROM comments c JOIN videos v ON c.aweme_id = v.aweme_id WHERE v.sec_uid = {self._placeholder} AND c.reply_comment_total > 0",
            (self.sec_uid,)
        )
        return [{'cid': row['cid'], 'aweme_id': row['aweme_id']} for row in rows]
    
    def get_videos_with_comments(self, source: str) -> Set[str]:
        return self._from_source(source, self._get_videos_with_comments_from_csv, self._get_videos_with_comments_from_db)
    
    def _get_videos_with_comments_from_csv(self) -> Set[str]:
        videos_with_comments = set()
        for aweme_id in self._get_video_ids_from_csv():
            if self._find_csv_path(aweme_id, 'comments.csv'):
                videos_with_comments.add(aweme_id)
        return videos_with_comments
    
    def _get_videos_with_comments_from_db(self) -> Set[str]:
        rows = self.db.query(
            f"SELECT DISTINCT c.aweme_id FROM comments c JOIN videos v ON c.aweme_id = v.aweme_id WHERE v.sec_uid = {self._placeholder}",
            (self.sec_uid,)
        )
        return {row['aweme_id'] for row in rows}
    
    def get_comments_with_replies(self, source: str) -> Set[str]:
        return self._from_source(source, self._get_comments_with_replies_from_csv, self._get_comments_with_replies_from_db)
    
    def _get_comments_with_replies_from_csv(self) -> Set[str]:
        comment_ids = set()
        for aweme_id in self._get_video_ids_from_csv():
            filepath = self._find_csv_path(aweme_id, 'replies.csv')
            if filepath:
                try:
                    with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
                        for row in csv.DictReader(f):
                            if row.get('reply_id'):
                                comment_ids.add(row['reply_id'])
                except (ValueError, IOError):
                    pass
        return comment_ids
    
    def _get_comments_with_replies_from_db(self) -> Set[str]:
        rows = self.db.query(
            f"SELECT DISTINCT r.cid FROM replies r JOIN videos v ON r.aweme_id = v.aweme_id WHERE v.sec_uid = {self._placeholder}",
            (self.sec_uid,)
        )
        return {row['cid'] for row in rows}
