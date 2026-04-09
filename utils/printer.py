from typing import Dict, List

from utils.field_config import UserManager




class Config:
    SERVICES = {}
    
    TYPE_NAMES: Dict[str, str] = {
        'video': '作品', 
        'comment': '评论', 
        'reply': '回复'
    }
    
    MEDIA_FIELDS: Dict[str, List[str]] = {
        'video': ['images', 'video', 'thumb'],
        'comment': ['image_list', 'avatars', 'stickers'],
        'reply': ['image_list', 'avatars', 'stickers']
    }
    
    FIELD_NAMES: Dict[str, str] = {
        'images': '图片', 'image_list': '图片', 'video': '视频', 'thumb': '缩略图',
        'videos': '视频', 'thumbs': '缩略图',
        'avatars': '头像', 'stickers': '表情'
    }
    
    @classmethod
    def init_services(cls, video_service, comment_service, reply_service):
        cls.SERVICES = {
            'video': video_service,
            'comment': comment_service,
            'reply': reply_service
        }


class Printer:
    
    @staticmethod
    def banner():
        print(f"""
    ╔═════════════════════════════════════════════════════╗
    ║         Douyin 数据采集与媒体资源下载系统           ║
    ╚═════════════════════════════════════════════════════╝
""")
    
    @staticmethod
    def separator(char: str = '=', width: int = 60):
        print(char * width)
    
    @staticmethod
    def user_header(user: dict, index: int, total: int):
        name = user.get('nickname', user['sec_uid'][:20] + '...')
        print(f"\n{'#'*60}")
        print(f"进度: [{index}/{total}] 处理用户 {name}")
        print(f"{'#'*60}")
    
    @staticmethod
    def task_info(user_manager: UserManager, data_type: str, users: list,
                  source: str = None, limit: int = 0, download_only: bool = False):
        type_name = Config.TYPE_NAMES[data_type]
        action = "下载" if download_only else "采集"
        
        print(f"共有 {len(users)} 个用户需要{action}{type_name}")
        
        if source:
            print(f"数据来源: {source.upper()}")
        
        storage = user_manager.get_storage_for_type(data_type)
        targets = [k.upper() for k, v in storage.items() if v]
        if targets:
            print(f"存储目标: {' + '.join(targets)}")
        
        if download_only:
            media_config = user_manager.get_media_download_for_type(data_type)
            enabled = [Config.FIELD_NAMES.get(k, k) for k, v in media_config.items() if v]
            if enabled:
                print(f"媒体下载: {', '.join(enabled)}")
        
        if limit > 0:
            limit_desc = {
                'video': f"{limit} 条{type_name}",
                'comment': f"前 {limit} 个视频的{type_name}",
                'reply': f"前 {limit} 个视频的{type_name}"
            }
            print(f"限制{action}: {limit_desc[data_type]}")
        
        print()
    
    @staticmethod
    def result(user: dict, stats: dict, data_type: str, user_manager: UserManager, is_download: bool = False):
        type_name = Config.TYPE_NAMES[data_type]
        name = user.get('nickname', user['sec_uid'][:20] + '...')
        
        print(f"\n{'='*60}")
        print(f"用户: {name}")
        print(f"{'='*60}")
        
        if is_download:
            print(f"  下载{type_name}媒体:")
            fields = Config.MEDIA_FIELDS[data_type]
            items = [f"{Config.FIELD_NAMES.get(k, k)}: {stats.get(k, 0)}" for k in fields]
            print(f"    {' | '.join(items)}")
            print(f"    更新URL: {stats.get('updated', 0)} 条")
        else:
            storage = user_manager.get_storage_for_type(data_type)
            csv_enabled = storage.get('csv', True)
            db_enabled = storage.get('database', True)
            
            new_parts = []
            if csv_enabled and db_enabled:
                new_csv = stats.get('new_csv', 0)
                new_db = stats.get('new_db', 0)
                new_parts.append(f"CSV: {new_csv}")
                new_parts.append(f"数据库: {new_db}")
            elif csv_enabled:
                new_parts.append(f"CSV: {stats.get('new_csv', 0)}")
            elif db_enabled:
                new_parts.append(f"数据库: {stats.get('new_db', 0)}")
            
            new_str = ' | '.join(new_parts) if new_parts else str(stats.get('new', 0))
            print(f"  {type_name}采集: {stats.get('total', 0)} 条 (新增: {new_str})")
            
            if stats.get('duration'):
                print(f"  耗时: {stats['duration']}")
        
        print(f"{'='*60}\n")
    
    @staticmethod
    def total(stats: dict, data_type: str, user_manager: UserManager, is_download: bool = False):
        type_name = Config.TYPE_NAMES[data_type]
        action = "下载" if is_download else "采集"
        user_count = stats.get('users', 1)
        
        if user_count <= 1:
            print(f"\n{'='*60}")
            print(f"{type_name}{action}完成!")
            print(f"{'='*60}\n")
            return
        
        print(f"\n{'='*60}")
        print(f"{type_name}{action}完成!")
        
        if is_download:
            fields = Config.MEDIA_FIELDS[data_type]
            items = [f"{Config.FIELD_NAMES.get(k, k)}: {stats.get(k, 0)}" for k in fields]
            print(f"  {' | '.join(items)}")
            print(f"  更新URL: {stats.get('updated', 0)} 条")
        else:
            print(f"  处理用户: {user_count} 个")
            
            storage = user_manager.get_storage_for_type(data_type)
            csv_enabled = storage.get('csv', True)
            db_enabled = storage.get('database', True)
            
            new_parts = []
            if csv_enabled and db_enabled:
                new_parts.append(f"CSV: {stats.get('count_csv', 0)}")
                new_parts.append(f"数据库: {stats.get('count_db', 0)}")
            elif csv_enabled:
                new_parts.append(f"CSV: {stats.get('count_csv', 0)}")
            elif db_enabled:
                new_parts.append(f"数据库: {stats.get('count_db', 0)}")
            
            new_str = ' | '.join(new_parts) if new_parts else str(stats.get('count', 0))
            print(f"  新增{type_name}: {new_str}")
        
        print(f"{'='*60}\n")
