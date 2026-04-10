import csv
import os
from typing import Dict, List

from core.database import get_database
from core.logger import logger
from utils.field_config import UserManager
from services.video_service import VideoService
from services.comment_service import CommentService
from services.reply_service import ReplyService


TYPE_NAMES: Dict[str, str] = {
    'video': '作品', 
    'comment': '评论', 
    'reply': '回复'
}

SERVICE_CLASSES = {
    'video': VideoService,
    'comment': CommentService,
    'reply': ReplyService
}


def import_csv_to_db(sec_uid: str = None):
    user_manager = UserManager()
    
    if sec_uid:
        users = [{'sec_uid': sec_uid, 'nickname': '指定用户'}]
    else:
        users = user_manager.get_active_users()
    
    if not users:
        print("没有需要导入的用户")
        return
    
    print(f"\n{'='*60}")
    print(f"CSV导入数据库")
    print(f"{'='*60}\n")
    
    type_configs = {}
    for data_type, service_class in SERVICE_CLASSES.items():
        type_configs[data_type] = {
            'table': service_class.table_name,
            'file': service_class.csv_filename,
            'id_field': service_class.id_field,
            'needs_sec_uid': data_type == 'video'
        }
    
    total_stats = {'users': 0}
    
    for user in users:
        user_sec_uid = user['sec_uid']
        user_name = user.get('nickname', user_sec_uid[:20] + '...')
        data_dir = os.path.join('data', user_sec_uid)
        
        if not os.path.exists(data_dir):
            print(f"跳过用户 {user_name}: 数据目录不存在")
            continue
        
        print(f"\n处理用户: {user_name}")
        print("-" * 40)
        
        db = get_database(sec_uid=user_sec_uid)
        user_stats = {}
        
        for data_type, config in type_configs.items():
            type_name = TYPE_NAMES[data_type]
            fields = user_manager.get_fields(data_type)
            
            if data_type == 'video':
                csv_path = os.path.join(data_dir, config['file'])
                csv_files = [csv_path] if os.path.exists(csv_path) else []
            else:
                csv_files = []
                for entry in os.listdir(data_dir):
                    year_month_path = os.path.join(data_dir, entry)
                    if os.path.isdir(year_month_path) and '-' in entry:
                        for aweme_id in os.listdir(year_month_path):
                            aweme_path = os.path.join(year_month_path, aweme_id)
                            if os.path.isdir(aweme_path):
                                csv_path = os.path.join(aweme_path, config['file'])
                                if os.path.exists(csv_path):
                                    csv_files.append(csv_path)
            
            if not csv_files:
                continue
            
            all_rows = []
            existing_ids = set()
            
            for csv_path in csv_files:
                try:
                    with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            item_id = row.get(config['id_field'])
                            if item_id and item_id not in existing_ids:
                                existing_ids.add(item_id)
                                filtered_row = {k: v for k, v in row.items() if k in fields}
                                if config['needs_sec_uid']:
                                    filtered_row['sec_uid'] = user_sec_uid
                                all_rows.append(filtered_row)
                except Exception as e:
                    print(f"  读取 {csv_path} 失败: {e}")
            
            if all_rows:
                count = db.insert_many(config['table'], all_rows)
                user_stats[data_type] = count
                print(f"  {type_name}: 导入 {count} 条 (共 {len(all_rows)} 条)")
            else:
                user_stats[data_type] = 0
        
        total_stats['users'] += 1
        for data_type, count in user_stats.items():
            total_stats[data_type] = total_stats.get(data_type, 0) + count
    
    print(f"\n{'='*60}")
    print("导入完成!")
    print(f"  处理用户: {total_stats['users']} 个")
    for data_type in ['video', 'comment', 'reply']:
        if data_type in total_stats:
            print(f"  {TYPE_NAMES[data_type]}: {total_stats[data_type]} 条")
    print(f"{'='*60}\n")
