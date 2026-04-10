#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
抖音用户作品和评论下载系统

命令格式:
    python main.py <类型> [来源] [选项]
    
    类型: video | comment | reply
    来源: csv | db (评论/回复必填)
"""

import asyncio
import argparse
import sys
import os
import atexit
from typing import Dict, List, Optional, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.logger import logger
from utils.field_config import UserManager
from utils.csv_importer import import_csv_to_db
from utils.printer import Config, Printer
from services.video_service import VideoService
from services.comment_service import CommentService
from services.reply_service import ReplyService




def cleanup():
    try:
        from core.database import SQLiteDatabase, MySQLDatabase
        SQLiteDatabase.close_all()
        MySQLDatabase.close_all()
    except Exception:
        pass
    
    try:
        from core.downloader import MediaDownloader
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(MediaDownloader.close_all())
        except RuntimeError:
            asyncio.run(MediaDownloader.close_all())
    except Exception:
        pass

    try:
        from core.api import DouyinAPI
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(DouyinAPI.close_instance())
        except RuntimeError:
            asyncio.run(DouyinAPI.close_instance())
    except Exception:
        pass
    
    logger.info("资源清理完成")


atexit.register(cleanup)

Config.init_services(VideoService, CommentService, ReplyService)


class TaskRunner:
    
    def __init__(self, data_type: str, source: str = None):
        self.data_type = data_type
        self.source = source
        self.user_manager = UserManager()
        self.cookie = self.user_manager.get_cookie()
        self.crawler_config = self.user_manager.get_crawler_config()
    
    def create_service(self, sec_uid: str, user: dict = None):
        service_class = Config.SERVICES[self.data_type]
        if self.data_type == 'video':
            return service_class(
                sec_uid, self.cookie,
                user.get('category_id', 2) if user else 2
            )
        return service_class(sec_uid, self.cookie)
    
    async def collect(self, users: list, limit: int = 0, skip_existing: bool = False):
        Printer.task_info(self.user_manager, self.data_type, users, 
                          self.source, limit, False)
        
        total = {'users': 0, 'count': 0, 'count_csv': 0, 'count_db': 0}
        
        for i, user in enumerate(users, 1):
            Printer.user_header(user, i, len(users))
            
            service = self.create_service(user['sec_uid'], user)
            
            params = {
                'page_size': self.crawler_config.get('page_size', 30),
                'delay': self.crawler_config.get('request_delay', 1.0),
                'limit': limit,
                'skip_existing': skip_existing
            }
            
            if self.source and self.data_type != 'video':
                params['source'] = self.source
            
            stats = await service.run(**params)
            Printer.result(user, stats, self.data_type, self.user_manager)
            
            total['users'] += 1
            total['count'] += stats.get('new', 0)
            total['count_csv'] += stats.get('new_csv', 0)
            total['count_db'] += stats.get('new_db', 0)
        
        Printer.total(total, self.data_type, self.user_manager)
    
    async def download(self, users: list):
        Printer.task_info(self.user_manager, self.data_type, users,
                          self.source, 0, True)
        
        total = {'users': len(users), 'updated': 0}
        for field in Config.MEDIA_FIELDS[self.data_type]:
            total[field] = 0
        
        for i, user in enumerate(users, 1):
            Printer.user_header(user, i, len(users))
            
            service = self.create_service(user['sec_uid'])
            stats = await service.run_download_only(source=self.source)
            
            Printer.result(user, stats, self.data_type, self.user_manager, is_download=True)
            
            for field in Config.MEDIA_FIELDS[self.data_type] + ['updated']:
                total[field] += stats.get(field, 0)
        
        Printer.total(total, self.data_type, self.user_manager, is_download=True)


def get_users(sec_uid: str = None) -> list:
    if sec_uid:
        return [{
            'sec_uid': sec_uid, 'nickname': '指定用户',
            'collect_videos': True, 'collect_comments': True, 'collect_replies': True
        }]
    
    users = UserManager().get_active_users()
    if not users:
        print("没有活跃的用户需要采集")
    return users


async def run_all(users: list, limit: int = 0, download_only: bool = False, skip_existing: bool = False):
    types_order = ['video', 'comment', 'reply']
    user_manager = UserManager()
    
    video_storage = user_manager.get_storage_for_type('video')
    auto_source = 'csv' if video_storage.get('csv', True) else 'db'
    
    for data_type in types_order:
        print(f"\n{'='*60}")
        print(f"开始处理: {Config.TYPE_NAMES[data_type]}")
        print(f"{'='*60}\n")
        
        source = auto_source if data_type != 'video' else None
        runner = TaskRunner(data_type, source)
        
        if download_only:
            await runner.download(users)
        else:
            await runner.collect(users, limit, skip_existing)
    
    print(f"\n{'='*60}")
    print("全部操作完成!")
    print(f"{'='*60}\n")


def show_users():
    users = UserManager().get_all_users()
    
    print(f"\n{'='*60}")
    print(f"用户列表 (共 {len(users)} 个)")
    print(f"{'='*60}")
    
    for i, user in enumerate(users, 1):
        sec_uid = user.get('sec_uid', '')
        status = "暂停" if sec_uid.startswith('#') or not user.get('enabled', True) else "活跃"
        tasks = [t for t in ['作品', '评论', '回复'] 
                 if user.get(f'collect_{t}s', True)]
        
        print(f"  {i}. {user.get('nickname', '未命名')}")
        print(f"     ID: {sec_uid[:30]}...")
        print(f"     状态: {status} | 采集: {', '.join(tasks)}")
    
    print(f"{'='*60}\n")


def print_help():
    print("""
    命令格式:
        python main.py <类型> [来源] [选项]
        python main.py --all [选项]

    类型:
        video    作品（API采集）
        comment  评论（需来源：csv/db）
        reply    回复（需来源：csv/db）

    来源:
        csv      从 CSV 文件读取
        db       从数据库读取

    选项:
        --all             全量采集：作品→评论→回复
        --download-only   仅下载媒体，不采集数据
        --limit N         限制采集视频数量
        --skip-existing   跳过已采集的数据（评论/回复按来源检查）
        --sec-uid <ID>    指定用户 sec_uid
        --import          将 CSV 数据导入数据库
        --users           显示用户列表

    常用示例:
        # 全量采集
        python main.py --all
        python main.py --all --limit 10
        python main.py --all --skip-existing

        # 单独采集
        python main.py video --limit 10
        python main.py comment csv --limit 10
        python main.py reply db --skip-existing

        # 仅下载媒体
        python main.py --all --download-only
        python main.py video csv --download-only

        # CSV导出DB / 用户列表
        python main.py --import
        python main.py --users

    --skip-existing 说明:
        检查 {aweme_id}/comments.csv或 {aweme_id}/replies.csv 是否存在，存在则跳过采集
        不支持video参数（API每次返回最新视频列表）
""")


def main():
    parser = argparse.ArgumentParser(description='抖音用户作品和评论下载系统', add_help=False)
    
    parser.add_argument('type', nargs='?', choices=['video', 'comment', 'reply'])
    parser.add_argument('source', nargs='?', choices=['csv', 'db'])
    parser.add_argument('--all', action='store_true', dest='all_types')
    parser.add_argument('--download-only', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--skip-existing', action='store_true', dest='skip_existing')
    parser.add_argument('--sec-uid', type=str)
    parser.add_argument('--import', action='store_true', dest='import_csv')
    parser.add_argument('--users', action='store_true')
    parser.add_argument('-h', '--help', action='store_true')
    
    args = parser.parse_args()
    
    Printer.banner()
    
    if args.help or (not args.type and not args.users and not args.all_types and not args.import_csv):
        print_help()
        return
    
    if args.users:
        show_users()
        return
    
    if args.import_csv:
        import_csv_to_db(args.sec_uid)
        return
    
    users = get_users(args.sec_uid)
    if not users:
        return
    
    if args.all_types:
        asyncio.run(run_all(users, args.limit, args.download_only, args.skip_existing))
        return
    
    data_type = args.type
    source = args.source
    
    if data_type != 'video' and not source:
        print(f"错误: {Config.TYPE_NAMES[data_type]}操作需要指定来源")
        print(f"示例: python main.py {data_type} csv")
        return
    
    if data_type == 'video' and args.download_only and not source:
        print("错误: 作品下载媒体需要指定来源")
        print("示例: python main.py video csv --download-only")
        return
    
    runner = TaskRunner(data_type, source)
    
    if args.download_only:
        asyncio.run(runner.download(users))
    else:
        asyncio.run(runner.collect(users, args.limit, args.skip_existing))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序已被用户中断")
        cleanup()
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)
