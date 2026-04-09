# 抖音用户作品和评论采集系统

多用户抖音数据采集工具，支持作品、评论、回复的采集与媒体下载。

## 功能特性

- **多用户管理** - 同时管理多个用户的采集任务
- **灵活控制** - 独立开启/关闭作品、评论、回复采集
- **媒体下载** - 支持图片、视频、头像、表情等媒体文件
- **双存储模式** - CSV + SQLite/MySQL 数据库
- **异步架构** - 高效并发请求与下载

## 目录结构

```
douyin_crawler/
├── main.py                 # 主程序入口
├── config.yaml             # 配置文件 (需自行创建)
├── cookie.yaml             # Cookie配置 (需自行创建)
│
├── core/                   # 核心模块
│   ├── api.py              # API请求
│   ├── database.py         # 数据库操作
│   ├── downloader.py       # 媒体下载
│   └── logger.py           # 日志管理
│
├── services/               # 业务服务
│   ├── base_service.py     # 服务基类
│   ├── storage.py          # 存储管理
│   ├── video_service.py    # 作品服务
│   ├── comment_service.py  # 评论服务
│   └── reply_service.py    # 回复服务
│
├── utils/                  # 工具函数
│   ├── field_config.py     # 配置管理
│   ├── helpers.py          # 通用工具
│   ├── csv_importer.py     # CSV导入
│   └── printer.py          # 终端输出
│
├── data/                   # 数据目录
│   └── {sec_uid}/
│       ├── data.db         # SQLite数据库
│       ├── videos.csv      # 作品数据
│       └── {年月}/{aweme_id}/
│           ├── comments.csv
│           └── replies.csv
│
└── upload/                 # 媒体文件
    └── {sec_uid}/
        ├── videos/         # 视频
        ├── images/         # 图片
        ├── thumbs/         # 缩略图
        ├── avatars/        # 头像
        └── stickers/       # 表情
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

**依赖**: pyyaml, httpx, tqdm, PyExecJS, pymysql

**系统要求**: Python 3.8+, Node.js

### 2. 配置文件

复制模板文件并填入真实信息：

```bash
cp config.yaml.example config.yaml
cp cookie.yaml.example cookie.yaml
```

#### config.yaml

```yaml
# ==================== 用户配置 ====================
users:
  - sec_uid: "MS4wLjABAAAA..."    # 用户ID，从用户主页URL获取
    nickname: "用户昵称"           # 显示名称，便于识别
    enabled: true                  # 是否启用该用户
    collect_videos: true           # 是否采集作品
    collect_comments: true         # 是否采集评论
    collect_replies: true          # 是否采集回复
    category_id: 2                 # 分类ID，用于区分不同用户

# ==================== 数据库配置 ====================
database:
  type: sqlite                     # 数据库类型: sqlite 或 mysql
  sqlite:
    path: null                     # SQLite路径，null表示使用默认路径
  mysql:                           # MySQL配置（type为mysql时生效）
    host: 127.0.0.1
    port: 3306
    user: your_username
    password: your_password
    database: your_database

# ==================== 存储配置 ====================
# 控制各类数据是否保存到数据库和CSV文件
storage:
  video: {database: true, csv: true}      # 作品存储
  comment: {database: true, csv: true}    # 评论存储
  reply: {database: true, csv: true}      # 回复存储

# ==================== 媒体下载配置 ====================
# 控制各类媒体文件是否下载
media_download:
  video:                                  # 作品相关媒体
    images: true                          # 图片
    videos: true                          # 视频
    thumbs: true                          # 缩略图
  comment:                                # 评论相关媒体
    images: true                          # 评论图片
    avatars: true                         # 用户头像
    stickers: true                        # 表情包
  reply:                                  # 回复相关媒体
    images: true
    avatars: true
    stickers: true

# ==================== 采集配置 ====================
crawler:
  page_size: 30                    # 每页请求数量（1-50）
  max_retries: 3                   # 最大重试次数
  request_delay: 1.0               # 请求间隔（秒），避免频繁请求
  download_threads: 3              # 媒体下载并发数
  timeout: 60                      # 请求超时时间（秒）
```

#### cookie.yaml

```yaml
cookie: "your_cookie_here"         # 抖音网页版Cookie，登录后获取
```

**获取Cookie**: 浏览器登录抖音 → F12开发者工具 → Network → 复制请求Cookie

**获取sec_uid**: 用户主页URL `https://www.douyin.com/user/MS4wLjABAAAA...`

## 使用方法

### 命令格式

```bash
python main.py <类型> [来源] [选项]
python main.py --all [选项]
```

### 参数说明

| 参数 | 说明 |
|------|------|
| `video` | 采集作品 |
| `comment` | 采集评论 (需指定来源: csv/db) |
| `reply` | 采集回复 (需指定来源: csv/db) |
| `--all` | 全部采集 (作品→评论→回复) |
| `--download-only` | 仅下载媒体 |
| `--limit N` | 限制视频数量 |
| `--skip-existing` | 跳过已采集的数据 |
| `--sec-uid <ID>` | 指定用户 |
| `--import` | CSV导入数据库 |
| `--users` | 显示用户列表 |

### 常用命令

```bash
# 全部采集
python main.py --all
python main.py --all --limit 10
python main.py --all --skip-existing

# 单独采集
python main.py video --limit 10
python main.py comment csv --limit 10
python main.py reply csv --skip-existing
python main.py reply db --skip-existing

# 下载媒体
python main.py --all --download-only
python main.py video csv --download-only

# 其他
python main.py --users
python main.py --import
```

### 参数详解

**--skip-existing**

跳过已采集的数据，检查规则：
- 评论：检查 `data/{sec_uid}/{年月}/{aweme_id}/comments.csv` 是否存在
- 回复：检查 `data/{sec_uid}/{年月}/{aweme_id}/replies.csv` 是否存在
- 作品：不支持（API每次返回最新视频列表，无法判断是否已采集）

```bash
python main.py --all --skip-existing        # 跳过已有评论/回复
python main.py reply csv --skip-existing    # 仅跳过已有回复
```

## 数据存储

| 类型 | CSV路径 | 数据表 |
|------|---------|--------|
| 作品 | `data/{sec_uid}/videos.csv` | videos |
| 评论 | `data/{sec_uid}/{年月}/{aweme_id}/comments.csv` | comments |
| 回复 | `data/{sec_uid}/{年月}/{aweme_id}/replies.csv` | replies |

**年月目录**: 按视频发布时间自动分目录，便于管理

## 技术实现

### --skip-existing 机制

跳过已采集数据的实现原理：

| 数据类型 | 检查方式 | 实现逻辑 |
|---------|---------|---------|
| 评论 | 检查视频是否已有评论 | 扫描 `{aweme_id}/comments.csv` 存在性 |
| 回复 | 检查评论是否已有回复 | 扫描 `{aweme_id}/replies.csv` 中的 `cid` 列表 |
| 作品 | 不支持 | API 返回最新列表，无增量标识 |

```python
# 评论跳过逻辑
existing = self.storage.get_videos_with_comments(source)
aweme_ids = [vid for vid in aweme_ids if vid not in existing]

# 回复跳过逻辑
existing = self.storage.get_comments_with_replies(source)
comments = [c for c in comments if c['cid'] not in existing]
```

### 批量处理机制

存储层使用缓存优化批量写入：

```python
# CSV缓存：避免重复读取文件
self._csv_cache = {}  # {aweme_id: set(existing_ids)}

# DB缓存：避免重复查询
self._db_cache = None  # set(existing_ids)

# 保存时去重
existing_ids = self._get_existing_ids_from_csv(aweme_id)
new_data = [item for item in data if item[id_field] not in existing_ids]
```

### 异步并发机制

使用 `asyncio.Semaphore` 控制并发：

```python
# 回复采集：并发请求多个评论的回复
semaphore = asyncio.Semaphore(concurrency)  # 默认3
tasks = [self._fetch_comment_reply(..., semaphore) for comment in comments]
results = await asyncio.gather(*tasks)

# 媒体下载：并发下载多个文件
self._semaphore = asyncio.Semaphore(max_workers)  # 默认3
async with self._semaphore:
    await download(url)
```

**并发配置**：

| 参数 | 配置位置 | 默认值 | 说明 |
|------|---------|-------|------|
| `download_threads` | config.yaml | 3 | 媒体下载并发数 |
| `concurrency` | reply_service | 3 | 回复采集并发数 |
| `request_delay` | config.yaml | 1.0 | 请求间隔（秒） |

## 常见问题

| 问题 | 解决方案 |
|------|----------|
| 采集失败 | 检查Cookie是否过期 |
| 暂停用户 | 设置 `enabled: false` |
| 评论报错"没有视频ID" | 先采集作品 |
| 签名失败 | 确保安装Node.js |

## 注意事项

1. 合理控制采集频率
2. Cookie定期过期需更新
3. 数据仅供学习研究

## License

[MIT](LICENSE)
