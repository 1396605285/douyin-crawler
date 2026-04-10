import asyncio
import httpx
import execjs
import urllib.parse
import random
import os
from typing import Dict, Optional, Tuple
from core.logger import logger

try:
    import cookiesparser
    COOKIESPARSER_AVAILABLE = True
except ImportError:
    COOKIESPARSER_AVAILABLE = False


class DouyinAPI:
    HOST = 'https://www.douyin.com'
    
    COMMON_PARAMS = {
        'device_platform': 'webapp',
        'aid': '6383',
        'channel': 'channel_pc_web',
        'update_version_code': '170400',
        'pc_client_type': '1',
        'pc_libra_divert': 'Windows',
        'version_code': '290100',
        'version_name': '29.1.0',
        'cookie_enabled': 'true',
        'screen_width': '1920',
        'screen_height': '1080',
        'browser_language': 'zh-CN',
        'browser_platform': 'Win32',
        'browser_name': 'Chrome',
        'browser_version': '132.0.0.0',
        'browser_online': 'true',
        'engine_name': 'Blink',
        'engine_version': '132.0.0.0',
        'os_name': 'Windows',
        'os_version': '10',
        'cpu_core_num': '16',
        'device_memory': '8',
        'platform': 'PC',
        'downlink': '10',
        'effective_type': '4g',
        'round_trip_time': '50',
    }
    
    COMMON_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "sec-ch-ua-platform": "Windows",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "referer": "https://www.douyin.com/?recommend=1",
        "priority": "u=1, i",
        "pragma": "no-cache",
        "cache-control": "no-cache",
        "accept-language": "zh-CN,zh;q=0.9",
        "accept": "application/json, text/plain, */*",
        "dnt": "1",
    }
    
    _instance = None

    def __new__(cls, cookie: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, cookie: str = None):
        if hasattr(self, '_initialized') and self._initialized:
            if cookie is not None:
                self.cookie = cookie
                self._parse_cookie()
            return
        
        self.cookie = cookie or ""
        self._sign_js = None
        self._cookie_dict = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._load_sign_js()
        self._parse_cookie()
        self._initialized = True

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=30.0),
                follow_redirects=True
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    def __del__(self):
        if self._client and not self._client.is_closed:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._client.aclose())
            except RuntimeError:
                try:
                    asyncio.run(self._client.aclose())
                except Exception:
                    pass
            except Exception:
                pass

    @classmethod
    async def close_instance(cls):
        if cls._instance:
            await cls._instance.close()

    def _load_sign_js(self):
        js_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'douyin.js')
        if os.path.exists(js_path):
            try:
                with open(js_path, 'r', encoding='utf-8') as f:
                    self._sign_js = execjs.compile(f.read())
                logger.debug("签名JS加载成功")
            except Exception as e:
                logger.warning(f"签名JS加载失败: {e}")
        else:
            logger.warning(f"签名JS文件不存在: {js_path}")

    def _parse_cookie(self):
        if not self.cookie:
            return
        if not COOKIESPARSER_AVAILABLE:
            logger.warning("cookiesparser库未安装，Cookie解析功能不可用")
            return
        try:
            self._cookie_dict = cookiesparser.parse(self.cookie)
        except Exception as e:
            logger.warning(f"Cookie解析失败: {e}")

    def _get_ms_token(self, length: int = 120) -> str:
        chars = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789='
        return ''.join(random.choice(chars) for _ in range(length))

    async def _prepare_params(self, params: Dict, headers: Dict) -> Tuple[Dict, Dict]:
        params = dict(params)
        params.update(self.COMMON_PARAMS)
        
        headers = dict(headers)
        headers.update(self.COMMON_HEADERS)
        
        if self.cookie:
            headers['cookie'] = self.cookie
        
        params['msToken'] = self._get_ms_token()
        
        if self._cookie_dict:
            params['screen_width'] = self._cookie_dict.get('dy_swidth', 1920)
            params['screen_height'] = self._cookie_dict.get('dy_sheight', 1080)
            params['cpu_core_num'] = self._cookie_dict.get('device_web_cpu_core', 16)
            params['device_memory'] = self._cookie_dict.get('device_web_memory_size', 8)
            
            s_v_web_id = self._cookie_dict.get('s_v_web_id')
            if s_v_web_id:
                params['verifyFp'] = s_v_web_id
                params['fp'] = s_v_web_id
            
            uifid = self._cookie_dict.get('UIFID_TEMP')
            if uifid:
                params['uifid'] = uifid
        
        query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])
        
        if self._sign_js:
            try:
                a_bogus = self._sign_js.call('sign_datail', query, headers["User-Agent"])
                params["a_bogus"] = a_bogus
            except Exception as e:
                logger.warning(f"签名失败: {e}")
        
        return params, headers

    async def _request(self, url: str, params: Dict, headers: Dict) -> Dict:
        try:
            client = await self._get_client()
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()

            content_type = response.headers.get('content-type', '')
            response_text = response.text

            logger.info(f"[调试] URL: {url}")
            logger.info(f"[调试] 响应状态: {response.status_code}")
            logger.info(f"[调试] Content-Type: {content_type}")
            logger.info(f"[调试] 响应内容长度: {len(response_text)}")
            logger.info(f"[调试] 响应内容前500字符: {response_text[:500] if response_text else '(空)'}")

            if not response_text or not response_text.strip():
                logger.error(f"[调试] 响应体为空!")
                return {}

            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP错误 [{e.response.status_code}]: {url}")
            try:
                logger.info(f"[调试] 错误响应内容: {e.response.text[:500]}")
            except Exception:
                pass
            return {}
        except httpx.TimeoutException:
            logger.error(f"请求超时: {url}")
            return {}
        except httpx.RequestError as e:
            logger.error(f"网络错误: {e}")
            return {}
        except Exception as e:
            logger.error(f"请求失败: {e}")
            logger.info(f"[调试] 响应状态: {e.response.status_code if hasattr(e, 'response') and e.response else 'N/A'}")
            try:
                if hasattr(e, 'response') and e.response:
                    logger.info(f"[调试] 错误响应内容: {e.response.text[:500]}")
            except Exception:
                pass
            return {}

    async def fetch_videos(self, sec_user_id: str, max_cursor: int = 0, count: int = 30) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/aweme/post/"
        params = {
            "sec_user_id": sec_user_id,
            "max_cursor": str(max_cursor),
            "count": str(count),
            "from_user_page": "1",
            "publish_video_strategy_type": "2",
            "show_live_replay_strategy": "1",
            "need_time_list": "1"
        }
        params, headers = await self._prepare_params(params, {})
        return await self._request(url, params, headers)

    async def fetch_comments(self, aweme_id: str, cursor: int = 0, count: int = 50) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/comment/list/"
        params = {
            "aweme_id": aweme_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0"
        }
        params, headers = await self._prepare_params(params, {})
        return await self._request(url, params, headers)

    async def fetch_replies(self, aweme_id: str, comment_id: str, cursor: int = 0, count: int = 50) -> Dict:
        url = "https://www.douyin.com/aweme/v1/web/comment/list/reply/"
        params = {
            "item_id": aweme_id,
            "comment_id": comment_id,
            "cursor": str(cursor),
            "count": str(count),
            "item_type": "0"
        }
        params, headers = await self._prepare_params(params, {})
        
        if self._sign_js:
            try:
                query = '&'.join([f'{k}={urllib.parse.quote(str(v))}' for k, v in params.items()])
                a_bogus = self._sign_js.call('sign_reply', query, headers["User-Agent"])
                params["a_bogus"] = a_bogus
            except Exception as e:
                logger.warning(f"签名失败: {e}")
        
        return await self._request(url, params, headers)
