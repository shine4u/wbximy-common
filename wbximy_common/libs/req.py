# encoding=utf8
import time
import base64
import requests
import logging
from typing import Dict, Optional
from threading import Lock, current_thread
from requests import Session, Response
import urllib3

logger = logging.getLogger(__name__)
urllib3.disable_warnings()

PROXY_DEFAULT = {"https": "http://10.99.138.95:30636", "http": "http://10.99.138.95:30636"}
HEADERS_DEFAULT = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/98.0.4758.102 Safari/537.36 MicroMessenger/6.8.0(0x16080000) NetType/WIFI '
                  'MiniProgramEnv/Mac MacWechat/WMPF XWEB/30515',
    'Accept-Language': 'zh-CN,zh',
}


class URLPat(object):
    def __init__(
            self,
            name: str,
            pat: str,
            validate_func=None,
            timeout=3.,
            tries=3,
            **kwargs,
    ):
        self.name = name
        self.pat = pat
        self.validate_func = validate_func or (lambda x: x.status_code == 200)
        self.timeout = timeout
        self.tries = tries
        self.custom_headers = kwargs


# 每个线程保持独立的requests.Session进行请求
class ReqManager(object):
    def __init__(
            self,
            pats: list[URLPat],
            proxies=None,
            default_headers=None,
            use_proxy=True,
    ):
        self.sessions: Dict[int, Session] = {}  # tid -> session
        self.sessions_lock = Lock()
        self.proxies = (proxies or PROXY_DEFAULT) if use_proxy else None
        self.default_headers = default_headers or HEADERS_DEFAULT
        self.pats = dict((x.name, x) for x in pats)

    # 1、params and payloads read from kwargs
    def request(self, pat: str, force_post=False, **kwargs) -> Optional[Response]:
        with self.sessions_lock:
            tid = current_thread().ident
            if tid not in self.sessions:
                self.sessions[tid] = Session()
            if self.sessions[tid] is None:
                self.sessions[tid] = Session()
            session = self.sessions[tid]

        pat_obj = self.pats[pat]
        url = pat_obj.pat
        for k, v in kwargs.copy().items():
            if '{' + k + '}' in url:
                url = url.replace('{' + k + '}', str(v))
                kwargs.pop(k)

        for try_id in range(pat_obj.tries):
            start_ts = time.time()
            try:
                response = session.request(
                    method='POST' if (kwargs or force_post) else 'GET',
                    url=url,
                    data=kwargs or {},
                    timeout=pat_obj.timeout,
                    headers=self.default_headers | (pat_obj.custom_headers or {}),
                    proxies=self.proxies,
                    verify=False,
                )
                proxy = response.cookies.get_dict().get('proxyBase', '')
                if proxy:
                    proxy = base64.standard_b64decode(proxy).decode()
                cost_ts = time.time() - start_ts
                status_code, size = response.status_code, len(response.content)
                validate_ret = pat_obj.validate_func(response)
                logger.info(f'RESPONSE #{try_id} {cost_ts:.1f} {status_code} {size:5d} {url} {proxy:20s}')
                if validate_ret:
                    return response
                if proxy:
                    del session.cookies['proxyBase']
            except requests.exceptions.ReadTimeout:
                logger.info(f'RESPONSE #{try_id} {url} ReadTimeout')
        logger.warning(f'{url} max tries={pat_obj.tries} exceed!')
        return None

    @staticmethod
    def response_validate_default(response: Response):
        if response.status_code >= 400:
            return None
        if len(response.text) > 0:
            return True
        return False
