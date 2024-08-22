# encoding=utf8

import time
import logging
from wbximy_common.clients.redis._redis import Redis

logger = logging.getLogger(__name__)


# UPDATE@20240816
# queue 基于redis zset
# 区分时效性任务和普通任务。超过队列长度，阻塞N秒，时效性任务额外的10%长度
# 只接受字符串数据
class RedisQueue(object):
    def __init__(self, name, max_length=1024, **kwargs):
        self.name: str = name
        self.max_length: int = max_length
        self.redis = Redis(**kwargs)

    # The __iter__ method simply returns self. This is needed so that the instance of the class is recognized as an iterator.
    def __iter__(self):
        return self

    # The __next__ method generates the next value and manages the state.
    def __next__(self):
        return self.pop(wait=-1)

    def pop(self, wait=0):
        if wait > 0:
            timeout = wait
        elif wait == 0:
            timeout = 1
        else:
            timeout = 0
        data = self.redis.bzpopmin(self.name, timeout=timeout)
        return data and data[1]

    def push(self, value: str, realtime=False, wait=5.0):
        assert isinstance(value, str)
        length = self.redis.zcard(self.name)
        max_length = int(self.max_length * 1.1) if realtime else self.max_length
        if length > max_length:
            logger.warning(f'sleep for length {length} greater than {max_length}')
            time.sleep(wait)
        score = time.time() - 1000000000 if realtime else time.time()
        return self.redis.zadd(self.name, {value: score})
