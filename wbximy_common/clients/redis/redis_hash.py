# encoding=utf8

from datetime import datetime
import logging
from typing import Type
from wbximy_common.clients.redis._redis import Redis

logger = logging.getLogger(__name__)


# UPDATE@20240816
# 只接受datetime和str
class RedisHash(object):
    def __init__(self, name, value_type=int, **kwargs):
        assert value_type in (int, datetime)
        self.redis = Redis(**kwargs)
        self.name: str = name
        self.value_type: Type = value_type

    def get(self, key):
        o = self.redis.hget(self.name, key=key)
        if o is None:
            return o
        if self.value_type == datetime:
            o = datetime.strptime(o, '%Y-%m-%d %H:%M:%S')
        return o

    def set(self, key, value):
        assert isinstance(value, self.value_type)
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
        return self.redis.hset(self.name, key=key, value=value)

    def __len__(self):
        return self.redis.hlen(self.name)
