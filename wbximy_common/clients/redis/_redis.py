# encoding=utf8

import logging
import redis
from wbximy_common.clients.tunnel import TunnelMixin

logger = logging.getLogger(__name__)


# 增加TunneledClient功能
class Redis(redis.Redis, TunnelMixin):
    def __init__(self, host='localhost', port=6379, password=None, db=0, tunnel=None, **kwargs):
        self.host, self.port, self.tunnel = host, port, tunnel
        self.mix()
        super().__init__(
            host=self.host,
            port=self.port,
            password=password,
            db=db,
            decode_responses=True,
            **kwargs,
        )
