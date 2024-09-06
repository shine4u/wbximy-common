# encoding=utf8

import logging
from typing import Dict, Tuple
from threading import Lock
from sshtunnel import SSHTunnelForwarder
from wbximy_common.libs.env import get_env_prop, get_env, get_proj_dir

logger = logging.getLogger(__name__)


# 可以通过隧道连接的Client， 隧道基于SSHTunnelForwarder
class TunnelMixin(object):
    tunnel_cache: Dict[Tuple[str, int], int] = dict()
    tunnel_cache_lock = Lock()

    def __init__(self, ):
        self._tunnel_server = None
        self.host, self.port, self.tunnel = None, None, None

    def mix(self, ):
        self._tunnel_server = None
        if self._use_tunnel():
            with self.tunnel_cache_lock:
                if (self.host, self.port) in self.tunnel_cache:
                    self.host, self.port = 'localhost', self.tunnel_cache[(self.host, self.port)]
                    return
                self._tunnel_server = SSHTunnelForwarder(
                    ssh_address_or_host=(get_env_prop('tunnel_hw.host'), 22),
                    ssh_username=get_env_prop('tunnel_hw.user'),
                    ssh_pkey=get_proj_dir() + '/' + 'work.pem',
                    remote_bind_address=(self.host, self.port)
                )
                self._tunnel_server.start()
                logger.info(f'localhost:{self._tunnel_server.local_bind_port} --> {self.host}:{self.port}')
                self.tunnel_cache[(self.host, self.port)] = self._tunnel_server.local_bind_port
                self.host, self.port = 'localhost', self._tunnel_server.local_bind_port

    def _use_tunnel(self, ) -> bool:
        if self.host is None or self.port is None:
            return False
        if self.host in ['localhost', '127.0.0.1', '82.156.19.64']:
            return False
        if self.tunnel is not None:
            return self.tunnel
        return get_env() == 'env_tyc_office'

    # def __del__(self):
    #    if self._tunnel_server:
    #        self._tunnel_server.close()
