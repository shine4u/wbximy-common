# encoding=utf8

import time
import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, Generator, TypeVar, Dict
import pymysql
from dbutils.steady_db import SteadyDBConnection, SteadyDBCursor
from dbutils.pooled_db import PooledDB
from dbutils.persistent_db import PersistentDB
from wbximy_common.clients.tunnel import TunnelMixin

logger = logging.getLogger(__name__)
DBType = TypeVar('DBType', PersistentDB, PooledDB)


class Connection:
    def __init__(self, conn, transaction=False):
        self.conn: SteadyDBConnection = conn
        self.cursor: Optional[SteadyDBCursor] = None
        self.transaction = transaction

    def __enter__(self):
        if self.transaction:
            self.conn.begin()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if self.transaction:
            self.conn.commit()

    def execute(self, sql, args):
        before_exec_time = time.time()
        logger.debug('conn=%s sql=%s args=%s', id(self.conn), sql, args)
        try:
            ret = self.cursor.execute(sql, args)
        except Exception as e:
            logger.warning('error sql=%s, args=%s e=%s', sql, args, e)
            raise e
        exec_time = time.time() - before_exec_time
        if exec_time > 5:
            logger.warning('slow query sql=%s args=%s cost=%.2f', sql, args, exec_time)
        return ret

    def __del__(self):
        self.conn.close()


class MySQLClient(TunnelMixin):

    _conn_pool_cache: Dict[str, DBType] = dict()  # 全局连接池
    _conn_pool_cache_lock = Lock()

    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            port: int = 3306,
            tunnel: Optional[bool] = None,
            lazy_init: bool = True,  # 延迟初始化 默认打开
            using_persistent_db: bool = True,  # 采用哪种连接池 persistent or pooled
            can_share: bool = True,  # using_persistent_db=True 当host/port 一致时 生效
            max_connections: int = 4,  # PooledDB有效，最大连接数
            max_write_per_minute: int = -1,  # -1为不限制 每分钟写入(insert/update)速度控制
            auto_limit: bool = True,  # SQL语句是否补全limit
    ):
        super().__init__()
        self.host, self.port, self.tunnel = host, port, tunnel
        self.mix()
        self._lazy_init = lazy_init
        self._using_persistent_db: bool = using_persistent_db
        self._can_share: bool = can_share
        self._user: str = user
        self._password: str = password
        self._conn_pool: Optional[DBType] = None
        self._max_connections = max_connections
        self._cur_write_minute: datetime = datetime.now()  # 速度控制，当前写入时间 分钟有效
        self._cur_write_count: int = 0  # 速度控制，当前写入时间下的计数 insert+update
        self._max_write_per_minute = max_write_per_minute
        self._auto_limit = auto_limit

        if not self._lazy_init:
            self._init_conn_pool()

    def _init_conn_pool(self):
        if self._conn_pool is not None:
            return
        with self._conn_pool_cache_lock:
            host_port = '{}:{}'.format(self.host, self.port)
            if self._can_share and self._using_persistent_db:
                if host_port in self._conn_pool_cache:
                    logger.info('persistent_db using cache for %s', host_port)
                    self._conn_pool = self._conn_pool_cache[host_port]
                    return self._conn_pool

            # pymysql.cursors.SSDictCursor
            cursor_class = pymysql.cursors.DictCursor  # pyMySQL内存占用问题  这里有线程问题 待定

            if self._using_persistent_db:
                self._conn_pool = PersistentDB(
                    creator=pymysql,
                    maxusage=None,
                    closeable=False,
                    ping=0,
                    host=self.host,
                    port=self.port,
                    user=self._user,
                    password=self._password,
                    database=None,  # 连接池可以跨database，所以创建连接池时，不指定database
                    charset='UTF8MB4',
                    setsession=['SET AUTOCOMMIT = 1'],
                    cursorclass=cursor_class,
                )
            else:
                self._conn_pool = PooledDB(
                    creator=pymysql,
                    maxconnections=self._max_connections,  # 连接池允许的最大连接数
                    # mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                    # maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
                    blocking=True,
                    host=self.host,
                    port=self.port,
                    user=self._user,
                    password=self._password,
                    database=None,  # 连接池可以跨database，所以创建连接池时，不指定database
                    charset='UTF8MB4',
                    setsession=['SET AUTOCOMMIT = 1'],  # 设置线程池是否打开自动更新
                    cursorclass=cursor_class,
                )
            logger.info('new conn pool for %s persistent_db=%s', host_port, self._using_persistent_db)
            if self._can_share and self._using_persistent_db:
                self._conn_pool_cache[host_port] = self._conn_pool
            return self._conn_pool

    def get_conn(self, transaction=False):
        if not self._conn_pool:
            self._init_conn_pool()
        conn = self._conn_pool.connection()
        return Connection(conn, transaction)

    def _do_write_check(self, incr):
        if self._max_write_per_minute >= 0 and incr > 0:
            if self._cur_write_minute + timedelta(minutes=1) < datetime.now():
                self._cur_write_minute = datetime.now()
                self._cur_write_count = 0
            if self._cur_write_count >= self._max_write_per_minute:
                logger.warning('reach max write per minute %s, sleep for...', self._max_write_per_minute)
                time.sleep(1.0)
            self._cur_write_count += incr

    def select(self, sql: str, args=None) -> Optional[dict]:
        with self.get_conn() as conn:
            if self._auto_limit and ' limit ' not in sql:
                sql = sql + ' limit 1'
                logger.debug('modified sql=%s', sql)
            conn.execute(sql, args)
            result = conn.cursor.fetchone()
            return result

    def select_many(self, sql: str, args=None) -> Generator[dict, None, None]:
        with self.get_conn() as conn:
            if self._auto_limit and ' limit ' not in sql:
                sql = sql + ' limit 40000'
                logger.debug('modified sql=%s', sql)
            conn.execute(sql, args)
            for result in conn.cursor.fetchall():
                yield result

    # 返回变更的行数
    def execute(self, sql: str, args=None) -> int:
        with self.get_conn() as conn:
            rows_affected = conn.execute(sql, args)
            self._do_write_check(incr=rows_affected)
            return rows_affected

    # 返回生效的row_id
    def insert(self, sql: str, args=None) -> int:
        with self.get_conn() as conn:
            conn.execute(sql, args)
            if conn.cursor.lastrowid > 0:
                self._do_write_check(incr=1)
            return conn.cursor.lastrowid
