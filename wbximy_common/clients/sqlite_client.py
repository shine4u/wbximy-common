# encoding=utf8

import time
import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, Generator, TypeVar, Dict
import sqlite3
from dbutils.pooled_db import PooledDB
from dbutils.persistent_db import PersistentDB

logger = logging.getLogger(__name__)
DBType = TypeVar('DBType', PersistentDB, PooledDB)


class Connection:
    def __init__(self, conn, transaction=False):
        self.conn = conn
        self.cursor = None
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
        self.conn.commit()
        exec_time = time.time() - before_exec_time
        if exec_time > 5:
            logger.warning('slow query sql=%s args=%s cost=%.2f', sql, args, exec_time)
        return ret

    def __del__(self):
        self.conn.close()


class SqliteClient(object):

    _conn_pool_cache: Dict[str, DBType] = dict()  # 全局连接池
    _conn_pool_cache_lock = Lock()

    def __init__(
            self,
            db_path: str = './sqlite.db',
            lazy_init: bool = True,  # 延迟初始化 默认打开
            using_persistent_db: bool = True,  # 采用哪种连接池 persistent or pooled
            can_share: bool = True,  # using_persistent_db=True 当host/port 一致时 生效
            max_connections: int = 4,  # PooledDB有效，最大连接数
            max_write_per_minute: int = -1,  # -1为不限制 每分钟写入(insert/update)速度控制
            auto_limit: bool = True,  # SQL语句是否补全limit
    ):
        super().__init__()
        self._lazy_init = lazy_init
        self._using_persistent_db: bool = using_persistent_db
        self._can_share: bool = can_share
        self._conn_pool: Optional[DBType] = None
        self._max_connections = max_connections
        self._cur_write_minute: datetime = datetime.now()  # 速度控制，当前写入时间 分钟有效
        self._cur_write_count: int = 0  # 速度控制，当前写入时间下的计数 insert+update
        self._max_write_per_minute = max_write_per_minute
        self._auto_limit = auto_limit
        self.db_path = db_path

        if not self._lazy_init:
            self._init_conn_pool()

    def _init_conn_pool(self):
        if self._conn_pool is not None:
            return
        with self._conn_pool_cache_lock:
            host_port = 'localhost:0'
            if self._can_share and self._using_persistent_db:
                if host_port in self._conn_pool_cache:
                    logger.info('persistent_db using cache for %s', host_port)
                    self._conn_pool = self._conn_pool_cache[host_port]
                    return self._conn_pool

            # pymysql.cursors.SSDictCursor
            # cursor_class = pymysql.cursors.DictCursor  # pyMySQL内存占用问题  这里有线程问题 待定
            cursor_class = sqlite3.Cursor

            if self._using_persistent_db:
                self._conn_pool = PersistentDB(
                    creator=sqlite3,
                    maxusage=None,
                    closeable=False,
                    ping=0,
                    database=self.db_path,
                )
            else:
                self._conn_pool = PooledDB(
                    creator=sqlite3,
                    maxconnections=self._max_connections,  # 连接池允许的最大连接数
                    # mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                    # maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
                    blocking=True,
                    database=self.db_path,
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

    def select(self, sql: str, params=None) -> Optional[dict]:
        params = params or {}
        with self.get_conn() as conn:
            if self._auto_limit and ' limit ' not in sql:
                sql = sql + ' limit 1'
                logger.debug('modified sql=%s', sql)
            conn.execute(sql, params)
            row = conn.cursor.fetchone()
            if not row:
                return row
            row_dict = dict(zip([c[0] for c in conn.cursor.description], row))
            return row_dict

    def select_many(self, sql: str, params=None) -> Generator[dict, None, None]:
        params = params or {}
        with self.get_conn() as conn:
            if self._auto_limit and ' limit ' not in sql:
                sql = sql + ' limit 40000'
                logger.debug('modified sql=%s', sql)
            conn.execute(sql, params)
            for row in conn.cursor.fetchall():
                row_dict = dict(zip([c[0] for c in conn.cursor.description], row))
                yield row_dict

    # 返回变更的行数
    def execute(self, sql: str, params=None) -> int:
        params = params or {}
        with self.get_conn() as conn:
            rows_affected = conn.execute(sql, params)
            self._do_write_check(incr=rows_affected)
            return rows_affected

    # 返回生效的row_id
    def insert(self, sql: str, params=None) -> int:
        params = params or {}
        with self.get_conn() as conn:
            conn.execute(sql, params)
            if conn.cursor.lastrowid > 0:
                self._do_write_check(incr=1)
            return conn.cursor.lastrowid
