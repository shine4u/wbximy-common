# encoding=utf8

import time
import logging
from abc import abstractmethod
from concurrent.futures import Future
from typing import List, Optional, Generator, Tuple
from concurrent.futures.thread import ThreadPoolExecutor
from wbximy_common.clients.redis.redis_hash import RedisHash
from wbximy_common.dao.mysql_dao import EntityType
from wbximy_common.dao.mysql_dao import MySQLDao, PKType

logger = logging.getLogger(__name__)


# 分库分表Dao hold a list of MySQLDao
class MySQLShardingDao(object):
    def __init__(
            self,
            sharding_key='id',
            **kwargs,
    ):
        self.sharding_key = sharding_key
        self.mysql_dao_list: List[MySQLDao] = self.get_sharding_dao_list(**kwargs)
        assert len(self.mysql_dao_list) > 0
        self.entity_class = self.mysql_dao_list[0].entity_class

    # 给定v 给出 分库分表位置
    @classmethod
    @abstractmethod
    def do_sharding(cls, v) -> int:
        pass

    @classmethod
    @abstractmethod
    def get_sharding_dao_list(cls, **kwargs) -> List[MySQLDao]:
        pass

    def get(self, **kwargs) -> Optional[EntityType]:
        if self.sharding_key not in kwargs:
            msg = f'{self.sharding_key} not in args'
            raise RuntimeError(msg)
        sharding_value = kwargs[self.sharding_key]
        dao = self.mysql_dao_list[self.do_sharding(sharding_value)]
        return dao.get(**kwargs)

    def get_many(self, **kwargs) -> Generator[EntityType, None, None]:
        if self.sharding_key not in kwargs:
            msg = f'{self.sharding_key} not in args'
            raise RuntimeError(msg)
        sharding_value = kwargs[self.sharding_key]
        dao = self.mysql_dao_list[self.do_sharding(sharding_value)]
        return dao.get_many(**kwargs)

    # 读取分库分表数据，并批量返回
    def sharding_scan(
            self,
            offsets_cache: RedisHash,  # 偏移量存储在redis
            start,  # offsets_cache 其次才使用start
            scan_key=None,
            infinite_wait_secs: int = 0,  # 0表示取完数立即退出，否则等待
            worker_num: int = 1,  # 读取MySQL线程数
            part_num: int = None,  # 只读取前part个分表
    ) -> Generator[List[EntityType], None, None]:
        scan_key = scan_key or self.sharding_key
        part_num = part_num or len(self.mysql_dao_list)

        with ThreadPoolExecutor(max_workers=worker_num, thread_name_prefix='sharding_scan') as executor:
            # check whether write start to cache.
            for part_id in range(part_num):
                if offsets_cache.get(f'{part_id:03d}') is None:
                    assert start is not None
                    offsets_cache.set(f'{part_id:03d}', start)

            # initialize the future_info
            futures: List[Tuple[Optional[Future], PKType, bool]] = []  # (future, last_offset, last_scan_empty)
            for part_id in range(part_num):
                futures.append((None, offsets_cache.get(f'{part_id:03d}'), False))

            while True:
                sleep_for_next_round = True

                # check whether to start a new job.
                part_id, offset, all_last_scan_empty = None, None, True
                for part_id_t in range(part_num):
                    future, offset_t, last_scan_empty = futures[part_id_t]
                    if future:
                        all_last_scan_empty = False
                    else:
                        # load offset_t from redis
                        offset_t_cache = offsets_cache.get(f'{part_id_t:03d}')
                        if offset_t_cache != offset_t:
                            logger.info(f'reload offset {part_id} {offset_t} -> {offset_t_cache}')
                            offset_t = offset_t_cache

                        if not last_scan_empty:
                            all_last_scan_empty = False
                        if (infinite_wait_secs == 0 or not last_scan_empty) and (offset is None or offset_t < offset):
                            part_id, offset = part_id_t, offset_t
                if all_last_scan_empty and infinite_wait_secs == 0:
                    break

                # start a new job.
                if offset is not None:
                    sleep_for_next_round = False
                    count = self.mysql_dao_list[part_id].batch_size
                    future_next = executor.submit(self.mysql_dao_list[part_id].scan_iter, offset, scan_key, count)
                    futures[part_id] = (future_next, offset, last_scan_empty)
                    # logger.info(f'start a new job {part_id}')

                # check if any future done.
                for part_id_t, (future, last_offset, last_scan_empty) in enumerate(futures):
                    if future and future.done():
                        sleep_for_next_round = False
                        next_offset, items = future.result()
                        logger.info(f'{part_id_t} {last_offset}->{next_offset} count={len(items)}')
                        yield items
                        offsets_cache.set(f'{part_id_t:03d}', next_offset)
                        futures[part_id_t] = (None, next_offset, next_offset == last_offset)

                if sleep_for_next_round:
                    # logger.info('nothing changed, sleep_for_next_round.')
                    time.sleep(1.0)
