# encoding=utf8

import logging
import time
from typing import Type, Dict, TypeVar, Optional, Generator, List, Tuple
from datetime import date, datetime

import pymysql.err
from pymysql import ProgrammingError
from wbximy_common.clients.mysql_client import MySQLClient
from wbximy_common.dao.base_entity import BaseEntity

logger = logging.getLogger(__name__)

EntityType = TypeVar('EntityType', BaseEntity, Dict)
PKType = TypeVar('PKType', int, str, datetime, date)


# MySQLDao：对应一张具体的物理表
class MySQLDao(MySQLClient):
    def __init__(self, db_tb_name: str, batch_size: int = 2000, entity_class: Type[EntityType] = None, **kwargs):
        self.db_tb_name: str = db_tb_name  # 指定库表名称
        self.batch_size: int = batch_size  # 批量读取数据的大小
        self.entity_class: Type[EntityType] = entity_class or dict  # 实体类
        super().__init__(**kwargs)

    def _to_entity(self, d: Optional[Dict]) -> Optional[EntityType]:
        if d is None:
            return None
        if issubclass(self.entity_class, dict):
            return d
        return self.entity_class.from_dict(d)

    def get(self, **kwargs) -> Optional[EntityType]:
        sql_where = ('where ' if kwargs else ' ') + ' and '.join(f'{k}=%({k})s' for k in kwargs.keys())
        sql = f'select * from {self.db_tb_name} {sql_where} limit 1'
        d = self.select(sql, args=kwargs)
        return self._to_entity(d)

    def get_by_id(self, _id: PKType):
        return self.get(id=_id)

    # limit should always set. default is self.batch_size
    def get_many(self, limit=None, **kwargs) -> Generator[EntityType, None, None]:
        limit = limit or self.batch_size
        sql_where = ' and '.join(f'{k} {"is" if v is None else "="} %({k})s' for k, v in kwargs.items())
        sql = f'select * from {self.db_tb_name} where {sql_where} limit %(limit)s'
        for d in self.select_many(sql, args=kwargs | {'limit': limit}):
            item = self._to_entity(d)
            if item is not None:
                yield item

    def get_max_id(self) -> PKType:
        sql = f'select max(id) from {self.db_tb_name}'
        d = self.select(sql)
        return d['max(id)']

    def table_exists(self) -> bool:
        try:
            self.get_by_id(1)
        except ProgrammingError:
            return False
        return True

    # 如果设置id，则按照id进行update， 如果未设置id，则进行insert ignore逻辑，返回是否变更
    def save_by_id(self, o: EntityType, ignore_create_update_time=True) -> bool:
        d = o.to_dict() if isinstance(o, BaseEntity) else o
        oid = d.pop('id')
        if ignore_create_update_time:
            d.pop('create_time', '')
            d.pop('update_time', '')
        sql_sets = ', '.join(f'{k}=%({k})s' for k in d)
        if not oid:
            sql = f'insert ignore {self.db_tb_name} set {sql_sets}'
            oid = self.insert(sql, args=d)
            if isinstance(o, BaseEntity):
                o.id = oid
            else:
                o['id'] = oid
            return oid > 0
        else:
            sql = f'update {self.db_tb_name} set {sql_sets} where id=%(id)s limit 1'
            try:
                changed = self.execute(sql, args=d | {'id': oid})
            except pymysql.err.IntegrityError:
                return False
            if changed > 1:
                logger.warning(f'changed={changed} > 1, error o={o.to_json()}')
            return changed == 1

    def save_by_group(self):
        pass

    # 不包括offset位置，选取「大约」count条数据， 大约：用于保证next_offset值的数据scan完整
    def scan_iter(self, offset: PKType, scan_key: str, count: int) -> Tuple[PKType, List[EntityType]]:
        sql = f'select * from {self.db_tb_name} where {scan_key} > %s limit %s'
        next_offset, items = offset, []
        for did, d in enumerate(self.select_many(sql=sql, args=(offset, int(count*1.2)))):
            if did >= count and d[scan_key] != next_offset:
                break
            next_offset = d[scan_key]
            item = self._to_entity(d)
            if item is not None:
                items.append(item)
        return next_offset, items

    # 根据索引循环遍历数据， 基于scan_iter
    def scan(self, start, scan_key='id', total=0, infinite_sleep_secs: int = 0) -> Generator[EntityType, None, None]:
        count, offset = 0, start
        while True:
            next_offset, items = self.scan_iter(offset=offset, scan_key=scan_key, count=self.batch_size)
            for item in items:
                yield item
                count += 1
                if 0 < total <= count:
                    break
            logger.info(f'{self.db_tb_name} offset {offset}->{next_offset}')
            if infinite_sleep_secs > 0 and next_offset == offset:
                logger.info(f'{self.db_tb_name} sleep {infinite_sleep_secs} for next scan')
                time.sleep(infinite_sleep_secs)
            if offset == next_offset or 0 < total <= count:
                break
            offset = next_offset
