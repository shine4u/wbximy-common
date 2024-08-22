# encoding=utf8

import logging
from typing import Type, Dict, TypeVar, Optional
from datetime import date, datetime
from wbximy_common.clients.sqlite_client import SqliteClient
from wbximy_common.dao.base_entity import BaseEntity

logger = logging.getLogger(__name__)

EntityType = TypeVar('EntityType', BaseEntity, Dict)
PKType = TypeVar('PKType', int, str, datetime, date)


# MySQLDao：对应一张具体的物理表
# 只有一次SQL的方法放到dao层，多个SQL构成事务放到service层
class SqliteDao(object):
    def __init__(
            self,
            tb_name: str,
            pk_name: str = 'id',
            batch_size: int = 2000,
            entity_class: Type[EntityType] = None,
            **kwargs,
    ):
        self.tb_name: str = tb_name  # 指定表名称
        self.pk_name: str = pk_name  # 主键
        self.batch_size: int = batch_size  # 批量读取数据的大小
        self.entity_class: Type[EntityType] = entity_class or dict  # 实体类
        self.db_client = SqliteClient(**kwargs)

    def _to_entity(self, d: Optional[Dict]) -> Optional[EntityType]:
        if d is None:
            return None
        if issubclass(self.entity_class, dict):
            return d
        return self.entity_class.from_dict(d)

    def _from_entity(self, item: EntityType) -> dict:
        if item is None:
            logger.warning('cannot from None, return emtpy dict')
            return {}
        if issubclass(self.entity_class, dict):
            return item
        return item.to_dict()

    def get(self, value, field: str = None) -> Optional[EntityType]:
        field = field or self.pk_name
        sql = f'select * from {self.tb_name} where {field}=:{field} limit 1'
        d = self.db_client.select(sql, {field: value})
        return self._to_entity(d)

    def get_ex(self, **kwargs) -> Optional[EntityType]:

        condition_sql = ' and '.join(f'{field}=:{field}' for field in kwargs.keys())
        sql = f'select * from {self.tb_name} where {condition_sql} limit 1'
        d = self.db_client.select(sql, kwargs)
        return self._to_entity(d)
