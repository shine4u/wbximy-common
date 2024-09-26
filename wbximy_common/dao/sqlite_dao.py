# encoding=utf8

import logging
from typing import Type, Dict, Optional
from wbximy_common.clients.sqlite_client import SqliteClient
from wbximy_common.dao.mysql_dao import EntityType, PKType

logger = logging.getLogger(__name__)


# SqliteDao 和 MySQLDao类似
class SqliteDao(SqliteClient):
    def __init__(self, tb_name: str, batch_size: int = 2000, entity_class: Type[EntityType] = None, **kwargs):
        self.tb_name: str = tb_name  # 指定表名称
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
        sql_where = ('where ' if kwargs else ' ') + ' and '.join(f'{k}=:{k}' for k in kwargs.keys())
        sql = f'select * from {self.tb_name} {sql_where} limit 1'
        d = self.select(sql, kwargs)
        return self._to_entity(d)

    def get_by_id(self, _id: PKType):
        return self.get(id=_id)
