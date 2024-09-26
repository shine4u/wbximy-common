# encoding=utf8

from __future__ import annotations
from typing import Optional
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)


# last update at 2024-09-26
# 基于pydantic.BaseModel 的 dataclass 基类
class CustomBaseModel(BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # if not self.logic_validate():
        #    raise ValueError('logic validate error %s' % self.dict())

    # __init__ 会 raise Exception
    # from_dict 不会 raise Exception， 业务使用方便
    @classmethod
    def from_dict(cls, d) -> Optional[BaseModel]:
        if d is None:
            return None
        try:
            o = cls.__new__(cls)
            o.__init__(**d)
            return o
        except Exception as e:
            logger.warning(f'e={e} d={d}')
            return None

    # 用于运行时合理性检查，暂时关闭，没有需求，且耗性能
    # def logic_validate(self) -> bool:
    #     logger.debug('%s', self.__dict__)
    #     return True

    def to_json(self) -> str:
        return self.model_dump_json(by_alias=True)

    # mode = json | python 区别是 如果是json则不会包含不能序列化的属性
    def to_dict(self) -> dict:
        return self.model_dump(by_alias=True, mode='json')

    # class Config:
    #    # https://stackoverflow.com/questions/62025723/how-to-validate-a-pydantic-object-after-editing-it
    #    validate_assignment = True
