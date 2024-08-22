# encoding=utf8

from __future__ import annotations
import logging
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)


class BaseEntity(BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # if not self.logic_validate():
        #    raise ValueError('logic validate error %s' % self.dict())

    # from_dict不会raise Exception
    @classmethod
    def from_dict(cls, d) -> Optional[BaseEntity]:
        if d is None:
            return None
        try:
            o = cls.__new__(cls)
            o.__init__(**d)
            return o
        except Exception as e:
            logger.warning(f'e={e} d={d}')
            return None

    def logic_validate(self) -> bool:
        logger.debug('%s', self.__dict__)
        return True

    def to_json(self):
        return self.model_dump_json(by_alias=True)

    def to_dict(self):
        return self.model_dump(by_alias=True, mode='json')

    # class Config:
    #    # https://stackoverflow.com/questions/62025723/how-to-validate-a-pydantic-object-after-editing-it
    #    validate_assignment = True
