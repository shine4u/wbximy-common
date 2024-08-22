# encoding=utf8

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# 正常的注册号： 140000 10 0028286
# 14082500A000118(实际的) -> 14082500000118A 特殊处理 SX
class _RegNumber(object):
    def __init__(self, area_code: str, ent_type: str, seq: int, is_mask_a: bool):
        self.area_code = area_code
        self.ent_type = ent_type
        self.seq = seq
        self.is_mask_a = is_mask_a

    def dump(self) -> str:
        if self.is_mask_a:
            return self.area_code + self.ent_type + self._calc_mask() + '%.6d' % self.seq
        else:
            return self.area_code + self.ent_type + '%.6d' % self.seq + self._calc_mask()

    def _calc_mask(self) -> str:
        if self.is_mask_a:
            return 'A'
        if self.ent_type == 'NA':
            return 'X'
        rem = 0
        for d in (self.area_code + self.ent_type + '%.6d' % self.seq):
            rem = (rem + ord(d) - ord('0')) % 10
            rem = ((rem if rem != 0 else 10) * 2) % 11
        mask = str((11 - rem) % 10)
        return mask


def _load_reg_number(s: str) -> Optional[_RegNumber]:
    if not isinstance(s, str) or len(s) != 15:
        logger.debug('bad reg_number [%s]', s)
        return None
    area_code, ent_type = s[:6], s[6:8]
    if not re.match(r'\d{6}', area_code):
        logger.warning('bad area_code %s', s)
        return None
    if not re.match(r'\d{2}', ent_type) and ent_type != 'NA':
        logger.warning('bad ent_type %s', s)
        return None
    if re.match(r'A\d{6}', s[8:]):
        return _RegNumber(area_code, ent_type, int(s[9:]), True)
    if re.match(r'\d{7}', s[8:]) or re.match(r'\d{6}X', s[8:]):
        reg_number = _RegNumber(area_code, ent_type, int(s[8:-1]), False)
        if s == reg_number.dump():
            return reg_number
        logger.warning('bad mask %s', s)
        return None
    logger.warning('bad reg_number [%s]', s)
    return None


# 判断是否是有效注册号
def reg_number_valid(s: str) -> bool:
    return _load_reg_number(s) is not None


# 判断注册号是否有效，并进行增减量
def reg_number_incr(s: str, incr: int = 1) -> Optional[str]:
    reg_number = _load_reg_number(s)
    if not reg_number:
        return None
    reg_number.seq += incr
    if reg_number.seq < 0 or reg_number.seq >= 1000000:
        logger.warning('incr=%s bad for %s', incr, s)
        return None
    return reg_number.dump()
