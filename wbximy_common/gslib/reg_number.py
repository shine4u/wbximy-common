# encoding=utf8

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# 正常的注册号： 140000 10 0028286
# 14082500A000118(实际的) -> 14082500000118A 特殊处理 SX
def _calc_mask(area_code: str, ent_type: str, seq: int, is_mask_a=False) -> str:
    if is_mask_a:
        return 'A'
    if ent_type == 'NA':
        return 'X'
    rem = 0
    for d in (area_code + ent_type + '%.6d' % seq):
        rem = (rem + ord(d) - ord('0')) % 10
        rem = ((rem if rem != 0 else 10) * 2) % 11
    return str((11 - rem) % 10)


def _parse_reg_number(s: str) -> Optional[Tuple[str, str, int, str]]:
    if not isinstance(s, str) or len(s) != 15:
        logger.debug(f'bad reg_number {s}')
        return None
    area_code, ent_type = s[:6], s[6:8]
    if not re.match(r'\d{6}', area_code):
        logger.warning(f'bad reg_number {s}, bad area_code')
        return None
    if not re.match(r'\d{2}', ent_type) and ent_type != 'NA':
        logger.warning(f'bad reg_number {s}, bad ent_type')
        return None
    if re.match(r'A\d{6}', s[8:]):
        return area_code, ent_type, int(s[9:]), 'A'
    if re.match(r'\d{6}[\dX]', s[8:]):
        if _calc_mask(area_code, ent_type, int(s[8:-1])) == s[-1]:
            return area_code, ent_type, int(s[8:-1]), s[-1]
        logger.warning(f'bad reg_number {s}, bad mask')
        return None
    logger.warning(f'bad reg_number {s}')
    return None


# 判断是否是有效注册号
def reg_number_valid(s: str) -> bool:
    return _parse_reg_number(s) is not None


# 判断注册号是否有效，并进行增减量
def reg_number_incr(s: str, incr: int = 1) -> Optional[str]:
    parsed = _parse_reg_number(s)
    if not parsed:
        return None
    area_code, ent_type, seq, mask = parsed
    seq += incr
    if not (0 <= seq < 1000000):
        logger.warning(f'incr={incr} bad for reg_number {s}')
        return None
    mask = _calc_mask(area_code, ent_type, seq, mask == 'A')

    return area_code + ent_type + mask + '%.6d' % seq if mask == 'A' else area_code + ent_type + '%.6d' % seq + mask
