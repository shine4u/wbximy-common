# encoding=utf8

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# 统一信用代码样例：
# （53）（640000）（MJX17369）97
# MJX17369-9为组织机构代码
_w = [3, 7, 9, 10, 5, 8, 4, 2, ]
_charset: str = ''.join([chr(ord('0') + d) for d in range(0, 10)] + [chr(ord('A') + d) for d in range(0, 26)])


def _calc_mask(s: str) -> str:
    a = sum((_w[idx] * _charset.index(ch)) for idx, ch in enumerate(s))
    rem = a % 11
    if rem == 1:
        mask = 'X'
    elif rem == 0:
        mask = '0'
    else:
        mask = str(11 - rem)
    return mask


def _parse_org_code(s: str) -> Optional[Tuple[str, str, str]]:
    if not isinstance(s, str) or len(s) != 9:
        logger.warning(f'bad org_code {s}')
        return None
    if _calc_mask(s[:-1]) == s[-1]:
        return s[:4], s[4:8], s[-1]
    logger.warning(f'bad org_code {s}, bad mask')
    return None


def org_code_valid(s: str) -> bool:
    return _parse_org_code(s) is not None


def org_code_incr(s: str, incr: int = 1, only_numbers=False) -> Optional[str]:
    parsed = _parse_org_code(s)
    if not parsed:
        return None
    org_code_header, seq_s, mask = parsed
    # right 4 characters of org_code is digits if only_numbers
    mod_number = 10 if only_numbers else 31

    seq = 0
    for char in seq_s:
        seq = seq * mod_number + _charset.index(char)
    seq += incr

    org_code_new = ''
    for i in range(4):
        org_code_new = _charset[seq % mod_number] + org_code_new
        seq = seq // mod_number
    if seq != 0:
        logger.warning(f'incr={incr} bad for org code {s}')
        return None
    return org_code_header + org_code_new + _calc_mask(org_code_new)
