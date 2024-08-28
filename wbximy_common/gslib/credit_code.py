# encoding=utf8

import logging
from typing import Optional, Tuple
from wbximy_common.gslib.org_code import org_code_valid, org_code_incr

logger = logging.getLogger(__name__)


# 统一信用代码样例：
# （53）（640000）（MJX17369）97
# 53640000MJX173680A
# 53640000MJX173672F

_w = [1, 3, 9, 27, 19, 26, 16, 17, 20, 29, 25, 13, 8, 24, 10, 30, 28, ]
_charset_num = ''.join([chr(ord('0') + d) for d in range(0, 10)])
_charset_alpha = ''.join([chr(ord('A') + d) for d in range(0, 26) if d not in [8, 14, 25, 18, 21]])
_charset = _charset_num + _charset_alpha


def _calc_mask(s: str) -> str:
    a = sum((_w[idx] * _charset.index(ch)) for idx, ch in enumerate(s))
    rem = a % 31
    if rem == 0:
        rem = 31
    mask = _charset[31 - rem]
    return mask


def _parse_credit_code(s: str) -> Optional[Tuple[str, str, str, str]]:
    if not isinstance(s, str) or len(s) != 18:
        logger.warning(f'bad credit_code {s}')
        return None
    if any(x not in _charset for x in s):
        logger.warning(f'bad credit_code {s}')
        return None
    credit_pref, appr_org_code, org_code = s[:2], s[2:8], s[8:17]
    if not org_code_valid(org_code):
        logger.warning(f'bad credit_code {s} bad org_code')
        return None
    if _calc_mask(s[:-1]) != s[-1]:
        logger.warning(f'bad credit_code mask {s}]')
        return None
    return credit_pref, appr_org_code, org_code, s[-1]


def credit_code_valid(s: str) -> bool:
    return _parse_credit_code(s) is not None


def credit_code_incr(s: str, incr: int = 1) -> Optional[str]:
    parsed = _parse_credit_code(s)
    if not parsed:
        return None
    credit_pref, appr_org_code, org_code, mask = parsed

    new_org_code = org_code_incr(org_code, incr=incr)
    if not new_org_code:
        return None
    return credit_pref + appr_org_code + new_org_code + _calc_mask(credit_pref + appr_org_code + new_org_code)
