# encoding=utf8

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# 统一信用代码样例：
# （53）（640000）（MJX17369）97
# 53640000MJX173680A
# 53640000MJX173672F
class _OrgCode(object):
    w = [3, 7, 9, 10, 5, 8, 4, 2, ]
    charset: str = ''.join([chr(ord('0') + d) for d in range(0, 10)] + [chr(ord('A') + d) for d in range(0, 26)])

    def __init__(self, org_code: str):
        self.org_code = org_code

    def calc(self) -> str:
        a = sum((self.w[idx] * self.charset.index(ch)) for idx, ch in enumerate(self.org_code))
        rem = a % 11
        if rem == 1:
            mask = 'X'
        elif rem == 0:
            mask = '0'
        else:
            mask = str(11 - rem)
        return self.org_code + mask


class _CreditCode(object):
    w = [1, 3, 9, 27, 19, 26, 16, 17, 20, 29, 25, 13, 8, 24, 10, 30, 28, ]
    charset_num = ''.join([chr(ord('0') + d) for d in range(0, 10)])
    charset_alpha = ''.join([chr(ord('A') + d) for d in range(0, 26) if d not in [8, 14, 25, 18, 21]])
    charset = charset_num + charset_alpha

    def __init__(self, base_code: str, area_code: str, org_code: str):
        self.base_code = base_code
        self.area_code = area_code
        self.org_code = org_code

    def calc(self) -> str:
        s = self.base_code + self.area_code + _OrgCode(self.org_code).calc()
        a = sum((self.w[idx] * self.charset.index(ch)) for idx, ch in enumerate(s))
        rem = a % 31
        if rem == 0:
            rem = 31
        mask = self.charset[31 - rem]
        return s + mask


def _load_credit_code(s: str) -> Optional[_CreditCode]:
    if not isinstance(s, str) or len(s) != 18:
        logger.debug('bad credit_code [%s]', s)
        return None
    if any(x not in _CreditCode.charset for x in s):
        logger.warning('bad credit_code [%s]', s)
        return None
    credit_code = _CreditCode(s[:2], s[2:8], s[8:16])
    org_code = _OrgCode(s[8:16])
    if org_code.calc() != s[8:17]:
        logger.warning('bad org_code mask [%s->%s]', org_code.calc(), s)
        return None
    if credit_code.calc() != s:
        logger.warning('bad credit_code mask [%s->%s]', credit_code.calc(), s)
        return None
    return credit_code


def credit_code_valid(s: str) -> bool:
    return _load_credit_code(s) is not None


def credit_code_incr(s: str, incr: int = 1) -> Optional[str]:
    credit_code = _load_credit_code(s)
    if not credit_code:
        return None
    if credit_code.base_code in ['51', '52', '53']:
        digit_num = 5
        mod_num = 10
    else:
        digit_num = 8
        mod_num = 31

    value = 0
    for i in range(8-digit_num, 8):
        value = value * mod_num + _CreditCode.charset.index(credit_code.org_code[i])
    value += incr
    org_code_new = ''
    for i in range(digit_num - 1, -1, -1):
        org_code_new = _CreditCode.charset[value % mod_num] + org_code_new
        value = value // mod_num
    # logger.info('org_code %s -> %s', credit_code.org_code, org_code_new)
    credit_code.org_code = credit_code.org_code[:8-digit_num] + org_code_new
    return credit_code.calc()

