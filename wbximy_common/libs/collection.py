# encoding=utf8

import logging
from typing import List, Generator, Dict
from itertools import groupby

logger = logging.getLogger(__name__)


# 列表分段
def split_parts(lst: List, sz: int) -> Generator[List, None, None]:
    for i in range(0, len(lst), sz):
        yield lst[i: min(i+sz, len(lst))]


def update_dict_value(d: Dict, k, old, new, force=False):
    if (force and k not in d) or (k in d and d[k] == old):
        d[k] = new


# refer: https://stackoverflow.com/questions/29645415/python-zip-by-key
def zip_by_key(lst0: List, lst1: List, key_func=None):
    lst0 = list((x, 0) for x in lst0)
    lst1 = list((x, 1) for x in lst1)
    key_func = (lambda x: x[0][0]) if not key_func else (lambda x: key_func(x[0]))
    for k, v in groupby(sorted(lst0 + lst1, key=key_func), key_func):
        v = list(v)
        if len(v) == 1:
            if v[0][1] == 0:
                yield v[0][0], None
            else:
                yield None, v[0][0]
        elif v[0][1] == 0:
            yield v[0][0], v[1][0]
        else:
            yield v[1][0], v[0][0]
