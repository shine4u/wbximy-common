# encoding=utf8

import re
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


def date2str(d: date) -> Optional[str]:
    if d is None:
        return None
    dt = datetime.combine(d, datetime.min.time())
    return dt.strftime('%Y-%m-%d')


def to_date(o) -> date:
    dt = to_datetime(o)
    return None if dt is None else dt.date()


def datetime2str(dt: datetime) -> Optional[str]:
    if dt is None:
        return None
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def to_datetime(o) -> Optional[datetime]:
    if o is None:
        return None
    if isinstance(o, float) or isinstance(o, int):
        # 认为是时间戳 time.time() 的值
        if o < 2667975961.8920162:  # in secs
            return datetime.fromtimestamp(o)
        else:  # in micro-secs
            return datetime.fromtimestamp(o / 1e3)
    if isinstance(o, datetime):
        return o
    if isinstance(o, date):
        return datetime(o.year, o.month, o.day)
    if isinstance(o, str):
        if o == '':
            return None
        if o == '0000-00-00 00:00:00':
            return None
        if o == '0000-00-00':
            return None
        mo = re.fullmatch(r'(\d{4}).?(\d{1,2}).?(\d{1,2})', o)
        if mo:
            # 2022/11/01
            year, month, day = [int(x) for x in mo.groups()]
            return datetime(year, month, day)
        mo = re.fullmatch(r'(\d{4})年(\d{1,2})月(\d{1,2})日', o)
        if mo:
            # 2018年08月24日
            year, month, day = [int(x) for x in mo.groups()]
            return datetime(year, month, day)
        if re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', o):
            # 2022-01-09 14:56:57
            return datetime.strptime(o, '%Y-%m-%d %H:%M:%S')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+', o):
            # 2022-11-09 14:56:57.475718
            return datetime.strptime(o, '%Y-%m-%d %H:%M:%S.%f')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', o):
            # 2022-11-09T14:56:57
            return datetime.strptime(o, '%Y-%m-%dT%H:%M:%S')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+', o):
            # 2022-11-09T14:56:57.475718
            return datetime.strptime(o, '%Y-%m-%dT%H:%M:%S.%f')
    logger.info('bad datetime %s [%s], return None', type(o), o)
    return None
