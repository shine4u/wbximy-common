# encoding=utf8

import sys
import traceback
import netifaces
import os
import re
import yaml
from functools import lru_cache
from typing import Optional, List


# 获取当前设备的网卡IP，这里假设机器只有一个IP
# https://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-from-nic-in-python
@lru_cache(None)
def get_my_ip() -> Optional[str]:
    ip = None
    for if_name in netifaces.interfaces():
        interface = netifaces.ifaddresses(if_name)
        if netifaces.AF_INET not in interface:
            continue
        if len(interface[netifaces.AF_INET]) == 0:
            continue
        ip = interface[netifaces.AF_INET][0].get('addr', None)
        if not ip:
            continue
        if ip != '127.0.0.1':
            break
    print(f'get_ip >> {ip}')
    return ip


# 获取默认的项目根目录
@lru_cache(None)
def get_proj_dir():
    cur_path = os.path.abspath(__file__)
    while True:
        new_path = os.path.dirname(cur_path)
        if new_path == cur_path:
            print('error get get_env_yml_path, at root path %s, no env.yaml? exit 1' % cur_path)
            exit(1)
        cur_path = new_path
        env_yml_path = os.path.join(cur_path, 'env.yml')
        if os.path.isfile(env_yml_path):
            return cur_path


# 选取匹配上的第一个，如果没有匹配上，则表示只能使用默认配置
@lru_cache(None)
def get_env() -> Optional[str]:
    my_ip = get_my_ip()
    if my_ip is None:
        print('get_env_prop cannot get my_ip, exit 1')
        exit(1)
    with open(os.path.join(get_proj_dir(), 'env.yml'), 'rb') as f:
        item = yaml.safe_load(f)
        for pat, env in item.get('env', {}).items():
            if re.fullmatch(pat, my_ip):
                print(f'get_env >> {env}')
                return env
    return None


# 获取配置值 根据ip正则匹配获取环境
@lru_cache(None)
def get_env_prop(path: str, default=None):
    env = get_env()
    with open(os.path.join(get_proj_dir(), 'env.yml'), 'rb') as f:
        item = yaml.safe_load(f)
        prop = None
        if env is not None:
            prop = _dfs_travel_path(item, [env, ] + path.split('.'))
        if not prop:
            prop = _dfs_travel_path(item, path.split('.'))
        # print('get_env_prop %s  %s -> %s' % (env, path, prop))
        if prop is None:
            if default is None:
                raise ValueError('prop=%s not found' % path)
            else:
                return default
        else:
            return prop


def _dfs_travel_path(item, path: List[str]):
    # print('item=%s path=%s' % (item, path))
    if len(path) == 0:
        return item
    if isinstance(item, dict):
        path_sub = path[0]
        if path_sub in item:
            x = _dfs_travel_path(item[path_sub], path[1:])
            if x is not None:
                return x
        if 'default' in item:
            return _dfs_travel_path(item['default'], path[1:])
    return None


def get_props_mysql(inst: str):
    return {
        'host': get_env_prop(inst + '.host', default='localhost'),
        'port': get_env_prop(inst + '.port', default=3306),
        'user': get_env_prop(inst + '.user', default='work'),
        'password': get_env_prop(inst + '.password', default='work'),
    }


def get_props_redis(inst: str):
    return {
        'host': get_env_prop(inst + '.host', default='localhost'),
        'port': get_env_prop(inst + '.port', default=6379),
        'password': get_env_prop(inst + '.password', default='work'),
    }


def get_props_obs(inst: str):
    return {
        'server': get_env_prop(inst + '.server'),
        'access_key_id': get_env_prop(inst + '.access_key_id'),
        'secret_access_key': get_env_prop(inst + '.secret_access_key'),
    }


def get_props_kafka(inst: str):
    return {
        'bootstrap_servers': get_env_prop(inst + '.bootstrap_servers'),
    }


class ConstantProps:
    MYSQL_MAIN = get_props_mysql(inst='mysql.tx_vps')
    MYSQL_GS_A = get_props_mysql(inst='mysql.rds116')
    MYSQL_GS_B = get_props_mysql(inst='mysql.rds117')
    MYSQL_QXB = [
        get_props_mysql(inst='mysql.qxb.a'),
        get_props_mysql(inst='mysql.qxb.b'),
        get_props_mysql(inst='mysql.qxb.c'),
        get_props_mysql(inst='mysql.qxb.d'),
    ]

    REDIS_GS = get_props_redis(inst='redis.gs')


def get_stack_info():
    s = ''.join(traceback.format_stack()[:-1] + traceback.format_exception(*sys.exc_info())[1:])
    # s = s.replace('\n', '').replace('\r', ' ')
    return s


def main():
    for prop_paths in [
        'a.b.p_int',
        'a.b.p_str',
        'a.b.p_int_env',
    ]:
        prop = get_env_prop(prop_paths)
        print(f'{prop_paths} --> {prop} {type(prop)}')


if __name__ == '__main__':
    main()
