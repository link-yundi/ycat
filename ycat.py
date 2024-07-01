# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2024/7/1 09:44
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import os

from filelock import FileLock
from joblib import dump, load

default_db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "catdb")


# ======================== 本地数据库 catdb ========================
def connect(db_path=default_db_path):
    global default_db_path
    default_db_path = db_path
    if not os.path.exists(default_db_path):
        os.makedirs(default_db_path)


def _tb_parse(tb: str) -> str:
    return os.path.join(*tb.split("."))


def tb_path(tb: str) -> str:
    tb = _tb_parse(tb)
    return os.path.join(default_db_path, tb)


def join(*s: str) -> str:
    return '.'.join(s)


def get(tb: str):
    """读取tb中的数据"""
    tbpath = tb_path(tb)
    if not os.path.exists(tbpath):
        return None
    lockfile = f'{tbpath}.lock'
    with FileLock(lockfile):
        return load(tbpath)


def put(data, tb: str):
    """覆盖tb"""
    tbpath = tb_path(tb)
    if not os.path.exists(tbpath):
        try:
            os.makedirs(os.path.dirname(tbpath))
        except FileExistsError as e:
            pass
    lockfile = f'{tbpath}.lock'
    with FileLock(lockfile):
        with open(tbpath, mode='wb') as f:
            dump(data, f)


def mmap(tb: str, mode='r+'):
    """
    内存映射
    """
    tbpath = tb_path(tb)
    lockfile = f'{tbpath}.lock'
    with FileLock(lockfile):
        return load(tbpath, mode)


def delete(tb: str):
    """删除某个表"""
    p = tb_path(tb)
    if has(tb):
        os.remove(p)
    lock_file = f'{p}.lock'
    if os.path.exists(lock_file):
        os.remove(lock_file)


def has(tb: str) -> bool:
    """是否存在指定的表"""
    if not os.path.exists(tb_path(tb)):
        return False
    return True
