# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2024/7/1 09:44
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""
import os
import re

import duckdb
from filelock import FileLock
from joblib import dump, load

from ycat.parse import extract_table_names_from_sql

HOME = os.environ['HOME']
CATDB = os.path.join(HOME, 'catdb')


# ======================== 本地数据库 catdb ========================
def connect(db_path=CATDB):
    global CATDB
    CATDB = db_path
    if not os.path.exists(CATDB):
        os.makedirs(CATDB)


def tb_path(tb: str) -> str:
    return os.path.join(CATDB, tb)


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


def mmap(tb: str, ):
    """
    内存映射
    """
    tbpath = tb_path(tb)
    lockfile = f'{tbpath}.lock'
    with FileLock(lockfile):
        return load(tbpath, "r+")


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


def to_duckdb(df, tb, partitions: list = None, n_jobs=3):
    """
    :param df: pandas.DataFrame
    :param tb: 表名,支持路径写法, a/b/c
    :param partitions: 根据哪些字段进行分区，默认不分区
    :param n_jobs:
    :return:
    """
    tbpath = tb_path(tb)
    if not os.path.exists(tbpath):
        try:
            os.makedirs(tbpath)
        except FileExistsError as e:
            pass
    if partitions is not None:
        for field in partitions:
            assert field in df.columns, f'dataframe must have Field `{field}`'
    duckdb.sql("set global pandas_analyze_sample=10000")
    if n_jobs > 1:
        duckdb.sql(f"set threads={n_jobs};")
    insert_sql = f"""
                copy (
                    select * from df
                ) 
                to '{tbpath}/data.parquet' (format parquet, overwrite_or_ignore);
                """
    depth = 0
    if partitions is not None:
        if len(partitions) > 0:
            insert_sql = f"""
                copy (
                    select * from df
                ) 
                to '{tbpath}' (format parquet, partition_by ({', '.join(partitions)}), overwrite_or_ignore);
                """
            depth = len(partitions)
    duckdb.sql(insert_sql)
    # 记录分区深度
    put(data=depth, tb=f"{tb}/depth")


def sql(query: str, ):
    """从duckdb中读取数据"""
    tbs = extract_table_names_from_sql(query)
    convertor = dict()
    for tb in tbs:
        db_path = tb_path(tb)
        format_tb = f"read_parquet('{db_path}/data.parquet')"
        depth = get(f"{tb}/depth")
        if depth > 0:
            format_tb = f"read_parquet('{db_path}{'/*' * depth}/*.parquet', hive_partitioning = true)"
        convertor[tb] = format_tb
    pattern = re.compile("|".join(re.escape(k) for k in convertor.keys()))
    new_query = pattern.sub(lambda m: convertor[m.group(0)], query)
    return duckdb.sql(new_query)
