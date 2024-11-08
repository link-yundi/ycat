# -*- coding: utf-8 -*-
"""
---------------------------------------------
Created on 2024/7/1 10:20
@author: ZhangYundi
@email: yundi.xxii@outlook.com
---------------------------------------------
"""

VERSION = '1.0.6'
from setuptools import setup, find_packages

setup(
    name='ycat',
    version=VERSION,
    install_requires=['filelock',
                      'joblib',
                      'duckdb',
                      'sqlparse'],

    author='ZhangYundi',
    author_email='yundi.xxii@outlook.com',
    packages=find_packages(include=['ycat', 'ycat.*']),
    description='Local File Database',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/link-yundi/ycat',

    scripts=[],
    package_data={},
)