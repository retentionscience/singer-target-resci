#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-resci",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_resci"],
    install_requires=[
        'singer-python>=5.0.12',
        'requests==2.18.4',
        'psutil==5.3.1',
    ],
    entry_points="""
    [console_scripts]
    target-resci=target_resci:main
    """,
    packages=["target_resci"],
    package_data = {},
    include_package_data=True,
)
