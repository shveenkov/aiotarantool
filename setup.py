# -*- coding: utf-8 -*-

import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def find_version():
    for line in open("aiotarantool.py"):
        if line.startswith("__version__"):
            return re.match(r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)

setup(
    name="aiotarantool",
    py_modules=["aiotarantool"],
    version=find_version(),
    author="Dmitry Shveenkov",
    author_email="shveenkov@mail.ru",
    url="https://github.com/shveenkov/aiotarantool",
    classifiers=[
        "Programming Language :: Python :: 3.4",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    install_requires=[
        "tarantool>=0.5.1",
    ],
    description="Tarantool connection driver for work with asyncio",
    long_description=open("README.rst").read()
)
