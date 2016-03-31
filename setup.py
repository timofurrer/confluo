# -*- coding: utf-8 -*-

"""
    Setup skynet service package.
"""

import ast
import re

from setuptools import setup, find_packages
from pip.req import parse_requirements


def get_version():
    """Gets the current version"""
    _version_re = re.compile(r"__VERSION__\s+=\s+(.*)")
    with open("service/__init__.py", "rb") as init_file:
        version = str(ast.literal_eval(_version_re.search(
            init_file.read().decode("utf-8")).group(1)))
    return version


def get_requirements():
    """Get a list of all requirements."""
    return list(x.name for x in parse_requirements("./requirements.txt", session=False))


setup(
    name="skynet-service",
    version=get_version(),
    license="MIT",

    description="Micro service framework for a component in skynet.",

    author="Timo Furrer",
    author_email="tuxtimo@gmail.com",

    url="https://github.com/timofurrer/skynet-service",

    packages=find_packages(),
    include_package_data=True,

    install_requires=get_requirements(),

    keywords=[
        "skynet", "service"
    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Utilities",
    ]
)
