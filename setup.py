# -*- coding: utf-8 -*-

"""
    Setup confluo package.
"""

import ast
import re

from setuptools import setup, find_packages


def get_version():
    """Gets the current version"""
    _version_re = re.compile(r"__VERSION__\s+=\s+(.*)")
    with open("confluo/__init__.py", "rb") as init_file:
        version = str(ast.literal_eval(_version_re.search(
            init_file.read().decode("utf-8")).group(1)))
    return version


setup(
    name="confluo",
    version=get_version(),
    license="MIT",

    description="Minimalist scalable microservice framework for distributed systems using AMQP/RabbitMQ",

    author="Timo Furrer",
    author_email="tuxtimo@gmail.com",

    url="https://github.com/timofurrer/confluo",

    packages=find_packages(),
    include_package_data=True,

    install_requires=["aioamqp"],

    keywords=[
        "confluo", "service",
        "amqp", "rabbitmq", "scalable",
        "destributed", "microservice",
        "framework", "library", "package", "module"

    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Utilities",
    ]
)
