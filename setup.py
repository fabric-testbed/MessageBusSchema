# coding: utf-8

import sys
from setuptools import setup, find_packages

NAME = "message_bus"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["connexion"]

setup(
    name=NAME,
    version=VERSION,
    description="Fabric Message Bus",
    author_email="kthare10@renci.org",
    url="",
    keywords=["Kafka", "Fabric Message Bus"],
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=False,
    entry_points={
        'console_scripts': ['message_bus=message_bus.__main__:main']},
    long_description="""\
    This is Fabric Message Bus
    """
)
