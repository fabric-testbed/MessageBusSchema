# coding: utf-8

from setuptools import setup, find_packages

from fabric import SchemaDir, MessageSchemaFile, KeySchemaFile

NAME = "fabric-message-bus"
VERSION = "0.3"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["confluent-kafka[avro]",
            "confluent-kafka"]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=NAME,
    version=VERSION,
    description="Fabric Python Message Bus Library",
    author="Komal Thareja",
    author_email="kthare10@renci.org",
    url="https://github.com/fabric-testbed/MessageBus",
    keywords=["Kafka", "Fabric Message Bus"],
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True,
    data_files=[(SchemaDir, [KeySchemaFile, MessageSchemaFile])],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
                  "Programming Language :: Python :: 3",
                  "License :: OSI Approved :: MIT License",
                  "Operating System :: OS Independent",
              ],
    python_requires='>=3.7'
)
