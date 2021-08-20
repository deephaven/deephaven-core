#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from setuptools import setup

setup(
    name='pydeephaven',
    version='0.0.1',
    packages=['pydeephaven'],
    url='https://deephaven.io/',
    license='Deephaven Community License Agreement Version 1.0',
    author='Deephaven Data Labs',
    author_email='jianfengmao@deephaven.io',
    description='The Deephaven Python Client',

    install_requires=['pyarrow',
                      'bitstring',
                      'grpcio',
                      'protobuf']
)
