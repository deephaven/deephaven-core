#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from setuptools import setup

setup(
    name='pydeephaven',
    version='0.1.0',
    packages=['barrage', 'barrage.flatbuf', 'pydeephaven', 'pydeephaven.proto'],
    url='https://deephaven.io/',
    license='',
    author='jianfengmao',
    author_email='jianfengmao@deephaven.io',
    description='',

    install_requires=['pandas',
                      'pyarrow',
                      'numpy',
                      'bitstring',
                      'grpcio',
                      'protobuf']
)
