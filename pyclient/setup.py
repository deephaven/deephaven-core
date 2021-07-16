from setuptools import setup

setup(
    name='deephaven',
    version='0.1.0',
    packages=['barrage', 'barrage.flatbuf', 'deephaven', 'deephaven.proto'],
    url='https://deephaven.io/',
    license='',
    author='jianfengmao',
    author_email='jianfengmao@deephaven.io',
    description='',

    install_requires=['pandas',
                      'pyarrow',
                      'flatbuffers',
                      'numpy',
                      'bitstring',
                      'grpcio',
                      'protobuf']
)
