#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import pathlib
from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='pydeephaven',
    version='0.17.0',
    description='The Deephaven Python Client',
    long_description=README,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests",)),
    url='https://deephaven.io/',
    license='Deephaven Community License Agreement Version 1.0',
    author='Deephaven Data Labs',
    author_email='python@deephaven.io',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3.7',
    ],
    python_requires='>3.7',
    install_requires=['pyarrow',
                      'bitstring',
                      'grpcio',
                      'protobuf']
)
