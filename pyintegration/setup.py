#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os
import pathlib
from setuptools.extern import packaging
from setuptools import find_namespace_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


# Versions should comply with PEP440.  For a discussion on single-sourcing
# the version across setup.py and the project code, see
# https://packaging.python.org/en/latest/single_source_version.html
# todo: does DH versions align w/ PEP440?
# see https://github.com/pypa/setuptools/blob/v40.8.0/setuptools/dist.py#L470
def normalize_version(version):
    return str(packaging.version.Version(version))


__deephaven_version__ = os.environ['DEEPHAVEN_VERSION']
__normalized_version__ = normalize_version(__deephaven_version__)

setup(name='deephaven2',
      version=__normalized_version__,
      description='Deephaven Engine Python Package',
      long_description=README,
      packages=find_namespace_packages(exclude=("tests",)),
      url='https://deephaven.io/',
      author='Deephaven Data Labs',
      author_email='python@deephaven.io',
      license='Deephaven Community License',
      test_loader='unittest:TestLoader',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Developers, Data Scientists',
          'Topic :: Software Development :: Build Tools',
          'License :: Other/Proprietary License',
          'Programming Language :: Python :: 3.7',
      ],
      keywords='Deephaven Development',
      install_requires=['deephaven-jpy=={}'.format(__normalized_version__),
                        'numpy', 'dill>=0.2.8', 'wrapt', 'pandas', 'numba;python_version>"3.0"',
                        'enum34;python_version<"3.4"'],
      )
