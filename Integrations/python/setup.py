"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
"""


from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
import os
from setuptools.extern import packaging


here = os.path.abspath(os.path.dirname(__file__))


# Versions should comply with PEP440.  For a discussion on single-sourcing
# the version across setup.py and the project code, see
# https://packaging.python.org/en/latest/single_source_version.html
# todo: does DH versions align w/ PEP440?
# see https://github.com/pypa/setuptools/blob/v40.8.0/setuptools/dist.py#L470
def normalize_version(ver):
    return str(packaging.version.Version(ver))


__deephaven_version__ = os.environ['DEEPHAVEN_VERSION']
__normalized_version__ = normalize_version(__deephaven_version__)


# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(name='deephaven',
      version=__normalized_version__,
      description='Python integrations for Deephaven',
      long_description=long_description,
      url='https://www.deephaven.io/',
      author='Deephaven Data Labs',
      author_email='python@deephaven.io',
      license='Deephaven Community License',
      test_loader='unittest:TestLoader',

      # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Build Tools',
          'License :: Other/Proprietary License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
      ],
      keywords='Deephaven Development',
      packages=find_packages(exclude=['docs', 'test']),
      install_requires=['deephaven-jpy=={}'.format(__normalized_version__),
                        'numpy', 'dill>=0.2.8', 'wrapt', 'pandas', 'numba;python_version>"3.0"',
                        'enum34;python_version<"3.4"',
                        'torch', 'torchsummary', 'torchvision', 'scikit-learn', 'tensorflow'],
      )
