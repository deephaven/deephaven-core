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

setup(
    name='deephaven-core',
    version=__normalized_version__,
    description='Deephaven Engine Python Package',
    long_description=README,
    long_description_content_type='text/markdown',
    packages=find_namespace_packages(exclude=("tests", "tests.*", "integration-tests", "test_helper")),
    url='https://deephaven.io/',
    author='Deephaven Data Labs',
    author_email='python@deephaven.io',
    license='Deephaven Community License',
    test_loader='unittest:TestLoader',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: Other/Proprietary License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='Deephaven Development',
    python_requires='>=3.8',
    install_requires=[
        'jpy>=0.14.0',
        'deephaven-plugin',
        'numpy',
        'pandas>=1.5.0',
        'pyarrow',
        # TODO(deephaven-core#3082): Remove numba dependency workarounds
        # It took 6 months for numba to support 3.11 after it was released, we want to make sure deephaven-core will be
        # installable when 3.12 is out. When we decide to upgrade to 3.12 or higher for testing/production, CI check
        # will alert us that numba isn't available.
        'numba; python_version < "3.12"',
    ],
    extras_require={
        "autocomplete": ["jedi==0.18.2"],
    },
    entry_points={
        'deephaven.plugin': ['registration_cls = deephaven.pandasplugin:PandasPluginRegistration']
    }
)
