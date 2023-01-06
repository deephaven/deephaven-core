#
#  Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import os
import pathlib
from setuptools.extern import packaging
from setuptools import find_namespace_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README_PyPi.md").read_text()


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
    name='deephaven-server',
    version=__normalized_version__,
    description='Deephaven Embedded Server Python Package',
    long_description=README,
    long_description_content_type='text/markdown',
    packages=find_namespace_packages(exclude=("tests")),
    package_data={'deephaven_server': ['jars/*']},
    url='https://deephaven.io/',
    author='Deephaven Data Labs',
    author_email='python@deephaven.io',
    license='Deephaven Community License',
    test_loader='unittest:TestLoader',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='Deephaven Development',
    python_requires='>=3.7',
    install_requires=[
        'jpy>=0.13.0',
        "java-utilities",
        f"deephaven-core[autocomplete]=={__normalized_version__}",
    ]
)
