#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os
import pathlib

# Note: pkg_resources is deprecated https://setuptools.pypa.io/en/latest/pkg_resources.html, and it is suggested
# to use an external library `packaging`. From the context of building a wheel though, we'd prefer to not have to
# install extra dependencies, at least until we can more properly manage the build environment (pyproject.toml).
# TODO(deephaven-core#2233): upgrade setup.py to pyproject.toml
from pkg_resources import parse_version
from setuptools import find_namespace_packages, setup

def _get_readme() -> str:
    # The directory containing this file
    HERE = pathlib.Path(__file__).parent
    # The text of the README file
    return (HERE / "README.md").read_text(encoding="utf-8")

def _normalize_version(java_version) -> str:
    partitions = java_version.partition("-")
    regular_version = partitions[0]
    local_segment = partitions[2]
    python_version = f"{regular_version}+{local_segment}" if local_segment else regular_version
    return str(parse_version(python_version))

def _compute_version():
    return _normalize_version(os.environ['DEEPHAVEN_VERSION'])

setup(
    name='deephaven-core',
    version=_compute_version(),
    description='Deephaven Engine Python Package',
    long_description=_get_readme(),
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
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    keywords='Deephaven Development',
    python_requires='>=3.8',
    install_requires=[
        'jpy>=1.0.0',
        'deephaven-plugin>=0.6.0',
        'numpy',
        'pandas>=1.5.0',
        'pyarrow',
        # TODO(deephaven-core#3082): Clarify dependency requirements wrt numba
        # It took 6 months for numba to support 3.11 after it was released, we want to make sure deephaven-core will be
        # installable when 3.13 is out. When we decide to upgrade to 3.13 or higher for testing/production, CI check
        # will alert us that numba isn't available.
        'numba; python_version < "3.13"',
    ],
    extras_require={
        "autocomplete": ["jedi==0.19.1"],
    },
    entry_points={
        'deephaven.plugin': ['registration_cls = deephaven.pandasplugin:PandasPluginRegistration']
    },
    package_data={'deephaven': ['py.typed']}
)
