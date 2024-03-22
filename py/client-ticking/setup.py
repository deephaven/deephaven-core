#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import os
import pathlib

# Note: pkg_resources is deprecated https://setuptools.pypa.io/en/latest/pkg_resources.html, and it is suggested
# to use an external library `packaging`. From the context of building a wheel though, we'd prefer to not have to
# install extra dependencies, at least until we can more properly manage the build environment (pyproject.toml).
# TODO(deephaven-core#2233): upgrade setup.py to pyproject.toml
from pkg_resources import parse_version
from setuptools import find_packages, setup, Extension
from Cython.Build import cythonize

def _get_readme() -> str:
    # The directory containing this file
    HERE = pathlib.Path(__file__).parent
    # The text of the README file
    return (HERE / "README.md").read_text(encoding="utf-8")

def _normalize_version(java_version):
    partitions = java_version.partition("-")
    regular_version = partitions[0]
    local_segment = partitions[2]
    python_version = f"{regular_version}+{local_segment}" if local_segment else regular_version
    return str(parse_version(python_version))

def _compute_version():
    return _normalize_version(os.environ['DEEPHAVEN_VERSION'])

_version = _compute_version()

setup(
    name='pydeephaven-ticking',
    version=_version,
    description='The Deephaven Python Client for Ticking Tables',
    long_description=_get_readme(),
    long_description_content_type="text/markdown",
    packages=find_packages(where="src", exclude=("tests",)),
    package_dir={"": "src"},
    url='https://deephaven.io/',
    license='Deephaven Community License Agreement Version 1.0',
    author='Deephaven Data Labs',
    author_email='python@deephaven.io',
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],

    ext_modules = cythonize(
        [Extension("pydeephaven_ticking._core",
                   sources=["src/pydeephaven_ticking/*.pyx"],
                   language="c++",
                   extra_compile_args=["-std=c++17"],
                   libraries=["dhcore_static"]
        )]),
    python_requires='>=3.8',
    install_requires=[f"pydeephaven=={_version}"],
    package_data={'pydeephaven_ticking': ['py.typed']}
)
