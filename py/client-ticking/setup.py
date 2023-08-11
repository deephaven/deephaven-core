#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import pathlib
from setuptools import find_packages, setup, Extension
from Cython.Build import cythonize

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='pydeephaven-ticking',
    version='0.28.0.dev',
    description='The Deephaven Python Client for Ticking Tables',
    long_description=README,
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
    ],

    ext_modules = cythonize(
        [Extension("pydeephaven_ticking._core",
                   sources=["src/pydeephaven_ticking/*.pyx"],
                   extra_compile_args=["-std=c++17"],
                   libraries=["dhcore"]
        )]),
    python_requires='>=3.8',
    install_requires=['pydeephaven==0.28.0']
)
