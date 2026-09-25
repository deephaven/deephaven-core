#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import os
import pathlib
import platform
from datetime import datetime, timezone

from Cython.Build import cythonize
from packaging.version import parse as parse_version
from setuptools import Extension, find_packages, setup


def _get_readme() -> str:
    # The directory containing this file
    HERE = pathlib.Path(__file__).parent
    # The text of the README file
    return (HERE / "README.md").read_text(encoding="utf-8")


def _snapshot_date() -> str:
    # Date-precision (UTC) timestamp used to build a PEP 440 dev pre-release for SNAPSHOTs.
    #
    # Reproducible builds: building the same source twice on different days would otherwise
    # yield different versions (e.g. .dev20260805 vs .dev20260806), so the artifact would not
    # be a deterministic function of the source. SOURCE_DATE_EPOCH is the cross-ecosystem
    # standard (https://reproducible-builds.org/specs/source-date-epoch/) for pinning build
    # time: an integer of seconds since the Unix epoch (UTC), conventionally the commit time
    # (git log -1 --pretty=%ct). When set, we use it instead of the wall clock so rebuilds of
    # the same commit produce an identical version; otherwise we fall back to "now".
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    when = (
        datetime.fromtimestamp(int(epoch), tz=timezone.utc)
        if epoch
        else datetime.now(tz=timezone.utc)
    )
    return when.strftime("%Y%m%d")


def _normalize_version(java_version):
    partitions = java_version.partition("-")
    regular_version = partitions[0]
    local_segment = partitions[2]
    if local_segment == "SNAPSHOT":
        python_version = f"{regular_version}.dev{_snapshot_date()}"
    elif local_segment:
        python_version = f"{regular_version}+{local_segment}"
    else:
        python_version = regular_version
    return str(parse_version(python_version))


def _compute_version():
    return _normalize_version(os.environ["DEEPHAVEN_VERSION"])


_version = _compute_version()
_system = platform.system()

if _system == "Windows":
    dhinstall = os.environ["DHINSTALL"]
    extra_compiler_args = ["/std:c++17", f"/I{dhinstall}\\include"]
    extra_link_args = [f"/LIBPATH:{dhinstall}\\lib"]

    # Ensure distutils uses the compiler and linker in %PATH%
    # You need an installation of Visual Studio 2022 with
    # * Python Development Workload
    # * Option "Python native development tools" enabled
    # And this should be run from the "x64 Native Tools Command Prompt" installed by VS
    # Note "x64_x86 Cross Tools Command Prompt" will NOT work.
    os.environ["DISTUTILS_USE_SDK"] = "y"
    os.environ["MSSdk"] = "y"
    libraries = ["dhcore_static", "ws2_32"]

else:
    extra_compiler_args = ["-std=c++17"]
    extra_link_args = []
    libraries = ["dhcore_static"]

setup(
    name="pydeephaven-ticking",
    version=_version,
    description="The Deephaven Python Client for Ticking Tables",
    long_description=_get_readme(),
    long_description_content_type="text/markdown",
    packages=find_packages(where="src", exclude=("tests",)),
    package_dir={"": "src"},
    url="https://deephaven.io/",
    license="Deephaven Community License Agreement Version 1.0",
    author="Deephaven Data Labs",
    author_email="python@deephaven.io",
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    ext_modules=cythonize(
        [
            Extension(
                "pydeephaven_ticking._core",
                sources=["src/pydeephaven_ticking/*.pyx"],
                language="c++",
                extra_compile_args=extra_compiler_args,
                extra_link_args=extra_link_args,
                libraries=libraries,
            )
        ]
    ),
    python_requires=">=3.9",
    install_requires=[f"pydeephaven=={_version}"],
    package_data={"pydeephaven_ticking": ["py.typed"]},
)
