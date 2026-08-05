#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import os
import pathlib
from datetime import datetime, timezone

from packaging.version import parse as parse_version
from setuptools import find_namespace_packages, setup


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


def _normalize_version(java_version) -> str:
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


setup(
    name="deephaven-core",
    version=_compute_version(),
    description="Deephaven Engine Python Package",
    long_description=_get_readme(),
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(
        exclude=("tests", "tests.*", "integration-tests", "test_helper")
    ),
    url="https://deephaven.io/",
    author="Deephaven Data Labs",
    author_email="python@deephaven.io",
    license="Deephaven Community License",
    test_loader="unittest:TestLoader",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
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
    keywords="Deephaven Development",
    python_requires=">=3.9",
    install_requires=[
        "jpy>=2.1.0",
        "deephaven-plugin>=0.6.0",
        "numpy",
        "pandas>=1.5.0",
        "pyarrow",
    ],
    extras_require={
        "autocomplete": ["jedi==0.19.1", "docstring_parser>=0.16"],
    },
    entry_points={
        "deephaven.plugin": [
            "registration_cls = deephaven.pandasplugin:PandasPluginRegistration"
        ]
    },
    package_data={"deephaven": ["py.typed"], "deephaven_internal": ["py.typed"]},
)
