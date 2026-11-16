#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

from deephaven_server import start_jvm

VM_OPTIONS_FILE_ARG = "-XX:VMOptionsFile="

# The default VM options each Java major version gets. Changing the options in
# buildSrc/src/main/groovy/io.deephaven.java-toolchain-conventions.gradle without updating them here fails these tests.
GENERIC_OPTIONS = [
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=100",
    "-XX:+UseStringDeduplication",
    "--enable-native-access=ALL-UNNAMED",
]
GC_LOCKER_OPTIONS = ["-XX:GCLockerRetryAllocationCount=128"]

# When the build overrides the defaults (deephaven.javaOpts or JAVA_OPTS), every version gets the override instead,
# which the build passes as DH_JAVA_OPTS_OVERRIDE. Possibly empty; None when there is none.
_override = os.environ.get("DH_JAVA_OPTS_OVERRIDE")
OVERRIDE = None if _override is None else _override.split()

# Versions to check, including one with no file of its own (22)
VERSIONS = (17, 21, 22, 25)


def _expected_options(java_version: int | None) -> list[str]:
    if OVERRIDE is not None:
        return OVERRIDE
    if java_version in (17, 21):
        return GENERIC_OPTIONS + GC_LOCKER_OPTIONS
    return GENERIC_OPTIONS


ALL_OPTIONS = GENERIC_OPTIONS + GC_LOCKER_OPTIONS + (OVERRIDE or [])

# Each file the wheel ships, with its options
EXPECTED_FILES = {
    "dh-default.vmoptions": _expected_options(22),
    "dh-default-17.vmoptions": _expected_options(17),
    "dh-default-21.vmoptions": _expected_options(21),
    "dh-default-25.vmoptions": _expected_options(25),
}

# Environment variables jpyutil consults when looking for the JVM
JPY_ENV_VARS = (
    "JPY_PY_CONFIG",
    "JPY_JVM_DLL",
    "JPY_JAVA_HOME",
    "JPY_JDK_HOME",
    "JPY_JRE_HOME",
    "JAVA_HOME",
    "JDK_HOME",
    "JRE_HOME",
    "JAVA_JRE",
)


def _options_in(vmoptions: pathlib.Path) -> list[str]:
    return vmoptions.read_text().split()


class VmOptionsFileTest(unittest.TestCase):
    """The embedded server's choice of VM options file, against the files this wheel ships. The Java homes are stand-ins
    holding a release file and an empty libjvm.so, since choosing the file reads nothing else. Each test hides any
    jpyconfig module and jpy environment variables, so only what the test sets is visible to jpyutil."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self._patch(
            mock.patch.dict(
                os.environ,
                {k: v for k, v in os.environ.items() if k not in JPY_ENV_VARS},
                clear=True,
            )
        )
        # A None entry makes "import jpyconfig" raise ImportError
        self._patch(mock.patch.dict(sys.modules, {"jpyconfig": None}))

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _java_home(self, java_version: int | None) -> pathlib.Path:
        home = pathlib.Path(tempfile.mkdtemp(dir=self._dir.name))
        (home / "lib" / "server").mkdir(parents=True)
        (home / "lib" / "server" / "libjvm.so").touch()
        if java_version is not None:
            (home / "release").write_text(f'JAVA_VERSION="{java_version}.0.1"\n')
        return home

    @staticmethod
    def _jvm_dll(home: pathlib.Path) -> str:
        return str(home / "lib" / "server" / "libjvm.so")

    def _assert_selects(self, jvm_dll: str | None, java_version: int | None):
        detected = start_jvm._java_major_version(jvm_dll)
        self.assertEqual(detected, java_version)
        vmoptions = start_jvm._default_vmoptions(detected)
        self.assertTrue(vmoptions.is_file(), f"{vmoptions} is not in the wheel")
        self.assertCountEqual(
            _options_in(vmoptions), _expected_options(java_version), vmoptions.name
        )

    def test_wheel_ships_the_expected_options(self):
        shipped = {p.name for p in start_jvm._jars_path().glob("*.vmoptions")}
        self.assertCountEqual(shipped, EXPECTED_FILES)
        for name, options in EXPECTED_FILES.items():
            with self.subTest(file=name):
                self.assertCountEqual(
                    _options_in(start_jvm._jars_path() / name), options
                )

    def test_options_for_each_version(self):
        for java_version in VERSIONS:
            with self.subTest(java_version=java_version):
                home = self._java_home(java_version)
                jvm_dll = start_jvm._find_jvm_dll(str(home), None)
                self.assertEqual(jvm_dll, self._jvm_dll(home))
                self._assert_selects(jvm_dll, java_version)

    def test_config_java_home_is_used_without_java_home(self):
        home = self._java_home(21)
        config = types.SimpleNamespace(java_home=str(home))
        self.assertEqual(start_jvm._find_jvm_dll(None, config), self._jvm_dll(home))

    def test_config_jvm_dll_wins_over_java_home(self):
        home_17, home_25 = self._java_home(17), self._java_home(25)
        config = types.SimpleNamespace(jvm_dll=self._jvm_dll(home_25))
        jvm_dll = start_jvm._find_jvm_dll(str(home_17), config)
        self.assertEqual(jvm_dll, self._jvm_dll(home_25))
        self._assert_selects(jvm_dll, 25)

    def test_jpyconfig_module_wins_over_java_home(self):
        home_17, home_25 = self._java_home(17), self._java_home(25)
        self._patch(
            mock.patch.dict(
                sys.modules,
                {"jpyconfig": types.SimpleNamespace(jvm_dll=self._jvm_dll(home_25))},
            )
        )
        jvm_dll = start_jvm._find_jvm_dll(str(home_17), None)
        self.assertEqual(jvm_dll, self._jvm_dll(home_25))
        self._assert_selects(jvm_dll, 25)

    def test_jpy_py_config_file_wins_over_java_home(self):
        home_17, home_25 = self._java_home(17), self._java_home(25)
        config_file = pathlib.Path(self._dir.name) / "jpyconfig.py"
        config_file.write_text(f"jvm_dll = {self._jvm_dll(home_25)!r}\n")
        self._patch(mock.patch.dict(os.environ, {"JPY_PY_CONFIG": str(config_file)}))
        jvm_dll = start_jvm._find_jvm_dll(str(home_17), None)
        self.assertEqual(jvm_dll, self._jvm_dll(home_25))
        self._assert_selects(jvm_dll, 25)

    def test_generic_file_when_version_is_unknown(self):
        home = self._java_home(None)
        self._assert_selects(start_jvm._find_jvm_dll(str(home), None), None)

    def test_start_jvm_loads_the_jvm_it_chose_options_for(self):
        home_17, home_25 = self._java_home(17), self._java_home(25)
        config = types.SimpleNamespace(jvm_dll=self._jvm_dll(home_25))
        with mock.patch.object(start_jvm.jvm, "init_jvm") as init_jvm:
            start_jvm.start_jvm(java_home=str(home_17), config=config)
        kwargs = init_jvm.call_args.kwargs
        self.assertEqual(kwargs["jvm_dll"], self._jvm_dll(home_25))
        vmoptions_files = [
            arg[len(VM_OPTIONS_FILE_ARG) :]
            for arg in kwargs["jvm_options"]
            if arg.startswith(VM_OPTIONS_FILE_ARG)
        ]
        self.assertEqual(len(vmoptions_files), 1, kwargs["jvm_options"])
        self.assertCountEqual(
            _options_in(pathlib.Path(vmoptions_files[0])), _expected_options(25)
        )


class StartedServerVmOptionsFileTest(unittest.TestCase):
    """The options the embedded server's JVM started with, in this test's real environment."""

    def test_started_jvm_has_its_versions_options(self):
        from deephaven_server import Server

        if Server.instance is None:
            Server().start()

        import jpy

        java_version = jpy.get_type("java.lang.Runtime").version().feature()
        input_args = list(
            jpy.get_type("java.lang.management.ManagementFactory")
            .getRuntimeMXBean()
            .getInputArguments()
            .toArray()
        )
        expected = _expected_options(java_version)
        for option in ALL_OPTIONS:
            if option in expected:
                self.assertIn(option, input_args)
            else:
                self.assertNotIn(option, input_args)


if __name__ == "__main__":
    unittest.main()
