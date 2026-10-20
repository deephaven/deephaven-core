#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import pathlib
import tempfile
import types
import unittest

from deephaven_server import start_jvm

PROPERTY = "deephaven.vmoptions.file"
GENERIC_FILE = "dh-default.vmoptions"

# The file each Java major version loads, including one with no file of its own. Mirrors VERSIONED_JAVA_OPTS in
# buildSrc/src/main/groovy/io.deephaven.java-toolchain-conventions.gradle.
EXPECTED_FILES = {
    17: "dh-default-17.vmoptions",
    21: "dh-default-21.vmoptions",
    22: GENERIC_FILE,
    25: "dh-default-25.vmoptions",
}


def _expected_file(java_version: int) -> str:
    return EXPECTED_FILES.get(java_version, GENERIC_FILE)


class VmOptionsFileTest(unittest.TestCase):
    """The embedded server's choice of VM options file, against the files this wheel ships. The Java homes are stand-ins
    holding only a release file, since choosing the file reads nothing else."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)

    def _java_home(self, release_contents) -> pathlib.Path:
        home = pathlib.Path(tempfile.mkdtemp(dir=self._dir.name))
        (home / "lib" / "server").mkdir(parents=True)
        if release_contents is not None:
            (home / "release").write_text(release_contents + "\n")
        return home

    def test_file_for_each_version(self):
        for java_version, expected in EXPECTED_FILES.items():
            with self.subTest(java_version=java_version):
                home = self._java_home(f'JAVA_VERSION="{java_version}.0.1"')
                detected = start_jvm._java_major_version(str(home), None)
                self.assertEqual(detected, java_version)
                vmoptions = start_jvm._default_vmoptions(detected)
                self.assertEqual(vmoptions.name, expected)
                self.assertTrue(vmoptions.is_file(), f"{vmoptions} is not in the wheel")
                self.assertEqual(
                    start_jvm._vmoptions_args(vmoptions),
                    [f"-XX:VMOptionsFile={vmoptions}", f"-D{PROPERTY}={vmoptions}"],
                )

    def test_version_found_from_jvm_shared_library(self):
        home = self._java_home('JAVA_VERSION="25.0.1"')
        config = types.SimpleNamespace(
            jvm_dll=str(home / "lib" / "server" / "libjvm.so")
        )
        self.assertEqual(start_jvm._java_major_version(None, config), 25)

    def test_generic_file_when_version_is_unknown(self):
        home = self._java_home(None)
        detected = start_jvm._java_major_version(str(home), None)
        self.assertIsNone(detected)
        self.assertEqual(start_jvm._default_vmoptions(detected).name, GENERIC_FILE)

    def test_started_jvm_records_its_file(self):
        from deephaven_server import Server

        if Server.instance is None:
            Server().start()

        import jpy

        java_version = jpy.get_type("java.lang.Runtime").version().feature()
        loaded = jpy.get_type("java.lang.System").getProperty(PROPERTY)
        self.assertIsNotNone(loaded, f"{PROPERTY} is not set")
        self.assertEqual(pathlib.Path(loaded).name, _expected_file(java_version))


if __name__ == "__main__":
    unittest.main()
