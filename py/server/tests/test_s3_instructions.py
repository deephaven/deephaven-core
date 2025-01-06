#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import jpy
import tempfile

from tests.testbase import BaseTestCase
from deephaven import DHError
from deephaven.experimental import s3

_JCredentials = jpy.get_type("io.deephaven.extensions.s3.Credentials")

class S3InstructionTest(BaseTestCase):
    """ Test cases for the s3 instructions """

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_default(self):
        s3_instructions = s3.S3Instructions()
        self.assertTrue(s3_instructions.j_object is not None)
        self.assertTrue(s3_instructions.j_object.regionName().isEmpty())
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.resolving().getClass())
        self.assertEqual(s3_instructions.j_object.maxConcurrentRequests(), 256)
        self.assertEqual(s3_instructions.j_object.readAheadCount(), 32)
        self.assertEqual(s3_instructions.j_object.fragmentSize(), 65536)
        self.assertEqual(s3_instructions.j_object.connectionTimeout().toSeconds(), 2)
        self.assertEqual(s3_instructions.j_object.readTimeout().toSeconds(), 2)
        self.assertTrue(s3_instructions.j_object.endpointOverride().isEmpty())
        self.assertEqual(s3_instructions.j_object.writePartSize(), 10485760)
        self.assertEqual(s3_instructions.j_object.numConcurrentWriteParts(), 64)
        self.assertTrue(s3_instructions.j_object.profileName().isEmpty())
        self.assertTrue(s3_instructions.j_object.configFilePath().isEmpty())
        self.assertTrue(s3_instructions.j_object.credentialsFilePath().isEmpty())

    def test_set_region_name(self):
        s3_instructions = s3.S3Instructions(region_name="us-west-2")
        self.assertEqual(s3_instructions.j_object.regionName().get(), "us-west-2")

    def test_set_max_concurrent_requests(self):
        s3_instructions = s3.S3Instructions(max_concurrent_requests=512)
        self.assertEqual(s3_instructions.j_object.maxConcurrentRequests(), 512)

    def test_set_read_ahead_count(self):
        s3_instructions = s3.S3Instructions(read_ahead_count=64)
        self.assertEqual(s3_instructions.j_object.readAheadCount(), 64)

    def test_set_fragment_size(self):
        s3_instructions = s3.S3Instructions(fragment_size=131072)
        self.assertEqual(s3_instructions.j_object.fragmentSize(), 131072)

    def test_set_connection_timeout(self):
        s3_instructions = s3.S3Instructions(connection_timeout="PT10s")
        self.assertEqual(s3_instructions.j_object.connectionTimeout().toSeconds(), 10)

    def test_set_read_timeout(self):
        s3_instructions = s3.S3Instructions(read_timeout="PT5s")
        self.assertEqual(s3_instructions.j_object.readTimeout().toSeconds(), 5)

    def test_set_endpoint_override(self):
        s3_instructions = s3.S3Instructions(endpoint_override="http://localhost:9000")
        self.assertEqual(s3_instructions.j_object.endpointOverride().get().toString(), "http://localhost:9000")

    def test_set_write_part_size(self):
        s3_instructions = s3.S3Instructions(write_part_size=20971520)
        self.assertEqual(s3_instructions.j_object.writePartSize(), 20971520)

    def test_set_num_concurrent_write_parts(self):
        s3_instructions = s3.S3Instructions(num_concurrent_write_parts=128)
        self.assertEqual(s3_instructions.j_object.numConcurrentWriteParts(), 128)

    def test_set_profile_name(self):
        s3_instructions = s3.S3Instructions(profile_name="test-user")
        self.assertEqual(s3_instructions.j_object.profileName().get(), "test-user")

    def test_set_config_file_path(self):
        with tempfile.NamedTemporaryFile() as temp_config_file:
            s3_instructions = s3.S3Instructions(config_file_path=temp_config_file.name)
            self.assertEqual(s3_instructions.j_object.configFilePath().get().toString(), temp_config_file.name)

    def test_set_credentials_file_path(self):
        with tempfile.NamedTemporaryFile() as temp_credentials_file:
            s3_instructions = s3.S3Instructions(credentials_file_path=temp_credentials_file.name)
            self.assertEqual(s3_instructions.j_object.credentialsFilePath().get().toString(), temp_credentials_file.name)

    def test_set_resolving_credentials(self):
        s3_instructions = s3.S3Instructions(credentials=s3.Credentials.resolving())
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.resolving().getClass())

    def test_set_anonymous_access(self):
        s3_instructions = s3.S3Instructions(anonymous_access=True)
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.anonymous().getClass())

        s3_instructions = s3.S3Instructions(credentials=s3.Credentials.anonymous())
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.anonymous().getClass())

    def test_set_default_credentials(self):
        s3_instructions = s3.S3Instructions(credentials=s3.Credentials.default())
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.defaultCredentials().getClass())

    def test_set_profile_credentials(self):
        s3_instructions = s3.S3Instructions(credentials=s3.Credentials.profile())
        self.assertTrue(s3_instructions.j_object.credentials().getClass() == _JCredentials.profile().getClass())

    def test_set_multiple_credentials(self):
        # Only one set of credentials can be set
        with self.assertRaises(DHError):
            s3.S3Instructions(anonymous_access=True, access_key_id="foo", secret_access_key="bar")
            self.fail("Expected ValueError")

        with self.assertRaises(DHError):
            s3.S3Instructions(anonymous_access=True, credentials=s3.Credentials.resolving())
            self.fail("Expected ValueError")

        with self.assertRaises(DHError):
            s3.S3Instructions(access_key_id="foo", secret_access_key="bar", credentials=s3.Credentials.resolving())
            self.fail("Expected ValueError")
