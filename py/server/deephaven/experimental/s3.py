#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from typing import Optional, Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.time import DurationLike, to_j_duration

# If we move S3 to a permanent module, we should remove this try/except block and just import the types directly.
try:
    _JCredentials = jpy.get_type("io.deephaven.extensions.s3.Credentials")
    _JS3Instructions = jpy.get_type("io.deephaven.extensions.s3.S3Instructions")
except Exception:
    _JCredentials = None
    _JS3Instructions = None

"""
    This module is useful for reading from and writing to S3-compatible APIs.
    Importing this module requires the S3 specific deephaven extensions (artifact name deephaven-extensions-s3) to be
    included in the package. This is an opt-out functionality included by default. If not included, importing this
    module will fail to find the java types.
"""
class S3Instructions(JObjectWrapper):
    """
    S3Instructions provides specialized instructions for reading from and writing to S3-compatible APIs.
    """

    j_object_type = _JS3Instructions or type(None)

    def __init__(self,
                 region_name: Optional[str] = None,
                 max_concurrent_requests: Optional[int] = None,
                 read_ahead_count: Optional[int] = None,
                 fragment_size: Optional[int] = None,
                 connection_timeout: Optional[DurationLike] = None,
                 read_timeout: Optional[DurationLike] = None,
                 access_key_id: Optional[str] = None,
                 secret_access_key: Optional[str] = None,
                 anonymous_access: bool = False,
                 endpoint_override: Optional[str] = None,
                 write_part_size: Optional[int] = None,
                 num_concurrent_write_parts: Optional[int] = None,
                 profile_name: Optional[str] = None,
                 config_file_path: Optional[str] = None,
                 credentials_file_path: Optional[str] = None,
                 use_profile_credentials: bool = False,
                 use_default_credentials: bool = False):

        """
        Initializes the instructions.

        Args:
            region_name (str): the region name for reading parquet files. If not provided, the default region will be
                picked by the AWS SDK from 'aws.region' system property, "AWS_REGION" environment variable, the
                {user.home}/.aws/credentials or {user.home}/.aws/config files, or from EC2 metadata service, if running
                in EC2. If no region name is derived from the above chain or the derived region name is incorrect for
                the bucket accessed, the correct region name will be derived internally, at the cost of one additional
                request.
            max_concurrent_requests (int): the maximum number of concurrent requests for reading files, default is 256.
            read_ahead_count (int): the number of fragments to send asynchronous read requests for while reading the
                current fragment. Defaults to 32, which means fetch the next 32 fragments in advance when reading the
                current fragment.
            fragment_size (int): the maximum size of each fragment to read in bytes, defaults to 65536. If
                there are fewer bytes remaining in the file, the fetched fragment can be smaller.
            connection_timeout (DurationLike): the amount of time to wait when initially establishing a connection
                before giving up and timing out. Can be expressed as an integer in nanoseconds, a time interval string,
                e.g. "PT00:00:00.001" or "PT1s", or other time duration types. Default to 2 seconds.
            read_timeout (DurationLike): the amount of time to wait when reading a fragment before giving up and timing
                out. Can be expressed as an integer in nanoseconds, a time interval string, e.g. "PT00:00:00.001" or
                "PT1s", or other time duration types. Default to 2 seconds.
            access_key_id (str): the access key for reading files. Both access key and secret access key must be
                provided to use static credentials. If you specify both access key and secret key, then you cannot
                provide other credentials like anonymous_access or use_profile_credentials.
            secret_access_key (str): the secret access key for reading files. Both access key and secret key must be
                provided to use static credentials.  If you specify both access key and secret key, then you cannot
                provide other credentials like anonymous_access or use_profile_credentials.
            anonymous_access (bool): use anonymous credentials, this is useful when the S3 policy has been set to allow
                anonymous access. Can't be combined with other credentials. By default, is False.
            endpoint_override (str): the endpoint to connect to. Callers connecting to AWS do not typically need to set
                this; it is most useful when connecting to non-AWS, S3-compatible APIs.
            write_part_size (int): The part or chunk size when writing to S3. The default is 10 MiB. The minimum allowed
                part size is 5242880. Higher part size may increase throughput but also increase memory usage. Writing
                a single file to S3 can be done in a maximum of 10,000 parts, so the maximum size of a single file that
                can be written is about 98 GiB for the default part size.
            num_concurrent_write_parts (int): the maximum number of parts or chunks that can be uploaded concurrently
                when writing to S3 without blocking, defaults to 64. Setting a higher value may increase throughput, but
                may also increase memory usage.
            profile_name (str): the profile name used for configuring the default region, credentials, etc., when
                reading or writing to S3. If not provided, the AWS SDK picks the profile name from the 'aws.profile'
                system property, the "AWS_PROFILE" environment variable, or defaults to the string "default".
                Setting a profile name assumes that the credentials are provided via this profile; if that is not the
                case, you must explicitly set credentials using the access_key_id and secret_access_key.
            config_file_path (str): the path to the configuration file to use for configuring the default region,
                role_arn, output etc. when reading or writing to S3. If not provided, the AWS SDK picks the configuration
                file from the 'aws.configFile' system property, the "AWS_CONFIG_FILE" environment variable, or defaults
                to "{user.home}/.aws/config".
                Setting a configuration file path assumes that the credentials are provided via the config and
                credentials files; if that is not the case, you must explicitly set credentials using the access_key_id
                and secret_access_key.
                For reference on the configuration file format, check
                https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
            credentials_file_path (str): the path to the credentials file to use for configuring the credentials, etc.
                when reading or writing to S3. If not provided, the AWS SDK picks the credentials file from the
                'aws.credentialsFile' system property, the "AWS_CREDENTIALS_FILE" environment variable, or defaults to
                "{user.home}/.aws/credentials".
                Setting a credentials file path assumes that the credentials are provided via the config and
                credentials files; if that is not the case, you must explicitly set credentials using the access_key_id
                and secret_access_key.
                For reference on the credentials file format, check
                https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
            use_profile_credentials (bool): use the profile_name for loading the credentials from the config and
                credentials file and fail if none found. Default is False. Cannot be combined with using other
                credentials, like anonymous_access or use_default_credentials.
            use_default_credentials (bool): use the default AWS SDK behavior for loading credentials from the
                environment, system properties, or instance profile credentials. Default is False. Cannot be combined
                with other credentials, like anonymous_access or use_default_credentials. For more details, check
                https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html.

        Raises:
            DHError: If unable to build the instructions object.
        """

        if not _JS3Instructions or not _JCredentials:
            raise DHError(message="S3Instructions requires the S3 specific deephaven extensions to be included in "
                                  "the package")

        try:
            builder = self.j_object_type.builder()

            if region_name is not None:
                builder.regionName(region_name)

            if max_concurrent_requests is not None:
                builder.maxConcurrentRequests(max_concurrent_requests)

            if read_ahead_count is not None:
                builder.readAheadCount(read_ahead_count)

            if fragment_size is not None:
                builder.fragmentSize(fragment_size)

            if connection_timeout is not None:
                builder.connectionTimeout(to_j_duration(connection_timeout))

            if read_timeout is not None:
                builder.readTimeout(to_j_duration(read_timeout))

            if ((access_key_id is not None and secret_access_key is None) or
                    (access_key_id is None and secret_access_key is not None)):
                raise DHError("Either both access_key_id and secret_access_key must be provided or neither")

            def throw_multiple_credentials_error(credentials1: str, credentials2: str):
                raise DHError(f"Only one set of credentials can be set, but found {credentials1} and {credentials2}")

            # Configure the credentials
            credentials_configured = None
            if access_key_id is not None:
                builder.credentials(_JCredentials.basic(access_key_id, secret_access_key))
                credentials_configured = "access_key_id"
            if anonymous_access:
                if credentials_configured:
                    throw_multiple_credentials_error(credentials_configured, "anonymous_access")
                builder.credentials(_JCredentials.anonymous())
                credentials_configured = "anonymous_access"
            if use_profile_credentials:
                if credentials_configured:
                    throw_multiple_credentials_error(credentials_configured, "use_profile_credentials")
                builder.credentials(_JCredentials.profile())
                credentials_configured = "use_profile_credentials"
            if use_default_credentials:
                if credentials_configured:
                    throw_multiple_credentials_error(credentials_configured, "use_default_credentials")
                builder.credentials(_JCredentials.defaultCredentials())
                credentials_configured = "use_default_credentials"

            if endpoint_override is not None:
                builder.endpointOverride(endpoint_override)

            if write_part_size is not None:
                builder.writePartSize(write_part_size)

            if num_concurrent_write_parts is not None:
                builder.numConcurrentWriteParts(num_concurrent_write_parts)

            if profile_name is not None:
                builder.profileName(profile_name)

            if config_file_path is not None:
                builder.configFilePath(config_file_path)

            if credentials_file_path is not None:
                builder.credentialsFilePath(credentials_file_path)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build S3 instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object
