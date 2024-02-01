#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import datetime
from typing import Optional, Union

import jpy
import numpy as np
import pandas as pd

from deephaven import time, DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.dtypes import Duration

# If we move S3 to a permanent module, we should remove this try/except block and just import the types directly.
try:
    _JAwsCredentials = jpy.get_type("io.deephaven.extensions.s3.AwsCredentials")
    _JS3Instructions = jpy.get_type("io.deephaven.extensions.s3.S3Instructions")
except Exception:
    _JAwsCredentials = None
    _JS3Instructions = None

"""
    This module is useful for reading files stored in S3.
    Importing this module requires the S3 specific deephaven extensions (artifact name deephaven-extensions-s3) to be
    included in the package. This is an opt-out functionality included by default. If not included, importing this
    module will fail to find the java types.
"""
class S3Instructions(JObjectWrapper):
    """
    S3Instructions provides specialized instructions for reading from AWS S3.
    """

    j_object_type = _JS3Instructions or type(None)

    def __init__(self,
                 aws_region_name: str,
                 max_concurrent_requests: Optional[int] = None,
                 read_ahead_count: Optional[int] = None,
                 fragment_size: Optional[int] = None,
                 max_cache_size: Optional[int] = None,
                 connection_timeout: Union[
                     Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta, None] = None,
                 read_timeout: Union[
                     Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta, None] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 endpoint_override: Optional[str] = None):

        """
        Initializes the instructions.

        Args:
            aws_region_name (str): the AWS region name for reading parquet files stored in AWS S3, mandatory parameter.
            max_concurrent_requests (int): the maximum number of concurrent requests for reading parquet files stored in S3.
                default is 50.
            read_ahead_count (int): the number of fragments to send asynchronous read requests for while reading the current
                fragment. Default to 1, which means fetch the next fragment in advance when reading the current fragment.
            fragment_size (int): the maximum size of each fragment to read from S3, defaults to 5 MB. If there are fewer
                bytes remaining in the file, the fetched fragment can be smaller.
            max_cache_size (int): the maximum number of fragments to cache in memory while reading, defaults to 32. This
                caching is done at the deephaven layer for faster access to recently read fragments.
            connection_timeout (Union[Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta]):
                the amount of time to wait when initially establishing a connection before giving up and timing out, can
                be expressed as an integer in nanoseconds, a time interval string, e.g. "PT00:00:00.001" or "PT1s", or
                other time duration types. Default to 2 seconds.
            read_timeout (Union[Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta]):
                the amount of time to wait when reading a fragment before giving up and timing out, can be expressed as
                an integer in nanoseconds, a time interval string, e.g. "PT00:00:00.001" or "PT1s", or other time
                duration types. Default to 2 seconds.
            aws_access_key_id (str): the AWS access key for reading parquet files stored in AWS S3. Both access key and
                secret key must be provided to use static credentials, else default credentials will be used from
                software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.
            aws_secret_access_key (str): the AWS secret access key for reading parquet files stored in AWS S3. Both
                access key and secret key must be provided to use static credentials, else default credentials will be
                used from software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.
            endpoint_override (str): the endpoint with which the SDK should communicate.

        Raises:
            DHError: If unable to build the instructions object.
        """

        if not _JS3Instructions or not _JAwsCredentials:
            raise DHError(message="S3Instructions requires the S3 specific deephaven extensions to be included in "
                                  "the package")

        try:
            builder = self.j_object_type.builder()
            builder.awsRegionName(aws_region_name)

            if max_concurrent_requests is not None:
                builder.maxConcurrentRequests(max_concurrent_requests)

            if read_ahead_count is not None:
                builder.readAheadCount(read_ahead_count)

            if fragment_size is not None:
                builder.fragmentSize(fragment_size)

            if max_cache_size is not None:
                builder.maxCacheSize(max_cache_size)

            if connection_timeout is not None:
                builder.connectionTimeout(time.to_j_duration(connection_timeout))

            if read_timeout is not None:
                builder.readTimeout(time.to_j_duration(read_timeout))

            if ((aws_access_key_id is not None and aws_secret_access_key is None) or
                    (aws_access_key_id is None and aws_secret_access_key is not None)):
                raise DHError("Either both aws_access_key_id and aws_secret_access_key must be provided or neither")

            if aws_access_key_id is not None:
                builder.credentials(_JAwsCredentials.basicCredentials(aws_access_key_id, aws_secret_access_key))

            if endpoint_override is not None:
                builder.endpointOverride(endpoint_override)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build S3 instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object
