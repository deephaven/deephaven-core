#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""
    This module is useful for reading and writing files stored in S3. Importing this module requires the S3 specific
    extensions to be included in the class path.
"""
import datetime
from typing import Optional, Union

import jpy
import numpy as np
import pandas as pd

from deephaven import time
from deephaven._wrapper import JObjectWrapper
from deephaven.dtypes import Duration


class S3Instructions(JObjectWrapper):
    """
    Used to provide specialized instructions for reading from AWS S3.

    Args:
        aws_region_name (str): the AWS region name for reading parquet files stored in AWS S3, mandatory parameter.
        max_concurrent_requests (int): the maximum number of concurrent requests for reading parquet files stored in S3,
            by default is 50.
        read_ahead_count (int): the number of fragments to send asynchronous read requests for while reading the current
            fragment, defaults to 1, which means fetch one next fragment in advance when reading current fragment.
        fragment_size (int): the maximum size of each fragment to read from S3, defaults to 5 MB. If there are fewer
            bytes remaining in the file, the fetched fragment can be smaller.
        max_cache_size (int): the maximum number of fragments to cache in memory while reading, defaults to 32. This
            caching is done at deephaven layer using a modulo-based logic. For example, if the max cache size is 32,
            fragment i will be cached in slot i % 32 and will stay there until it is evicted by a more recent fragment.
        connection_timeout (Union[Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta]):
            the amount of time to wait when initially establishing a connection before giving up and timing out, can be
            expressed as an integer in nanoseconds, a time interval string, e.g. "PT00:00:00.001" or "PT1s", or other
            time duration types. Default to 2 seconds.
        read_timeout (Union[Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta]):
            the amount of time to wait when reading a fragment before giving up and timing out, can be expressed as an
            integer in nanoseconds, a time interval string, e.g. "PT00:00:00.001" or "PT1s", or other time duration
            types. Default to 2 seconds.
    """

    j_object_type = jpy.get_type("io.deephaven.extensions.s3.S3Instructions")

    def __init__(self,
                 aws_region_name: str,
                 max_concurrent_requests: Optional[int] = None,
                 read_ahead_count: Optional[int] = None,
                 fragment_size: Optional[int] = None,
                 max_cache_size: Optional[int] = None,
                 connection_timeout: Union[
                     Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta, None] = None,
                 read_timeout: Union[
                     Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta, None] = None):

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

        self._j_object = builder.build()

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object
