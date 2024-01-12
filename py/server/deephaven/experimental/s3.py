#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""
    This module is useful for reading and writing files stored in S3. Importing this module requires the S3 specific
    extensions to be included in the class path.
"""

import jpy

_JDuration = jpy.get_type("java.time.Duration")
_JS3Instructions = jpy.get_type("io.deephaven.extensions.s3.S3Instructions")

def _build_s3_instructions(
        aws_region_name: str,
        max_concurrent_requests: int = None,
        read_ahead_count: int = None,
        fragment_size: int = None,
        max_cache_size: int = None,
        connection_timeout: _JDuration = None,
        read_timeout: _JDuration = None,
):
    """
    Build specialized instructions for accessing files stored in AWS S3.

    Args:
        aws_region_name (str): the AWS region name for reading parquet files stored in AWS S3, by default None
        max_concurrent_requests (int): the maximum number of concurrent requests for reading parquet files stored in S3,
            by default 50.
        read_ahead_count (int): the number of fragments to send asynchronous read requests for while reading the current
            fragment, defaults to 1.
        fragment_size (int): the maximum size of each fragment to read from S3. The fetched fragment can be smaller than
            this in case fewer bytes remaining in the file, defaults to 5 MB.
        max_cache_size (int): the maximum number of fragments to cache in memory while reading, defaults to 32.
        connection_timeout (Duration): the amount of time to wait when initially establishing a connection before giving
            up and timing out, defaults to 2 seconds.
        read_timeout (Duration): the amount of time to wait when reading a fragment before giving up and timing out,
            defaults to 2 seconds.
    """

    builder = _JS3Instructions.builder()
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
        builder.connectionTimeout(connection_timeout)

    if read_timeout is not None:
        builder.readTimeout(read_timeout)

    return builder.build()

