//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.configuration.Configuration;
import io.deephaven.iceberg.util.FileIOAdapterBase;

abstract class S3FileIOAdapterBase extends FileIOAdapterBase {
    /**
     * Whether to use the S3 client from the S3FileIO or create a new one.
     */
    static final boolean USE_S3_CLIENT_FROM_FILE_IO = Configuration.getInstance().getBooleanWithDefault(
            "Iceberg.useS3ClientFromFileIO", true);
}
