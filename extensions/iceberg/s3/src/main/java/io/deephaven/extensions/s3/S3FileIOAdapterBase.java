//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.configuration.Configuration;
import io.deephaven.iceberg.util.FileIOAdapterBase;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED_DEFAULT;

abstract class S3FileIOAdapterBase extends FileIOAdapterBase {
    /**
     * Whether to use the S3 client from the S3FileIO or create a new one.
     */
    static final boolean USE_S3_CLIENT_FROM_FILE_IO = Configuration.getInstance().getBooleanWithDefault(
            "Iceberg.useS3ClientFromFileIO", true);

    static final String GCS_URI_SCHEME = GCSSeekableChannelProviderPlugin.GCS_URI_SCHEME;

    static void verifyNotCRTClient(@NotNull final Map<String, String> properties) {
        if (PropertyUtil.propertyAsBoolean(properties, S3_CRT_ENABLED, S3_CRT_ENABLED_DEFAULT)) {
            // We cannot check for `instanceOf S3CrtAsyncClient` because it is marked as @SdkInternalApi
            throw new IllegalArgumentException("S3 CRT client is not supported by Deephaven. Please set " +
                    "\"" + S3_CRT_ENABLED + "\" as \"false\" in catalog properties.");
            // TODO (DH-19253): Add support for S3CrtAsyncClient
        }
    }
}
