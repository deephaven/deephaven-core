//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import software.amazon.awssdk.services.s3.S3AsyncClient;

import static io.deephaven.extensions.s3.S3Constants.S3_SCHEMES;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED_DEFAULT;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files to S3. This adapter re-uses the
 * {@link S3AsyncClient} embedded in an {@link S3FileIO}. It is only used when {@link #USE_S3_CLIENT_FROM_FILE_IO} is
 * {@code true}.
 */
@AutoService(FileIOAdapter.class)
public final class S3FileIOAdapter extends S3FileIOAdapterBase {

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final FileIO io) {
        return isCompatible(uriScheme, io.getClass());
    }

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final Class<?> ioClass) {
        return USE_S3_CLIENT_FROM_FILE_IO
                && S3_SCHEMES.contains(uriScheme)
                && S3FileIO.class.isAssignableFrom(ioClass);
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions) {
        if (!isCompatible(uriScheme, io)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme +
                    ", io " + io.getClass().getName() + ", special instructions " + specialInstructions);
        }
        if (PropertyUtil.propertyAsBoolean(io.properties(), S3_CRT_ENABLED, S3_CRT_ENABLED_DEFAULT)) {
            // We cannot check for `instanceOf S3CrtAsyncClient` because it is marked as @SdkInternalApi
            throw new IllegalArgumentException("S3 CRT client is not supported by Deephaven. Please set " +
                    "\"" + S3_CRT_ENABLED + "\" as \"false\" in catalog properties.");
            // TODO (DH-19253): Add support for S3CrtAsyncClient
        }
        final S3Instructions s3Instructions =
                (specialInstructions == null) ? S3Instructions.DEFAULT : (S3Instructions) specialInstructions;
        final S3AsyncClient s3AsyncClient = ((S3FileIO) io).asyncClient();

        // Create a universal provider which can read/write to all S3 URIs (S3, S3A, S3N), so if different data files
        // have different S3 URI schemes, we can still read/write them using the same provider.
        return UniversalS3SeekableChannelProviderPlugin.createUniversalS3Provider(
                S3_SCHEMES, s3Instructions, s3AsyncClient);
    }
}
