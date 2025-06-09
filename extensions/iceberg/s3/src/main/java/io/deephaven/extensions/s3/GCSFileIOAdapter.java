//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files to GCS. This adapter re-uses the
 * {@link S3AsyncClient} embedded in an {@link S3FileIO}. It is only used when {@link #USE_S3_CLIENT_FROM_FILE_IO} is
 * {@code true}.
 */
@AutoService(FileIOAdapter.class)
public final class GCSFileIOAdapter extends S3FileIOAdapterBase {

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
                && GCS_URI_SCHEME.equals(uriScheme)
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
        verifyNotCRTClient(io.properties());
        final S3AsyncClient s3AsyncClient = ((S3FileIO) io).asyncClient();
        return GCSSeekableChannelProviderPlugin.createGCSSeekableChannelProvider(specialInstructions, s3AsyncClient);
    }
}
