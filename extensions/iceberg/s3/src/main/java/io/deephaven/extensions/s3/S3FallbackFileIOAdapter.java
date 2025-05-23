//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.util.channel.SeekableChannelsProvider;

import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.extensions.s3.S3Constants.S3_SCHEMES;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files to S3. This adapter is used when
 * {@link #USE_S3_CLIENT_FROM_FILE_IO} is {@code false}.
 */
@AutoService(FileIOAdapter.class)
public final class S3FallbackFileIOAdapter extends S3FileIOAdapterBase {

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
        return !USE_S3_CLIENT_FROM_FILE_IO
                && S3_SCHEMES.contains(uriScheme);
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
        final S3Instructions s3Instructions =
                (specialInstructions == null) ? S3Instructions.DEFAULT : (S3Instructions) specialInstructions;

        // Create a universal provider which can read/write to all S3 URIs (S3, S3A, S3N), so if different data files
        // have different S3 URI schemes, we can still read/write them using the same provider.
        return UniversalS3SeekableChannelProviderPlugin.createUniversalS3Provider(S3_SCHEMES, s3Instructions);
    }

}
