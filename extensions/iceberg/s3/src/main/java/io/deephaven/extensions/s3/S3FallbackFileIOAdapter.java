//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
                && S3Constants.S3_SCHEMES.contains(uriScheme);
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
        return UniversalS3SeekableChannelProviderPlugin.createUniversalS3Provider(uriScheme, s3Instructions);
    }

}
