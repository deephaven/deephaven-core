//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.auto.service.AutoService;
import io.deephaven.base.FileUtils;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files with {@value FileUtils#FILE_URI_SCHEME} scheme.
 */
@AutoService(FileIOAdapter.class)
public final class LocalFileIOAdapter extends FileIOAdapterBase {

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final FileIO io) {
        final boolean compatibleScheme = FileUtils.FILE_URI_SCHEME.equals(uriScheme);
        final boolean compatibleIO = io instanceof HadoopFileIO || io instanceof RelativeFileIO;
        return compatibleScheme && compatibleIO;
    }

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final Class<?> ioClass) {
        final boolean compatibleScheme = FileUtils.FILE_URI_SCHEME.equals(uriScheme);
        final boolean compatibleIO = HadoopFileIO.class.isAssignableFrom(ioClass) ||
                RelativeFileIO.class.isAssignableFrom(ioClass);
        return compatibleScheme && compatibleIO;
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
        return SeekableChannelsProviderLoader.getInstance().load(FileUtils.FILE_URI_SCHEME, specialInstructions);
    }
}
