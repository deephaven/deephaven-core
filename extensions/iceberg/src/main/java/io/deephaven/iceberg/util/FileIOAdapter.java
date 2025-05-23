//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ServiceLoader;

import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;

/**
 * A plugin interface for providing {@link SeekableChannelsProvider} implementations for different URI schemes using a
 * given {@link FileIO} implementation.
 */
public interface FileIOAdapter {

    /**
     * Create a {@link FileIOAdapter} that is compatible with the given URI scheme and {@link FileIO} implementation.
     *
     * @param tableLocation The {@link Table#location()} URI of the table.
     * @param io The {@link FileIO} implementation to use.
     */
    static FileIOAdapter fromServiceLoader(
            @NotNull final URI tableLocation,
            @NotNull final FileIO io) {
        final String scheme = tableLocation.getScheme();
        for (final FileIOAdapter adapter : ServiceLoader.load(FileIOAdapter.class)) {
            if (adapter.isCompatible(scheme, io)) {
                return adapter;
            }
        }
        if (io instanceof ResolvingFileIO) {
            final ResolvingFileIO resolvingFileIO = (ResolvingFileIO) io;
            final Class<?> delegateFileIOClass = resolvingFileIO.ioClass(tableLocation.toString());
            for (final FileIOAdapter adapter : ServiceLoader.load(FileIOAdapter.class)) {
                if (adapter.isCompatible(scheme, delegateFileIOClass)) {
                    throw new UnsupportedOperationException(
                            "ResolvingFileIO is not supported by Deephaven, please set \"" + FILE_IO_IMPL + "\" as \"" +
                                    delegateFileIOClass.getName() + "\" in catalog properties.");
                }
            }
        }
        throw new UnsupportedOperationException("No adapter found for FileIO " + io.getClass());
    }

    /**
     * Check if this adapter is compatible with the given URI scheme and file IO.
     */
    boolean isCompatible(@NotNull String uriScheme, @NotNull final FileIO io);

    /**
     * Check if this adapter is compatible with the given URI scheme and file IO of the given class.
     */
    boolean isCompatible(@NotNull String uriScheme, @NotNull final Class<?> ioClass);

    /**
     * Create a new {@link SeekableChannelsProvider} compatible for reading from and writing to the given URI scheme
     * using the given {@link FileIO} implementation. For example, for an "s3" URI, we will create a
     * {@link SeekableChannelsProvider} which can read files from S3.
     *
     * @param uriScheme The URI scheme
     * @param io The {@link FileIO} implementation to use.
     * @param specialInstructions An optional object to pass special instructions to the provider.
     */
    SeekableChannelsProvider createProvider(
            @NotNull String uriScheme,
            @NotNull FileIO io,
            @Nullable Object specialInstructions);
}
