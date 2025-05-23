//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class FileIOAdapterBase implements FileIOAdapter {

    @Override
    public final SeekableChannelsProvider createProvider(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object object) {
        if (!isCompatible(uriScheme, io)) {
            throw new IllegalArgumentException(
                    "Arguments not compatible, provided uri scheme " + uriScheme +
                            ", io " + io.getClass().getName() + ", special instructions " + object);
        }
        return createProviderImpl(uriScheme, io, object);
    }

    protected abstract SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions);
}
