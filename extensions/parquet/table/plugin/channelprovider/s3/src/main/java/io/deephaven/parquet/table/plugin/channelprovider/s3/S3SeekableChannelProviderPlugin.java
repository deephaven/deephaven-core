/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.plugin.channelprovider.s3;

import com.google.auto.service.AutoService;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.base.util.SeekableChannelsProviderPlugin;
import io.deephaven.parquet.table.S3Instructions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from S3.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class S3SeekableChannelProviderPlugin implements SeekableChannelsProviderPlugin {
    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object config) {
        return config instanceof S3Instructions && ParquetFileReader.S3_URI_SCHEME.equals(uri.getScheme());
    }

    @Override
    public SeekableChannelsProvider impl(@NotNull final URI uri, @Nullable final Object config) {
        if (!isCompatible(uri, config)) {
            if (!(config instanceof S3Instructions)) {
                throw new IllegalArgumentException("Must provide S3Instructions to read files from S3");
            }
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        final S3Instructions s3Instructions = (S3Instructions) config;
        return new S3SeekableChannelProvider(s3Instructions);
    }
}
