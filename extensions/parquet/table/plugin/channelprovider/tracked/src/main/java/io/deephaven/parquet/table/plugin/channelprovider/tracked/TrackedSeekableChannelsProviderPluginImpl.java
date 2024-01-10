/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.plugin.channelprovider.tracked;

import com.google.auto.service.AutoService;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.base.util.SeekableChannelsProviderPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from local disk.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class TrackedSeekableChannelsProviderPluginImpl implements SeekableChannelsProviderPlugin {
    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object object) {
        return object == null && (uri.getScheme() == null || uri.getScheme().equals(ParquetFileReader.FILE_URI_SCHEME));
    }

    @Override
    public SeekableChannelsProvider impl(@NotNull final URI uri, @Nullable final Object object) {
        if (!isCompatible(uri, object)) {
            if (object != null) {
                throw new IllegalArgumentException("Arguments not compatible, provided non null object");
            }
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        return new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance());
    }
}
