//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.trackedfile;

import com.google.auto.service.AutoService;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from local disk.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class TrackedSeekableChannelsProviderPlugin implements SeekableChannelsProviderPlugin {

    static final String FILE_URI_SCHEME = "file";

    @Override
    public boolean isCompatible(@NotNull final URI uri, @Nullable final Object object) {
        return FILE_URI_SCHEME.equals(uri.getScheme());
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final URI uri, @Nullable final Object object) {
        if (!isCompatible(uri, object)) {
            if (object != null) {
                throw new IllegalArgumentException("Arguments not compatible, provided non null object");
            }
            throw new IllegalArgumentException("Arguments not compatible, provided uri " + uri);
        }
        return new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance());
    }
}
