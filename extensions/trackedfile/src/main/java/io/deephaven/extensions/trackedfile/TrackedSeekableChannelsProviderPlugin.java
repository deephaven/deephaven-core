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

import static io.deephaven.base.FileUtils.FILE_URI_SCHEME;

/**
 * {@link SeekableChannelsProviderPlugin} implementation used for reading files from local disk.
 */
@AutoService(SeekableChannelsProviderPlugin.class)
public final class TrackedSeekableChannelsProviderPlugin implements SeekableChannelsProviderPlugin {

    @Override
    public boolean isCompatible(@NotNull final String uriScheme, @Nullable final Object object) {
        return FILE_URI_SCHEME.equals(uriScheme);
    }

    @Override
    public SeekableChannelsProvider createProvider(@NotNull final String uriScheme, @Nullable final Object object) {
        if (!isCompatible(uriScheme, object)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme);
        }
        if (object != null) {
            throw new IllegalArgumentException("Arguments not compatible, provided non null object");
        }
        return new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance());
    }
}
