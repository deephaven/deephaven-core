//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A plugin interface for providing {@link SeekableChannelsProvider} implementations for different URI schemes, e.g. S3.
 * Check out {@link SeekableChannelsProviderLoader} for more details.
 */
public interface SeekableChannelsProviderPlugin {
    /**
     * Check if this plugin is compatible with the given URI scheme and config object.
     */
    boolean isCompatible(@NotNull String uriScheme, @Nullable Object config);

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI scheme and config object.
     */
    SeekableChannelsProvider createProvider(@NotNull String uriScheme, @Nullable Object object);
}
