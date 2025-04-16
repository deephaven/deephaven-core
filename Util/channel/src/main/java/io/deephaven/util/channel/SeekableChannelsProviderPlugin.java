//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

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
     * Check if this plugin is compatible with all of the given URI schemes and config object.
     */
    boolean isCompatible(@NotNull Set<String> uriSchemes, @Nullable Object config);

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI scheme and config object.
     */
    SeekableChannelsProvider createProvider(@NotNull String uriScheme, @Nullable Object object);

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI schemes and config object.
     */
    SeekableChannelsProvider createProvider(@NotNull Set<String> uriSchemes, @Nullable Object object);
}
