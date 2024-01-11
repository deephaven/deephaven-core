package io.deephaven.parquet.base.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * A plugin interface for providing {@link SeekableChannelsProvider} implementations for different URI schemes, e.g. S3.
 * Check out {@link SeekableChannelsProviderLoader} for more details.
 */
public interface SeekableChannelsProviderPlugin {
    /**
     * Check if this plugin is compatible with the given URI and config object.
     */
    boolean isCompatible(@NotNull final URI uri, @Nullable final Object config);

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI and config object.
     */
    SeekableChannelsProvider createProvider(@NotNull final URI uri, @Nullable final Object object);
}
