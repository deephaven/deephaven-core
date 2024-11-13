//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * A service loader class for loading {@link SeekableChannelsProviderPlugin} implementations at runtime and provide
 * {@link SeekableChannelsProvider} implementations for different URI schemes, e.g., S3.
 */
public final class SeekableChannelsProviderLoader {

    private static volatile SeekableChannelsProviderLoader instance;

    /**
     * Get a static a {@link SeekableChannelsProviderLoader} instance that is loading with
     * {@link SeekableChannelsProviderPlugin} provided via {@link ServiceLoader#load(Class)}.
     *
     * @return The {@link SeekableChannelsProviderLoader} instance.
     */
    public static SeekableChannelsProviderLoader getInstance() {
        SeekableChannelsProviderLoader localInstance;
        if ((localInstance = instance) == null) {
            synchronized (SeekableChannelsProviderLoader.class) {
                if ((localInstance = instance) == null) {
                    instance = localInstance = new SeekableChannelsProviderLoader();
                }
            }
        }
        return localInstance;
    }

    private final List<SeekableChannelsProviderPlugin> providers;

    private SeekableChannelsProviderLoader() {
        providers = new ArrayList<>();
        // Load the plugins
        for (final SeekableChannelsProviderPlugin plugin : ServiceLoader.load(SeekableChannelsProviderPlugin.class)) {
            providers.add(plugin);
        }
    }

    /**
     * Create a new {@link SeekableChannelsProvider} compatible for reading from and writing to the given URI scheme.
     * For example, for a "S3" URI, we will create a {@link SeekableChannelsProvider} which can read files from S3.
     *
     * @param uriScheme The URI scheme
     * @param specialInstructions An optional object to pass special instructions to the provider.
     * @return A {@link SeekableChannelsProvider} for the given URI scheme.
     */
    public SeekableChannelsProvider load(@NotNull final String uriScheme, @Nullable final Object specialInstructions) {
        for (final SeekableChannelsProviderPlugin plugin : providers) {
            if (plugin.isCompatible(uriScheme, specialInstructions)) {
                return plugin.createProvider(uriScheme, specialInstructions);
            }
        }
        throw new UnsupportedOperationException("No plugin found for uri scheme: " + uriScheme);
    }
}
