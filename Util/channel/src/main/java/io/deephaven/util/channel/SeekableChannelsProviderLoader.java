//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * A service loader class for loading {@link SeekableChannelsProviderPlugin} implementations at runtime and provide
 * {@link SeekableChannelsProvider} implementations for different URI schemes, e.g., S3.
 */
public final class SeekableChannelsProviderLoader {

    private static volatile SeekableChannelsProviderLoader instance;

    public static SeekableChannelsProviderLoader getInstance() {
        if (instance == null) {
            instance = new SeekableChannelsProviderLoader();
        }
        return instance;
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
     * Create a new {@link SeekableChannelsProvider} based on given URI and object using the plugins loaded by the
     * {@link ServiceLoader}. For example, for a "S3" URI, we will create a {@link SeekableChannelsProvider} which can
     * read files from S3.
     *
     * @param uri The URI
     * @param object An optional object to pass to the {@link SeekableChannelsProviderPlugin} implementations.
     * @return A {@link SeekableChannelsProvider} for the given URI.
     */
    public SeekableChannelsProvider fromServiceLoader(@NotNull final URI uri, @Nullable final Object object) {
        for (final SeekableChannelsProviderPlugin plugin : providers) {
            if (plugin.isCompatible(uri, object)) {
                return plugin.createProvider(uri, object);
            }
        }
        throw new UnsupportedOperationException("No plugin found for uri: " + uri);
    }
}
