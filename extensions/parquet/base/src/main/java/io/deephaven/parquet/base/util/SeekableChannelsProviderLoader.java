/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

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
    }

    public SeekableChannelsProvider fromServiceLoader(@NotNull final URI uri, @Nullable final Object object) {
        if (providers.isEmpty()) {
            // Load the plugins
            for (final SeekableChannelsProviderPlugin plugin : ServiceLoader
                    .load(SeekableChannelsProviderPlugin.class)) {
                providers.add(plugin);
            }
        }
        for (final SeekableChannelsProviderPlugin plugin : providers) {
            if (plugin.isCompatible(uri, object)) {
                return plugin.impl(uri, object);
            }
        }
        throw new UnsupportedOperationException("No plugin found for uri: " + uri);
    }
}
