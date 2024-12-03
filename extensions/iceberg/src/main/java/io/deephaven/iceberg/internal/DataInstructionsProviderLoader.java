//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * A service loader class for loading {@link DataInstructionsProviderPlugin} implementations at runtime which provide
 * {@link DataInstructionsProviderLoader} implementations for different URI schemes.
 */
public final class DataInstructionsProviderLoader {
    /**
     * The list of plugins loaded by the {@link ServiceLoader}.
     */
    private static volatile List<DataInstructionsProviderPlugin> cachedProviders;

    /**
     * Ensure that the {@link DataInstructionsProviderPlugin plugins} are loaded exactly once.
     */
    private static List<DataInstructionsProviderPlugin> ensureProviders() {
        List<DataInstructionsProviderPlugin> localProviders;
        if ((localProviders = cachedProviders) == null) {
            synchronized (DataInstructionsProviderLoader.class) {
                if ((localProviders = cachedProviders) == null) {
                    localProviders = new ArrayList<>();
                    // Load the plugins
                    for (final DataInstructionsProviderPlugin plugin : ServiceLoader
                            .load(DataInstructionsProviderPlugin.class)) {
                        localProviders.add(plugin);
                    }
                    cachedProviders = localProviders;
                }
            }
        }
        return localProviders;
    }

    /**
     * Create a {@link DataInstructionsProviderLoader} instance for the given property collection with a static list of
     * {@link DataInstructionsProviderPlugin} provided via {@link ServiceLoader#load(Class)}.
     *
     * @param properties The property collection.
     * @return A {@link DataInstructionsProviderLoader} instance.
     */
    public static DataInstructionsProviderLoader create(final Map<String, String> properties) {
        return new DataInstructionsProviderLoader(properties, ensureProviders());
    }

    /**
     * The properties collection for this instance.
     */
    private final Map<String, String> properties;

    /**
     * The local list of plugins loaded by the {@link ServiceLoader}.
     */
    private final List<DataInstructionsProviderPlugin> providers;

    /**
     * Create a new {@link DataInstructionsProviderLoader} instance for the given property collection.
     *
     * @param properties The property collection.
     */
    private DataInstructionsProviderLoader(
            final Map<String, String> properties,
            final List<DataInstructionsProviderPlugin> providers) {
        this.properties = Objects.requireNonNull(properties);
        this.providers = Objects.requireNonNull(providers);
    }

    /**
     * Create a new data instructions object compatible with reading from and writing to the given URI scheme. For
     * example, for an "S3" URI scheme will create an {@code S3Instructions} object which can read files from S3.
     *
     * @param uriScheme The URI scheme
     * @return A data instructions object for the given URI scheme or null if one cannot be found
     */
    public Object load(@NotNull final String uriScheme) {
        for (final DataInstructionsProviderPlugin plugin : providers) {
            final Object pluginInstructions = plugin.createInstructions(uriScheme, properties);
            if (pluginInstructions != null) {
                return pluginInstructions;
            }
        }
        // No plugin found for this URI scheme and property collection.
        return null;
    }
}
