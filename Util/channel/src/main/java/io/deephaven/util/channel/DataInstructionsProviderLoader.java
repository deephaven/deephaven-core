//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;

/**
 * A service loader class for loading {@link DataInstructionsProviderPlugin} implementations at runtime which provide
 * {@link DataInstructionsProviderLoader} implementations for different URI paths.
 */
public final class DataInstructionsProviderLoader {
    /**
     * Get a {@link DataInstructionsProviderLoader} instance for the given property collection.
     *
     * @param properties The property collection.
     * @return A {@link DataInstructionsProviderLoader} instance.
     */
    public static DataInstructionsProviderLoader getInstance(final Map<String, String> properties) {
        return new DataInstructionsProviderLoader(properties);
    }

    /**
     * The properties collection for this instance.
     */
    private final Map<String, String> properties;

    /**
     * The list of plugins loaded by the {@link ServiceLoader}.
     */
    private final List<DataInstructionsProviderPlugin> providers;

    /**
     * Create a new {@link DataInstructionsProviderLoader} instance for the given property collection.
     *
     * @param properties The property collection.
     */
    private DataInstructionsProviderLoader(final Map<String, String> properties) {
        this.properties = properties;
        providers = new ArrayList<>();
        // Load the plugins
        for (final DataInstructionsProviderPlugin plugin : ServiceLoader.load(DataInstructionsProviderPlugin.class)) {
            providers.add(plugin);
        }
    }

    /**
     * Create a new {@link SeekableChannelsProvider} compatible for reading from and writing to the given URI, using the
     * plugins loaded by the {@link ServiceLoader}. For example, for a "S3" URI, we will create a
     * {@link SeekableChannelsProvider} which can read files from S3.
     *
     * @param uri The URI
     * @return A {@link SeekableChannelsProvider} for the given URI.
     */
    public Object fromServiceLoader(@NotNull final URI uri) {
        for (final DataInstructionsProviderPlugin plugin : providers) {
            final Object pluginInstructions = plugin.createInstructions(uri, properties);
            if (pluginInstructions != null) {
                return pluginInstructions;
            }
        }
        // No plugin found for this URI and property collection.
        return null;
    }
}
