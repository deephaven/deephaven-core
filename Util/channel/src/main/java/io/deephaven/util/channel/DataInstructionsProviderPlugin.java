//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Map;

/**
 * A plugin interface for providing {@link DataInstructionsProviderPlugin} implementations for different property
 * collections and URI values. Check out {@link DataInstructionsProviderLoader} for more details.
 */
public interface DataInstructionsProviderPlugin {
    /**
     * Create a data instructions object for the given URI.
     */
    Object createInstructions(@NotNull URI uri, @NotNull final Map<String, String> properties);
}
