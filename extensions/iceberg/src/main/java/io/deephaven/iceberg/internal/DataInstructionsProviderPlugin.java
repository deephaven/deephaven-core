//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * A plugin interface for providing {@link DataInstructionsProviderPlugin} implementations for different property
 * collections and URI schemes. Check out {@link DataInstructionsProviderLoader} for more details.
 */
public interface DataInstructionsProviderPlugin {
    /**
     * Create a data instructions object for the given URI scheme.
     */
    Object createInstructions(@NotNull String uriScheme, @NotNull Map<String, String> properties);
}
