package io.deephaven.parquet.table;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

public interface DeferredSpecialInstructions<DEFERRED_TYPE> {

    /**
     * Get the deferred object for the specified URI. Implementations should be thread-safe, and should cache supplied results as necessary.
     *
     * @param uri The URI to provide special instructions for
     * @return The special instructions
     */
    DEFERRED_TYPE get(@NotNull URI uri);

    static <DEFERRED_TYPE> DEFERRED_TYPE resolve(@NotNull final URI uri, @Nullable final Object specialInstructions) {
        if (specialInstructions instanceof DeferredSpecialInstructions) {
            //noinspection unchecked
            return ((DeferredSpecialInstructions<DEFERRED_TYPE>) specialInstructions).get(uri);
        }
        //noinspection unchecked
        return (DEFERRED_TYPE) specialInstructions;
    }
}
