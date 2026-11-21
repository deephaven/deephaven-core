//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service responsible for {@link TableLocation} discovery.
 */
public interface TableDataService extends NamedImplementation {

    /**
     * Request a {@link TableLocationProvider} from this service.
     *
     * @param tableKey The {@link TableKey} to lookup
     * @return A {@link TableLocationProvider} for the specified {@link TableKey}
     */
    @NotNull
    TableLocationProvider getTableLocationProvider(@NotNull TableKey tableKey);

    /**
     * Request the single raw {@link TableLocationProvider} from this service that has the {@link TableLocation} for
     * {@code tableKey} and {@code tableLocationKey}. A raw {@link TableLocationProvider} does not compose multiple
     * {@link TableLocationProvider TableLocationProviders} or delegate to other implementations.
     *
     * @param tableKey The {@link TableKey} to lookup
     * @param tableLocationKey The {@link TableLocationKey} to lookup
     * @return The raw {@link TableLocationProvider} that has the {@link TableLocation} for {@code tableKey} and
     *         {@code tableLocationKey}, or {@code null} if there is none
     * @throws TableDataException If more than one {@link TableLocationProvider} has the {@link TableLocation}
     *
     */
    @Nullable
    TableLocationProvider getRawTableLocationProvider(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey);

    /**
     * Forget all state for subsequent requests for all tables.
     */
    void reset();

    /**
     * Forget all state for subsequent requests for a single table.
     *
     * @param tableKey {@link TableKey} to forget state for
     */
    void reset(@NotNull TableKey tableKey);

    /**
     * Get an optional name for this service, or null if no name is defined.
     *
     * @return The service name, or null
     */
    @Nullable
    default String getName() {
        return null;
    }

    /**
     * Stop any processes, release resources, and clear all cached state held by this {@link TableDataService}.
     * <p>
     * {@code shutdown()} is used to retire a service that may be replaced by one with different configuration or
     * underlying sources, so existing subscribers must not silently continue against stale state: any
     * {@link TableLocationProvider.Listener}s and {@link TableLocation.Listener}s obtained from this service are
     * delivered a terminal {@link TableDataException} error before their subscriptions are dropped. This subsumes the
     * effect of {@link #reset()} (cached state is cleared).
     *
     * @implNote Implementations must be idempotent: repeated calls after the first are effectively no-ops. Shutdown may
     *           be invoked concurrently with discovery and subscription activity; once it has completed, the behavior
     *           of all other methods on this instance is undefined and callers should not use the service further. The
     *           default implementation is a no-op, appropriate for services that hold no resources or subscribers.
     */
    default void shutdown() {}

    /**
     * Get a detailed description string.
     *
     * @return A description string
     * @implNote Defaults to {@link Object#toString()}
     */
    default String describe() {
        return toString();
    }
}
