//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service responsible for {@link TableLocation} discovery.
 */
public interface TableDataService {

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
     * Get a detailed description string.
     *
     * @return A description string
     * @implNote Defaults to {@link Object#toString()}
     */
    default String describe() {
        return toString();
    }
}
