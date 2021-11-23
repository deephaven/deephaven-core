/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
     * @implNote Defaults to {@link #toString()}
     */
    default String describe() {
        return toString();
    }
}
