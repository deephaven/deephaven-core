//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when a {@link TableLocation} that was in use by a {@link Table} is removed.
 */
public class TableLocationRemovedException extends TableDataException {
    private final ImmutableTableLocationKey[] locationKeys;

    public TableLocationRemovedException(
            @NotNull final String message,
            @NotNull final ImmutableTableLocationKey... locationKeys) {
        super(message);
        this.locationKeys = locationKeys;
    }

    public ImmutableTableLocationKey[] getLocationKeys() {
        return locationKeys;
    }
}
