package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when a {@link TableLocation} has been removed from a provider.
 */
public class TableLocationRemovedException extends TableDataException {
    private final TableKey tableKey;
    private final TableLocationKey locationKey;

    public TableLocationRemovedException(@NotNull final TableLocation location, @NotNull final String message) {
        super(message);
        this.tableKey = location.getTableKey();
        this.locationKey = location.getKey();
    }

    public TableKey getTableKey() {
        return tableKey;
    }

    public TableLocationKey getTableLocationKey() {
        return locationKey;
    }
}
