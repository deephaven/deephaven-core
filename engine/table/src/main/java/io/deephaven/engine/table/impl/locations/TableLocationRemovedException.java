package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when a {@link TableLocation} has been removed from a provider.
 */
public class TableLocationRemovedException extends TableDataException {
    private final ImmutableTableLocationKey[] locationKeys;

    public TableLocationRemovedException(@NotNull final String message, @NotNull final ImmutableTableLocationKey... locationKeys) {
        super(message);
        this.locationKeys = locationKeys;
    }

    public ImmutableTableLocationKey[] getLocationKeys() {
        return locationKeys;
    }
}
