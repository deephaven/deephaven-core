package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;

public class InvalidatedRegionException extends TableDataException {
    public InvalidatedRegionException(@NotNull final String message) {
        super(message);
    }
}
