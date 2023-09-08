package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;

public class PoisonedRegionException extends TableDataException {
    public PoisonedRegionException(@NotNull final String message) {
        super(message);
    }
}
