//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import org.jetbrains.annotations.NotNull;

public class InvalidatedRegionException extends TableDataException {
    public InvalidatedRegionException(@NotNull final String message) {
        super(message);
    }
}
