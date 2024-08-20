//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when a constituent table of a {@link UnionSourceManager} encounters an error.
 */
public class ConstituentTableException extends UncheckedDeephavenException {
    public ConstituentTableException(@NotNull final String description, @NotNull final Throwable cause) {
        super(description, cause);
    }
}
