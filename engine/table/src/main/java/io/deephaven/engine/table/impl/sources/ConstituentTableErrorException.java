package io.deephaven.engine.table.impl.sources;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

public class ConstituentTableErrorException extends UncheckedDeephavenException {
    public ConstituentTableErrorException(@NotNull final String description, @NotNull final Throwable cause) {
        super(description, cause);
    }
}
