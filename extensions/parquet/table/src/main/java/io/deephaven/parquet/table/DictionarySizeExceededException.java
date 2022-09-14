package io.deephaven.parquet.table;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

final class DictionarySizeExceededException extends UncheckedDeephavenException {
    public DictionarySizeExceededException(@NotNull final String message) {
        super(message);
    }
}
