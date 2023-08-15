package io.deephaven.engine.util.file;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown on refreshing a file handle which has been marked invalid.
 */
public class InvalidFileHandleException extends UncheckedDeephavenException {
    public InvalidFileHandleException(@NotNull final String reason) {
        super(reason);
    }
}
