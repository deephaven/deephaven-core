package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class UpdateGraphConflictException extends UncheckedDeephavenException {

    public UpdateGraphConflictException(final String reason) {
        super(reason);
    }
}
