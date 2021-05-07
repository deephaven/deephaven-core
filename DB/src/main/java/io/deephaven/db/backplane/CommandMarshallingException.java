package io.deephaven.db.backplane;

import io.deephaven.UncheckedDeephavenException;

public class CommandMarshallingException extends UncheckedDeephavenException {
    public CommandMarshallingException(final String reason, final Exception cause) {
        super(reason, cause);
    }
}
