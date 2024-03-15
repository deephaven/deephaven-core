//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class UpdateGraphConflictException extends UncheckedDeephavenException {

    public UpdateGraphConflictException(final String reason) {
        super(reason);
    }
}
