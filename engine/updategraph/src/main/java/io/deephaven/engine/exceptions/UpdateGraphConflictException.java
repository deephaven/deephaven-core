//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class UpdateGraphConflictException extends UncheckedDeephavenException {

    public UpdateGraphConflictException(final String reason) {
        super(reason);
    }
}
