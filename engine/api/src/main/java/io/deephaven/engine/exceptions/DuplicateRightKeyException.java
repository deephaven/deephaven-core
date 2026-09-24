//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * Thrown by a natural join whose {@link io.deephaven.api.NaturalJoinType join type} rejects duplicates when the right
 * table holds more than one row for a key that a left row matches.
 */
public class DuplicateRightKeyException extends UncheckedDeephavenException {

    public DuplicateRightKeyException(String reason) {
        super(reason);
    }
}
