//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * Thrown by an exact join ({@link io.deephaven.api.NaturalJoinType#EXACTLY_ONE_MATCH}) when a left row has no matching
 * right row.
 */
public class ExactJoinMissingKeyException extends UncheckedDeephavenException {

    public ExactJoinMissingKeyException(String reason) {
        super(reason);
    }
}
