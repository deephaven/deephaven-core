//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * Thrown by a join when a pair of matched columns from the left and right tables do not have the same type.
 */
public class MismatchedJoinKeyException extends UncheckedDeephavenException {

    public MismatchedJoinKeyException(String reason) {
        super(reason);
    }
}
