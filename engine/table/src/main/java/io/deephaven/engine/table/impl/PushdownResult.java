//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseable;

/**
 * Records the results of a push-down predicate filter operation.
 */
public class PushdownResult implements SafeCloseable {
    /**
     * Rows that match the predicate.
     */
    private final WritableRowSet match;

    /**
     * Rows that might match the predicate but would need to be tested to be certain.
     */
    private final WritableRowSet maybeMatch;

    private PushdownResult(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.match = match;
        this.maybeMatch = maybeMatch;
    }

    public static PushdownResult of(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        return new PushdownResult(match, maybeMatch);
    }

    public WritableRowSet match() {
        return match;
    }

    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    @Override
    public void close() {
        match.close();
        maybeMatch.close();
    }
}
