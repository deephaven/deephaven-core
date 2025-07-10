//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.time.DateTimeUtils;

import java.time.Instant;

/**
 * Utility methods for Parquet pushdown operations.
 */
abstract class ParquetPushdownUtils {
    private static final long NANOS_PER_SECOND = DateTimeUtils.SECOND;

    /**
     * Converts nanoseconds from the Epoch to an {@link Instant}.
     */
    static Instant epochNanosToInstant(final long nanos) {
        return Instant.ofEpochSecond(nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND);
    }
}
