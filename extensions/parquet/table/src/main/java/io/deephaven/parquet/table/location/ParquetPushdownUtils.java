//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.PrimitiveType;

import java.time.Instant;

/**
 * Utility methods for Parquet pushdown operations.
 */
final class ParquetPushdownUtils {
    private static final long NANOS_PER_SECOND = DateTimeUtils.SECOND;

    /**
     * Converts nanoseconds from the Epoch to an {@link Instant}.
     */
    static Instant epochNanosToInstant(final long nanos) {
        return Instant.ofEpochSecond(nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND);
    }


    static boolean areStatisticsUsable(final Statistics<?> statistics) {
        if (statistics == null || !statistics.hasNonNullValue()) {
            return false;
        }
        if (statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            // Not expected to have null min/max values, but if they are null, we cannot determine min/max
            return false;
        }
        final PrimitiveType parquetColType = statistics.type();
        if (parquetColType.columnOrder() != ColumnOrder.typeDefined()) {
            // We only handle typeDefined min/max right now; if new orders get defined in the future, they need to be
            // explicitly handled
            return false;
        }
        return true;
    }
}
