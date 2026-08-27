//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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


    /**
     * Whether {@code statistics} proves that the corresponding row group holds no null values.
     * <p>
     * Parquet {@code min}/{@code max} statistics summarize non-null values only, so a caller that must account for
     * nulls cannot learn anything about them from {@link #areStatisticsUsable(Statistics) usable} min/max values alone;
     * it has to consult the null count instead. This is deliberately kept separate from
     * {@link #areStatisticsUsable(Statistics)}: {@code null_count} is an optional field, and folding it into that gate
     * would disable min/max pushdown entirely for writers that omit it.
     * <p>
     * Note that {@code null_count} is more trustworthy than {@code min}/{@code max}, not less. It is not subject to the
     * corrupt-statistics / sort-order gate that discards old writers' min/max values, because a count carries no
     * ordering. The test below is also written in the safe direction: an absent count reads as
     * {@link Statistics#isNumNullsSet()} {@code == false} rather than as a count of zero, so it can never be mistaken
     * for a proof that there are no nulls.
     * <p>
     * The count is read as a number of null <i>rows</i>, which holds only for a column with no repetition -- for a
     * repeated column one row spans many leaf values and the count is of leaf nulls.
     * {@code ParquetTableLocation.isSupportedForPushdown} declines repeated columns, so no such column reaches here.
     *
     * @param statistics the row group statistics for the column, possibly {@code null}
     */
    static boolean isProvenFreeOfNulls(final Statistics<?> statistics) {
        if (statistics == null) {
            return false;
        }
        return statistics.isNumNullsSet() && statistics.getNumNulls() == 0;
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
