//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Category(OutOfBandTest.class)
public class ParquetPushdownUtilsTest {
    private static Statistics<?> doubleStats(final double minInc, final double maxInc) {
        return doubleStats(minInc, maxInc, 0L);
    }

    private static Statistics<?> doubleStats(final double minInc, final double maxInc, final long numNulls) {
        final PrimitiveType col = Types.required(DOUBLE).named("doubleCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(minInc)))
                .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(maxInc)))
                .withNumNulls(numNulls)
                .build();
    }

    /**
     * Statistics without a {@code null_count}, as an optional-field-omitting writer produces.
     */
    private static Statistics<?> doubleStatsWithoutNullCount(final double minInc, final double maxInc) {
        final PrimitiveType col = Types.required(DOUBLE).named("doubleCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(minInc)))
                .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(maxInc)))
                .build();
    }

    @Test
    public void testStatsUsable() {
        assertTrue(ParquetPushdownUtils.areStatisticsUsable(doubleStats(10, 100.0)));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(null));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(doubleStats(-99.0, Double.NaN)));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(doubleStats(Double.NaN, 0.0)));
    }

    @Test
    public void testProvenFreeOfNulls() {
        // A reported count of zero is the only proof that there are no nulls.
        assertTrue(ParquetPushdownUtils.isProvenFreeOfNulls(doubleStats(10.0, 100.0, 0L)));
        assertFalse(ParquetPushdownUtils.isProvenFreeOfNulls(doubleStats(10.0, 100.0, 1L)));
        assertFalse(ParquetPushdownUtils.isProvenFreeOfNulls(null));

        // An absent null_count must never be mistaken for a count of zero. The getter returns a -1 sentinel rather
        // than throwing, so isNumNullsSet() is the only safe test; pin both, since a sentinel that ever became 0
        // would silently defeat the guard.
        final Statistics<?> noNullCount = doubleStatsWithoutNullCount(10.0, 100.0);
        assertFalse(noNullCount.isNumNullsSet());
        assertEquals(-1L, noNullCount.getNumNulls());
        assertFalse(ParquetPushdownUtils.isProvenFreeOfNulls(noNullCount));
        // The min/max check and the null-count check are independent: these same statistics have min/max worth
        // reading even though their null count is absent.
        assertTrue(ParquetPushdownUtils.areStatisticsUsable(noNullCount));

        // A repeated column, whose count is over leaf values rather than rows, never reaches here: it is declined
        // upstream by ParquetTableLocation.isSupportedForPushdown.
    }
}
