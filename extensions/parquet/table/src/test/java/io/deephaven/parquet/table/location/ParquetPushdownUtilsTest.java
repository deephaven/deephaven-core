//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Category(OutOfBandTest.class)
public class ParquetPushdownUtilsTest {
    private static Statistics<?> doubleStats(final double minInc, final double maxInc) {
        final PrimitiveType col = Types.required(DOUBLE).named("doubleCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(minInc)))
                .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(maxInc)))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void testStatsUsable() {
        assertTrue(ParquetPushdownUtils.areStatisticsUsable(doubleStats(10, 100.0)));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(null));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(doubleStats(-99.0, Double.NaN)));
        assertFalse(ParquetPushdownUtils.areStatisticsUsable(doubleStats(Double.NaN, 0.0)));
    }
}
