//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;


import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetTableLocationTest {
    @Test
    public void testCostSortedValues() {
        assertThat(ParquetTableLocation.PushdownMode.costSortedValues()
                .stream()
                .mapToLong(ParquetTableLocation.PushdownMode::filterCost))
                .isSorted();
    }
}
