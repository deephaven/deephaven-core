//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;


import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetTableLocationTest {
    @Test
    public void testCostSortedValues() {
        assertThat(ParquetTableLocation.costSortedModes()
                .stream()
                .mapToLong(RegionedPushdownHelper.PushdownMode::filterCost))
                .isSorted();
    }
}
