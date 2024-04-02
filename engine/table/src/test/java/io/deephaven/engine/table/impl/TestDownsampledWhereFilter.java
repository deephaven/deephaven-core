//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.DownsampledWhereFilter;
import org.junit.Rule;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;

public class TestDownsampledWhereFilter {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testDownsampledWhere() {
        Random random = new Random(42);
        int size = 1000;

        final QueryTable table = getTable(false, size, random, initColumnInfos(new String[] {"Timestamp", "doubleCol"},
                new SortedInstantGenerator(DateTimeUtils.parseInstant("2015-09-11T09:30:00 NY"),
                        DateTimeUtils.parseInstant("2015-09-11T10:00:00 NY")),
                new DoubleGenerator(0, 100)));

        Table downsampled = table.where(new DownsampledWhereFilter("Timestamp", 60_000_000_000L));
        Table standardWay =
                table.updateView("TimeBin=upperBin(Timestamp, 60000000000)").lastBy("TimeBin").dropColumns("TimeBin");

        TableTools.showWithRowSet(downsampled);
        TableTools.showWithRowSet(standardWay);

        assertTableEquals(downsampled, standardWay);
    }

    @Test
    public void testDownsampledWhereLowerFirst() {
        Random random = new Random(42);
        int size = 1000;

        final QueryTable table = getTable(false, size, random, initColumnInfos(new String[] {"Timestamp", "doubleCol"},
                new SortedInstantGenerator(DateTimeUtils.parseInstant("2015-09-11T09:30:00 NY"),
                        DateTimeUtils.parseInstant("2015-09-11T10:00:00 NY")),
                new DoubleGenerator(0, 100)));

        Table downsampled = table.where(new DownsampledWhereFilter("Timestamp", 60_000_000_000L,
                DownsampledWhereFilter.SampleOrder.LOWERFIRST));
        Table standardWay =
                table.updateView("TimeBin=lowerBin(Timestamp, 60000000000)").firstBy("TimeBin").dropColumns("TimeBin");

        TableTools.showWithRowSet(downsampled);
        TableTools.showWithRowSet(standardWay);

        assertTableEquals(downsampled, standardWay);
    }
}
