/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.SortedDateTimeGenerator;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.DownsampledWhereFilter;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;

public class TestDownsampledWhereFilter {
    private SafeCloseable executionContext;

    @Before
    public void setUp() throws Exception {
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @After
    public void tearDown() throws Exception {
        executionContext.close();
    }

    @Test
    public void testDownsampledWhere() {
        Random random = new Random(42);
        int size = 1000;

        final QueryTable table = getTable(false, size, random, initColumnInfos(new String[] {"Timestamp", "doubleCol"},
                new SortedDateTimeGenerator(DateTimeUtils.convertDateTime("2015-09-11T09:30:00 NY"),
                        DateTimeUtils.convertDateTime("2015-09-11T10:00:00 NY")),
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
                new SortedDateTimeGenerator(DateTimeUtils.convertDateTime("2015-09-11T09:30:00 NY"),
                        DateTimeUtils.convertDateTime("2015-09-11T10:00:00 NY")),
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
