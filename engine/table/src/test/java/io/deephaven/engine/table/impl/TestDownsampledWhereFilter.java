/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.DownsampledWhereFilter;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.table.impl.TstUtils.*;

public class TestDownsampledWhereFilter {
    @Test
    public void testDownsampledWhere() throws IOException {
        Random random = new Random(42);
        List<Table> tables = new ArrayList<>();

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

        String diff = TableTools.diff(downsampled, standardWay, 10);
        TestCase.assertEquals("", diff);
    }

    @Test
    public void testDownsampledWhereLowerFirst() throws IOException {
        Random random = new Random(42);
        List<Table> tables = new ArrayList<>();

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

        String diff = TableTools.diff(downsampled, standardWay, 10);
        TestCase.assertEquals("", diff);
    }
}
