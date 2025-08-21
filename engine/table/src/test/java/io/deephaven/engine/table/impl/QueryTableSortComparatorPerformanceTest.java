//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.StringGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

@Category(OutOfBandTest.class)
public class QueryTableSortComparatorPerformanceTest extends QueryTableTestBase {
    @SuppressWarnings("rawtypes")
    public void testComparatorPerformance() throws CsvReaderException {
        final Random random = new Random(0);
        // For better results, use a bigger table; but I don't want to make a test take forever. With 10M rows;
        // the difference ends up being around 15% for both. With 1M rows it is close or 12% for reverse order.
        final QueryTable queryTable = getTable(false, 100_000, random,
                initColumnInfos(new String[] {"Value1", "Sentinel"},
                        new StringGenerator(),
                        new IntGenerator(0, 100000)));

        final Comparator naturalOrder = Comparator.nullsFirst(Comparator.naturalOrder());
        final Comparator reverseOrder = Comparator.nullsFirst(Comparator.naturalOrder()).reversed();

        final StringBuilder csvBuilder = new StringBuilder();

        csvBuilder.append("Ascending,Descending,ComparatorAsc,ComparatorDesc\n");
        for (int iter = 0; iter < 5; ++iter) {
            // noinspection MismatchedQueryAndUpdateOfCollection - we don't want these to be somehow optimized away
            final List<Table> results = new ArrayList<>();
            final long t0 = System.nanoTime();
            results.add(queryTable.sort("Value1"));
            final long t1 = System.nanoTime();
            results.add(queryTable.sortDescending("Value1"));
            final long t2 = System.nanoTime();
            results.add(queryTable.sort(ComparatorSortColumn.asc("Value1", naturalOrder, true)));
            final long t3 = System.nanoTime();
            results.add(queryTable.sort(ComparatorSortColumn.asc("Value1", reverseOrder, true)));
            final long t4 = System.nanoTime();
            csvBuilder.append(t1 - t0).append(",").append(t2 - t1).append(",").append(t3 - t2).append(",")
                    .append(t4 - t3).append("\n");
            results.clear();
        }

        final Table result = CsvTools.readCsv(new ByteArrayInputStream(csvBuilder.toString().getBytes()));
        final Table avg =
                result.avgBy().update("AscChange=ComparatorAsc/Ascending", "DescChange=ComparatorDesc/Descending");
        TableTools.show(result);
        TableTools.show(avg);
    }
}
