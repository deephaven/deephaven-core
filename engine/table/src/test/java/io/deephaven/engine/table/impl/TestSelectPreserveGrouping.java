/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

public class TestSelectPreserveGrouping extends QueryTableTestBase {
    private static final String ROOT = "TestSelectPreserveGrouping_Root";

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            cleanupPersistence(ROOT);
        }
    }

    private static void cleanupPersistence(String root) {
        System.gc();
        System.gc();
        int tries = 0;
        boolean success = false;
        do {
            try {
                FileUtils.deleteRecursively(new File(root));
                success = true;
            } catch (Exception e) {
                System.gc();
                tries++;
            }
        } while (!success && tries < 10);
    }

    public void testPreserveGrouping() {
        final Table x = TstUtils.testTable(TstUtils.colGrouped("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));

        final DataIndexer xIndexer = DataIndexer.of(x.getRowSet());
        assertTrue(xIndexer.hasDataIndex(x.getColumnSource("Sym")));
        assertFalse(xIndexer.hasDataIndex(x.getColumnSource("Sentinel")));

        QueryScope.addParam("switchColumnValue", 1);
        final Table xs = x.select("Sym", "SentinelDoubled=Sentinel*2", "Foo=switchColumnValue", "Sentinel");
        assertTableEquals(x, xs.view("Sym", "Sentinel"));

        final DataIndexer xsIndexer = DataIndexer.of(xs.getRowSet());
        assertTrue(xsIndexer.hasDataIndex(xs.getColumnSource("Sym")));
        assertFalse(xsIndexer.hasDataIndex(xs.getColumnSource("SentinelDoubled")));
        assertFalse(xsIndexer.hasDataIndex(xs.getColumnSource("Foo")));
        assertFalse(xsIndexer.hasDataIndex(xs.getColumnSource("Sentinel")));

        final Table x2 = TstUtils.testTable(TstUtils.i(0, 1 << 16, 2 << 16, 3 << 16, 4 << 16, 5 << 16).toTracking(),
                TstUtils.colGrouped("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));

        final Table xu = x2.update("Sym2=Sym");
        assertTableEquals(x2, xu.view("Sym=Sym2", "Sentinel"));

        final DataIndexer xuIndexer = DataIndexer.of(xu.getRowSet());
        assertTrue(xuIndexer.hasDataIndex(xu.getColumnSource("Sym")));
        assertTrue(xuIndexer.hasDataIndex(xu.getColumnSource("Sym2")));
        assertFalse(xuIndexer.hasDataIndex(xu.getColumnSource("Sentinel")));
    }

    public void testPreserveDeferredGrouping() throws IOException {
        final File testDirectory = Files.createTempDirectory("DeferredGroupingTest").toFile();
        final File dest = new File(testDirectory, "Table.parquet");
        try {
            final ColumnHolder<?> symHolder = TstUtils.colGrouped("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA");
            final ColumnHolder<?> sentinelHolder = intCol("Sentinel", 1, 2, 3, 4, 5, 6);

            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            final TrackingRowSet rowSet = RowSetFactory.flat(6).toTracking();
            columns.put("Sym", TstUtils.getTestColumnSource(rowSet, symHolder));
            columns.put("Sentinel", TstUtils.getTestColumnSource(rowSet, sentinelHolder));
            final TableDefinition definition = TableDefinition.of(
                    ColumnDefinition.ofString("Sym"),
                    ColumnDefinition.ofInt("Sentinel"));
            final Table x = new QueryTable(definition, rowSet, columns);

            DataIndexer.of(x.getRowSet()).getOrCreateDataIndex(x, "Sym");

            System.out.println(x.getDefinition());
            ParquetTools.writeTable(x, dest);

            final Table readBack = ParquetTools.readTable(dest);
            TableTools.showWithRowSet(readBack);

            assertTrue(DataIndexer.of(readBack.getRowSet()).hasDataIndex(readBack.getColumnSource("Sym")));

            final Table xs = x.select("Sym", "Sentinel=Sentinel*2", "Foo=Sym", "Sent2=Sentinel");

            final DataIndexer xsIndexer = DataIndexer.of(xs.getRowSet());
            assertTrue(xsIndexer.hasDataIndex(xs.getColumnSource("Sym")));
            assertTrue(xsIndexer.hasDataIndex(xs.getColumnSource("Foo")));
            assertSame(xs.getColumnSource("Sym"), xs.getColumnSource("Foo"));
            assertFalse(xsIndexer.hasDataIndex(xs.getColumnSource("Sentinel")));
            assertFalse(xsIndexer.hasDataIndex(xs.getColumnSource("Sent2")));

            final Table xs2 = x.select("Foo=Sym", "Sentinel=Sentinel*2", "Foo2=Foo", "Foo3=Sym");

            final DataIndexer xs2Indexer = DataIndexer.of(xs2.getRowSet());
            assertTrue(xs2Indexer.hasDataIndex(xs2.getColumnSource("Foo")));
            assertFalse(xs2Indexer.hasDataIndex(xs2.getColumnSource("Sentinel")));
            assertTrue(xs2Indexer.hasDataIndex(xs2.getColumnSource("Foo2")));
            assertSame(xs2.getColumnSource("Foo2"), xs2.getColumnSource("Foo"));
            assertTrue(xs2Indexer.hasDataIndex(xs2.getColumnSource("Foo3")));
        } finally {
            FileUtils.deleteRecursively(testDirectory);
        }
    }
}
