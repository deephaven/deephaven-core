//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.*;

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

    @Test
    public void testPreserveGrouping() {
        final Table x = TstUtils.testTable(TstUtils.colIndexed("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));

        assertTrue(DataIndexer.hasDataIndex(x, "Sym"));
        assertFalse(DataIndexer.hasDataIndex(x, "Sentinel"));

        QueryScope.addParam("switchColumnValue", 1);
        final Table xs = x.select("Sym", "SentinelDoubled=Sentinel*2", "Foo=switchColumnValue", "Sentinel");
        assertTableEquals(x, xs.view("Sym", "Sentinel"));

        assertTrue(DataIndexer.hasDataIndex(xs, "Sym"));
        assertFalse(DataIndexer.hasDataIndex(xs, "SentinelDoubled"));
        assertFalse(DataIndexer.hasDataIndex(xs, "Foo"));
        assertFalse(DataIndexer.hasDataIndex(xs, "Sentinel"));

        final Table x2 = TstUtils.testTable(TstUtils.i(0, 1 << 16, 2 << 16, 3 << 16, 4 << 16, 5 << 16).toTracking(),
                TstUtils.colIndexed("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));

        final Table xu = x2.update("Sym2=Sym");
        assertTableEquals(x2, xu.view("Sym=Sym2", "Sentinel"));

        assertTrue(DataIndexer.hasDataIndex(xu, "Sym"));
        assertTrue(DataIndexer.hasDataIndex(xu, "Sym2"));
        assertFalse(DataIndexer.hasDataIndex(xu, "Sentinel"));
    }

    /**
     * An alias made before the select shares the indexed column's source, so the select copies one indexed source to
     * two result columns. Each alias gets an index, and an index on another column is added once.
     */
    @Test
    public void testPreserveGroupingOfAliasedColumn() {
        final Table x = TstUtils.testTable(TstUtils.colIndexed("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                TstUtils.colIndexed("Exch", "N", "Q", "N", "Q", "N", "Q"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));
        final Table aliased = x.updateView("Sym2 = Sym");
        assertSame(aliased.getColumnSource("Sym"), aliased.getColumnSource("Sym2"));

        final Table xs = aliased.select();
        assertTableEquals(aliased, xs);
        assertNotSame(xs.getColumnSource("Sym"), xs.getColumnSource("Sym2"));

        assertTrue(DataIndexer.hasDataIndex(xs, "Sym"));
        assertTrue(DataIndexer.hasDataIndex(xs, "Sym2"));
        assertTrue(DataIndexer.hasDataIndex(xs, "Exch"));
        assertFalse(DataIndexer.hasDataIndex(xs, "Sentinel"));
    }

    /**
     * A multi-column index is remapped once per combination of the key columns' aliases, and an index on a single key
     * column once per alias of that column.
     */
    @Test
    public void testPreserveMultiColumnGroupingOfAliasedColumns() {
        final Table x = TstUtils.testTable(
                stringCol("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"),
                stringCol("Exch", "N", "Q", "N", "Q", "N", "Q"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6));
        DataIndexer.getOrCreateDataIndex(x, "Sym", "Exch");
        DataIndexer.getOrCreateDataIndex(x, "Sym");

        final Table xs = x.updateView("Sym2 = Sym", "Exch2 = Exch").select();
        assertTableEquals(x.updateView("Sym2 = Sym", "Exch2 = Exch"), xs);

        for (final String sym : new String[] {"Sym", "Sym2"}) {
            assertTrue(DataIndexer.hasDataIndex(xs, sym));
            for (final String exch : new String[] {"Exch", "Exch2"}) {
                assertTrue(DataIndexer.hasDataIndex(xs, sym, exch));
            }
        }
        assertFalse(DataIndexer.hasDataIndex(xs, "Exch"));
        assertFalse(DataIndexer.hasDataIndex(xs, "Exch2"));
    }

    @Test
    public void testPreserveDeferredGrouping() throws IOException {
        final File testDirectory = Files.createTempDirectory("DeferredGroupingTest").toFile();
        final File dest = new File(testDirectory, "Table.parquet");
        try {
            final ColumnHolder<?> symHolder = TstUtils.colIndexed("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA");
            final ColumnHolder<?> sentinelHolder = intCol("Sentinel", 1, 2, 3, 4, 5, 6);

            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            final TrackingRowSet rowSet = RowSetFactory.flat(6).toTracking();
            columns.put("Sym", TstUtils.getTestColumnSource(rowSet, symHolder));
            columns.put("Sentinel", TstUtils.getTestColumnSource(rowSet, sentinelHolder));
            final TableDefinition definition = TableDefinition.of(
                    ColumnDefinition.ofString("Sym"),
                    ColumnDefinition.ofInt("Sentinel"));
            final Table x = new QueryTable(definition, rowSet, columns);

            DataIndexer.getOrCreateDataIndex(x, "Sym");

            System.out.println(x.getDefinition());
            ParquetTools.writeTable(x, dest.getPath());

            final Table readBack = ParquetTools.readTable(dest.getPath());
            TableTools.showWithRowSet(readBack);

            assertTrue(DataIndexer.hasDataIndex(readBack, "Sym"));

            final Table xs = x.select("Sym", "Sentinel=Sentinel*2", "Foo=Sym", "Sent2=Sentinel");

            assertTrue(DataIndexer.hasDataIndex(xs, "Sym"));
            assertTrue(DataIndexer.hasDataIndex(xs, "Foo"));
            assertSame(xs.getColumnSource("Sym"), xs.getColumnSource("Foo"));
            assertFalse(DataIndexer.hasDataIndex(xs, "Sentinel"));
            assertFalse(DataIndexer.hasDataIndex(xs, "Sent2"));

            final Table xs2 = x.select("Foo=Sym", "Sentinel=Sentinel*2", "Foo2=Foo", "Foo3=Sym");

            assertTrue(DataIndexer.hasDataIndex(xs2, "Foo"));
            assertFalse(DataIndexer.hasDataIndex(xs2, "Sentinel"));
            assertTrue(DataIndexer.hasDataIndex(xs2, "Foo2"));
            assertSame(xs2.getColumnSource("Foo2"), xs2.getColumnSource("Foo"));
            assertTrue(DataIndexer.hasDataIndex(xs2, "Foo3"));
        } finally {
            FileUtils.deleteRecursively(testDirectory);
        }
    }
}
