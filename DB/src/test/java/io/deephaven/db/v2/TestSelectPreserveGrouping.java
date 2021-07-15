package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.tables.utils.TableTools.intCol;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;

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
            }catch(Exception e) {
                System.gc();
                tries++;
            }
        } while (!success && tries < 10);
    }

    public void testPreserveGrouping() {
        final Table x = TstUtils.testTable(TstUtils.cG("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA"), intCol("Sentinel", 1, 2, 3, 4, 5, 6));
        assertTrue(x.getIndex().hasGrouping(x.getColumnSource("Sym")));
        assertFalse(x.getIndex().hasGrouping(x.getColumnSource("Sentinel")));

        QueryScope.addParam("switchColumnValue", 1);
        final Table xs = x.select("Sym", "SentinelDoubled=Sentinel*2", "Foo=switchColumnValue", "Sentinel");
        assertTableEquals(x, xs.view("Sym", "Sentinel"));

        assertTrue(xs.getIndex().hasGrouping(xs.getColumnSource("Sym")));
        assertFalse(xs.getIndex().hasGrouping(xs.getColumnSource("SentinelDoubled")));
        assertFalse(xs.getIndex().hasGrouping(xs.getColumnSource("Foo")));
        assertFalse(xs.getIndex().hasGrouping(xs.getColumnSource("Sentinel")));
    }

    public void testPreserveDeferredGrouping() throws IOException {
        final File testDirectory = Files.createTempDirectory("DeferredGroupingTest").toFile();
        final File dest = new File(testDirectory, "Table.parquet");
        try {
            final ColumnHolder symHolder = TstUtils.cG("Sym", "AAPL", "AAPL", "BRK", "BRK", "TSLA", "TLSA");
            final ColumnHolder sentinelHolder = intCol("Sentinel", 1, 2, 3, 4, 5, 6);

            final Map<String, ColumnSource> columns = new LinkedHashMap<>();
            final Index index = Index.FACTORY.getFlatIndex(6);
            columns.put("Sym", TstUtils.getTreeMapColumnSource(index, symHolder));
            columns.put("Sentinel", TstUtils.getTreeMapColumnSource(index, sentinelHolder));
            final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("Sym").withGrouping(),
                ColumnDefinition.ofInt("Sentinel"));
            final Table x = new QueryTable(definition, index, columns);

            assertTrue(x.getDefinition().getColumn("Sym").isGrouping());

            System.out.println(x.getDefinition());
            ParquetTools.writeTable(x, dest);

            final Table readBack = ParquetTools.readTable(dest);
            TableTools.showWithIndex(readBack);

            assertTrue(readBack.getIndex().hasGrouping(readBack.getColumnSource("Sym")));

            final Table xs = x.select("Sym", "Sentinel=Sentinel*2", "Foo=Sym", "Sent2=Sentinel");

            assertTrue(xs.getIndex().hasGrouping(xs.getColumnSource("Sym")));
            assertTrue(xs.getIndex().hasGrouping(xs.getColumnSource("Foo")));
            assertSame(xs.getColumnSource("Sym"), xs.getColumnSource("Foo"));
            assertFalse(xs.getIndex().hasGrouping(xs.getColumnSource("Sentinel")));
            assertFalse(xs.getIndex().hasGrouping(xs.getColumnSource("Sent2")));

            final Table xs2 = x.select("Foo=Sym", "Sentinel=Sentinel*2", "Foo2=Foo", "Foo3=Sym");

            assertTrue(xs2.getIndex().hasGrouping(xs2.getColumnSource("Foo")));
            assertFalse(xs2.getIndex().hasGrouping(xs2.getColumnSource("Sentinel")));
            assertTrue(xs2.getIndex().hasGrouping(xs2.getColumnSource("Foo2")));
            assertSame(xs2.getColumnSource("Foo2"), xs2.getColumnSource("Foo"));
            assertTrue(xs2.getIndex().hasGrouping(xs2.getColumnSource("Foo3")));
        } finally {
            FileUtils.deleteRecursively(testDirectory);
        }
    }
}
