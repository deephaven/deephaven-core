//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

/**
 * Unit tests for Parquet symbol tables
 */
public class TestSymbolTableSource {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private File dataDirectory;

    @Before
    public final void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(TestSymbolTableSource.class.getName()).toFile();
    }

    @After
    public final void tearDown() {
        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDirectory);
        }
    }

    /**
     * Verify that a parquet writing encodes a simple low-cardinality String column using a dictionary, and that we can
     * correctly read this back via the {@link SymbolTableSource} interface.
     */
    @Test
    public void testWriteAndReadSymbols() {
        final Table t = TableTools.emptyTable(100).update("TheBestColumn=`S`+ (k % 10)", "Sentinel=k");
        final File toWrite = new File(dataDirectory, "table.parquet");
        ParquetTools.writeTable(t, toWrite.getPath());

        // Make sure we have the expected symbol table (or not)
        final Table readBack = ParquetTools.readTable(toWrite.getPath());
        final SymbolTableSource<String> source =
                (SymbolTableSource<String>) readBack.getColumnSource("TheBestColumn", String.class);
        Assert.assertTrue(source.hasSymbolTable(readBack.getRowSet()));

        final Table expected = TableTools.emptyTable(10).update("ID=k", "Symbol=`S` + k");
        final Table syms = source.getStaticSymbolTable(t.getRowSet(), false);

        assertTableEquals(expected, syms);
    }
}
