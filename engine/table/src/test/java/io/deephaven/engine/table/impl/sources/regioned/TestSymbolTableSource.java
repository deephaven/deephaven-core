package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.FileUtils;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.table.ParquetTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SymbolTableSource}.
 */
public class TestSymbolTableSource {
    private File dataDirectory;

    @Before
    public final void setUp() throws IOException {
        final Path rootPath = Files.createTempDirectory(Paths.get(Configuration.getInstance().getWorkspacePath()),
                "TestSymbolTables-");
        dataDirectory = rootPath.toFile();
    }

    @After
    public final void tearDown() {
        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDirectory);
        }
    }

    /**
     * Test that the db can mix partitions that contain symbol tables with those that do not, regardless of what the
     * TableDefinition claims it has.
     */
    @Test
    public void testWriteAndReadSymbols() {
        final Table t = TableTools.emptyTable(10).update("TheBestColumn=`S`+k");
        final File toWrite = new File(dataDirectory, "table.parquet");
        ParquetTools.writeTable(t, toWrite, t.getDefinition());

        // Make sure we have the expected symbol table (or not)
        final Table readBack = ParquetTools.readTable(toWrite);
        // noinspection unchecked
        SymbolTableSource<String> source = (SymbolTableSource) readBack.getColumnSource("TheBestColumn");
        assertTrue(source.hasSymbolTable(readBack.getRowSet()));
    }
}
