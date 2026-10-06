//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
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

    @Test
    public void testSymbolTableDataIndexLookup() {
        final Table t = TableTools.emptyTable(100)
                .update("TheBestColumn=i==9?(String)null:`S`+(i%10)", "Sentinel=i");
        final File toWrite = new File(dataDirectory, "table.parquet");
        ParquetTools.writeTable(t, toWrite.getPath());

        // Make sure we have the expected symbol table (or not)
        final Table readBack = ParquetTools.readTable(toWrite.getPath());
        final SymbolTableSource<String> source =
                (SymbolTableSource<String>) readBack.getColumnSource("TheBestColumn", String.class);
        Assert.assertTrue(source.hasSymbolTable(readBack.getRowSet()));

        final DataIndex index = DataIndexer.getOrCreateDataIndex(readBack, "TheBestColumn");
        Assert.assertTrue("index instanceof TableBackedDataIndex", index instanceof TableBackedDataIndex);
        final DataIndex.RowKeyLookup rkl = index.rowKeyLookup();

        for (int i = 0; i < 9; i++) {
            final String key = "S" + i;
            final long rowKey = rkl.apply(key, false);
            Assert.assertEquals(i, rowKey);
        }
        // Assert null lookup is correct.
        final long rowKey = rkl.apply(null, false);
        Assert.assertEquals(9, rowKey);

        // A chunk of keys, including null and one the index does not hold, finds what each key finds alone.
        final String[] keys = {"S3", null, "S0", "NotAKey", "S8"};
        final long[] expectedRowKeys = {3, 9, 0, RowSequence.NULL_ROW_KEY, 8};
        // noinspection unchecked
        final Chunk<Values>[] keyChunks = new Chunk[] {ObjectChunk.chunkWrap(keys)};
        try (final WritableLongChunk<RowKeys> rowKeys = WritableLongChunk.makeWritableChunk(keys.length)) {
            rkl.apply(keyChunks, rowKeys, false);
            Assert.assertEquals(keys.length, rowKeys.size());
            for (int ki = 0; ki < keys.length; ++ki) {
                Assert.assertEquals(keys[ki], expectedRowKeys[ki], rowKeys.get(ki));
                Assert.assertEquals(keys[ki], rkl.apply(keys[ki], false), rowKeys.get(ki));
            }
        }
    }

    /**
     * This won't fail after 41.0 due to the table-level filtering replacing AbstractColumnSource#match. Used to test
     * bugfix against 0.40.x.
     */
    @Test
    public void testFilterIndexedSymbolTable() {
        final Table t = TableTools.emptyTable(100)
                .update("TheBestColumn=i==9?(String)null:`S`+(i%10)", "Sentinel=i");
        final File toWrite = new File(dataDirectory, "table.parquet");
        ParquetTools.writeTable(t, toWrite.getPath());

        // Make sure we have the expected symbol table (or not)
        final Table readBack = ParquetTools.readTable(toWrite.getPath());
        final SymbolTableSource<String> source =
                (SymbolTableSource<String>) readBack.getColumnSource("TheBestColumn", String.class);
        Assert.assertTrue(source.hasSymbolTable(readBack.getRowSet()));

        final DataIndex index = DataIndexer.getOrCreateDataIndex(readBack, "TheBestColumn");
        Assert.assertTrue("index instanceof TableBackedDataIndex", index instanceof TableBackedDataIndex);
        // materialize the index table
        final Table indexTable = index.table();

        Table filtered;
        Table expected;

        filtered = readBack.where("TheBestColumn in `S0`");
        expected = TableTools.emptyTable(10).update("TheBestColumn=`S0`", "Sentinel=i*10");
        assertTableEquals(expected, filtered);

        // Assert null filtering is correct.
        filtered = readBack.where("TheBestColumn in null");
        expected = TableTools.emptyTable(1).update("TheBestColumn=(String)null", "Sentinel=9");
        assertTableEquals(expected, filtered);
    }
}
