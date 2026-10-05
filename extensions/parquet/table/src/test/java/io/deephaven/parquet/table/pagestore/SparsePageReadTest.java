//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.FileUtils;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.table.ParquetTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Sparse reads must return exactly what whole-page materialization returns.
 */
public class SparsePageReadTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int NUM_ROWS = 200_000;

    private static final String[] FILTERS = {
            "ii % 13 == 0",
            "ii % 997 == 3",
            "ii % 20011 == 0",
            "ii < 100",
            "ii >= 99990 && ii < 130010",
            "ii % 5000 < 3",
    };

    private File dataDirectory;
    private double oldMaxDensity;
    private int oldReadsBeforeCaching;

    @Before
    public void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(SparsePageReadTest.class.getName()).toFile();
        // Take the sparse path for every uncached page.
        oldMaxDensity = ColumnChunkPageStore.setSparseReadMaxDensity(1.0);
        oldReadsBeforeCaching = ColumnChunkPageStore.setSparseReadsBeforeCaching(Integer.MAX_VALUE);
    }

    @After
    public void tearDown() {
        ColumnChunkPageStore.setSparseReadMaxDensity(oldMaxDensity);
        ColumnChunkPageStore.setSparseReadsBeforeCaching(oldReadsBeforeCaching);
        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDirectory);
        }
    }

    /**
     * Every type, with scattered nulls and a null run spanning several pages. {@code Str} has too many values to stay
     * dictionary-encoded, so its later pages are PLAIN; {@code Sym} stays dictionary-encoded.
     */
    private String writeTestTable() {
        final Table table = TableTools.emptyTable(NUM_ROWS).update(
                "IsNull = ii % 7 == 0 || (ii >= 100000 && ii < 130000)",
                "B = IsNull ? NULL_BYTE : (byte) ii",
                "C = IsNull ? NULL_CHAR : (char) ('A' + ii % 26)",
                "S = IsNull ? NULL_SHORT : (short) ii",
                "I = IsNull ? NULL_INT : (int) ii",
                "L = IsNull ? NULL_LONG : ii",
                "F = IsNull ? NULL_FLOAT : (float) ii",
                "D = IsNull ? NULL_DOUBLE : ii / 3.0",
                "Str = IsNull ? null : `value-` + ii",
                "Sym = IsNull ? null : `sym-` + (ii % 50)",
                "BD = IsNull ? null : java.math.BigDecimal.valueOf(ii, 2)",
                "Inst = IsNull ? null : DateTimeUtils.epochNanosToInstant(ii * 1000)",
                "LD = IsNull ? null : java.time.LocalDate.ofEpochDay(ii % 10000)",
                "Arr = IsNull ? null : new int[] {(int) ii, (int) ii + 1}");
        final String path = new File(dataDirectory, "table.parquet").getPath();
        ParquetTools.writeTable(table, path);
        return path;
    }

    @Test
    public void testFiltersMatchDense() {
        assertSparseMatchesDense(writeTestTable(), true);
    }

    @Test
    public void testReferenceFilesMatchDense() {
        // The V1/V2 and mixed-encoding files are not written by Deephaven, and the latter cover both page stores.
        assertSparseMatchesDense(resource("/ReferenceParquetV1PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceParquetV2PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithOffsetIndex.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithoutOffsetIndex.parquet"), true);
        // Non-nullable columns, whose pages have no definition levels; V1 with an offset index, V2 without.
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV1.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV2.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceNullStringDictEncoded1.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetFileWithDifferentPageSizes1.parquet"), false);
        // Repeated columns only, so these must fall back to materializing pages.
        assertSparseMatchesDense(resource("/ReferenceParquetArrayData.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetVectorData.parquet"), false);
    }

    @Test
    public void testCachedPagesAreReused() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        final Table dense = dense(() -> source.select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        assertTableEquals(dense.where(FILTERS[0]), source.where(FILTERS[0]).select());
        assertEquals("pages materialized by the dense read serve the sparse one",
                before, ColumnChunkPageStore.sparseFillCount());
    }

    /**
     * Small chunks split each page across many fills. In order, each fill resumes where the previous one stopped.
     * Reversed, each starts before the previous one on the same context, so only the first reads sparsely; the rest
     * materialize and cache their pages.
     */
    @Test
    public void testChunkedFills() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final RowSet rows = expected.where("ii % 11 == 0").getRowSet();
        long forwardFills = 0;
        for (final boolean reverse : new boolean[] {false, true}) {
            final Table actual = ParquetTools.readTable(path);
            final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
            final long opensBefore = ColumnChunkPageStore.sparseOpenCount();
            for (final String column : expected.getDefinition().getColumnNamesArray()) {
                assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column), rows,
                        reverse);
            }
            final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
            final long opens = ColumnChunkPageStore.sparseOpenCount() - opensBefore;
            assertTrue("fills should read sparsely", fills > 0);
            if (reverse) {
                assertTrue("reversed fills go dense: " + fills + " sparse fills, against " + forwardFills
                        + " in order", fills * 100 < forwardFills);
            } else {
                assertTrue("in-order fills resume: " + opens + " opens for " + fills + " fills", opens * 10 < fills);
                forwardFills = fills;
            }
        }
    }

    /**
     * Each select is a separate pass over the same rows. After {@code readsBeforeCaching} sparse passes, the pages are
     * materialized and cached, and later passes read them from the cache.
     */
    @Test
    public void testRepeatedReadsCache() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        final int readsBeforeCaching = 2;
        ColumnChunkPageStore.setSparseReadsBeforeCaching(readsBeforeCaching);
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        for (int pass = 1; pass <= readsBeforeCaching + 2; ++pass) {
            final long before = ColumnChunkPageStore.sparseFillCount();
            assertTableEquals(expected, filtered.select());
            final long fills = ColumnChunkPageStore.sparseFillCount() - before;
            if (pass <= readsBeforeCaching) {
                assertTrue("pass " + pass + " should read sparsely", fills > 0);
            } else {
                assertEquals("pass " + pass + " should read cached pages", 0, fills);
            }
        }
    }

    /**
     * A sort's redirection fills the source once per output chunk, each fill scattered over every page.
     */
    @Test
    public void testRedirectedReadsMatchDense() {
        final String path = writeTestTable();
        final String hash = "H_ = (ii * 2654435761L) % 1000003L";
        final Table expected = dense(() -> ParquetTools.readTable(path).updateView(hash).sort("H_").select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        final Table actual = ParquetTools.readTable(path).updateView(hash).sort("H_").select();
        assertTableEquals(expected, actual);
        assertTrue("the first chunk should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    private static void assertFillsMatch(
            final String column,
            final ColumnSource<?> expected,
            final ColumnSource<?> actual,
            final RowSet rows,
            final boolean reverse) {
        final int chunkSize = 37;
        final List<RowSet> chunks = new ArrayList<>();
        try (final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                chunks.add(it.getNextRowSequenceWithLength(chunkSize).asRowSet().copy());
            }
        }
        if (reverse) {
            Collections.reverse(chunks);
        }
        try (final ChunkSource.FillContext expectedContext = expected.makeFillContext(chunkSize);
                final ChunkSource.FillContext actualContext = actual.makeFillContext(chunkSize);
                final WritableChunk<Values> expectedChunk = expected.getChunkType().makeWritableChunk(chunkSize);
                final WritableChunk<Values> actualChunk = actual.getChunkType().makeWritableChunk(chunkSize)) {
            for (final RowSet chunk : chunks) {
                expected.fillChunk(expectedContext, expectedChunk, chunk);
                actual.fillChunk(actualContext, actualChunk, chunk);
                assertEquals(column, expectedChunk.size(), actualChunk.size());
                for (int ii = 0; ii < expectedChunk.size(); ++ii) {
                    final Object expectedValue = ChunkBoxer.boxedGet(expectedChunk, ii);
                    final Object actualValue = ChunkBoxer.boxedGet(actualChunk, ii);
                    assertTrue(column + "[" + ii + "]: " + expectedValue + " != " + actualValue,
                            Objects.deepEquals(expectedValue, actualValue));
                }
                chunk.close();
            }
        }
    }

    private static void assertSparseMatchesDense(final String path, final boolean expectSparse) {
        for (final String filter : FILTERS) {
            final Table expected = dense(() -> ParquetTools.readTable(path).where(filter).select());
            final long before = ColumnChunkPageStore.sparseFillCount();
            // A fresh read, so that no page is already cached.
            final Table actual = ParquetTools.readTable(path).where(filter).select();
            assertTableEquals(expected, actual);
            if (expectSparse && expected.size() > 0) {
                assertTrue(path + " where " + filter + " should read sparsely",
                        ColumnChunkPageStore.sparseFillCount() > before);
            }
        }
    }

    private static Table dense(final java.util.function.Supplier<Table> read) {
        final double maxDensity = ColumnChunkPageStore.setSparseReadMaxDensity(0);
        try {
            return read.get();
        } finally {
            ColumnChunkPageStore.setSparseReadMaxDensity(maxDensity);
        }
    }

    private static String resource(final String name) {
        return SparsePageReadTest.class.getResource(name).getFile();
    }
}
