//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.FileUtils;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.SafeCloseable;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Sparse reads must return exactly what whole-page materialization returns.
 * <p>
 * Tests that assert a read is served from what an earlier read cached pin the cache with
 * {@link PageCache#pinTouchedPages()}. The cache holds pages softly, so a GC between the reads that clears soft
 * references would make the later read decode again and fail the assertion.
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
    private int oldMissesBeforeFullCaching;

    @Before
    public void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(SparsePageReadTest.class.getName()).toFile();
        // Take the sparse path for every uncached page.
        oldMaxDensity = ColumnChunkPageStore.setSparseReadMaxDensity(1.0);
        oldMissesBeforeFullCaching = ColumnChunkPageStore.setSparseMissesBeforeFullCaching(Integer.MAX_VALUE);
    }

    @After
    public void tearDown() {
        ColumnChunkPageStore.setSparseReadMaxDensity(oldMaxDensity);
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(oldMissesBeforeFullCaching);
        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDirectory);
        }
    }

    /**
     * Most column types, with scattered nulls and a null run spanning several pages. {@code Str} has too many values
     * for a dictionary, so it is PLAIN; {@code Sym} is dictionary-encoded.
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

    /**
     * Formulas read their inputs with {@code getChunk}, which must read sparsely like {@code fillChunk}: for primitive
     * columns, and for {@code Instant} and {@code Boolean} columns read as objects through their native regions.
     */
    @Test
    public void testGetChunkReadsSparsely() {
        final String path = writeTestTable();
        final String[][] directAndFormula = {
                {"L", "X = L + 1"},
                {"Inst", "X = isNull(Inst) ? NULL_LONG : epochNanos(Inst)"},
                {"IsNull", "X = IsNull == true"},
        };
        for (final String[] pair : directAndFormula) {
            final long[] fills = new long[2];
            for (int ii = 0; ii < 2; ++ii) {
                final String column = pair[ii];
                final Table expected =
                        dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).view(column).select());
                final long before = ColumnChunkPageStore.sparseFillCount();
                // A fresh read, so that no page is already cached.
                assertTableEquals(expected, ParquetTools.readTable(path).where(FILTERS[0]).view(column).select());
                fills[ii] = ColumnChunkPageStore.sparseFillCount() - before;
            }
            assertTrue(pair[0] + " should read sparsely", fills[0] > 0);
            assertEquals(pair[1] + " should read as sparsely as " + pair[0], fills[0], fills[1]);
        }
    }

    @Test
    public void testReferenceFilesMatchDense() {
        // The V1/V2 and mixed-encoding files are not written by Deephaven, and the latter cover both page stores.
        assertSparseMatchesDense(resource("/ReferenceParquetV1PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceParquetV2PageData.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithOffsetIndex.parquet"), true);
        assertSparseMatchesDense(resource("/ParquetDataWithMixedEncodingWithoutOffsetIndex.parquet"), true);
        // Non-nullable columns, so no definition levels: V1 with an offset index; V2 without one, with uncompressed
        // pages in a snappy column chunk.
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV1.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceRequiredColumnsV2.parquet"), true);
        assertSparseMatchesDense(resource("/ReferenceNullStringDictEncoded1.parquet"), true);
        // Repeated columns only, so these must fall back to materializing pages.
        assertSparseMatchesDense(resource("/ReferenceParquetFileWithDifferentPageSizes1.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetArrayData.parquet"), false);
        assertSparseMatchesDense(resource("/ReferenceParquetVectorData.parquet"), false);
    }

    /**
     * Rebuilds {@code ReferenceRequiredColumnsV1.parquet} in {@code src/test/resources}: non-nullable columns in 4 KiB
     * snappy V1 pages with an offset index, dictionary encoded only for {@code Sym}. Remove the {@link Ignore} to run
     * it. {@code ReferenceRequiredColumnsV2.py} writes the V2 file, which parquet-java can't.
     */
    @Ignore("Generates a checked-in test resource")
    @Test
    public void generateRequiredColumnsV1File() throws IOException {
        final int numRows = 5_000;
        final MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.INT32).named("I")
                .required(PrimitiveTypeName.INT64).named("L")
                .required(PrimitiveTypeName.DOUBLE).named("D")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("Str")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("Sym")
                .named("schema");
        final SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        // The test's working directory is the project's.
        final File dest = new File("src/test/resources", "ReferenceRequiredColumnsV1.parquet");
        try (final ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new LocalOutputFile(dest.toPath()))
                .withType(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withWriterVersion(WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(4 * 1024)
                .withDictionaryEncoding(false)
                .withDictionaryEncoding("Sym", true)
                .build()) {
            for (int ii = 0; ii < numRows; ++ii) {
                final Group group = groupFactory.newGroup();
                group.add("I", ii);
                group.add("L", ii * 3L);
                group.add("D", ii / 3.0);
                group.add("Str", "value-" + ii);
                group.add("Sym", "sym-" + ii % 50);
                writer.write(group);
            }
        }
    }

    @Test
    public void testCachedPagesAreReused() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            final Table dense = dense(() -> source.select());
            final long before = ColumnChunkPageStore.sparseFillCount();
            assertTableEquals(dense.where(FILTERS[0]), source.where(FILTERS[0]).select());
            assertEquals("pages materialized by the dense read serve the sparse one",
                    before, ColumnChunkPageStore.sparseFillCount());
        }
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
     * A context publishes the rows it decoded from its last page when it closes.
     */
    @Test
    public void testPendingPublishedOnClose() {
        final String path = writeTestTable();
        final String filter = "ii < 4000 && ii % 13 == 0";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(filter).select());
        final Table filtered = ParquetTools.readTable(path).where(filter);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
        }
    }

    /**
     * A fill context crosses row groups, and so page stores, publishing each store's rows as it leaves it.
     */
    @Test
    public void testRowGroupsShareContext() {
        final Table table = TableTools.emptyTable(NUM_ROWS).update("L = ii", "Str = `value-` + ii");
        final String path = new File(dataDirectory, "rowGroups.parquet").getPath();
        ParquetTools.writeTable(table, path,
                ParquetInstructions.builder().setRowGroupInfo(RowGroupInfo.maxRows(30_000)).build());
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
        }
    }

    /**
     * Pages that don't start at a multiple of the RowSet block size, read at about 10% density in fills large enough
     * that their rows are stored as bitmaps. Shifting a RowSet per fill, or appending one bitmap to another, once
     * allocated quadratically in the rows per block.
     */
    @Test
    public void testUnalignedPagesAllocateLinearly() {
        final int numRows = 900_000;
        final Table table = TableTools.emptyTable(numRows).update("L = ii");
        final String path = new File(dataDirectory, "unaligned.parquet").getPath();
        // 300,000 longs per page, so every page after the first starts unaligned, and each takes several fills.
        ParquetTools.writeTable(table, path, ParquetInstructions.builder().setTargetPageSize(2_400_000).build());
        ColumnChunkPageStore.setSparseReadMaxDensity(0.125);
        final ColumnSource<?> expected = dense(() -> ParquetTools.readTable(path).select()).getColumnSource("L");
        final ColumnSource<?> actual = ParquetTools.readTable(path).getColumnSource("L");
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long ii = 0; ii < numRows; ii += 10) {
            builder.appendKey(ii);
        }
        try (final RowSet rows = builder.build();
                final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            final long valueBytes = Long.BYTES * rows.size();
            final long denseAllocated = dense(() -> allocatedByFills(ParquetTools.readTable(path).getColumnSource("L"),
                    rows));
            for (final String pass : new String[] {"decode", "hit"}) {
                final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
                final long hitsBefore = ColumnChunkPageStore.sparseHitCount();
                final long allocated = allocatedByFills(actual, rows);
                final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
                final long hits = ColumnChunkPageStore.sparseHitCount() - hitsBefore;
                assertTrue(pass + ": " + fills + " sparse fills, " + hits + " hits",
                        pass.equals("decode") ? fills > 0 : fills == 0 && hits > 0);
                if (pass.equals("decode")) {
                    // About 2/3 of a dense read.
                    assertTrue("decodes allocated " + allocated + " bytes, against " + denseAllocated + " dense",
                            allocated < denseAllocated);
                } else {
                    // About 1/8 of the value bytes, all per fill rather than per row.
                    assertTrue("hits allocated " + allocated + " bytes for " + valueBytes + " bytes of values",
                            allocated < valueBytes / 2);
                }
            }
            assertFillsMatch("L", expected, actual, rows, false);
        }
    }

    private static long allocatedByFills(final ColumnSource<?> source, final RowSet rows) {
        // Several fills per page, each with enough rows to be stored as a bitmap, appending to the rows pending for it.
        final int chunkSize = 8192;
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();
        try (final ChunkSource.FillContext context = source.makeFillContext(chunkSize);
                final WritableChunk<Values> chunk = source.getChunkType().makeWritableChunk(chunkSize);
                final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            final long before = threads.getThreadAllocatedBytes(threadId);
            while (it.hasMore()) {
                source.fillChunk(context, chunk, it.getNextRowSequenceWithLength(chunkSize));
            }
            return threads.getThreadAllocatedBytes(threadId) - before;
        }
    }

    /**
     * Later passes over the same rows are served from the values the first pass cached, and don't count toward
     * promotion.
     */
    @Test
    public void testRepeatedReadsHitValueCache() {
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).where(FILTERS[0]).select());
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(1);
        final Table filtered = ParquetTools.readTable(path).where(FILTERS[0]);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPass("first", expected, filtered, Reads.DECODE);
            assertPass("second", expected, filtered, Reads.HIT);
            assertPass("third", expected, filtered, Reads.HIT);
        }
    }

    /**
     * Rows that are a subset of a page's cached values are served from them, whether near each other or far apart.
     */
    @Test
    public void testSubsetReadsHit() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            // Decodes and caches every 13th row.
            assertPass("superset", dense(() -> ParquetTools.readTable(path).where("ii % 13 == 0").select()),
                    source.where("ii % 13 == 0"), Reads.DECODE);
            // Every other cached row: each seek steps over one cached range.
            assertPass("subset", dense(() -> ParquetTools.readTable(path).where("ii % 26 == 0").select()),
                    source.where("ii % 26 == 0"), Reads.HIT);
            // Every 100th cached row: each seek skips more cached ranges than it steps over, so it finds its position.
            assertPass("far subset", dense(() -> ParquetTools.readTable(path).where("ii % 1300 == 0").select()),
                    source.where("ii % 1300 == 0"), Reads.HIT);
        }
    }

    /**
     * A pass that needs rows the cache lacks decodes just those, and merges them into the cache.
     */
    @Test
    public void testMergeThenHit() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {"ii % 26 == 0", "ii % 26 == 13", "ii % 13 == 0"};
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.HIT};
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Requested ranges that start, end, or both, inside cached runs, including runs joined across the boundary between
     * hundreds.
     */
    @Test
    public void testRangesStraddleCachedRuns() {
        final String path = writeTestTable();
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {
                // Cached: [0, 9] of every 100.
                "ii % 100 < 10",
                // Cached, then missing.
                "ii % 100 >= 5 && ii % 100 < 15",
                // Missing, cached, missing: [90, 119] across each boundary between hundreds.
                "ii % 100 >= 90 || ii % 100 < 20",
                // Within one cached run.
                "ii % 100 >= 3 && ii % 100 < 17",
                // Inside the joined runs at both ends.
                "ii % 100 >= 95 || ii % 100 < 18",
        };
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.DECODE, Reads.HIT, Reads.HIT};
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Merging stops, and the page is cached whole, once the cached rows would exceed the density cutoff.
     */
    @Test
    public void testMergePromotesOnDensity() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseReadMaxDensity(0.05);
        final Table source = ParquetTools.readTable(path);
        // Each filter alone is under the cutoff; together they are over it. The second pass may decode part of a page
        // before crossing the cutoff, but then caches the page whole, so the third pass reads whole pages.
        final String first = "ii % 26 == 0";
        final Table expected = dense(() -> ParquetTools.readTable(path).where(first).select());
        // Select serially. A parallel select splits a page among contexts, each of which checks the cutoff against only
        // the published rows and its own, so none may promote the page, and the merged values exceed the cutoff.
        final long oldMinimumParallelSelectRows = QueryTable.MINIMUM_PARALLEL_SELECT_ROWS;
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = Long.MAX_VALUE;
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPass("first", expected, source.where(first), Reads.DECODE);
            final String second = "ii % 26 == 13";
            assertPass("second", dense(() -> ParquetTools.readTable(path).where(second).select()),
                    source.where(second), Reads.ANY);
            assertPass("third", expected, source.where(first), Reads.DENSE);
        } finally {
            QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = oldMinimumParallelSelectRows;
        }
    }

    /**
     * Merging stops, and the page is cached whole, after {@code sparseMissesBeforeFullCaching} misses.
     */
    @Test
    public void testMergePromotesOnLimit() {
        final String path = writeTestTable();
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final Table source = ParquetTools.readTable(path);
        final String[] filters = {"ii % 39 == 0", "ii % 39 == 13", "ii % 39 == 26", "ii % 39 == 0"};
        final Reads[] reads = {Reads.DECODE, Reads.DECODE, Reads.DENSE, Reads.DENSE};
        try (final SafeCloseable ignored = PageCache.pinTouchedPages()) {
            assertPasses(path, source, filters, reads);
        }
    }

    /**
     * Assert pass {@code ii} reads {@code filters[ii]} from {@code source} as {@code reads[ii]} says.
     */
    private static void assertPasses(final String path, final Table source, final String[] filters,
            final Reads[] reads) {
        for (int ii = 0; ii < filters.length; ++ii) {
            final String filter = filters[ii];
            assertPass(filter, dense(() -> ParquetTools.readTable(path).where(filter).select()), source.where(filter),
                    reads[ii]);
        }
    }

    /**
     * Readers on separate threads merge into and read from the same pages' cached values.
     */
    @Test
    public void testConcurrentReadersMatchDense() throws Exception {
        final String path = writeTestTable();
        assertConcurrentReadersMatchDense(dense(() -> ParquetTools.readTable(path).select()),
                ParquetTools.readTable(path)).close();
    }

    /**
     * Readers' own rows are sparse and their shared rows dense, so pages are cached whole and their sparse values
     * dropped while other readers publish to them.
     */
    @Test
    public void testConcurrentDenseAndSparseReadersMatchDense() throws Exception {
        ColumnChunkPageStore.setSparseReadMaxDensity(0.125);
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final long before = ColumnChunkPageStore.sparseFillCount();
        assertConcurrentReadersMatchDense(expected, ParquetTools.readTable(path)).close();
        assertTrue("the readers' own rows should read sparsely", ColumnChunkPageStore.sparseFillCount() > before);
    }

    /**
     * Concurrent readers promote pages while others publish to them. Afterward, every page is cached either whole or
     * with all the rows read, so another pass decodes nothing.
     */
    @Test
    public void testConcurrentReadersPromote() throws Exception {
        ColumnChunkPageStore.setSparseMissesBeforeFullCaching(2);
        final String path = writeTestTable();
        final Table expected = dense(() -> ParquetTools.readTable(path).select());
        final Table actual = ParquetTools.readTable(path);
        try (final SafeCloseable ignored = PageCache.pinTouchedPages();
                final RowSet allRows = assertConcurrentReadersMatchDense(expected, actual)) {
            final long before = ColumnChunkPageStore.sparseFillCount();
            for (final String column : expected.getDefinition().getColumnNamesArray()) {
                assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column), allRows,
                        false);
            }
            assertEquals("a pass after the readers should decode nothing", before,
                    ColumnChunkPageStore.sparseFillCount());
        }
    }

    /**
     * @return The union of the readers' rows, which the caller must close
     */
    private static RowSet assertConcurrentReadersMatchDense(final Table expected, final Table actual)
            throws Exception {
        final int numThreads = 4;
        final List<RowSet> rows = new ArrayList<>();
        for (int tt = 0; tt < numThreads; ++tt) {
            rows.add(expected.where("ii % 17 == " + tt).getRowSet().copy());
        }
        final RowSet allRows = expected.where("ii % 17 < " + numThreads).getRowSet().copy();
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int tt = 0; tt < numThreads; ++tt) {
                final RowSet threadRows = rows.get(tt);
                futures.add(executor.submit(() -> {
                    for (int rep = 0; rep < 3; ++rep) {
                        for (final String column : expected.getDefinition().getColumnNamesArray()) {
                            assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column),
                                    threadRows, false);
                            assertFillsMatch(column, expected.getColumnSource(column), actual.getColumnSource(column),
                                    allRows, false);
                        }
                    }
                }));
            }
            for (final Future<?> future : futures) {
                future.get();
            }
        } catch (final Exception | Error e) {
            allRows.close();
            throw e;
        } finally {
            executor.shutdownNow();
            rows.forEach(RowSet::close);
        }
        return allRows;
    }

    private enum Reads {
        /** Some fills decode. */
        DECODE,
        /** No fill decodes, and some are served from cached values. */
        HIT,
        /** No fill decodes or uses cached values: the pages are cached whole. */
        DENSE,
        /** Only the result is checked. */
        ANY
    }

    private static void assertPass(final String name, final Table expected, final Table filtered, final Reads reads) {
        final long fillsBefore = ColumnChunkPageStore.sparseFillCount();
        final long hitsBefore = ColumnChunkPageStore.sparseHitCount();
        assertTableEquals(expected, filtered.select());
        final long fills = ColumnChunkPageStore.sparseFillCount() - fillsBefore;
        final long hits = ColumnChunkPageStore.sparseHitCount() - hitsBefore;
        final String counts = name + ": " + fills + " sparse fills, " + hits + " hits";
        switch (reads) {
            case DECODE:
                assertTrue(counts, fills > 0);
                break;
            case HIT:
                assertTrue(counts, fills == 0 && hits > 0);
                break;
            case DENSE:
                assertTrue(counts, fills == 0 && hits == 0);
                break;
            case ANY:
                break;
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

    private static <T> T dense(final java.util.function.Supplier<T> read) {
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
