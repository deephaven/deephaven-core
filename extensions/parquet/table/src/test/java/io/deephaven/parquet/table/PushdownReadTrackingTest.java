//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.ThreadSafeCounter;
import io.deephaven.base.stats.Value;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.LongFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that file reads performed by a pushdown filter, which run on {@link OperationInitializationThreadPool}
 * worker threads, are attributed to the {@code where()} operation's
 * {@link io.deephaven.engine.table.impl.perf.PerformanceEntry}. The ground truth is the process-wide
 * {@code FileHandle.readSizeBytes} statistic, which counts every byte read through a
 * {@link io.deephaven.engine.util.file.FileHandle} regardless of thread.
 */
public class PushdownReadTrackingTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private File rootFile;

    @Before
    public void setUp() {
        rootFile = new File(System.getProperty("java.io.tmpdir"), "PushdownPerformanceTest-" + System.nanoTime());
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        if (rootFile != null && rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
    }

    /**
     * A single sorted file propagates its sort order to the {@link QueryTable}, so the table-level
     * {@link io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager} performs the pushdown as a binary search.
     * That search runs entirely within a single worker job, which is also the job that runs the completion callback.
     */
    @Test
    public void testTableLevelSortedPushdown() {
        runScenario(1);
    }

    /**
     * Multiple files exercise sorted region pushdown, which fans the binary search out across regions on multiple
     * worker threads.
     */
    @Test
    public void testRegionSortedPushdown() {
        runScenario(4);
    }

    private void runScenario(final int partitionCount) {
        final int size = 200_000;

        final Table source = TableTools.emptyTable(size).update("Value = (int)(ii)");
        final Table sortedAsc = source.sort("Value");

        final String destPath;
        if (partitionCount == 1) {
            final File dest = new File(rootFile, "sortedSingle.parquet");
            ParquetTools.writeTable(sortedAsc, dest.getPath());
            destPath = dest.getPath();
        } else {
            final File destDir = new File(rootFile, "sortedFlat");
            destDir.mkdirs();
            final long splitSize = sortedAsc.size() / partitionCount;
            for (int i = 0; i < partitionCount; i++) {
                final long start = i * splitSize;
                final long end = i == partitionCount - 1 ? sortedAsc.size() : (i + 1) * splitSize;
                final Table slice = SortedColumnsAttribute.withOrderForColumn(
                        sortedAsc.slice(start, end), "Value", SortingOrder.Ascending);
                ParquetTools.writeTable(slice,
                        Path.of(destDir.getPath(), "table_" + String.format("%05d", i) + ".parquet").toString());
            }
            destPath = destDir.getPath();
        }

        // Coalesce up front so that any footer/metadata reads happen before we start measuring.
        final Table fromDisk = ParquetTools.readTable(destPath).coalesce();

        // A multi-threaded operation initializer forces the where() to use the OperationInitializerJobScheduler, so the
        // pushdown work runs on worker threads rather than the calling thread.
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, 8);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);

        final boolean restoreMetadata = QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA;

        final QueryPerformanceRecorder recorder =
                QueryPerformanceRecorder.newQuery("testPushdown", null, QueryPerformanceNugget.DEFAULT_FACTORY);

        final long globalDelta;
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = threadPool::shutdown;
                final SafeCloseable ignored3 =
                        () -> QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA = restoreMetadata) {
            // Disable the row-group metadata action so the filter is satisfied by the region data action (binary
            // search), which actually reads bytes through FileHandle during the pushdown.
            QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA = true;

            recorder.startQuery();
            final long globalBefore = FileHandle.getTotalReadSizeBytes();
            System.out.println("Global before: " + globalBefore);
            final Table result = fromDisk.where("Value >= 150000");
            final long globalAfter = FileHandle.getTotalReadSizeBytes();
            System.out.println("Global after: " + globalAfter);
            globalDelta = globalAfter - globalBefore;
            assertEquals(50_000, result.size());
            recorder.endQuery();
        }

        final List<QueryPerformanceNugget> ops = recorder.getOperationLevelPerformanceData();
        QueryPerformanceNugget whereNugget = null;
        for (final QueryPerformanceNugget nugget : ops) {
            if (nugget.getDescription() != null && nugget.getDescription().startsWith("where(")) {
                whereNugget = nugget;
            }
        }
        assertNotNull("where() operation nugget should be recorded", whereNugget);

        final long whereDataReadBytes = whereNugget.getDataReadBytes();

        final String message = "partitionCount=" + partitionCount
                + ": where() nugget dataReadBytes=" + whereDataReadBytes
                + ", FileHandle.readSizeBytes=" + globalDelta;

        // The pushdown must actually have read data through FileHandle for this test to be meaningful.
        assertTrue(message, globalDelta > 0);

        // The where() operation's PerformanceEntry must account for the bytes read by the pushdown on the worker
        // threads.
        assertEquals(message, globalDelta, whereDataReadBytes);
    }
}
