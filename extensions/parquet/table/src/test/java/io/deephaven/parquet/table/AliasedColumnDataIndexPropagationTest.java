//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 20: a {@code select} that aliases an indexed column threw when the table carried a data index on a
 * <em>second</em> column.
 *
 * <pre>
 * java.lang.IllegalStateException: Attempted to add a duplicate index
 *     RemappedDataIndex-1274547241[1] for key columns [Col2]
 *     at DataIndexer.addDataIndex
 *     at QueryTable.propagateDataIndexes
 * </pre>
 *
 * <p>
 * {@code QueryTable.propagateDataIndexes} carries the source table's data indexes onto a {@code select} result by
 * remapping their key column sources. Because {@code select} may produce several aliases of one source column, it
 * builds a <em>list</em> of old-to-new maps -- one per unique combination of alias choices -- and then, for every
 * index, remaps it once per map.
 *
 * <p>
 * But a {@link io.deephaven.engine.table.impl.dataindex.RemappedDataIndex} is determined by the mappings for its
 * <em>own</em> key columns; entries for any other column are ignored. So when one indexed column has two aliases (which
 * is what produces two maps) and a second indexed column has just one (mapped identically in both), the second column's
 * index was remapped twice to the very same thing, and the second {@code addDataIndex} was correctly rejected as a
 * duplicate.
 *
 * <p>
 * The defect is in {@code QueryTable}, but reaching it needs a table whose row set {@code select} preserves rather than
 * flattens -- {@code propagateDataIndexes} returns early otherwise. A parquet table in a single location qualifies; the
 * same query over several locations flattens and so never reached the bug. That is why the reproducer is here rather
 * than in {@code engine-table}, and why the row count is irrelevant but the location count is not.
 *
 * <p>
 * Fuzzer seed {@code 6547813402343559854L}.
 */
public class AliasedColumnDataIndexPropagationTest {

    private static final String ROOT_FILENAME = AliasedColumnDataIndexPropagationTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    /**
     * {@code Part} is the partitioning column, and so is automatically indexed; {@code Key} gets an explicit index.
     * {@code partitions} controls how many locations the table is written to, which is what decides whether
     * {@code select} preserves the row set.
     */
    private static Table source(final int rows, final int partitions) {
        final short[] key = new short[rows];
        final byte[] part = new byte[rows];
        final BigInteger[] payload = new BigInteger[rows];
        for (int ii = 0; ii < rows; ++ii) {
            key[ii] = ii % 4 == 0 ? QueryConstants.NULL_SHORT : (short) (ii % 3);
            part[ii] = (byte) (ii % partitions);
            payload[ii] = BigInteger.valueOf(ii);
        }
        return TableTools.newTable(
                TableTools.shortCol("Key", key),
                TableTools.byteCol("Part", part),
                TableTools.col("Payload", payload));
    }

    /** Write {@code source} key-value partitioned on {@code Part}, with an explicit index on {@code Key}. */
    private Table read(final String name, final int rows, final int partitions, final boolean indexKey) {
        final String dest = Path.of(rootFile.getPath(), name).toString();
        final ParquetInstructions.Builder writeInstructions = ParquetInstructions.builder();
        if (indexKey) {
            writeInstructions.addIndexColumns("Key");
        }
        ParquetTools.writeKeyValuePartitionedTable(
                source(rows, partitions).partitionBy("Part"), dest, writeInstructions.build());
        return ParquetTools.readTable(dest);
    }

    /** The reported case: one location, an index on {@code Key} and on {@code Part}, and an alias of {@code Key}. */
    @Test
    public void aliasOfAnIndexedColumnInASingleLocation() {
        final Table selected = read("reported", 3, 1, true).updateView("KeyAlias = Key").select();
        assertEquals(3, selected.size());
    }

    /** The row count is irrelevant -- it is the location count that decides whether the bug is reachable. */
    @Test
    public void rowCountIsIrrelevant() {
        for (final int rows : new int[] {1, 2, 3, 200}) {
            final Table selected = read("rows" + rows, rows, 1, true).updateView("KeyAlias = Key").select();
            assertEquals("rows=" + rows, rows, selected.size());
        }
    }

    /** Several locations: {@code select} flattens, so this never reached the defect and must stay working. */
    @Test
    public void severalLocationsUnaffected() {
        final Table selected = read("multi", 200, 3, true).updateView("KeyAlias = Key").select();
        assertEquals(200, selected.size());
    }

    /**
     * Aliasing in the {@code select} itself, rather than in an {@code updateView} that the {@code select} then
     * materializes. Verified to <em>not</em> reproduce the defect before the fix, so this is a control rather than a
     * second repro; kept because it is the shape a reader would expect to be equivalent, and it must keep working.
     */
    @Test
    public void aliasIntroducedBySelectItself() {
        final Table selected = read("direct", 3, 1, true).select("Key", "KeyAlias = Key", "Part", "Payload");
        assertEquals(3, selected.size());
        assertEquals(4, selected.getDefinition().getColumnNames().size());
    }

    /** Three aliases of the indexed column, so more than two maps are in play. */
    @Test
    public void threeAliases() {
        final Table selected = read("three", 3, 1, true)
                .updateView("A = Key", "B = Key", "C = Key").select();
        assertEquals(3, selected.size());
    }

    /** Both indexed columns aliased twice, the cross product of alias choices. */
    @Test
    public void bothIndexedColumnsAliased() {
        final Table selected = read("cross", 3, 1, true)
                .updateView("KeyA = Key", "KeyB = Key", "PartA = Part", "PartB = Part").select();
        assertEquals(3, selected.size());
    }

    /**
     * The point of the deduplication is to drop only the <em>redundant</em> remappings. Every alias of an indexed
     * column, and the un-aliased indexed column itself, must still carry an index on the result.
     */
    @Test
    public void everyAliasStillCarriesAnIndex() {
        final Table selected = read("indexes", 3, 1, true)
                .updateView("KeyA = Key", "KeyB = Key").select();
        for (final String columnName : new String[] {"Key", "KeyA", "KeyB", "Part"}) {
            assertTrue("index on " + columnName, DataIndexer.hasDataIndex(selected, columnName));
        }
    }

    /**
     * And the surviving indexes must be usable and correct: a filter answered with an index has to agree with the same
     * filter over a table that has none.
     */
    @Test
    public void indexedFiltersStillAgree() {
        final Table selected = read("filters", 200, 1, true)
                .updateView("KeyA = Key", "KeyB = Key").select();
        // The same data in memory, which carries no data indexes at all, so it answers the filters the plain way.
        final Table unindexed = source(200, 1).updateView("KeyA = Key", "KeyB = Key").select();
        for (final String filter : new String[] {"Key == 1", "KeyA == 1", "KeyB == 1", "Key == null",
                "KeyA == null", "Part == 0", "Key in 1, 2", "KeyA in 1, 2"}) {
            assertEquals(filter, unindexed.where(filter).size(), selected.where(filter).size());
        }
    }

    /** Without a second indexed column there was only ever one map, so this always worked. */
    @Test
    public void aliasWithNoSecondIndexUnaffected() {
        final Table selected = read("noindex", 3, 1, false).updateView("KeyAlias = Key").select();
        assertEquals(3, selected.size());
    }

    /** Without an alias there is no collision and no second map. */
    @Test
    public void noAliasUnaffected() {
        final Table selected = read("noalias", 3, 1, true).select();
        assertEquals(3, selected.size());
    }
}
