//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 22: with a {@code _metadata} file, every location read its row group statistics from the
 * <em>first</em> file in the dataset, so filters were pruned against another file's data and rows were silently
 * dropped.
 *
 * <p>
 * {@code ParquetTableLocation.pushdownRowGroupMetadata} indexes {@code parquetMetadata.getBlocks()} with the row group
 * position <em>within the location</em>:
 *
 * <pre>
 * blocks.get(rgIdx).getColumns().get(columnIndex).getStatistics()
 * </pre>
 *
 * <p>
 * That holds only while {@code parquetMetadata} describes this file alone. Read through a {@code _metadata} file it
 * describes the whole dataset -- five blocks for a five-partition table -- and the location owns just a slice of them.
 * Every single-row-group location therefore asked for {@code blocks.get(0)}: the first file's statistics. The class
 * already carries {@code rowGroupIndices} for exactly this translation, and the other two readers of the metadata use
 * it; only this path did not.
 *
 * <p>
 * The consequence is silent wrong results, in the direction that loses data: a row group is dropped whenever another
 * file's min/max or null count happens to exclude the filter. Nothing throws, and the answer is reported as exact.
 *
 * <p>
 * Fuzzer seed {@code 3109160350218645036L}, which reached it through {@code where(isNull(Col1))} on a partitioned
 * {@code LocalTime} column: the null lived in a partition whose statistics were never consulted, while the first file's
 * said {@code numNulls=0}.
 */
public class MetadataFileRowGroupStatisticsTest {

    private static final String ROOT_FILENAME = MetadataFileRowGroupStatisticsTest.class.getName() + "_root";

    private static final int PARTITIONS = 5;
    private static final int ROWS_PER_PARTITION = 20;

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
     * Each partition holds a disjoint band of values, so reading a neighbour's statistics is always detectable rather
     * than accidentally harmless. Nulls appear only in the last partition, which is the shape the fuzzer found.
     */
    private static Table source() {
        final int rows = PARTITIONS * ROWS_PER_PARTITION;
        final byte[] part = new byte[rows];
        final int[] value = new int[rows];
        final LocalTime[] time = new LocalTime[rows];
        final int[] nullable = new int[rows];
        for (int ii = 0; ii < rows; ++ii) {
            final int partition = ii / ROWS_PER_PARTITION;
            final int within = ii % ROWS_PER_PARTITION;
            part[ii] = (byte) partition;
            value[ii] = partition * 1000 + within;
            time[ii] = LocalTime.ofSecondOfDay(partition * 1000L + within);
            // Only the last partition contains nulls, so a first-file null count of zero excludes them all.
            nullable[ii] = partition == PARTITIONS - 1 && within % 5 == 0
                    ? QueryConstants.NULL_INT
                    : partition * 1000 + within;
        }
        return TableTools.newTable(
                TableTools.byteCol("Part", part),
                TableTools.intCol("Value", value),
                TableTools.col("Time", time),
                TableTools.intCol("Nullable", nullable));
    }

    /** Write key-value partitioned with {@code _metadata}, and confirm the metadata file was really produced. */
    private Table readWithMetadataFiles(final String name) {
        final String dest = Path.of(rootFile.getPath(), name).toString();
        ParquetTools.writeKeyValuePartitionedTable(
                source().partitionBy("Part"), dest,
                ParquetInstructions.builder().setGenerateMetadataFiles(true).build());
        assertTrue("_metadata written", new File(dest, "_metadata").exists());
        return ParquetTools.readTable(dest);
    }

    /** The oracle: the same data with no parquet, and so no statistics to prune against. */
    private static Table oracle() {
        return source();
    }

    private static void assertAgrees(final String label, final Table disk, final Filter filter) {
        assertEquals(label, oracle().where(filter).size(), disk.where(filter).size());
    }

    /** The reported case: a null that lives only in a partition whose statistics were never read. */
    @Test
    public void isNullFindsNullsOutsideTheFirstFile() {
        final Table disk = readWithMetadataFiles("isnull");
        assertEquals(4, disk.where(Filter.isNull(ColumnName.of("Nullable"))).size());
        assertAgrees("isNull(Nullable)", disk, Filter.isNull(ColumnName.of("Nullable")));
    }

    /** A value that exists only in a later partition, outside every other partition's min/max band. */
    @Test
    public void equalityMatchesInEveryPartition() {
        final Table disk = readWithMetadataFiles("equality");
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            final int value = partition * 1000 + 5;
            assertEquals("Value == " + value, 1, disk.where("Value == " + value).size());
        }
    }

    /**
     * The same, as a range entirely inside one partition's band. The two bounds are applied as separate {@code where}
     * calls on purpose: written as one string they parse to a single formula {@code ConditionFilter}, which the row
     * group metadata action does not serve, and the test would not reach the code under change.
     */
    @Test
    public void rangeMatchesInEveryPartition() {
        final Table disk = readWithMetadataFiles("range");
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            final int low = partition * 1000 + 5;
            final String label = "Value in [" + low + ", " + (low + 4) + "]";
            assertEquals(label, 5, disk.where("Value >= " + low).where("Value <= " + (low + 4)).size());
        }
    }

    /** A {@code LocalTime} column, the type the fuzzer found this with. */
    @Test
    public void localTimeMatchesInEveryPartition() {
        final Table disk = readWithMetadataFiles("localtime");
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            final LocalTime time = LocalTime.ofSecondOfDay(partition * 1000L + 5);
            final String filter = "Time == '" + time + "'";
            assertEquals(filter, 1, disk.where(filter).size());
        }
    }

    /** Nothing may be lost across the whole table, whatever the filter's shape. */
    @Test
    public void everyRowIsStillReachable() {
        final Table disk = readWithMetadataFiles("all");
        assertEquals(PARTITIONS * ROWS_PER_PARTITION, disk.size());
        assertEquals(PARTITIONS * ROWS_PER_PARTITION, disk.where("Value >= 0").size());
        assertEquals(PARTITIONS * ROWS_PER_PARTITION - 4,
                disk.where(Filter.isNotNull(ColumnName.of("Nullable"))).size());
    }

    /**
     * A slice, as the fuzz case had: pushdown then sees a selection that covers only part of each location, which is
     * the path that turned the wrong statistics into a dropped row.
     */
    @Test
    public void slicedSelectionAgreesWithMemory() {
        final Table disk = readWithMetadataFiles("sliced");
        final long size = disk.size();
        // Two slices: the shape the fuzz case used, and one reaching the last partition, which is where the
        // nulls and the highest value band live. The first alone would not have caught this.
        final Table[] slices = {disk.slice(size / 4, size - size / 4), disk.slice(size / 2, size)};
        for (final Table sliced : slices) {
            final Table slicedOracle = sliced.select();
            for (final Filter filter : new Filter[] {
                    Filter.isNull(ColumnName.of("Nullable")),
                    Filter.isNotNull(ColumnName.of("Nullable")),
                    RawString.of("Value == 4005"),
                    RawString.of("Value == 2005")}) {
                assertEquals(sliced.size() + " " + filter,
                        slicedOracle.where(filter).size(), sliced.where(filter).size());
            }
        }
    }

    /** Without {@code _metadata} the metadata is per file, so this path was always correct and must stay so. */
    @Test
    public void withoutMetadataFilesUnaffected() {
        final String dest = Path.of(rootFile.getPath(), "nometa").toString();
        ParquetTools.writeKeyValuePartitionedTable(
                source().partitionBy("Part"), dest, ParquetInstructions.EMPTY);
        final Table disk = ParquetTools.readTable(dest);
        assertEquals(4, disk.where(Filter.isNull(ColumnName.of("Nullable"))).size());
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            assertEquals(1, disk.where("Value == " + (partition * 1000 + 5)).size());
        }
    }

    /** A filter that matches nothing must still match nothing -- the fix must not simply keep every row group. */
    @Test
    public void absentValuesStillMatchNothing() {
        final Table disk = readWithMetadataFiles("absent");
        assertEquals(0, disk.where("Value == 999").size());
        assertEquals(0, disk.where("Value == -1").size());
        assertEquals(0, disk.where("Value > 100000").size());
    }
}
