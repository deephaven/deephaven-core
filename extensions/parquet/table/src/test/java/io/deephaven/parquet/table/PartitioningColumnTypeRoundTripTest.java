//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 5: a key-value partitioned dataset did not round trip its partitioning column's <em>type</em>.
 *
 * <p>
 * A partitioning column's values live in the directory name, not in any file's parquet schema, so a reader given no
 * {@link io.deephaven.engine.table.TableDefinition} had only the key text to go on and handed it to
 * {@code CsvTools.readCsv} for inference. That is content-directed, so a {@code String} column read back as whichever
 * type its values happened to look like:
 *
 * <table>
 * <tr>
 * <th>written keys</th>
 * <th>read back as</th>
 * </tr>
 * <tr>
 * <td>{@code "aa"}, {@code "bb"}</td>
 * <td>{@code String}</td>
 * </tr>
 * <tr>
 * <td>{@code "a"}, {@code "b"}</td>
 * <td>{@code char}</td>
 * </tr>
 * <tr>
 * <td>{@code "1"}, {@code "2"}</td>
 * <td>{@code int}</td>
 * </tr>
 * <tr>
 * <td>{@code "true"}, {@code "false"}</td>
 * <td>{@code Boolean}</td>
 * </tr>
 * <tr>
 * <td>{@code "1.5"}, {@code "2.5"}</td>
 * <td>{@code double}</td>
 * </tr>
 * </table>
 *
 * <p>
 * Two of those lose data rather than merely changing type: {@code "01"} read back as {@code 1}, and a twenty-digit key
 * read back as a {@code double}, so neither value could reproduce the directory it came from. The type also depended on
 * the data, so one day's keys could infer differently from the next day's.
 *
 * <p>
 * The writer knew the type all along. It now records it in each leaf file's
 * {@link io.deephaven.parquet.table.metadata.TableInfo}, and the read path prefers that over inference.
 *
 * <p>
 * Found while writing {@code EmptyPartitionedTableWriteTest}, where {@code where("Key == `a`")} matched zero of two
 * rows because {@code Key} had become a {@code char}.
 */
public class PartitioningColumnTypeRoundTripTest {

    private static final String ROOT_FILENAME = PartitioningColumnTypeRoundTripTest.class.getName() + "_root";

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

    private String dir(final String name) {
        final File d = Path.of(rootFile.getPath(), name).toFile();
        // noinspection ResultOfMethodCallIgnored
        d.mkdirs();
        return d.getPath();
    }

    /** Write {@code source} partitioned by {@code Key}, read it back with no definition, and return the result. */
    private Table roundTrip(final String name, final Table source, final ParquetInstructions instructions) {
        final String dest = dir(name);
        ParquetTools.writeKeyValuePartitionedTable(source.partitionBy("Key"), dest, instructions);
        return ParquetTools.readTable(dest);
    }

    private Table roundTrip(final String name, final Table source) {
        return roundTrip(name, source, ParquetInstructions.EMPTY);
    }

    private static Table stringKeyed(final String... keys) {
        final int[] values = new int[keys.length];
        for (int ii = 0; ii < keys.length; ++ii) {
            values[ii] = ii;
        }
        return TableTools.newTable(TableTools.stringCol("Key", keys), TableTools.intCol("Value", values));
    }

    private static void assertKeyType(final Class<?> expected, final Table table) {
        assertEquals(expected, table.getDefinition().getColumn("Key").getDataType());
    }

    /**
     * The heart of the finding: every one of these was written as a {@code String} column, and every one must read back
     * as a {@code String} column regardless of what its values look like.
     */
    @Test
    public void stringKeysRoundTripAsStringWhateverTheyLookLike() {
        assertKeyType(String.class, roundTrip("multichar", stringKeyed("aa", "bb")));
        assertKeyType(String.class, roundTrip("singlechar", stringKeyed("a", "b")));
        assertKeyType(String.class, roundTrip("digits", stringKeyed("1", "2")));
        assertKeyType(String.class, roundTrip("bools", stringKeyed("true", "false")));
        assertKeyType(String.class, roundTrip("decimals", stringKeyed("1.5", "2.5")));
        assertKeyType(String.class, roundTrip("dates", stringKeyed("2024-01-01", "2024-01-02")));
    }

    /** The two cases that lost data, not just type. */
    @Test
    public void keysThatInferenceCorrupted() {
        final Table leadingZero = roundTrip("leadingzero", stringKeyed("01", "02"));
        assertKeyType(String.class, leadingZero);
        assertEquals(List.of("01", "02"), keysOf(leadingZero));

        final Table hugeInteger = roundTrip("hugeint", stringKeyed("99999999999999999999", "1"));
        assertKeyType(String.class, hugeInteger);
        assertEquals(List.of("1", "99999999999999999999"), keysOf(hugeInteger));
    }

    /** The single-character case that gave the finding away, filtered the way it originally failed. */
    @Test
    public void singleCharacterKeyIsFilterableAsAString() {
        final Table disk = roundTrip("filter", TableTools.newTable(
                TableTools.stringCol("Key", "a", "a", "b"),
                TableTools.intCol("Value", 1, 2, 3)));
        assertKeyType(String.class, disk);
        assertEquals(3, disk.size());
        assertEquals(2, disk.where("Key == `a`").size());
        assertEquals(2, disk.select().where("Key == `a`").size());
    }

    /**
     * Recording the type must preserve non-String types too, not coerce everything to String. Asserted against each
     * source's own {@code Key} type, which is the round-trip property itself. Note that a Deephaven boolean column is
     * the boxed {@code Boolean}, so that is what comes back.
     */
    @Test
    public void nonStringKeyTypesArePreserved() {
        assertKeyRoundTrips("int_key", TableTools.newTable(
                TableTools.intCol("Key", 1, 2), TableTools.intCol("Value", 10, 20)));
        assertKeyRoundTrips("long_key", TableTools.newTable(
                TableTools.longCol("Key", 1L, 2L), TableTools.intCol("Value", 10, 20)));
        assertKeyRoundTrips("char_key", TableTools.newTable(
                TableTools.charCol("Key", 'a', 'b'), TableTools.intCol("Value", 10, 20)));
        assertKeyRoundTrips("bool_key", TableTools.newTable(
                TableTools.booleanCol("Key", true, false), TableTools.intCol("Value", 10, 20)));
        assertKeyRoundTrips("double_key", TableTools.newTable(
                TableTools.doubleCol("Key", 1.5, 2.5), TableTools.intCol("Value", 10, 20)));
        assertKeyRoundTrips("date_key", TableTools.newTable(
                TableTools.col("Key", LocalDate.parse("2024-01-01"), LocalDate.parse("2024-01-02")),
                TableTools.intCol("Value", 10, 20)));
    }

    private void assertKeyRoundTrips(final String name, final Table source) {
        final Class<?> sourceType = source.getDefinition().getColumn("Key").getDataType();
        final Table disk = roundTrip(name, source);
        assertEquals(name, sourceType, disk.getDefinition().getColumn("Key").getDataType());
        assertEquals(name, source.size(), disk.size());
    }

    /** Values, not just types, must survive for non-String keys. */
    @Test
    public void nonStringKeyValuesArePreserved() {
        final Table disk = roundTrip("int_values", TableTools.newTable(
                TableTools.intCol("Key", 7, 7, 9),
                TableTools.intCol("Value", 1, 2, 3)));
        assertEquals(3, disk.size());
        assertEquals(2, disk.where("Key == 7").size());
    }

    /** Metadata files were already correct, via _common_metadata; they must stay so. */
    @Test
    public void metadataFilesStillRoundTrip() {
        assertKeyType(String.class, roundTrip("meta", stringKeyed("a", "b"),
                new ParquetInstructions.Builder().setGenerateMetadataFiles(true).build()));
    }

    /** An explicitly supplied definition was already authoritative and must remain so. */
    @Test
    public void explicitDefinitionStillWins() {
        final String dest = dir("explicit");
        ParquetTools.writeKeyValuePartitionedTable(stringKeyed("1", "2").partitionBy("Key"), dest,
                ParquetInstructions.EMPTY);
        final Table disk = ParquetTools.readTable(dest, ParquetInstructions.EMPTY.withTableDefinition(
                io.deephaven.engine.table.TableDefinition.of(
                        ColumnDefinition.ofString("Key").withPartitioning(),
                        ColumnDefinition.ofInt("Value"))));
        assertKeyType(String.class, disk);
        assertEquals(2, disk.size());
    }

    /**
     * A dataset Deephaven did not write -- or one written before the type was recorded -- has nothing to read the type
     * from, so inference remains the documented fallback. Built here by writing plain, unpartitioned parquet files into
     * partition-shaped directories, which is what such a dataset looks like.
     */
    @Test
    public void datasetWithoutRecordedTypesStillInfers() {
        final String dest = dir("foreign");
        for (final String key : new String[] {"a", "b"}) {
            final File partitionDir = Path.of(dest, "Key=" + key).toFile();
            // noinspection ResultOfMethodCallIgnored
            partitionDir.mkdirs();
            ParquetTools.writeTable(TableTools.newTable(TableTools.intCol("Value", 1)),
                    Path.of(partitionDir.getPath(), "data.parquet").toString());
        }
        final Table disk = ParquetTools.readTable(dest);
        assertKeyType(char.class, disk);
        assertEquals(2, disk.size());
    }

    private static List<String> keysOf(final Table table) {
        return table.selectDistinct("Key").sort("Key").objectColumnIterator("Key")
                .stream().map(String::valueOf).collect(java.util.stream.Collectors.toList());
    }
}
