//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.SortedColumnsExclusion;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Parquet file metadata -- row-group statistics, sortedness and data indexes -- must be looked up in the key space it
 * was written in: global row-group indices for {@code _metadata} layouts, and parquet column names rather than
 * Deephaven column names whenever renames are in play. Each test compares a pushdown-capable read against an in-memory
 * {@code select()} of the same data, which runs no pushdown and consults no file metadata.
 */
public class ParquetMetadataKeyingTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private static void assertFilterMatchesOracle(final Table fromDisk, final String filter) {
        // select() materializes into memory, so the expected side runs no pushdown at all
        final Table oracle = fromDisk.select();
        assertTableEquals(oracle.where(filter).coalesce(), fromDisk.where(filter).coalesce());
    }

    private static Optional<SortingOrder> sortOrder(final Table fromDisk, final String columnName) {
        return SortedColumnsAttribute.getOrderForColumn(fromDisk.coalesce(), columnName);
    }

    private String path(final String name) {
        return new File(tempDir.getRoot(), name).getPath();
    }

    // region _metadata layouts

    private static final int PARTITION_COUNT = 3;
    private static final int ROWS_PER_PARTITION = 2_000;

    /** Writes three partitions with disjoint id ranges: 0-1999, 2000-3999 and 4000-5999. */
    private String writePartitionedDataset(final String name, final boolean generateMetadataFiles) {
        final String dest = path(name);
        final PartitionedTable partitioned = TableTools.emptyTable(PARTITION_COUNT * ROWS_PER_PARTITION)
                .update("part = (int) (ii / " + ROWS_PER_PARTITION + ")", "id = ii")
                .partitionBy("part");
        ParquetTools.writeKeyValuePartitionedTable(partitioned, dest, ParquetInstructions.builder()
                .setGenerateMetadataFiles(generateMetadataFiles)
                .build());
        return dest;
    }

    /**
     * Under a {@code _metadata} layout every file's metadata carries the combined block list of all files, so the
     * statistics for a location's row group must be found by its global index; the non-first files must not be tested
     * against the first file's statistics.
     */
    @Test
    public void metadataLayoutRowGroupStatistics() {
        final String dest = writePartitionedDataset("withMetadata", true);
        assertTrue(new File(dest, "_metadata").exists());

        final Table fromDisk = ParquetTools.readTable(dest);
        assertEquals(1_000, fromDisk.where("id >= 5000").size());
        assertFilterMatchesOracle(fromDisk, "id >= 5000");
        assertFilterMatchesOracle(fromDisk, "id >= 2500 && id < 3500");
        assertFilterMatchesOracle(fromDisk, "id = 4321");
    }

    @Test
    public void directoryLayoutRowGroupStatistics() {
        final Table fromDisk = ParquetTools.readTable(writePartitionedDataset("withoutMetadata", false));
        assertEquals(1_000, fromDisk.where("id >= 5000").size());
        assertFilterMatchesOracle(fromDisk, "id >= 5000");
    }

    // endregion _metadata layouts

    // region dictionary pushdown

    /** Two dictionary-encoded string columns with disjoint value sets. */
    private String writeDictionaryTable() {
        final String dest = path("dictionary.parquet");
        ParquetTools.writeTable(TableTools.emptyTable(500).update("B = `b` + (ii % 5)", "C = `c` + (ii % 5)"), dest);
        return dest;
    }

    @Test
    public void dictionaryWithoutRenames() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable());
        assertEquals(100, fromDisk.where("B = `b1`").size());
        assertFilterMatchesOracle(fromDisk, "B = `b1`");
    }

    /**
     * Parquet {@code B} is read as Deephaven {@code A} and parquet {@code C} as Deephaven {@code B}. A filter on
     * {@code A} must consult the dictionary of parquet {@code B}, not that of the column whose Deephaven name is
     * {@code B}.
     */
    @Test
    public void dictionaryWithRenameSwap() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .addColumnNameMapping("C", "B")
                .build());
        assertEquals(100, fromDisk.select().where("A = `b1`").size());
        assertFilterMatchesOracle(fromDisk, "A = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
        assertFilterMatchesOracle(fromDisk, "A in `b1`, `b3`");
    }

    @Test
    public void dictionaryWithRename() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "Sym")
                .build());
        assertFilterMatchesOracle(fromDisk, "Sym = `b1`");
    }

    /**
     * With a column resolver there is no fallback that treats a Deephaven name as a parquet name, so a parquet name
     * passed where a Deephaven name is expected resolves to a nonexistent column.
     */
    @Test
    public void dictionaryWithColumnResolver() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofString("Sym")))
                .setColumnResolverFactory((tableKey, tableLocationKey) -> ParquetColumnResolverMap.builder()
                        .putMap("Sym", List.of("B"))
                        .build())
                .build());
        assertEquals(100, fromDisk.select().where("Sym = `b1`").size());
        assertFilterMatchesOracle(fromDisk, "Sym = `b1`");
    }

    /**
     * The first row group has too many distinct values for a dictionary and falls back to plain encoding; the second is
     * dictionary-encoded. Whether a column has a usable dictionary is a question for every row group, not the first.
     */
    @Test
    public void dictionaryInLaterRowGroupOnly() {
        final String dest = path("laterDictionary.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(2_000).update("S = ii < 1_000 ? `u` + ii : `k` + (ii % 5)"),
                dest,
                ParquetInstructions.builder()
                        .setRowGroupInfo(RowGroupInfo.maxRows(1_000))
                        .setMaximumDictionaryKeys(100)
                        .build());
        final ParquetMetadata metadata =
                new ParquetTableLocationKey(new File(dest).toURI(), 0, null, ParquetInstructions.EMPTY).getMetadata();
        assertEquals(2, metadata.getBlocks().size());
        assertFalse(metadata.getBlocks().get(0).getColumns().get(0).hasDictionaryPage());
        assertTrue(metadata.getBlocks().get(1).getColumns().get(0).hasDictionaryPage());

        final Table fromDisk = ParquetTools.readTable(dest);
        assertEquals(200, fromDisk.where("S = `k1`").size());
        assertFilterMatchesOracle(fromDisk, "S = `k1`");
        assertFilterMatchesOracle(fromDisk, "S in `k1`, `u5`");
        assertFilterMatchesOracle(fromDisk, "S = `u999`");
    }

    // endregion dictionary pushdown

    // region sortedness

    private static final int SORTED_TABLE_SIZE = 1_000;

    /** {@code X} is ascending, so the writer records it as sorted; {@code Y} is a permutation of the same range. */
    private static Table sortedSource() {
        return TableTools.emptyTable(SORTED_TABLE_SIZE)
                .update("X = (int) ii", "Y = (int) ((ii * 613) % " + SORTED_TABLE_SIZE + ")")
                .sort("X");
    }

    private String writeSortedTable(final ParquetInstructions writeInstructions) {
        final String dest = path("sorted.parquet");
        ParquetTools.writeTable(sortedSource(), dest, writeInstructions);
        return dest;
    }

    @Test
    public void sortednessWithoutRenames() {
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(ParquetInstructions.EMPTY));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "X"));
        assertEquals(500, fromDisk.where("X >= 500").size());
        assertFilterMatchesOracle(fromDisk, "X >= 500");
    }

    /**
     * Parquet {@code Y} (unsorted) is read as Deephaven {@code X} and parquet {@code X} (sorted) as Deephaven
     * {@code Z}. The file's sortedness describes parquet {@code X}, so it must land on Deephaven {@code Z}.
     */
    @Test
    public void sortednessWithReadRenameCollision() {
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(ParquetInstructions.EMPTY),
                ParquetInstructions.builder()
                        .addColumnNameMapping("Y", "X")
                        .addColumnNameMapping("X", "Z")
                        .build());
        assertEquals(Optional.empty(), sortOrder(fromDisk, "X"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "Z"));
        assertFilterMatchesOracle(fromDisk, "X >= 500");
        assertFilterMatchesOracle(fromDisk, "Z >= 500");
    }

    /** A sorted column renamed on write keeps its sortedness under its parquet name. */
    @Test
    public void sortednessWithWriteRename() {
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(ParquetInstructions.builder()
                .addColumnNameMapping("P", "X")
                .build()));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "P"));
        assertFilterMatchesOracle(fromDisk, "P >= 500");
    }

    /**
     * Deephaven {@code X} (sorted) is written as parquet {@code Y} and Deephaven {@code Y} (unsorted) as parquet
     * {@code X}. Read back without renames, the sortedness belongs to {@code Y}.
     */
    @Test
    public void sortednessWithWriteRenameSwap() {
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(ParquetInstructions.builder()
                .addColumnNameMapping("Y", "X")
                .addColumnNameMapping("X", "Y")
                .build()));
        assertEquals(Optional.empty(), sortOrder(fromDisk, "X"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "Y"));
        assertFilterMatchesOracle(fromDisk, "X >= 500");
        assertFilterMatchesOracle(fromDisk, "Y >= 500");
    }

    // endregion sortedness

    // region sortedness exclusions

    private String writeSortedBy(final String sortColumn) {
        final String dest = path("sortedBy" + sortColumn + ".parquet");
        ParquetTools.writeTable(TableTools.emptyTable(100)
                .update("S = String.format(`s%03d`, ii)", "D = (double) ii", "I = (int) ii")
                .sort(sortColumn), dest);
        return dest;
    }

    private static ParquetInstructions excluding(final SortedColumnsExclusion... exclusions) {
        return ParquetInstructions.builder().addSortedColumnsExclusions(exclusions).build();
    }

    @Test
    public void sortednessExclusions() {
        final String sortedByS = writeSortedBy("S");
        final String sortedByD = writeSortedBy("D");
        final String sortedByI = writeSortedBy("I");

        // No exclusions: every declared sort is kept
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(ParquetTools.readTable(sortedByS), "S"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(ParquetTools.readTable(sortedByD), "D"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(ParquetTools.readTable(sortedByI), "I"));

        final ParquetInstructions noStrings = excluding(SortedColumnsExclusion.STRING);
        assertEquals(Optional.empty(), sortOrder(ParquetTools.readTable(sortedByS, noStrings), "S"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(ParquetTools.readTable(sortedByD, noStrings), "D"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(ParquetTools.readTable(sortedByI, noStrings), "I"));

        final ParquetInstructions noStringsOrFloats =
                excluding(SortedColumnsExclusion.STRING, SortedColumnsExclusion.FLOATING_POINT);
        assertEquals(Optional.empty(), sortOrder(ParquetTools.readTable(sortedByS, noStringsOrFloats), "S"));
        assertEquals(Optional.empty(), sortOrder(ParquetTools.readTable(sortedByD, noStringsOrFloats), "D"));
        assertEquals(Optional.of(SortingOrder.Ascending),
                sortOrder(ParquetTools.readTable(sortedByI, noStringsOrFloats), "I"));

        final ParquetInstructions noSorts = excluding(SortedColumnsExclusion.ALL_COLUMNS);
        assertEquals(Optional.empty(), sortOrder(ParquetTools.readTable(sortedByI, noSorts), "I"));

        // Exclusions survive copying the instructions
        assertEquals(Optional.empty(),
                sortOrder(ParquetTools.readTable(sortedByS, new ParquetInstructions.Builder(noStrings).build()), "S"));
    }

    // endregion sortedness exclusions

    // region data indexes

    /** Two string columns with disjoint value sets, with a data index on {@code B}. */
    private String writeIndexedTable() {
        final String dest = path("indexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(500).update("B = `b` + (ii % 5)", "C = `c` + (ii % 7)"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B").build());
        return dest;
    }

    @Test
    public void dataIndexWithoutRenames() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "B"));
        assertFilterMatchesOracle(fromDisk, "B = `b1`");
    }

    /** The index on parquet {@code B} is an index on Deephaven {@code A}. */
    @Test
    public void dataIndexWithReadRename() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .build());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "A"));
        assertFilterMatchesOracle(fromDisk, "A = `b1`");
    }

    /**
     * Parquet {@code B} is read as Deephaven {@code A} and parquet {@code C} as Deephaven {@code B}. The index on
     * parquet {@code B} belongs to Deephaven {@code A}; Deephaven {@code B} has none.
     */
    @Test
    public void dataIndexWithReadRenameSwap() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .addColumnNameMapping("C", "B")
                .build());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "A"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "B"));
        assertFilterMatchesOracle(fromDisk, "A = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
    }

    // endregion data indexes

    // region column resolvers

    /** Sorted by {@code B}, with a data index on {@code B}. */
    private String writeSortedIndexedTable() {
        final String dest = path("sortedIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = `b` + (int) (ii / 10)", "C = (int) ii").sort("B"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B").build());
        return dest;
    }

    private static ParquetInstructions resolving(final TableDefinition definition, final String... dhToParquet) {
        final ParquetColumnResolverMap.Builder resolverMap = ParquetColumnResolverMap.builder();
        for (int ii = 0; ii < dhToParquet.length; ii += 2) {
            resolverMap.putMap(dhToParquet[ii], List.of(dhToParquet[ii + 1]));
        }
        final ParquetColumnResolverMap resolver = resolverMap.build();
        return ParquetInstructions.builder()
                .setTableDefinition(definition)
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .build();
    }

    /**
     * With a column resolver, the file's sort columns and data index key columns, which name parquet columns, are
     * translated through the resolver to the Deephaven columns reading them.
     */
    @Test
    public void metadataWithColumnResolver() {
        final Table fromDisk = ParquetTools.readTable(writeSortedIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofString("Sym"), ColumnDefinition.ofInt("C")),
                "Sym", "B", "C", "C"));
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "Sym"));
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "Sym"));
        assertEquals(10, DataIndexer.getDataIndex(fromDisk, "Sym").table().size());
        assertFilterMatchesOracle(fromDisk, "Sym = `b3`");
        assertFilterMatchesOracle(fromDisk, "Sym >= `b5`");
    }

    /**
     * When two Deephaven columns read the same parquet column, neither can be identified as the one the metadata
     * describes, so the sortedness and the data index are dropped rather than guessed.
     */
    @Test
    public void metadataWithColumnResolverReadingOneColumnTwice() {
        final Table fromDisk = ParquetTools.readTable(writeSortedIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofString("Sym"), ColumnDefinition.ofString("Sym2")),
                "Sym", "B", "Sym2", "B"));
        assertEquals(Optional.empty(), sortOrder(fromDisk, "Sym"));
        assertEquals(Optional.empty(), sortOrder(fromDisk, "Sym2"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "Sym"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "Sym2"));
        assertFilterMatchesOracle(fromDisk, "Sym = `b3`");
    }

    /**
     * When no Deephaven column reads the sorted and indexed parquet column, the metadata describing it is dropped.
     */
    @Test
    public void metadataForColumnNotRead() {
        final Table fromDisk = ParquetTools.readTable(writeSortedIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofInt("C")),
                "C", "C"));
        assertEquals(Optional.empty(), sortOrder(fromDisk, "C"));
        assertTrue(SortedColumnsAttribute.getSortedColumns(fromDisk.coalesce()).isEmpty());
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "C"));
        assertFilterMatchesOracle(fromDisk, "C >= 50");
    }

    /**
     * The location's data index methods take Deephaven column names; a name that reads no column of the file has no
     * index.
     */
    @Test
    public void dataIndexForUntranslatableColumn() {
        final String dest = writeSortedIndexedTable();
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .build();
        final ParquetTableLocation location = new ParquetTableLocation(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationKey(new File(dest).toURI(), 0, null, instructions),
                instructions);
        assertTrue(location.hasDataIndex("A"));
        // Deephaven B does not exist: parquet B is read as A
        assertFalse(location.hasDataIndex("B"));
        assertThrows(TableDataException.class, () -> location.loadDataIndex("B"));
        assertEquals(List.of(List.of("A")),
                location.getDataIndexColumns().stream().map(List::of).collect(Collectors.toList()));
    }

    // endregion column resolvers
}
