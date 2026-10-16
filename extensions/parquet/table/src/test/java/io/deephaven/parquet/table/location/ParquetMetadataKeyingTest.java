//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTableWriter;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.codec.BigDecimalCodec;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import io.deephaven.util.codec.UTF8StringAsByteArrayCodec;
import io.deephaven.vector.IntVector;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Parquet file metadata -- row-group statistics, dictionaries, sortedness, data indexes and the footer's column type
 * entries, such as codecs -- must be looked up in the key space it was written in: global row-group indices for
 * {@code _metadata} layouts, and parquet column names rather than Deephaven column names whenever renames or a column
 * resolver are in play. Most tests compare a pushdown-capable read against an in-memory {@code select()} of the same
 * data, which runs no pushdown and consults no file metadata; the rest check the metadata a read finds, or the values
 * and types it reads, directly.
 */
public class ParquetMetadataKeyingTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private static void assertFilterMatchesOracle(final Table fromDisk, final String filter) {
        // select() materializes into memory, so the expected side runs no pushdown at all. It keeps the RowSet, and
        // with it any data index already loaded from the file, so the oracle gets a fresh RowSet without one.
        final Table materialized = fromDisk.select();
        final Table oracle =
                new QueryTable(materialized.getRowSet().copy().toTracking(), materialized.getColumnSourceMap());
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
    public void dictionaryWithReadRenameShift() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .addColumnNameMapping("C", "B")
                .build());
        assertEquals(100, fromDisk.select().where("A = `b1`").size());
        assertFilterMatchesOracle(fromDisk, "A = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
        assertFilterMatchesOracle(fromDisk, "A in `b1`, `b3`");
    }

    /**
     * Parquet {@code B} is read as Deephaven {@code C} and parquet {@code C} as Deephaven {@code B}. A filter on
     * {@code C} must consult the dictionary of parquet {@code B}, not that of parquet {@code C}.
     */
    @Test
    public void dictionaryWithReadRenameSwap() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "C")
                .addColumnNameMapping("C", "B")
                .build());
        assertEquals(100, fromDisk.select().where("C = `b1`").size());
        assertFilterMatchesOracle(fromDisk, "C = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
        assertFilterMatchesOracle(fromDisk, "C in `b1`, `b3`");
    }

    @Test
    public void dictionaryWithReadRename() {
        final Table fromDisk = ParquetTools.readTable(writeDictionaryTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "Sym")
                .build());
        assertFilterMatchesOracle(fromDisk, "Sym = `b1`");
    }

    /**
     * With a column resolver there is no fallback that treats a Deephaven name as a parquet name, so asking the
     * location for the filter's column by its parquet name, {@code B}, rather than its Deephaven name, {@code Sym},
     * would find no column at all.
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

    /**
     * The metadata records a sort column by its parquet name, and every reader validates it as a Deephaven column name,
     * so a sorted column renamed on write to a parquet name that is not one is written without its sortedness.
     */
    @Test
    public void sortednessWithWriteRenameToNonColumnName() {
        final ParquetInstructions renamed = ParquetInstructions.builder()
                .addColumnNameMapping("x col", "X")
                .build();
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(renamed), renamed);
        assertEquals(Optional.empty(), sortOrder(fromDisk, "X"));
        assertFilterMatchesOracle(fromDisk, "X >= 500");
    }

    /**
     * Parquet {@code Y} (unsorted) is read as Deephaven {@code X}, which hides parquet {@code X} (sorted): no column
     * reads it. The file's sortedness describes parquet {@code X}, so no column is sorted.
     */
    @Test
    public void sortednessOfHiddenColumn() {
        final Table fromDisk = ParquetTools.readTable(writeSortedTable(ParquetInstructions.EMPTY),
                ParquetInstructions.builder()
                        .addColumnNameMapping("Y", "X")
                        .setTableDefinition(TableDefinition.of(ColumnDefinition.ofInt("X")))
                        .build());
        assertEquals(Optional.empty(), sortOrder(fromDisk, "X"));
        assertFilterMatchesOracle(fromDisk, "X >= 500");
    }

    /**
     * Deephaven {@code X}, which is sorted, maps to parquet {@code Y} but is not written; Deephaven {@code Y}, which is
     * not sorted, is written as parquet {@code Y}. The file must not record parquet {@code Y} as sorted.
     */
    @Test
    public void sortednessOfColumnNotWritten() {
        final String dest = path("projected.parquet");
        ParquetTools.writeTable(sortedSource(), dest, ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofInt("Y")))
                .addColumnNameMapping("Y", "X")
                .build());
        final Table fromDisk = ParquetTools.readTable(dest);
        assertEquals(Optional.empty(), sortOrder(fromDisk, "Y"));
        assertFilterMatchesOracle(fromDisk, "Y >= 500");
    }

    // endregion sortedness

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
    public void dataIndexWithReadRenameShift() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "A")
                .addColumnNameMapping("C", "B")
                .build());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "A"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "B"));
        assertFilterMatchesOracle(fromDisk, "A = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
    }

    /**
     * Parquet {@code B} is read as Deephaven {@code C} and parquet {@code C} as Deephaven {@code B}. The index on
     * parquet {@code B} belongs to Deephaven {@code C}, and its keys are parquet {@code B}'s values; Deephaven
     * {@code B} has none.
     */
    @Test
    public void dataIndexWithReadRenameSwap() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "C")
                .addColumnNameMapping("C", "B")
                .build());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "C"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "B"));
        assertTableEquals(TableTools.emptyTable(5).update("C = `b` + ii"),
                DataIndexer.getDataIndex(fromDisk, "C").table().view("C").sort("C"));
        assertFilterMatchesOracle(fromDisk, "C = `b1`");
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
    }

    /** An int column {@code B}, with a data index on it. */
    private String writeIntIndexedTable() {
        final String dest = path("intIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = (int) (ii % 5)", "C = (int) ii"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B").build());
        return dest;
    }

    /** Two string columns, with a composite data index on {@code (B, C)}. */
    private String writeCompositeIndexedTable() {
        final String dest = path("compositeIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = `b` + (ii % 5)", "C = `c` + (ii % 7)"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B", "C").build());
        return dest;
    }

    /** A BigInteger column, written as a decimal with codec metadata, with a data index on it. */
    private String writeBigIntegerIndexedTable() {
        final String dest = path("bigIntegerIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = java.math.BigInteger.valueOf(ii % 5)", "C = (int) ii"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B").build());
        return dest;
    }

    /**
     * The footer records a column's type metadata, such as the codec that reads a decimal back as a BigInteger, by
     * parquet name, so a read that renames the column finds it under that name.
     */
    @Test
    public void codecColumnWithReadRename() {
        final Table fromDisk = ParquetTools.readTable(writeBigIntegerIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "Sym")
                .build());
        assertEquals(BigInteger.class, fromDisk.getDefinition().getColumn("Sym").getDataType());
        assertEquals(BigInteger.class, indexKeyType(fromDisk, "Sym"));
        assertTableEquals(TableTools.emptyTable(100).update("Sym = java.math.BigInteger.valueOf(ii % 5)"),
                fromDisk.view("Sym"));
    }

    /** The footer's vector type entry is recorded by parquet name too, so a renamed vector column is read as one. */
    @Test
    public void vectorColumnWithReadRename() {
        final String dest = path("vector.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(10).update("V = new io.deephaven.vector.IntVectorDirect((int) ii)"),
                dest);
        final Table fromDisk = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .addColumnNameMapping("V", "W")
                .build());
        assertEquals(IntVector.class, fromDisk.getDefinition().getColumn("W").getDataType());
    }

    /** A read may declare a parquet INT32 as a long; the index is read the same way, so its keys are longs too. */
    @Test
    public void dataIndexWithReadRenameWidening() {
        final Table fromDisk = ParquetTools.readTable(writeIntIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("B", "Sym")
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofLong("Sym"), ColumnDefinition.ofInt("C")))
                .build());
        assertEquals(long.class, indexKeyType(fromDisk, "Sym"));
        assertFilterMatchesOracle(fromDisk, "Sym = 3");
    }

    /** {@link #writeIndexedTable()}, with {@code B} written as parquet {@code b col}, not a valid Deephaven name. */
    private String writeIndexedTableUnderNonColumnName() {
        final String dest = path("indexedNonColumnName.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(500).update("B = `b` + (ii % 5)", "C = `c` + (ii % 7)"),
                dest,
                ParquetInstructions.builder().addColumnNameMapping("b col", "B").addIndexColumns("B").build());
        return dest;
    }

    /**
     * An index on a column written under a parquet name that is not a valid Deephaven column name is written, and is
     * found whether the read maps that name back or legalizes it.
     */
    @Test
    public void dataIndexWithWriteRenameToNonColumnName() {
        final String dest = writeIndexedTableUnderNonColumnName();
        final Table mapped = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .addColumnNameMapping("b col", "B")
                .build());
        assertTrue(DataIndexer.hasDataIndex(mapped, "B"));
        assertFilterMatchesOracle(mapped, "B = `b1`");

        final Table legalized = ParquetTools.readTable(dest);
        assertTrue(DataIndexer.hasDataIndex(legalized, "b_col"));
        assertFilterMatchesOracle(legalized, "b_col = `b1`");
    }

    /**
     * Parquet {@code C} is read as Deephaven {@code B}, which hides parquet {@code B}, the indexed column: no column
     * reads it, so no column has its index.
     */
    @Test
    public void dataIndexOfHiddenColumn() {
        final Table fromDisk = ParquetTools.readTable(writeIndexedTable(), ParquetInstructions.builder()
                .addColumnNameMapping("C", "B")
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofString("B")))
                .build());
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "B"));
        assertFilterMatchesOracle(fromDisk, "B = `c1`");
    }

    /**
     * A vector column is a parquet LIST, read through the path to its only leaf rather than by its name alone; it is
     * still one whole column, so an index on it is found, with or without a column resolver.
     */
    @Test
    public void dataIndexOnVectorColumn() {
        final String dest = path("vectorIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("V = new io.deephaven.vector.IntVectorDirect((int) (ii % 3), 7)",
                        "C = (int) ii"),
                dest,
                ParquetInstructions.builder().addIndexColumns("V").build());

        final Table fromDisk = ParquetTools.readTable(dest);
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "V"));
        assertEquals(3, DataIndexer.getDataIndex(fromDisk, "V").table().size());

        final ParquetColumnResolverMap resolver = ParquetColumnResolverMap.builder()
                .putMap("W", List.of("V", "list", "element"))
                .putMap("C", List.of("C"))
                .build();
        final TableDefinition resolvedDefinition = TableDefinition.of(
                ColumnDefinition.fromGenericType("W", IntVector.class, int.class),
                ColumnDefinition.ofInt("C"));
        final Table resolved = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .setTableDefinition(resolvedDefinition)
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .build());
        assertTrue(DataIndexer.hasDataIndex(resolved, "W"));
        assertEquals(3, DataIndexer.getDataIndex(resolved, "W").table().size());
        assertEquals(IntVector.class, indexKeyType(resolved, "W"));

        // A name mapping alongside the resolver is ignored, for the index read as for the data, so the key is still
        // read as the vector the column reads
        final Table mapped = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .setTableDefinition(resolvedDefinition)
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .addColumnNameMapping("V", "W")
                .build());
        assertEquals(3, DataIndexer.getDataIndex(mapped, "W").table().size());
        assertEquals(IntVector.class, indexKeyType(mapped, "W"));
    }

    private static Class<?> indexKeyType(final Table table, final String keyColumnName) {
        return DataIndexer.getDataIndex(table, keyColumnName).table().getDefinition().getColumn(keyColumnName)
                .getDataType();
    }

    private static String legacyGroupingFile() {
        return ParquetMetadataKeyingTest.class.getResource("/ParquetDataWithLegacyGroupingInfo.parquet").getFile();
    }

    /** A legacy grouping sidecar stores its key as {@code dh_key}, which is read as the indexed column is: widened. */
    @Test
    public void legacyGroupingWithReadRenameWidening() {
        final Table fromDisk = ParquetTools.readTable(legacyGroupingFile(), ParquetInstructions.builder()
                .addColumnNameMapping("gcol", "Sym")
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofLong("Sym")))
                .build());
        assertEquals(long.class, indexKeyType(fromDisk, "Sym"));
        assertEquals(500, DataIndexer.getDataIndex(fromDisk, "Sym").table().size());
        assertFilterMatchesOracle(fromDisk, "Sym = 3");
    }

    /** The sidecar's position columns keep their own types when the indexed column is named like one of them. */
    @Test
    public void legacyGroupingForColumnNamedLikePosition() {
        final Table fromDisk = ParquetTools.readTable(legacyGroupingFile(), ParquetInstructions.builder()
                .addColumnNameMapping("gcol", "dh_begin_pos")
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofInt("dh_begin_pos")))
                .build());
        assertEquals(500, DataIndexer.getDataIndex(fromDisk, "dh_begin_pos").table().size());
        assertFilterMatchesOracle(fromDisk, "dh_begin_pos = 3");
    }

    /**
     * With a column resolver, the grouping sidecar of the parquet column a Deephaven column reads is that column's
     * index, and its {@code dh_key} is read as that column is: widened.
     */
    @Test
    public void legacyGroupingWithColumnResolver() {
        final Table fromDisk = ParquetTools.readTable(legacyGroupingFile(), resolving(
                TableDefinition.of(ColumnDefinition.ofLong("Sym")),
                "Sym", "gcol"));
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "Sym"));
        assertEquals(long.class, indexKeyType(fromDisk, "Sym"));
        assertEquals(500, DataIndexer.getDataIndex(fromDisk, "Sym").table().size());
        assertFilterMatchesOracle(fromDisk, "Sym = 3");
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
     * The index table is read through a resolver derived from the location's, which finds each key column where the
     * column it indexes is. Here the location's resolver swaps them -- Deephaven {@code B} reads parquet {@code C} and
     * Deephaven {@code C} reads parquet {@code B} -- so the index table's parquet {@code C} is read as its key column
     * {@code B}, and its parquet {@code B} as {@code C}.
     */
    @Test
    public void compositeDataIndexWithColumnResolverSwap() {
        final Table fromDisk = ParquetTools.readTable(writeCompositeIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofString("B"), ColumnDefinition.ofString("C")),
                "B", "C", "C", "B"));
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "B", "C"));
        // Built in memory, so it has no data index; select() would carry the one under test
        final Table expected = TableTools.emptyTable(100).update("B = `c` + (ii % 7)", "C = `b` + (ii % 5)");
        final Table expectedKeys = expected.selectDistinct("B", "C").sort("B", "C");
        assertTableEquals(expectedKeys,
                DataIndexer.getDataIndex(fromDisk, "B", "C").table().view("B", "C").sort("B", "C"));
        assertTableEquals(expectedKeys, fromDisk.selectDistinct("B", "C").sort("B", "C"));
    }

    /**
     * A column resolver is the alternative to name mappings, and with one, the mappings are ignored: here one would
     * read parquet {@code C} as {@code Sym}, but the resolver reads parquet {@code B} as {@code Sym}, for the
     * sortedness and the index table as for the data.
     */
    @Test
    public void dataIndexWithColumnResolverAndNameMapping() {
        final ParquetColumnResolverMap resolver = ParquetColumnResolverMap.builder()
                .putMap("Sym", List.of("B"))
                .putMap("C", List.of("C"))
                .build();
        final Table fromDisk = ParquetTools.readTable(writeSortedIndexedTable(), ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofString("Sym"), ColumnDefinition.ofInt("C")))
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .addColumnNameMapping("C", "Sym")
                .build());
        assertEquals(Optional.of(SortingOrder.Ascending), sortOrder(fromDisk, "Sym"));
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "Sym"));
        assertEquals(10, DataIndexer.getDataIndex(fromDisk, "Sym").table().size());
        assertFilterMatchesOracle(fromDisk, "Sym = `b3`");
    }

    /**
     * The index table is read through a resolver derived from the location's, which finds the key column under its
     * parquet name, {@code b col}, though that is not a valid Deephaven column name; nothing is legalized.
     */
    @Test
    public void dataIndexWithColumnResolverOnNonColumnName() {
        final ParquetColumnResolverMap resolver = ParquetColumnResolverMap.builder()
                .putMap("Sym", List.of("b col"))
                .putMap("C", List.of("C"))
                .build();
        final Table fromDisk = ParquetTools.readTable(writeIndexedTableUnderNonColumnName(), ParquetInstructions
                .builder()
                .setTableDefinition(
                        TableDefinition.of(ColumnDefinition.ofString("Sym"), ColumnDefinition.ofString("C")))
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .build());
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "Sym"));
        assertEquals(5, DataIndexer.getDataIndex(fromDisk, "Sym").table().size());
        assertFilterMatchesOracle(fromDisk, "Sym = `b3`");
    }

    /** A resolver may read a parquet INT32 as a long; the index is read the same way, so its keys are longs too. */
    @Test
    public void dataIndexWithColumnResolverWidening() {
        final Table fromDisk = ParquetTools.readTable(writeIntIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofLong("Sym"), ColumnDefinition.ofInt("C")),
                "Sym", "B", "C", "C"));
        assertEquals(long.class, indexKeyType(fromDisk, "Sym"));
        assertFilterMatchesOracle(fromDisk, "Sym = 3");
    }

    /**
     * Two Deephaven columns reading one parquet column can't be told apart, so neither translates to a parquet name,
     * and a composite index with as many key columns is not taken for theirs.
     */
    @Test
    public void compositeDataIndexForColumnReadTwice() {
        final String dest = writeCompositeIndexedTable();
        final ParquetInstructions instructions = resolving(
                TableDefinition.of(ColumnDefinition.ofString("Sym"), ColumnDefinition.ofString("Sym2")),
                "Sym", "B", "Sym2", "B");
        final ParquetTableLocation location = new ParquetTableLocation(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationKey(new File(dest).toURI(), 0, null, instructions),
                instructions);
        assertFalse(location.hasDataIndex("Sym", "Sym2"));
        assertThrows(TableDataException.class, () -> location.loadDataIndex("Sym", "Sym2"));
    }

    /** A resolved key column whose codec metadata the footer records by its parquet name keeps it in the index. */
    @Test
    public void dataIndexWithColumnResolverOnCodecColumn() {
        final Table fromDisk = ParquetTools.readTable(writeBigIntegerIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.fromGenericType("Sym", BigInteger.class),
                        ColumnDefinition.ofInt("C")),
                "Sym", "B", "C", "C"));
        assertEquals(BigInteger.class, indexKeyType(fromDisk, "Sym"));
        assertTableEquals(TableTools.emptyTable(5).update("Sym = java.math.BigInteger.valueOf(ii)"),
                DataIndexer.getDataIndex(fromDisk, "Sym").table().view("Sym").sort("Sym"));
    }

    /**
     * A key column read with an explicit codec, here a string column written as bytes and read as raw bytes, is read
     * with that codec in the index too, since the index table is read with the indexed table's instructions.
     */
    @Test
    public void dataIndexWithColumnResolverAndExplicitCodec() {
        final String dest = path("codecIndexed.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = `b` + (int) (ii / 10)", "C = (int) ii"),
                dest,
                ParquetInstructions.builder()
                        .addColumnCodec("B", UTF8StringAsByteArrayCodec.class.getName())
                        .addIndexColumns("B")
                        .build());
        final ParquetColumnResolverMap resolver = ParquetColumnResolverMap.builder()
                .putMap("Sym", List.of("B"))
                .putMap("C", List.of("C"))
                .build();
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setTableDefinition(TableDefinition.of(
                        ColumnDefinition.fromGenericType("Sym", byte[].class, byte.class),
                        ColumnDefinition.ofInt("C")))
                .setColumnResolverFactory((tableKey, tableLocationKey) -> resolver)
                .addColumnCodec("Sym", SimpleByteArrayCodec.class.getName())
                .build();
        final ParquetTableLocation location = new ParquetTableLocation(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationKey(new File(dest).toURI(), 0, null, instructions),
                instructions);
        final Table indexTable = location.loadDataIndex("Sym").table().select();
        assertEquals(10, indexTable.size());
        assertTrue(indexTable.getColumnSource("Sym").get(indexTable.getRowSet().firstRowKey()) instanceof byte[]);
    }

    /**
     * Codecs are registered by Deephaven column name. Here the read registers parquet {@code C}'s codec under the name
     * {@code C} is read as, as inference would, and that name is also the name of the index table's row set column; the
     * row set column must still decode with the codec the index file's footer records.
     */
    @Test
    public void dataIndexWithCodecColumnNamedLikeRowSet() {
        final String rowSetName = ParquetTableWriter.INDEX_ROW_SET_COLUMN_NAME;
        final String dest = path("codecNamedLikeRowSet.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("B = `b` + (ii % 5)", "C = `c` + ii"),
                dest,
                ParquetInstructions.builder()
                        .addColumnCodec("C", UTF8StringAsByteArrayCodec.class.getName())
                        .addIndexColumns("B")
                        .build());
        final Table fromDisk = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .addColumnNameMapping("C", rowSetName)
                .addColumnCodec(rowSetName, UTF8StringAsByteArrayCodec.class.getName())
                .build());
        assertEquals(5, DataIndexer.getDataIndex(fromDisk, "B").table().select().size());
        assertFilterMatchesOracle(fromDisk, "B = `b1`");
    }

    /**
     * Each column is decoded with the codec the footer records for the parquet column it reads, found under that
     * column's parquet name rather than its Deephaven name. Two columns written with codecs of different scales are
     * read through a resolver under other names, then swapped.
     */
    @Test
    public void codecColumnsWithColumnResolver() {
        final String dest = path("codecs.parquet");
        ParquetTools.writeTable(
                TableTools.emptyTable(10).update(
                        "P = java.math.BigDecimal.valueOf(ii, 1)", "Q = java.math.BigDecimal.valueOf(ii * 7, 4)"),
                dest,
                ParquetInstructions.builder()
                        .addColumnCodec("P", BigDecimalCodec.class.getName(), "20,1")
                        .addColumnCodec("Q", BigDecimalCodec.class.getName(), "20,4")
                        .build());
        // Read without a resolver, each column is decoded with its own codec
        final Table plain = ParquetTools.readTable(dest).select();
        final ColumnDefinition<BigDecimal> d1 = ColumnDefinition.fromGenericType("D1", BigDecimal.class);
        final ColumnDefinition<BigDecimal> d2 = ColumnDefinition.fromGenericType("D2", BigDecimal.class);
        assertTableEquals(plain.renameColumns("D1 = P", "D2 = Q"),
                ParquetTools.readTable(dest, resolving(TableDefinition.of(d1, d2), "D1", "P", "D2", "Q")));

        final ColumnDefinition<BigDecimal> p = ColumnDefinition.fromGenericType("P", BigDecimal.class);
        final ColumnDefinition<BigDecimal> q = ColumnDefinition.fromGenericType("Q", BigDecimal.class);
        assertTableEquals(plain.renameColumns("P = Q", "Q = P").view("P", "Q"),
                ParquetTools.readTable(dest, resolving(TableDefinition.of(p, q), "P", "Q", "Q", "P")));
    }

    /** A key column named like the index table's row set column can't be told apart from it, so it has no index. */
    @Test
    public void dataIndexForColumnNamedLikeRowSet() {
        final String rowSetName = ParquetTableWriter.INDEX_ROW_SET_COLUMN_NAME;
        final Table fromDisk = ParquetTools.readTable(writeSortedIndexedTable(), resolving(
                TableDefinition.of(ColumnDefinition.ofString(rowSetName), ColumnDefinition.ofInt("C")),
                rowSetName, "B", "C", "C"));
        assertFalse(DataIndexer.hasDataIndex(fromDisk, rowSetName));
        assertFilterMatchesOracle(fromDisk, rowSetName + " = `b3`");
    }

    /**
     * The location's data index methods take Deephaven column names; a name that reads no column of the file has no
     * index. The location has no definition, so the index table's definition is inferred, under the same renames.
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
        final Table indexTable = location.loadDataIndex("A").table();
        assertTrue(indexTable.hasColumns("A"));
        assertEquals(10, indexTable.size());
        // Deephaven B does not exist: parquet B is read as A
        assertFalse(location.hasDataIndex("B"));
        assertThrows(TableDataException.class, () -> location.loadDataIndex("B"));
        assertEquals(List.of(List.of("A")),
                location.getDataIndexColumns().stream().map(List::of).collect(Collectors.toList()));
    }

    // endregion column resolvers

    // region partitioning columns

    /**
     * A table partitioned by {@code P}, with a composite data index on {@code (P, B)}. The index metadata records the
     * partitioning column, which the files do not have, under its parquet name like the rest.
     */
    private String writePartitionedCompositeIndexedTable(final ParquetInstructions renames) {
        final String dest = path("partitionedCompositeIndexed");
        ParquetTools.writeKeyValuePartitionedTable(
                TableTools.emptyTable(300)
                        .update("P = (int) (ii % 3)", "B = `b` + (ii % 5)", "C = (int) ii")
                        .partitionBy("P"),
                dest,
                new ParquetInstructions.Builder(renames).addIndexColumns("P", "B").build());
        return dest;
    }

    private static void assertPartitionedCompositeIndex(final Table fromDisk, final String keyColumnName) {
        assertTrue(DataIndexer.hasDataIndex(fromDisk, "P", keyColumnName));
        final Table expectedKeys = TableTools.emptyTable(300)
                .update("P = (int) (ii % 3)", keyColumnName + " = `b` + (ii % 5)")
                .selectDistinct("P", keyColumnName)
                .sort("P", keyColumnName);
        assertTableEquals(expectedKeys, DataIndexer.getDataIndex(fromDisk, "P", keyColumnName).table()
                .view("P", keyColumnName).sort("P", keyColumnName));
        assertFilterMatchesOracle(fromDisk, "P = 1 && " + keyColumnName + " = `b2`");
    }

    /**
     * Both index key columns are renamed on write and on read: partitioning {@code P} to parquet {@code pp}, which only
     * the index metadata records, and {@code B} to parquet {@code pb}, which is read as {@code Sym}.
     */
    @Test
    public void compositeDataIndexOnPartitioningColumnWithRenames() {
        final String dest = writePartitionedCompositeIndexedTable(ParquetInstructions.builder()
                .addColumnNameMapping("pp", "P")
                .addColumnNameMapping("pb", "B")
                .build());
        final Table fromDisk = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .addColumnNameMapping("pp", "P")
                .addColumnNameMapping("pb", "Sym")
                .build());
        assertPartitionedCompositeIndex(fromDisk, "Sym");
    }

    /**
     * With a column resolver, which ignores renames, a partitioning key column is recorded under its own name, and the
     * other key column is found through the resolver.
     */
    @Test
    public void compositeDataIndexOnPartitioningColumnWithColumnResolver() {
        final String dest = writePartitionedCompositeIndexedTable(ParquetInstructions.builder()
                .addColumnNameMapping("pb", "B")
                .build());
        final Table fromDisk = ParquetTools.readTable(dest, resolving(
                TableDefinition.of(
                        ColumnDefinition.ofInt("P").withPartitioning(),
                        ColumnDefinition.ofString("Sym"),
                        ColumnDefinition.ofInt("C")),
                "Sym", "pb", "C", "C"));
        assertPartitionedCompositeIndex(fromDisk, "Sym");
    }

    /**
     * A rename that gives a file column the name of a partitioning column doesn't make the partitioning column read
     * that file column: partitioning {@code P}'s values still come from the location. The file's composite index on
     * parquet {@code B} and {@code C} is therefore not an index on {@code P} and {@code C}.
     */
    @Test
    public void compositeDataIndexForFileColumnRenamedLikePartitioningColumn() {
        final String dest = path("renamedLikePartitioning");
        ParquetTools.writeKeyValuePartitionedTable(
                TableTools.emptyTable(300)
                        .update("P = (int) (ii % 3)", "B = (int) (10 + ii % 5)", "C = `c` + (ii % 7)")
                        .partitionBy("P"),
                dest,
                ParquetInstructions.builder().addIndexColumns("B", "C").build());
        final Table fromDisk = ParquetTools.readTable(dest, ParquetInstructions.builder()
                .addColumnNameMapping("B", "P")
                .build());
        assertTrue(fromDisk.getDefinition().getColumn("P").isPartitioning());
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "P", "C"));
        assertFilterMatchesOracle(fromDisk, "P = 1 && C = `c2`");
    }

    /**
     * A file column with a partitioning column's name is not what the partitioning column reads, so the file's index on
     * that file column is not an index on the partitioning column.
     */
    @Test
    public void compositeDataIndexForFileColumnNamedLikePartitioningColumn() {
        final String dest = path("namedLikePartitioning");
        ParquetTools.writeTable(
                TableTools.emptyTable(100).update("P = (int) (ii % 3)", "C = `c` + (ii % 7)"),
                new File(dest, "P=5/data.parquet").getPath(),
                ParquetInstructions.builder().addIndexColumns("P", "C").build());
        final Table fromDisk = ParquetTools.readTable(dest);
        assertTrue(fromDisk.getDefinition().getColumn("P").isPartitioning());
        assertFalse(DataIndexer.hasDataIndex(fromDisk, "P", "C"));
        assertFilterMatchesOracle(fromDisk, "P = 5 && C = `c2`");
    }

    // endregion partitioning columns
}
