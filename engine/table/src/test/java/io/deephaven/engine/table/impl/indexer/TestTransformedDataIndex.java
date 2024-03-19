//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.indexer;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

@Category(OutOfBandTest.class)
public class TestTransformedDataIndex extends RefreshingTableTestCase {
    private static final int INITIAL_SIZE = 100;
    private static final int STEP_SIZE = 100;
    private static final int MAX_STEPS = 100;
    private ColumnInfo<?, ?>[] columnInfo;
    private QueryTable testTable;
    private List<DataIndex> dataIndexes;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2");
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol");
        columnInfo[3] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");
        testTable = TstUtils.getTable(INITIAL_SIZE, new Random(0), columnInfo);
        testTable.setRefreshing(true);

        // Add some data indexes; they will be retained by our parent class's LivenessScope
        dataIndexes = new ArrayList<>();
        dataIndexes.add(DataIndexer.getOrCreateDataIndex(testTable, "Sym"));
        dataIndexes.add(DataIndexer.getOrCreateDataIndex(testTable, "Sym2"));
        dataIndexes.add(DataIndexer.getOrCreateDataIndex(testTable, "Sym", "Sym2"));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        dataIndexes = null;
    }

    @Test
    public void testFullIndexLookup() {
        final Random random = new Random(0);

        for (final DataIndex dataIndex : dataIndexes) {
            assertRefreshing(dataIndex);

            final ColumnSource<?>[] columns = dataIndex.keyColumnNames().stream()
                    .map(testTable::getColumnSource)
                    .toArray(ColumnSource[]::new);
            assertLookupFromTable(testTable, dataIndex, columns);
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                final ColumnSource<?>[] columns = dataIndex.keyColumnNames().stream()
                        .map(testTable::getColumnSource)
                        .toArray(ColumnSource[]::new);
                assertLookupFromTable(testTable, dataIndex, columns);
            }
        }
    }

    @Test
    public void testRemappedDataIndexAllColumns() {
        for (final DataIndex dataIndex : dataIndexes) {
            // Map dummy columns to the key columns
            final Map<ColumnSource<?>, ColumnSource<?>> firstRemap = new HashMap<>();
            dataIndex.keyColumnNames().forEach(name -> firstRemap.put(testTable.getColumnSource(name),
                    InMemoryColumnSource.makeImmutableSource(Integer.class, null)));
            final DataIndex firstRemappedIndex = dataIndex.remapKeyColumns(firstRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), firstRemappedIndex.keyColumns());

            // Map new dummy columns to the old dummy columns (second-level)
            final Map<ColumnSource<?>, ColumnSource<?>> secondRemap = new HashMap<>();
            firstRemap.forEach((oldColumn, dummy) -> secondRemap.put(dummy,
                    InMemoryColumnSource.makeImmutableSource(Integer.class, null)));
            final DataIndex secondRemappedIndex = firstRemappedIndex.remapKeyColumns(secondRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), secondRemappedIndex.keyColumns());

            // Map even newer dummy columns to the old dummy columns (third-level)
            final Map<ColumnSource<?>, ColumnSource<?>> thirdRemap = new HashMap<>();
            secondRemap.forEach((oldColumn, dummy) -> thirdRemap.put(dummy,
                    InMemoryColumnSource.makeImmutableSource(Integer.class, null)));
            final DataIndex thirdRemappedIndex = secondRemappedIndex.remapKeyColumns(secondRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), thirdRemappedIndex.keyColumns());
        }
    }

    @Test
    public void testRemappedDataIndexOnlyFirstColumns() {
        for (final DataIndex dataIndex : dataIndexes) {
            final ColumnSource<?> firstDummy = InMemoryColumnSource.makeImmutableSource(Integer.class, null);
            final Map<ColumnSource<?>, ColumnSource<?>> firstRemap =
                    Map.of(testTable.getColumnSource(dataIndex.keyColumnNames().get(0)), firstDummy);
            final DataIndex firstRemappedIndex = dataIndex.remapKeyColumns(firstRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), firstRemappedIndex.keyColumns());

            final ColumnSource<?> secondDummy = InMemoryColumnSource.makeImmutableSource(Integer.class, null);
            final Map<ColumnSource<?>, ColumnSource<?>> secondRemap = Map.of(firstDummy, secondDummy);
            final DataIndex secondRemappedIndex = firstRemappedIndex.remapKeyColumns(secondRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), firstRemappedIndex.keyColumns());

            final ColumnSource<?> thirdDummy = InMemoryColumnSource.makeImmutableSource(Integer.class, null);
            final Map<ColumnSource<?>, ColumnSource<?>> thirdRemap = Map.of(secondDummy, thirdDummy);
            final DataIndex thirdRemappedIndex = secondRemappedIndex.remapKeyColumns(thirdRemap);
            // Verify that the original and remapped indexes point to the same index table columns
            assertEquals(dataIndex.keyColumns(), thirdRemappedIndex.keyColumns());
        }
    }


    @Test
    public void testMultiColumnOutOfOrderLookup() {
        final Random random = new Random(0);
        final DataIndexer dataIndexer = DataIndexer.of(testTable.getRowSet());

        final DataIndex dataIndex = DataIndexer.getDataIndex(testTable, "Sym", "Sym2");
        assertNotNull(dataIndex);

        // Make sure the index is found with the re-ordered column names.
        assertTrue(DataIndexer.hasDataIndex(testTable, "Sym2", "Sym"));
        final DataIndex tmp1 = DataIndexer.getDataIndex(testTable, "Sym2", "Sym");
        assertEquals(dataIndex, tmp1);

        final ColumnSource<?>[] columnsReordered = new ColumnSource<?>[] {
                testTable.getColumnSource("Sym2"),
                testTable.getColumnSource("Sym")
        };
        // Make sure the index is found with re-ordered columns.
        assertTrue(dataIndexer.hasDataIndex(columnsReordered));
        final DataIndex tmp2 = dataIndexer.getDataIndex(columnsReordered);
        assertEquals(dataIndex, tmp2);

        final ColumnSource<?>[] columns = Arrays.stream(new String[] {"Sym", "Sym2"})
                .map(testTable::getColumnSource)
                .toArray(ColumnSource[]::new);
        final ColumnSource<?>[] reorderedColumns = Arrays.stream(new String[] {"Sym2", "Sym"})
                .map(testTable::getColumnSource)
                .toArray(ColumnSource[]::new);

        assertRefreshing(dataIndex);
        assertLookupFromTable(testTable, dataIndex, columns);
        assertLookupFromTable(testTable, dataIndex, reorderedColumns);

        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            assertRefreshing(dataIndex);
            assertLookupFromTable(testTable, dataIndex, columns);
            assertLookupFromTable(testTable, dataIndex, reorderedColumns);
        }
    }

    @Test
    public void testIntersectAndInvert() {
        final Random random = new Random(0);

        try (final RowSet reduced = testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
            final RowSet tableRowSet = testTable.getRowSet();
            final DataIndexTransformer intersectTransformer =
                    DataIndexTransformer.builder().intersectRowSet(reduced).build();
            final Function<RowSet, RowSet> intersectMutator = (rs) -> rs.intersect(reduced);
            final DataIndexTransformer invertTransformer =
                    DataIndexTransformer.builder().invertRowSet(tableRowSet).build();
            final Function<RowSet, RowSet> invertMutator = tableRowSet::invert;
            final DataIndexTransformer intersectInvertTransformer = DataIndexTransformer.builder()
                    .intersectRowSet(reduced)
                    .invertRowSet(tableRowSet)
                    .build();
            final Function<RowSet, RowSet> intersectInvertMutator = (rs) -> tableRowSet.invert(rs.intersect(reduced));

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                // Test each transform individually.
                BasicDataIndex subIndex = dataIndex.transform(intersectTransformer);
                assertStatic(subIndex);
                assertLookupMutator(dataIndex, subIndex, intersectMutator);

                subIndex = dataIndex.transform(invertTransformer);
                assertStatic(subIndex);
                assertLookupMutator(dataIndex, subIndex, invertMutator);

                subIndex = dataIndex.transform(intersectInvertTransformer);
                assertStatic(subIndex);
                assertLookupMutator(dataIndex, subIndex, intersectInvertMutator);
            }
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            try (final RowSet reduced =
                    testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
                final RowSet tableRowSet = testTable.getRowSet();
                final DataIndexTransformer intersectTransformer =
                        DataIndexTransformer.builder().intersectRowSet(reduced).build();
                final Function<RowSet, RowSet> intersectMutator = (rs) -> rs.intersect(reduced);
                final DataIndexTransformer invertTransformer =
                        DataIndexTransformer.builder().invertRowSet(tableRowSet).build();
                final Function<RowSet, RowSet> invertMutator = tableRowSet::invert;
                final DataIndexTransformer intersectInvertTransformer = DataIndexTransformer.builder()
                        .intersectRowSet(reduced)
                        .invertRowSet(tableRowSet)
                        .build();
                final Function<RowSet, RowSet> intersectInvertMutator =
                        (rs) -> tableRowSet.invert(rs.intersect(reduced));

                for (final DataIndex dataIndex : dataIndexes) {
                    assertRefreshing(dataIndex);

                    // Test each transform individually.
                    BasicDataIndex subIndex = dataIndex.transform(intersectTransformer);
                    assertStatic(subIndex);
                    assertLookupMutator(dataIndex, subIndex, intersectMutator);

                    subIndex = dataIndex.transform(invertTransformer);
                    assertStatic(subIndex);
                    assertLookupMutator(dataIndex, subIndex, invertMutator);

                    subIndex = dataIndex.transform(intersectInvertTransformer);
                    assertStatic(subIndex);
                    assertLookupMutator(dataIndex, subIndex, intersectInvertMutator);
                }
            }
        }
    }

    @Test
    public void testSortByFirstRowKey() {
        final Random random = new Random(0);

        final DataIndexTransformer transformer = DataIndexTransformer.builder().sortByFirstRowKey(true).build();

        for (final DataIndex dataIndex : dataIndexes) {
            assertRefreshing(dataIndex);

            BasicDataIndex subIndex = dataIndex.transform(transformer);
            assertRefreshing(subIndex);
            assertSortedByFirstRowKey(subIndex);
            assertLookup(dataIndex, subIndex);
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                BasicDataIndex subIndex = dataIndex.transform(transformer);
                assertRefreshing(subIndex);
                assertSortedByFirstRowKey(subIndex);
                assertLookup(dataIndex, subIndex);
            }
        }
    }

    @Test
    public void testSortIntersect() {
        final Random random = new Random(0);

        try (final RowSet reduced = testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
            final DataIndexTransformer transformer = DataIndexTransformer.builder()
                    .intersectRowSet(reduced)
                    .sortByFirstRowKey(true)
                    .build();
            final Function<RowSet, RowSet> mutator = (rs) -> rs.intersect(reduced);

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                BasicDataIndex subIndex = dataIndex.transform(transformer);
                assertStatic(subIndex);
                assertSortedByFirstRowKey(subIndex);
                assertLookupMutator(dataIndex, subIndex, mutator);
            }
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            try (final RowSet reduced =
                    testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
                final DataIndexTransformer transformer = DataIndexTransformer.builder()
                        .intersectRowSet(reduced)
                        .sortByFirstRowKey(true)
                        .build();
                final Function<RowSet, RowSet> mutator = (rs) -> rs.intersect(reduced);

                for (final DataIndex dataIndex : dataIndexes) {
                    assertRefreshing(dataIndex);

                    BasicDataIndex subIndex = dataIndex.transform(transformer);
                    assertStatic(subIndex);
                    assertSortedByFirstRowKey(subIndex);
                    assertLookupMutator(dataIndex, subIndex, mutator);
                }
            }
        }
    }

    @Test
    public void testSortIntersectInvert() {
        final Random random = new Random(0);

        try (final RowSet reduced = testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
            final RowSet tableRowSet = testTable.getRowSet();
            final DataIndexTransformer transformer = DataIndexTransformer.builder()
                    .intersectRowSet(reduced)
                    .invertRowSet(tableRowSet)
                    .sortByFirstRowKey(true)
                    .build();
            final Function<RowSet, RowSet> mutator = (rs) -> tableRowSet.invert(rs.intersect(reduced));

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                BasicDataIndex subIndex = dataIndex.transform(transformer);
                assertStatic(subIndex);
                assertSortedByFirstRowKey(subIndex);
                assertLookupMutator(dataIndex, subIndex, mutator);
            }
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            try (final RowSet reduced =
                    testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
                final RowSet tableRowSet = testTable.getRowSet();
                final DataIndexTransformer transformer = DataIndexTransformer.builder()
                        .intersectRowSet(reduced)
                        .invertRowSet(tableRowSet)
                        .sortByFirstRowKey(true)
                        .build();
                final Function<RowSet, RowSet> mutator = (rs) -> tableRowSet.invert(rs.intersect(reduced));

                for (final DataIndex dataIndex : dataIndexes) {
                    assertRefreshing(dataIndex);

                    BasicDataIndex subIndex = dataIndex.transform(transformer);
                    assertStatic(subIndex);
                    assertSortedByFirstRowKey(subIndex);
                    assertLookupMutator(dataIndex, subIndex, mutator);
                }
            }
        }
    }


    private void assertStatic(final BasicDataIndex subIndex) {
        assertFalse(subIndex.isRefreshing());
        assertFalse(subIndex.table().isRefreshing());
    }

    private void assertRefreshing(final BasicDataIndex subIndex) {
        assertTrue(subIndex.isRefreshing());
        assertTrue(subIndex.table().isRefreshing());
    }

    private void assertLookup(final DataIndex fullIndex,
            final BasicDataIndex subIndex) {
        final Table subIndexTable = subIndex.table();

        final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup();
        final ToLongFunction<Object> subIndexRowKeyMappingFunction = DataIndexUtils.buildRowKeyMappingFunction(
                subIndex.table(), subIndex.keyColumnNames().toArray(String[]::new));

        ChunkSource.WithPrev<?> subKeys = DataIndexUtils.makeBoxedKeySource(subIndex.keyColumns());

        try (final CloseableIterator<Object> subKeyIt =
                ChunkedColumnIterator.make(subKeys, subIndexTable.getRowSet())) {

            while (subKeyIt.hasNext()) {
                final Object subKey = subKeyIt.next();

                // Verify the row sets at the lookup keys match.
                final long subRowKey = subIndexRowKeyMappingFunction.applyAsLong(subKey);
                final long fullRowKey = fullIndexRowKeyLookup.apply(subKey, false);

                assertEquals(subIndex.rowSetColumn().get(subRowKey), fullIndex.rowSetColumn().get(fullRowKey));
            }
        }
    }

    private void assertLookupMutator(
            final DataIndex fullIndex,
            final BasicDataIndex subIndex,
            final Function<RowSet, RowSet> mutator) {
        final Table fullIndexTable = fullIndex.table();
        final ToLongFunction<Object> subIndexRowKeyMappingFunction = DataIndexUtils.buildRowKeyMappingFunction(
                subIndex.table(), subIndex.keyColumnNames().toArray(String[]::new));

        ChunkSource.WithPrev<?> fullKeys = DataIndexUtils.makeBoxedKeySource(fullIndex.keyColumns());

        try (final CloseableIterator<Object> fullKeyIt =
                ChunkedColumnIterator.make(fullKeys, fullIndexTable.getRowSet());
                final CloseableIterator<RowSet> fullRowSetIt =
                        ChunkedColumnIterator.make(fullIndex.rowSetColumn(), fullIndexTable.getRowSet())) {

            while (fullKeyIt.hasNext()) {
                final Object fullKey = fullKeyIt.next();
                final RowSet fullRowSet = fullRowSetIt.next();

                // Is the key in the sub-index?
                final long subRowKey = subIndexRowKeyMappingFunction.applyAsLong(fullKey);
                if (subRowKey == NULL_ROW_KEY) {
                    // Verify applying the mutator to the full row set results in an empty row set.
                    assertTrue(mutator.apply(fullRowSet).isEmpty());
                } else {
                    // The row set from the lookup must match the computed row set.
                    assertEquals(subIndex.rowSetColumn().get(subRowKey), mutator.apply(fullRowSet));
                }
            }
        }
    }

    private void assertLookupFromTable(
            final Table sourceTable,
            final DataIndex fullIndex,
            final ColumnSource<?>[] columns) {
        final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup(columns);
        final ColumnSource<RowSet> fullIndexRowSetColumn = fullIndex.rowSetColumn();

        ChunkSource.WithPrev<?> tableKeys = DataIndexUtils.makeBoxedKeySource(columns);

        // Iterate through the entire source table and verify the lookup row set is valid and contains this row.
        try (final RowSet.Iterator rsIt = sourceTable.getRowSet().iterator();
                final CloseableIterator<Object> keyIt =
                        ChunkedColumnIterator.make(tableKeys, sourceTable.getRowSet())) {

            while (rsIt.hasNext() && keyIt.hasNext()) {
                final long rowKey = rsIt.nextLong();
                final Object key = keyIt.next();

                // Verify the row sets at the lookup keys match.
                final long fullRowKey = fullIndexRowKeyLookup.apply(key, false);
                Assert.geqZero(fullRowKey, "fullRowKey");

                final RowSet fullRowSet = fullIndexRowSetColumn.get(fullRowKey);
                assertNotNull(fullRowSet);

                assertTrue(fullRowSet.containsRange(rowKey, rowKey));
            }
        }
    }

    private void assertSortedByFirstRowKey(final BasicDataIndex subIndex) {
        final Table subIndexTable = subIndex.table();

        try (final CloseableIterator<RowSet> subRowSetIt =
                ChunkedColumnIterator.make(subIndex.rowSetColumn(), subIndexTable.getRowSet())) {

            long lastKey = -1;
            while (subRowSetIt.hasNext()) {
                final RowSet subRowSet = subRowSetIt.next();

                // Assert sorted-ness of the sub-index.
                Assert.gt(subRowSet.firstRowKey(), "subRowSet.firstRowKey()", lastKey, "lastKey");
                lastKey = subRowSet.firstRowKey();
            }
        }
    }
}
