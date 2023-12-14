package io.deephaven.engine.table.impl.indexer;

import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.BaseDataIndex;
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

@Category(OutOfBandTest.class)
public class TestDerivedDataIndex extends RefreshingTableTestCase {
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

        // Add some data indexes.
        final DataIndexer dataIndexer = DataIndexer.of(testTable.getRowSet());
        dataIndexer.createDataIndex(testTable, "Sym");
        dataIndexer.createDataIndex(testTable, "Sym2");
        dataIndexer.createDataIndex(testTable, "Sym", "Sym2");

        dataIndexes = new ArrayList<>();
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym2"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym", "Sym2"));
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
    public void testImmutable() {
        final Random random = new Random(0);

        final DataIndexTransformer transformer = DataIndexTransformer.builder().immutable(true).build();

        for (final DataIndex dataIndex : dataIndexes) {
            assertRefreshing(dataIndex);

            BasicDataIndex subIndex = dataIndex.transform(transformer);
            assertImmutable(subIndex);
            assertLookup(dataIndex, subIndex);
        }

        // Transform and validate
        for (int ii = 0; ii < MAX_STEPS; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(STEP_SIZE, random, testTable, columnInfo));

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                BasicDataIndex subIndex = dataIndex.transform(transformer);
                assertImmutable(subIndex);
                assertLookup(dataIndex, subIndex);
            }
        }
    }

    @Test
    public void testIntersectImmutable() {
        final Random random = new Random(0);

        try (final RowSet reduced = testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
            final DataIndexTransformer transformer = DataIndexTransformer.builder()
                    .intersectRowSet(reduced)
                    .immutable(true)
                    .build();
            final Function<RowSet, RowSet> mutator = (rs) -> rs.intersect(reduced);

            for (final DataIndex dataIndex : dataIndexes) {
                assertRefreshing(dataIndex);

                BasicDataIndex subIndex = dataIndex.transform(transformer);
                assertStatic(subIndex);
                assertImmutable(subIndex);
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
                        .immutable(true)
                        .build();
                final Function<RowSet, RowSet> mutator = (rs) -> rs.intersect(reduced);

                for (final DataIndex dataIndex : dataIndexes) {
                    assertRefreshing(dataIndex);

                    BasicDataIndex subIndex = dataIndex.transform(transformer);
                    assertStatic(subIndex);
                    assertImmutable(subIndex);
                    assertLookupMutator(dataIndex, subIndex, mutator);
                }
            }
        }
    }

    @Test
    public void testSortImmutable() {
        final Random random = new Random(0);

        final DataIndexTransformer transformer = DataIndexTransformer.builder()
                .sortByFirstRowKey(true)
                .immutable(true)
                .build();

        for (final DataIndex dataIndex : dataIndexes) {
            assertRefreshing(dataIndex);

            BasicDataIndex subIndex = dataIndex.transform(transformer);
            assertStatic(subIndex);
            assertImmutable(subIndex);
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
                assertStatic(subIndex);
                assertImmutable(subIndex);
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
        Assert.eqFalse(subIndex.isRefreshing(), "subIndex.isRefreshing()");
        Assert.eqFalse(subIndex.table().isRefreshing(), "subIndex.table().isRefreshing()");
    }

    private void assertRefreshing(final BasicDataIndex subIndex) {
        Assert.eqTrue(subIndex.isRefreshing(), "subIndex.isRefreshing()");
        Assert.eqTrue(subIndex.table().isRefreshing(), "subIndex.table().isRefreshing()");
    }

    private void assertImmutable(final BasicDataIndex subIndex) {
        final Table subIndexTable = subIndex.table();

        // This transform should result in a static index.
        Assert.eqFalse(subIndexTable.isRefreshing(), "subIndexTable.isRefreshing()");

        // This transform should result in a flat table.
        Assert.eqFalse(subIndexTable.isFlat(), "subIndexTable.isFlat()");

        final boolean allImmutable = subIndexTable.getColumnSourceMap().values().stream()
                .allMatch(ColumnSource::isImmutable);
        Assert.eqTrue(allImmutable, "All columns are immutable");
    }

    private void assertLookup(final DataIndex fullIndex,
            final BasicDataIndex subIndex) {
        final Table subIndexTable = subIndex.table();

        final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup();
        final TObjectLongHashMap<Object> subIndexKeyMap =
                BaseDataIndex.buildKeyMap(subIndex.table(), subIndex.keyColumnNames());

        ChunkSource.WithPrev<?> subKeys = DataIndexUtils.makeBoxedKeySource(subIndex.indexKeyColumns());

        try (final CloseableIterator<Object> subKeyIt =
                ChunkedColumnIterator.make(subKeys, subIndexTable.getRowSet())) {

            while (subKeyIt.hasNext()) {
                final Object subKey = subKeyIt.next();

                // Verify the row sets at the lookup keys match.
                final long subRowKey = subIndexKeyMap.get(subKey);
                final long fullRowKey = fullIndexRowKeyLookup.apply(subKey, false);

                Assert.eqTrue(
                        subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey)),
                        "subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey))");
            }
        }
    }

    private void assertLookupMutator(
            final DataIndex fullIndex,
            final BasicDataIndex subIndex,
            final Function<RowSet, RowSet> mutator) {
        final Table fullIndexTable = fullIndex.table();
        final TObjectLongHashMap<Object> subIndexKeyMap =
                BaseDataIndex.buildKeyMap(subIndex.table(), subIndex.keyColumnNames());

        ChunkSource.WithPrev<?> fullKeys = DataIndexUtils.makeBoxedKeySource(fullIndex.indexKeyColumns());

        try (final CloseableIterator<Object> fullKeyIt =
                ChunkedColumnIterator.make(fullKeys, fullIndexTable.getRowSet());
                final CloseableIterator<RowSet> fullRowSetIt =
                        ChunkedColumnIterator.make(fullIndex.rowSetColumn(), fullIndexTable.getRowSet())) {

            while (fullKeyIt.hasNext()) {
                final Object fullKey = fullKeyIt.next();
                final RowSet fullRowSet = fullRowSetIt.next();

                // Is the key in the sub-index?
                final long subRowKey = subIndexKeyMap.get(fullKey);
                if (subRowKey == NULL_ROW_KEY) {
                    // Verify applying the mutator to the full row set results in an empty row set.
                    Assert.eqTrue(mutator.apply(fullRowSet).isEmpty(), "mutator.apply(fullRowSet).isEmpty()");
                } else {
                    // The row set from the lookup must match the computed row set.
                    Assert.eqTrue(subIndex.rowSetColumn().get(subRowKey).equals(mutator.apply(fullRowSet)),
                            "subIndex.rowSetColumn().get(subRowKey).equals(mutator.apply(fullRowSet))");
                }
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
