package io.deephaven.engine.table.impl.indexer;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
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
    @Test
    public void testIntersectAndInvert() {
        final int size = 100;
        final Random random = new Random(0);
        final int maxSteps = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2");
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol");
        columnInfo[3] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable testTable = TstUtils.getTable(size, random, columnInfo);
        testTable.setRefreshing(true);

        // Add some data indexes.
        final DataIndexer dataIndexer = DataIndexer.of(testTable.getRowSet());
        dataIndexer.createDataIndex(testTable, "Sym");
        dataIndexer.createDataIndex(testTable, "Sym2");
        dataIndexer.createDataIndex(testTable, "Sym", "Sym2");

        final List<DataIndex> dataIndexes = new ArrayList<>();
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym2"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym", "Sym2"));

        try (final RowSet reduced = testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
            final RowSet tableRowSet = testTable.getRowSet();
            final DataIndexTransformer intersectTransformer =
                    DataIndexTransformer.builder().intersectRowSet(reduced).build();
            final Function<RowSet, RowSet> intersectMutator = (rs) -> rs.intersect(reduced);
            final DataIndexTransformer invertTransformer =
                    DataIndexTransformer.builder().invertRowSet(tableRowSet).build();
            final Function<RowSet, RowSet> invertMutator = (rs) -> tableRowSet.invert(rs);
            final DataIndexTransformer intersectInvertTransformer = DataIndexTransformer.builder()
                    .intersectRowSet(reduced)
                    .invertRowSet(tableRowSet)
                    .build();
            final Function<RowSet, RowSet> intersectInvertMutator = (rs) -> tableRowSet.invert(rs.intersect(reduced));

            for (final DataIndex dataIndex : dataIndexes) {
                validateIntersectInvert(dataIndex, dataIndex.transform(intersectTransformer), intersectMutator);
                validateIntersectInvert(dataIndex, dataIndex.transform(invertTransformer), invertMutator);
                validateIntersectInvert(dataIndex, dataIndex.transform(intersectInvertTransformer),
                        intersectInvertMutator);
            }
        }

        // Transform and validate
        for (int ii = 0; ii < maxSteps; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(size, random, testTable, columnInfo));

            try (final RowSet reduced =
                    testTable.getRowSet().subSetByPositionRange(0, testTable.getRowSet().size() / 4)) {
                final RowSet tableRowSet = testTable.getRowSet();
                final DataIndexTransformer intersectTransformer =
                        DataIndexTransformer.builder().intersectRowSet(reduced).build();
                final Function<RowSet, RowSet> intersectMutator = (rs) -> rs.intersect(reduced);
                final DataIndexTransformer invertTransformer =
                        DataIndexTransformer.builder().invertRowSet(tableRowSet).build();
                final Function<RowSet, RowSet> invertMutator = (rs) -> tableRowSet.invert(rs);
                final DataIndexTransformer intersectInvertTransformer = DataIndexTransformer.builder()
                        .intersectRowSet(reduced)
                        .invertRowSet(tableRowSet)
                        .build();
                final Function<RowSet, RowSet> intersectInvertMutator =
                        (rs) -> tableRowSet.invert(rs.intersect(reduced));

                for (final DataIndex dataIndex : dataIndexes) {
                    validateIntersectInvert(dataIndex, dataIndex.transform(intersectTransformer), intersectMutator);
                    validateIntersectInvert(dataIndex, dataIndex.transform(invertTransformer), invertMutator);
                    validateIntersectInvert(dataIndex, dataIndex.transform(intersectInvertTransformer),
                            intersectInvertMutator);
                }
            }
        }
    }

    @Test
    public void testSortByFirstRowKey() {
        final int size = 100;
        final Random random = new Random(0);
        final int maxSteps = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2");
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol");
        columnInfo[3] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable testTable = TstUtils.getTable(size, random, columnInfo);
        testTable.setRefreshing(true);

        // Add some data indexes.
        final DataIndexer dataIndexer = DataIndexer.of(testTable.getRowSet());
        dataIndexer.createDataIndex(testTable, "Sym");
        dataIndexer.createDataIndex(testTable, "Sym2");
        dataIndexer.createDataIndex(testTable, "Sym", "Sym2");

        final List<DataIndex> dataIndexes = new ArrayList<>();
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym2"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym", "Sym2"));

        final DataIndexTransformer sortTransformer = DataIndexTransformer.builder().sortByFirstRowKey(true).build();

        for (final DataIndex dataIndex : dataIndexes) {
            validateSortByFirstRowKey(dataIndex, dataIndex.transform(sortTransformer));
        }

        // Transform and validate
        for (int ii = 0; ii < maxSteps; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(size, random, testTable, columnInfo));

            for (final DataIndex dataIndex : dataIndexes) {
                validateSortByFirstRowKey(dataIndex, dataIndex.transform(sortTransformer));
            }
        }
    }

    @Test
    public void testImmutable() {
        final int size = 100;
        final Random random = new Random(0);
        final int maxSteps = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2");
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol");
        columnInfo[3] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable testTable = TstUtils.getTable(size, random, columnInfo);
        testTable.setRefreshing(true);

        // Add some data indexes.
        final DataIndexer dataIndexer = DataIndexer.of(testTable.getRowSet());
        dataIndexer.createDataIndex(testTable, "Sym");
        dataIndexer.createDataIndex(testTable, "Sym2");
        dataIndexer.createDataIndex(testTable, "Sym", "Sym2");

        final List<DataIndex> dataIndexes = new ArrayList<>();
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym2"));
        dataIndexes.add(dataIndexer.getDataIndex(testTable, "Sym", "Sym2"));

        final DataIndexTransformer immutableTransformer = DataIndexTransformer.builder().immutable(true).build();

        for (final DataIndex dataIndex : dataIndexes) {
            validateImmutable(dataIndex, dataIndex.transform(immutableTransformer));
        }

        // Transform and validate
        for (int ii = 0; ii < maxSteps; ++ii) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(size, random, testTable, columnInfo));

            for (final DataIndex dataIndex : dataIndexes) {
                validateImmutable(dataIndex, dataIndex.transform(immutableTransformer));
            }
        }
    }

    private void validateIntersectInvert(
            final DataIndex fullIndex,
            final DataIndex subIndex,
            final Function<RowSet, RowSet> mutator) {
        final Table fullIndexTable = fullIndex.table();
        final Table subIndexTable = subIndex.table();
        final DataIndex.RowKeyLookup subIndexRowKeyLookup = subIndex.rowKeyLookup();

        // This transform should result in a static index.
        Assert.eqFalse(subIndexTable.isRefreshing(), "subIndexTable.isRefreshing()");

        // Iterate through the full and sub indexes and validate that they are correct.
        ChunkSource.WithPrev<?> fullKeys = DataIndexUtils.makeBoxedKeySource(fullIndex.indexKeyColumns());
        ChunkSource.WithPrev<?> subKeys = DataIndexUtils.makeBoxedKeySource(subIndex.indexKeyColumns());

        try (final CloseableIterator<Object> fullKeyIt =
                ChunkedColumnIterator.make(fullKeys, fullIndexTable.getRowSet());
                final CloseableIterator<RowSet> fullRowSetIt =
                        ChunkedColumnIterator.make(fullIndex.rowSetColumn(), fullIndexTable.getRowSet());
                final CloseableIterator<Object> subKeyIt =
                        ChunkedColumnIterator.make(subKeys, subIndexTable.getRowSet());
                final CloseableIterator<RowSet> subRowSetIt =
                        ChunkedColumnIterator.make(subIndex.rowSetColumn(), subIndexTable.getRowSet())) {

            Object subKey = subKeyIt.next();
            RowSet subRowSet = subRowSetIt.next();

            while (fullKeyIt.hasNext()) {
                final Object fullKey = fullKeyIt.next();
                final RowSet fullRowSet = fullRowSetIt.next();

                // Do the keys match?
                if (DataIndexUtils.keysEqual(subKey, fullKey)) {
                    try (final RowSet computed = mutator.apply(fullRowSet)) {
                        // The sub-index row set must match the computed row set.
                        Assert.eqTrue(subRowSet.equals(computed), "subRowSet.equals(computed)");

                        // The row set from the lookup must match the computed row set.
                        final long key = subIndexRowKeyLookup.apply(fullKey, false);
                        Assert.eqTrue(subIndex.rowSetColumn().get(key).equals(computed),
                                "subIndex.rowSetColumn().get(key).equals(computed)");
                    }
                    // Advance the sub index.
                    subKey = subKeyIt.hasNext() ? subKeyIt.next() : null;
                    subRowSet = subRowSetIt.hasNext() ? subRowSetIt.next() : null;

                } else {
                    // Lookup must return NULL_ROW_KEY.
                    Assert.eq(
                            subIndexRowKeyLookup.apply(fullKey, false),
                            "subIndexRowKeyLookup.apply(fullKey, false)",
                            NULL_ROW_KEY,
                            "NULL_ROW_KEY");
                }
            }
        }
    }

    private void validateSortByFirstRowKey(
            final DataIndex fullIndex,
            final DataIndex subIndex) {
        final Table fullIndexTable = fullIndex.table();
        final Table subIndexTable = subIndex.table();

        final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup();
        final DataIndex.RowKeyLookup subIndexRowKeyLookup = subIndex.rowKeyLookup();

        // This transform should not force a static result.
        Assert.eq(fullIndexTable.isRefreshing(), "fullIndexTable.isRefreshing()",
                subIndexTable.isRefreshing(), "subIndexTable.isRefreshing()");

        // Iterate through the full and sub indexes and validate that they are correct.
        ChunkSource.WithPrev<?> subKeys = DataIndexUtils.makeBoxedKeySource(subIndex.indexKeyColumns());

        try (final CloseableIterator<Object> subKeyIt = ChunkedColumnIterator.make(subKeys, subIndexTable.getRowSet());
                final CloseableIterator<RowSet> subRowSetIt =
                        ChunkedColumnIterator.make(subIndex.rowSetColumn(), subIndexTable.getRowSet())) {

            long lastKey = -1;
            while (subKeyIt.hasNext()) {
                final Object subKey = subKeyIt.next();
                final RowSet subRowSet = subRowSetIt.next();

                // Assert sorted-ness of the sub-index.
                Assert.gt(subRowSet.firstRowKey(), "subRowSet.firstRowKey()", lastKey, "lastKey");
                lastKey = subRowSet.firstRowKey();

                // Verify the row set at position lookup of the sub-index matches the row set at full index position.
                final long subRowKey = subIndexRowKeyLookup.apply(subKey, false);
                final long fullRowKey = fullIndexRowKeyLookup.apply(subKey, false);

                Assert.eqTrue(
                        subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey)),
                        "subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey))");
            }
        }
    }

    private void validateImmutable(
            final DataIndex fullIndex,
            final DataIndex subIndex) {
        final Table subIndexTable = subIndex.table();

        final DataIndex.RowKeyLookup fullIndexRowKeyLookup = fullIndex.rowKeyLookup();
        final DataIndex.RowKeyLookup subIndexRowKeyLookup = subIndex.rowKeyLookup();

        // This transform should result in a static index.
        Assert.eqFalse(subIndexTable.isRefreshing(), "subIndexTable.isRefreshing()");

        // This transform should result in a flat table.
        Assert.eqFalse(subIndexTable.isFlat(), "subIndexTable.isFlat()");

        // Iterate through the full and sub indexes and validate that they are correct.
        ChunkSource.WithPrev<?> subKeys = DataIndexUtils.makeBoxedKeySource(subIndex.indexKeyColumns());

        try (final CloseableIterator<Object> subKeyIt = ChunkedColumnIterator.make(subKeys, subIndexTable.getRowSet());
             final CloseableIterator<RowSet> subRowSetIt =
                     ChunkedColumnIterator.make(subIndex.rowSetColumn(), subIndexTable.getRowSet())) {

            while (subKeyIt.hasNext()) {
                final Object subKey = subKeyIt.next();

                // Verify the row set at position lookup of the sub-index matches the row set at full index position.
                final long subRowKey = subIndexRowKeyLookup.apply(subKey, false);
                final long fullRowKey = fullIndexRowKeyLookup.apply(subKey, false);

                Assert.eqTrue(
                        subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey)),
                        "subIndex.rowSetColumn().get(subRowKey).equals(fullIndex.rowSetColumn().get(fullRowKey))");
            }
        }
    }
}
