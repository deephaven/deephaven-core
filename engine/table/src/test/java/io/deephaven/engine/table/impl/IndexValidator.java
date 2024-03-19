//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;

import java.util.*;

import static io.deephaven.engine.table.impl.dataindex.DataIndexUtils.lookupKeysEqual;

/**
 * This class listens to a table and on each update verifies that the indexes returned by the table's RowSet for a set
 * of columns are still valid. It is meant to be used as part of a unit test for incremental updates, to ensure that
 * stale indexes are not left between table updates.
 */
public class IndexValidator extends InstrumentedTableUpdateListenerAdapter {

    private final Table sourceTable;
    private final List<String[]> indexColumns;
    private final List<DataIndex> indexes;
    private final String context;

    private int validationCount = 0;

    public IndexValidator(String context, Table sourceTable, List<List<String>> indexColumns) {
        this(context, sourceTable, convertListToArray(indexColumns));
    }

    private static Collection<String[]> convertListToArray(List<List<String>> indexColumns) {
        Collection<String[]> collectionOfArrays = new ArrayList<>();
        for (List<String> columnSet : indexColumns) {
            collectionOfArrays.add(columnSet.toArray(new String[0]));
        }
        return collectionOfArrays;
    }

    private IndexValidator(String context, Table sourceTable, Collection<String[]> indexColumns) {
        super("index validator " + context, sourceTable, false);

        this.context = context;
        this.sourceTable = sourceTable;

        try (final SafeCloseable ignored = sourceTable.isRefreshing() ? LivenessScopeStack.open() : null) {
            final List<String[]> foundIndexColumns = new ArrayList<>();
            final List<DataIndex> foundIndexes = new ArrayList<>();
            for (String[] keyColumns : indexColumns) {
                final DataIndex index = DataIndexer.getDataIndex(sourceTable, keyColumns);
                if (index != null) {
                    Assert.eq(sourceTable.isRefreshing(), "sourceTable.isRefreshing()",
                            index.isRefreshing(), "index.isRefreshing()");
                    foundIndexColumns.add(keyColumns);
                    foundIndexes.add(index);
                    if (index.isRefreshing()) {
                        manage(index);
                    }
                }
            }
            this.indexColumns = foundIndexColumns;
            indexes = foundIndexes;
        }

        validateIndexes();

        sourceTable.addUpdateListener(this);
    }

    @Override
    public boolean canExecute(final long step) {
        return super.canExecute(step) && indexes.stream().map(DataIndex::table).allMatch(t -> t.satisfied(step));
    }

    public static void validateIndex(final Table sourceTable, final String[] keyColumns, final boolean usePrev) {
        final DataIndex index = DataIndexer.getDataIndex(sourceTable, keyColumns);
        if (index == null) {
            return;
        }
        final Table indexTable = index.table();

        final ColumnSource<?>[] sourceKeyColumns =
                Arrays.stream(keyColumns).map(sourceTable::getColumnSource).toArray(ColumnSource[]::new);

        ChunkSource<Values> sourceKeys = DataIndexUtils.makeBoxedKeySource(sourceKeyColumns);
        if (usePrev) {
            sourceKeys = ((ChunkSource.WithPrev<Values>) sourceKeys).getPrevSource();
        }
        final RowSet sourceTableRowSet = usePrev ? sourceTable.getRowSet().prev() : sourceTable.getRowSet();

        final ColumnSource<RowSet> indexRowSets = usePrev ? index.rowSetColumn().getPrevSource() : index.rowSetColumn();
        ChunkSource<Values> indexKeys = DataIndexUtils.makeBoxedKeySource(index.keyColumns(sourceKeyColumns));
        if (usePrev) {
            indexKeys = ((ChunkSource.WithPrev<Values>) indexKeys).getPrevSource();
        }
        final RowSet indexTableRowSet = usePrev ? indexTable.getRowSet().prev() : indexTable.getRowSet();
        final DataIndex.RowKeyLookup indexLookup = index.rowKeyLookup();

        // This is a lot of parallel iterators over the same RowSet, but this is test/validation code and its easier to
        // read this way.
        try (final WritableRowSet visitedRowSet = RowSetFactory.empty();
                final CloseableIterator<RowSet> indexRowSetsIterator =
                        ChunkedColumnIterator.make(indexRowSets, indexTableRowSet);
                final CloseableIterator<?> indexKeysIterator = ChunkedColumnIterator.make(indexKeys, indexTableRowSet);
                final RowSet.Iterator indexTableRowSetIterator = indexTableRowSet.iterator()) {
            while (indexKeysIterator.hasNext()) {
                final RowSet indexRowSet = indexRowSetsIterator.next(); // Already in prev space if usePrev
                final Object indexKey = indexKeysIterator.next();
                final long indexTableRowKey = indexTableRowSetIterator.nextLong();

                // Validate that we haven't visited any row keys in the index row set yet
                Assert.assertion(!visitedRowSet.overlaps(indexRowSet), "!visitedRowSet.overlaps(indexRowSet)");
                // Validate that all row keys in the index row set are part of the source table's row set
                Assert.assertion(indexRowSet.subsetOf(sourceTableRowSet), "indexRowSet.subsetOf(sourceTableRowSet)");
                visitedRowSet.insert(indexRowSet);

                // Validate that every row the index bucket claims to include has the right key
                try (final CloseableIterator<?> sourceKeysIterator =
                        ChunkedColumnIterator.make(sourceKeys, indexRowSet)) {
                    sourceKeysIterator.forEachRemaining((final Object sourceKey) -> {
                        Assert.assertion(lookupKeysEqual(sourceKey, indexKey), "lookupKeysEqual(sourceKey, indexKey)");
                    });
                }

                // Validate that the index lookup returns the right row key
                Assert.eq(indexTableRowKey, "indexTableRowKey",
                        indexLookup.apply(indexKey, usePrev), "indexLookup.apply(indexKey, usePrev)");
            }
            // Validate that we visit every row
            Assert.equals(sourceTableRowSet, "sourceTableRowSet", visitedRowSet, "visitedRowSet");
        }
    }

    private void validateIndexes() {
        for (String[] indexColumn : indexColumns) {
            validateIndex(sourceTable, indexColumn, false);
            if (sourceTable.isRefreshing()) {
                validateIndex(sourceTable, indexColumn, true);
            }
        }
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        validateIndexes();
        validationCount++;
        System.out.println("Validation Count for " + context + ": " + validationCount);
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail("Failure for context " + context + ": " + originalException.getMessage());
    }
}
