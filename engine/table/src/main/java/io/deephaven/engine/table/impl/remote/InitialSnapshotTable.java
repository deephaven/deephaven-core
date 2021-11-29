/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.remote;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.util.*;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

public class InitialSnapshotTable extends QueryTable {
    protected final Setter<?>[] setters;
    protected int capacity;
    protected WritableRowSet freeset = RowSetFactory.empty();
    protected final WritableRowSet populatedRows;
    protected final WritableRowSet[] populatedCells;
    protected WritableColumnSource<?>[] writableSources;
    protected WritableRowRedirection rowRedirection;

    private final BitSet subscribedColumns;

    protected InitialSnapshotTable(Map<String, ? extends ColumnSource<?>> result,
            WritableColumnSource<?>[] writableSources,
            WritableRowRedirection rowRedirection, BitSet subscribedColumns) {
        super(RowSetFactory.empty().toTracking(), result);
        this.subscribedColumns = subscribedColumns;
        this.writableSources = writableSources;
        this.setters = new Setter[writableSources.length];
        this.populatedCells = new WritableRowSet[writableSources.length];
        for (int ii = 0; ii < writableSources.length; ++ii) {
            setters[ii] = getSetter(writableSources[ii]);
            this.populatedCells[ii] = RowSetFactory.fromKeys();
        }
        this.rowRedirection = rowRedirection;
        this.populatedRows = RowSetFactory.fromKeys();
    }

    public BitSet getSubscribedColumns() {
        return subscribedColumns;
    }

    public boolean isSubscribedColumn(int column) {
        return subscribedColumns == null || subscribedColumns.get(column);
    }

    @SuppressWarnings("rawtypes")
    protected Setter<?> getSetter(final WritableColumnSource source) {
        if (source.getType() == byte.class) {
            return (Setter<byte[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == char.class) {
            return (Setter<char[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == double.class) {
            return (Setter<double[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == float.class) {
            return (Setter<float[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == int.class) {
            return (Setter<int[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == long.class || source.getType() == DateTime.class) {
            return (Setter<long[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == short.class) {
            return (Setter<short[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else if (source.getType() == Boolean.class) {
            return (Setter<byte[]>) (array, arrayIndex, destIndex) -> source.set(destIndex, array[arrayIndex]);
        } else {
            return (Setter<Object[]>) (array, arrayIndex, destIndex) -> {
                // noinspection unchecked
                source.set(destIndex, array[arrayIndex]);
            };
        }
    }

    protected void processInitialSnapshot(InitialSnapshot snapshot) {
        final RowSet viewPort = snapshot.viewport;
        final RowSet addedRowSet = snapshot.rowsIncluded;
        try (final WritableRowSet newlyPopulated =
                viewPort == null ? addedRowSet.copy() : snapshot.rowSet.subSetForPositions(viewPort)) {
            if (viewPort != null) {
                newlyPopulated.retain(addedRowSet);
            }

            final RowSet destinationRowSet = getFreeRows(newlyPopulated.size());

            final RowSet.Iterator addedIt = addedRowSet.iterator();
            final RowSet.Iterator destIt = destinationRowSet.iterator();

            long nextInViewport = -1;
            final RowSet.Iterator populationIt;
            if (viewPort == null) {
                populationIt = null;
            } else {
                populationIt = newlyPopulated.iterator();
                if (populationIt.hasNext()) {
                    nextInViewport = populationIt.nextLong();
                }
            }

            int arrayIndex = 0;
            while (addedIt.hasNext()) {
                final long addedKey = addedIt.nextLong();
                final boolean found = viewPort == null || addedKey == nextInViewport;

                if (found) {
                    final long destIndex = destIt.nextLong();
                    for (int ii = 0; ii < setters.length; ii++) {
                        if (subscribedColumns.get(ii) && snapshot.dataColumns[ii] != null) {
                            // noinspection unchecked,rawtypes
                            ((Setter) setters[ii]).set(snapshot.dataColumns[ii], arrayIndex, destIndex);
                        }
                    }
                    final long prevIndex = rowRedirection.put(addedKey, destIndex);
                    Assert.assertion(prevIndex == -1, "prevIndex == -1", prevIndex, "prevIndex");
                    if (populationIt != null) {
                        nextInViewport = populationIt.hasNext() ? populationIt.nextLong() : -1;
                    }
                }
                arrayIndex++;
            }

            for (int ii = 0; ii < setters.length; ii++) {
                if (subscribedColumns.get(ii) && snapshot.dataColumns[ii] != null) {
                    final WritableRowSet ix = populatedCells[ii];
                    ix.insert(newlyPopulated);
                }
            }
            populatedRows.insert(newlyPopulated);
        }
        getRowSet().writableCast().insert(snapshot.rowSet);
    }

    protected RowSet getFreeRows(long size) {
        boolean needsResizing = false;
        if (capacity == 0) {
            capacity = Integer.highestOneBit((int) Math.max(size * 2, 8));
            freeset = RowSetFactory.flat(capacity);
            needsResizing = true;
        } else if (freeset.size() < size) {
            int allocatedSize = (int) (capacity - freeset.size());
            int prevCapacity = capacity;
            do {
                capacity *= 2;
            } while ((capacity - allocatedSize) < size);
            freeset.insertRange(prevCapacity, capacity - 1);
            needsResizing = true;
        }
        if (needsResizing) {
            for (ColumnSource<?> source : getColumnSources()) {
                ((WritableColumnSource<?>) source).ensureCapacity(capacity);
            }
        }
        RowSet result = freeset.subSetByPositionRange(0, (int) size);
        Assert.assertion(result.size() == size, "result.size() == size");
        freeset = freeset.subSetByPositionRange((int) size, (int) freeset.size());
        return result;
    }

    protected interface Setter<T> {
        void set(T array, int arrayIndex, long destIndex);
    }

    public static InitialSnapshotTable setupInitialSnapshotTable(Table originalTable, InitialSnapshot snapshot) {
        return setupInitialSnapshotTable(originalTable.getDefinition(), snapshot);
    }

    public static InitialSnapshotTable setupInitialSnapshotTable(Table originalTable, InitialSnapshot snapshot,
            BitSet subscribedColumns) {
        return setupInitialSnapshotTable(originalTable.getDefinition(), snapshot, subscribedColumns);
    }

    public static InitialSnapshotTable setupInitialSnapshotTable(TableDefinition definition, InitialSnapshot snapshot) {
        BitSet allColumns = new BitSet(definition.getColumns().length);
        allColumns.set(0, definition.getColumns().length);
        return setupInitialSnapshotTable(definition, snapshot, allColumns);
    }

    public static InitialSnapshotTable setupInitialSnapshotTable(TableDefinition definition, InitialSnapshot snapshot,
            BitSet subscribedColumns) {
        final ColumnDefinition<?>[] columns = definition.getColumns();
        WritableColumnSource<?>[] writableSources = new WritableColumnSource[columns.length];
        WritableRowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(8);
        LinkedHashMap<String, ColumnSource<?>> finalColumns = new LinkedHashMap<>();
        for (int i = 0; i < columns.length; i++) {
            writableSources[i] = ArrayBackedColumnSource.getMemoryColumnSource(0, columns[i].getDataType(),
                    columns[i].getComponentType());
            finalColumns.put(columns[i].getName(),
                    new WritableRedirectedColumnSource<>(rowRedirection, writableSources[i], 0));
        }
        // This table does not run, so we don't need to tell our row redirection or column source to start
        // tracking
        // prev values.

        InitialSnapshotTable initialSnapshotTable =
                new InitialSnapshotTable(finalColumns, writableSources, rowRedirection, subscribedColumns);
        initialSnapshotTable.processInitialSnapshot(snapshot);
        return initialSnapshotTable;
    }
}
