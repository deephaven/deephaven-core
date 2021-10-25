/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.remote;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.ColumnDefinition;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.RedirectedColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.RedirectionIndex;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

public class InitialSnapshotTable extends QueryTable {
    protected final Setter<?>[] setters;
    protected int capacity;
    protected TrackingMutableRowSet freeset = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
    protected final TrackingMutableRowSet populatedRows;
    protected final TrackingMutableRowSet[] populatedCells;
    protected WritableSource<?>[] writableSources;
    protected RedirectionIndex redirectionIndex;

    private final BitSet subscribedColumns;

    protected InitialSnapshotTable(Map<String, ? extends ColumnSource<?>> result, WritableSource<?>[] writableSources,
            RedirectionIndex redirectionIndex, BitSet subscribedColumns) {
        super(RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), result);
        this.subscribedColumns = subscribedColumns;
        this.writableSources = writableSources;
        this.setters = new Setter[writableSources.length];
        this.populatedCells = new TrackingMutableRowSet[writableSources.length];
        for (int ii = 0; ii < writableSources.length; ++ii) {
            setters[ii] = getSetter(writableSources[ii]);
            this.populatedCells[ii] = RowSetFactoryImpl.INSTANCE.getRowSetByValues();
        }
        this.redirectionIndex = redirectionIndex;
        this.populatedRows = RowSetFactoryImpl.INSTANCE.getRowSetByValues();
    }

    public BitSet getSubscribedColumns() {
        return subscribedColumns;
    }

    public boolean isSubscribedColumn(int column) {
        return subscribedColumns == null || subscribedColumns.get(column);
    }

    @SuppressWarnings("rawtypes")
    protected Setter<?> getSetter(final WritableSource source) {
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
        } else if (source.getType() == long.class || source.getType() == DBDateTime.class) {
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
        final TrackingMutableRowSet viewPort = snapshot.viewport;
        final TrackingMutableRowSet addedRowSet = snapshot.rowsIncluded;
        final TrackingMutableRowSet newlyPopulated = viewPort == null ? addedRowSet : snapshot.rowSet.subSetForPositions(viewPort);
        if (viewPort != null) {
            newlyPopulated.retain(addedRowSet);
        }

        final TrackingMutableRowSet destinationRowSet = getFreeRows(newlyPopulated.size());

        final TrackingMutableRowSet.Iterator addedIt = addedRowSet.iterator();
        final TrackingMutableRowSet.Iterator destIt = destinationRowSet.iterator();

        long nextInViewport = -1;
        final TrackingMutableRowSet.Iterator populationIt;
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
                final long prevIndex = redirectionIndex.put(addedKey, destIndex);
                Assert.assertion(prevIndex == -1, "prevIndex == -1", prevIndex, "prevIndex");
                if (populationIt != null) {
                    nextInViewport = populationIt.hasNext() ? populationIt.nextLong() : -1;
                }
            }
            arrayIndex++;
        }

        for (int ii = 0; ii < setters.length; ii++) {
            if (subscribedColumns.get(ii) && snapshot.dataColumns[ii] != null) {
                final TrackingMutableRowSet ix = populatedCells[ii];
                ix.insert(newlyPopulated);
            }
        }
        populatedRows.insert(newlyPopulated);

        getIndex().insert(snapshot.rowSet);
    }

    protected TrackingMutableRowSet getFreeRows(long size) {
        boolean needsResizing = false;
        if (capacity == 0) {
            capacity = Integer.highestOneBit((int) Math.max(size * 2, 8));
            freeset = RowSetFactoryImpl.INSTANCE.getFlatRowSet(capacity);
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
                ((WritableSource<?>) source).ensureCapacity(capacity);
            }
        }
        TrackingMutableRowSet result = freeset.subSetByPositionRange(0, (int) size);
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
        WritableSource<?>[] writableSources = new WritableSource[columns.length];
        RedirectionIndex redirectionIndex = RedirectionIndex.FACTORY.createRedirectionIndex(8);
        LinkedHashMap<String, ColumnSource<?>> finalColumns = new LinkedHashMap<>();
        for (int i = 0; i < columns.length; i++) {
            writableSources[i] = ArrayBackedColumnSource.getMemoryColumnSource(0, columns[i].getDataType(),
                    columns[i].getComponentType());
            finalColumns.put(columns[i].getName(),
                    new RedirectedColumnSource<>(redirectionIndex, writableSources[i], 0));
        }
        // This table does not refresh, so we don't need to tell our redirection rowSet or column source to start
        // tracking
        // prev values.

        InitialSnapshotTable initialSnapshotTable =
                new InitialSnapshotTable(finalColumns, writableSources, redirectionIndex, subscribedColumns);
        initialSnapshotTable.processInitialSnapshot(snapshot);
        return initialSnapshotTable;
    }
}
