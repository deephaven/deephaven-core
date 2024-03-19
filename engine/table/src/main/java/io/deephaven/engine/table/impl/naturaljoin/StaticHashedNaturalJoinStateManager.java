//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.StaticNaturalJoinStateManager;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;

import java.util.function.LongUnaryOperator;

public abstract class StaticHashedNaturalJoinStateManager extends StaticNaturalJoinStateManager {

    protected StaticHashedNaturalJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public abstract void buildFromLeftSide(final Table leftTable, ColumnSource<?>[] leftSources,
            final IntegerArraySource leftHashSlots);

    public abstract void buildFromRightSide(final Table rightTable, ColumnSource<?>[] rightSources);

    public abstract void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections);

    public abstract void decorateWithRightSide(Table rightTable, ColumnSource<?>[] rightSources);

    public abstract WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch,
            IntegerArraySource leftHashSlots, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable, boolean exactMatch,
            LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromRedirections(QueryTable leftTable,
            boolean exactMatch, RowSet indexTableRowSet, LongArraySource leftRedirections,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromHashSlots(QueryTable leftTable,
            boolean exactMatch, RowSet indexTableRowSet, IntegerArraySource leftHashSlots,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    protected WritableRowRedirection buildIndexedRowRedirection(QueryTable leftTable, boolean exactMatch,
            RowSet indexTableRowSet, LongUnaryOperator groupPositionToRightSide, ColumnSource<RowSet> leftRowSets,
            JoinControl.RedirectionType redirectionType) {
        final int rowSetCount = indexTableRowSet.intSize();
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat()) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                for (int ii = 0; ii < rowSetCount; ++ii) {
                    final long rightSide = groupPositionToRightSide.applyAsLong(ii);
                    checkExactMatch(exactMatch, ii, rightSide);
                    final RowSet leftRowSetForKey = leftRowSets.get(indexTableRowSet.get(ii));
                    leftRowSetForKey.forAllRowKeys((long ll) -> innerIndex[(int) ll] = rightSide);
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                for (int ii = 0; ii < rowSetCount; ++ii) {
                    final long rightSide = groupPositionToRightSide.applyAsLong(ii);

                    checkExactMatch(exactMatch, ii, rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        final RowSet leftRowSetForKey = leftRowSets.get(indexTableRowSet.get(ii));
                        leftRowSetForKey.forAllRowKeys((long ll) -> sparseRedirections.set(ll, rightSide));
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());

                for (int ii = 0; ii < rowSetCount; ++ii) {
                    final long rightSide = groupPositionToRightSide.applyAsLong(ii);

                    checkExactMatch(exactMatch, ii, rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        final RowSet leftRowSetForKey = leftRowSets.get(indexTableRowSet.get(ii));
                        leftRowSetForKey.forAllRowKeys((long ll) -> rowRedirection.put(ll, rightSide));
                    }
                }

                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    public void errorOnDuplicates(IntegerArraySource leftHashSlots, long size,
            LongUnaryOperator indexPositionToRightSide, LongUnaryOperator firstLeftKey) {
        for (int ii = 0; ii < size; ++ii) {
            final long rightSide = indexPositionToRightSide.applyAsLong(ii);
            if (rightSide == DUPLICATE_RIGHT_VALUE) {
                throw new IllegalStateException("Natural Join found duplicate right key for "
                        + extractKeyStringFromSourceTable(firstLeftKey.applyAsLong(ii)));
            }
        }
    }

    public void errorOnDuplicatesIndexed(IntegerArraySource leftHashSlots, long size,
            ObjectArraySource<RowSet> rowSetSource) {
        throw new UnsupportedOperationException();
    }

    public void errorOnDuplicatesSingle(IntegerArraySource leftHashSlots, long size, RowSet rowSet) {
        throw new UnsupportedOperationException();
    }
}
