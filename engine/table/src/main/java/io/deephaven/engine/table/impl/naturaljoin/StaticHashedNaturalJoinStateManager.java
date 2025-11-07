//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
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

    protected StaticHashedNaturalJoinStateManager(
            ColumnSource<?>[] keySourcesForErrorMessages,
            NaturalJoinType joinType,
            boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
    }

    public abstract void buildFromLeftSide(
            final Table leftTable,
            final ColumnSource<?>[] leftSources,
            final IntegerArraySource leftHashSlots);

    public abstract void buildFromRightSide(
            final Table rightTable,
            final ColumnSource<?>[] rightSources);

    public abstract void decorateLeftSide(
            final RowSet leftRowSet,
            final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections);

    public abstract void decorateWithRightSide(
            final Table rightTable,
            final ColumnSource<?>[] rightSources);

    public abstract WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable,
            IntegerArraySource leftHashSlots, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable,
            LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromRedirections(QueryTable leftTable,
            RowSet indexTableRowSet, LongArraySource leftRedirections,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromHashSlots(QueryTable leftTable,
            RowSet indexTableRowSet, IntegerArraySource leftHashSlots,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    protected WritableRowRedirection buildIndexedRowRedirection(QueryTable leftTable,
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
                    checkExactMatch(ii, rightSide);
                    final RowSet leftRowSetForKey = leftRowSets.get(indexTableRowSet.get(ii));
                    leftRowSetForKey.forAllRowKeys((long ll) -> innerIndex[(int) ll] = rightSide);
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                for (int ii = 0; ii < rowSetCount; ++ii) {
                    final long rightSide = groupPositionToRightSide.applyAsLong(ii);

                    checkExactMatch(ii, rightSide);
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

                    checkExactMatch(ii, rightSide);
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
