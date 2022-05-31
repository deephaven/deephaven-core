/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.updategraph.LogicalClock;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.Function;

public class TrackingWritableRowSetImpl extends WritableRowSetImpl implements TrackingWritableRowSet {

    private transient OrderedLongSet prevInnerSet;
    /**
     * Protects prevImpl. Only updated in checkPrev() and initializePreviousValue() (this later supposed to be used only
     * right after the constructor, in special cases).
     */
    private transient volatile long changeTimeStep;

    private transient volatile TrackingRowSet.Indexer indexer;

    public TrackingWritableRowSetImpl() {
        this(OrderedLongSet.EMPTY);
    }

    public TrackingWritableRowSetImpl(final OrderedLongSet innerSet) {
        super(innerSet);
        this.prevInnerSet = OrderedLongSet.EMPTY;
        changeTimeStep = -1;
    }

    private OrderedLongSet checkAndGetPrev() {
        if (LogicalClock.DEFAULT.currentStep() == changeTimeStep) {
            return prevInnerSet;
        }
        synchronized (this) {
            final long currentClockStep = LogicalClock.DEFAULT.currentStep();
            if (currentClockStep == changeTimeStep) {
                return prevInnerSet;
            }
            prevInnerSet.ixRelease();
            prevInnerSet = getInnerSet().ixCowRef();
            changeTimeStep = currentClockStep;
            return prevInnerSet;
        }
    }

    @Override
    public <INDEXER_TYPE extends TrackingRowSet.Indexer> INDEXER_TYPE indexer(
            @NotNull final Function<TrackingRowSet, INDEXER_TYPE> indexerFactory) {
        // noinspection unchecked
        INDEXER_TYPE localIndexer = (INDEXER_TYPE) indexer;
        if (localIndexer == null) {
            synchronized (this) {
                // noinspection unchecked
                localIndexer = (INDEXER_TYPE) indexer;
                if (localIndexer == null) {
                    indexer = localIndexer = indexerFactory.apply(this);
                }
            }
        }
        return localIndexer;
    }

    @Override
    public void preMutationHook() {
        checkAndGetPrev();
    }

    @Override
    protected void postMutationHook() {
        final TrackingRowSet.Indexer localIndexer = indexer;
        if (localIndexer != null) {
            localIndexer.rowSetChanged();
        }
    }

    @Override
    public TrackingWritableRowSet toTracking() {
        throw new UnsupportedOperationException("Already tracking! You must copy() before toTracking()");
    }

    @Override
    public void close() {
        prevInnerSet.ixRelease();
        prevInnerSet = null; // Force NPE on use after tracking
        changeTimeStep = -1;
        indexer = null;
        super.close();
    }

    @Override
    public void initializePreviousValue() {
        prevInnerSet.ixRelease();
        prevInnerSet = OrderedLongSet.EMPTY;
        changeTimeStep = -1;
    }

    @Override
    public long sizePrev() {
        return checkAndGetPrev().ixCardinality();
    }

    @Override
    public WritableRowSet copyPrev() {
        return new WritableRowSetImpl(checkAndGetPrev().ixCowRef());
    }

    @Override
    public long getPrev(final long rowPosition) {
        if (rowPosition < 0) {
            return -1;
        }
        return checkAndGetPrev().ixGet(rowPosition);
    }

    @Override
    public long findPrev(long rowKey) {
        return checkAndGetPrev().ixFind(rowKey);
    }

    @Override
    public long firstRowKeyPrev() {
        return checkAndGetPrev().ixFirstKey();
    }

    @Override
    public long lastRowKeyPrev() {
        return checkAndGetPrev().ixLastKey();
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException {
        super.readExternal(in);
        initializePreviousValue();
    }
}
