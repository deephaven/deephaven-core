//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.updategraph.LogicalClock;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.Function;

public class TrackingWritableRowSetImpl extends WritableRowSetImpl implements TrackingWritableRowSet {

    private final LogicalClock clock;
    private final WritableRowSetImpl prev;

    private transient OrderedLongSet prevInnerSet;

    /**
     * Protects {@link #prevInnerSet}. Only updated in checkAndGetPrev() and initializePreviousValue().
     */
    private transient volatile long changeTimeStep;

    private transient volatile TrackingRowSet.Indexer indexer;

    public TrackingWritableRowSetImpl() {
        this(OrderedLongSet.EMPTY);
    }

    public TrackingWritableRowSetImpl(final OrderedLongSet innerSet) {
        super(innerSet);
        clock = ExecutionContext.getContext().getUpdateGraph().clock();
        prev = new UnmodifiableRowSetImpl();
        prevInnerSet = OrderedLongSet.EMPTY;
        changeTimeStep = -1;
    }

    private OrderedLongSet checkAndGetPrev() {
        if (clock.currentStep() == changeTimeStep) {
            return prevInnerSet;
        }
        synchronized (this) {
            final long currentClockStep = clock.currentStep();
            if (currentClockStep == changeTimeStep) {
                return prevInnerSet;
            }
            prevInnerSet.ixRelease();
            prevInnerSet = getInnerSet().ixCowRef();
            prev.assign(prevInnerSet.ixCowRef());
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
    public <INDEXER_TYPE extends Indexer> INDEXER_TYPE indexer() {
        // noinspection unchecked
        return (INDEXER_TYPE) indexer;
    }

    @Override
    protected void preMutationHook() {
        checkAndGetPrev();
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
    public RowSet prev() {
        checkAndGetPrev();
        return prev;
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

    /**
     * An unmodifiable view of a {@link WritableRowSetImpl}.
     */
    private static class UnmodifiableRowSetImpl extends WritableRowSetImpl {

        public UnmodifiableRowSetImpl() {}

        @Override
        protected final void preMutationHook() {
            throw new UnsupportedOperationException("Unmodifiable view must never be mutated");
        }

        @Override
        protected final void postMutationHook() {
            throw new UnsupportedOperationException("Unmodifiable view must never be mutated");
        }

        @Override
        public final void close() {
            throw new UnsupportedOperationException("Unmodifiable view must never be closed");
        }
    }
}
