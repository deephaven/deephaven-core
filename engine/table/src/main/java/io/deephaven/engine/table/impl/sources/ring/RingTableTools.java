package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction;
import io.deephaven.engine.table.impl.sources.ring.AddsToRingsListener.Init;

import java.util.Objects;

public class RingTableTools {

    /**
     * Constructs a "ring" table, whereby the latest {@code capacity} rows from the {@code parent} are retained and
     * re-indexed by the resulting ring table. Latest is determined solely by the {@link TableUpdate#added()} updates,
     * {@link TableUpdate#removed()} are ignored; and {@link TableUpdate#modified()} / {@link TableUpdate#shifted()} are
     * not expected. In particular, this is a useful construction with
     * {@link io.deephaven.engine.table.impl.StreamTableTools#isStream(Table) stream tables} which do not retain their
     * own data for more than an update cycle.
     *
     * @param parent the parent
     * @param capacity the capacity
     * @param initialize if the resulting table should source initial data from the snapshot of {@code parent}
     * @return the ring table
     */
    public static Table of(Table parent, int capacity, boolean initialize) {
        return QueryPerformanceRecorder.withNugget("RingTableTools.of", () -> {
            final BaseTable baseTable = (BaseTable) parent.coalesce();
            final SwapListener swapListener = baseTable.createSwapListenerIfRefreshing(SwapListener::new);
            return new RingTableSnapshotFunction(parent, capacity, initialize, swapListener).constructResults();
        });
    }

    private static class RingTableSnapshotFunction implements SnapshotFunction {
        private final Table parent;
        private final int capacity;
        private final boolean initialize;
        private final SwapListener swapListener;

        private Table results;

        public RingTableSnapshotFunction(Table parent, int capacity, boolean initialize, SwapListener swapListener) {
            this.parent = Objects.requireNonNull(parent);
            this.capacity = capacity;
            this.initialize = initialize;
            this.swapListener = Objects.requireNonNull(swapListener);
        }

        public Table constructResults() {
            ConstructSnapshot.callDataSnapshotFunction(RingTableSnapshotFunction.class.getSimpleName(),
                    swapListener.makeSnapshotControl(), this);
            return Objects.requireNonNull(results);
        }

        @Override
        public boolean call(boolean usePrev, long beforeClockValue) {
            final Init init = !initialize ? Init.NONE : usePrev ? Init.FROM_PREVIOUS : Init.FROM_CURRENT;
            results = AddsToRingsListener.of(swapListener, parent, capacity, init);
            return true;
        }
    }
}
