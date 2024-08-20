//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot.SnapshotFunction;
import io.deephaven.engine.table.impl.sources.ring.AddsToRingsListener.Init;
import io.deephaven.util.SafeCloseable;

import java.util.Objects;

public class RingTableTools {

    /**
     * Equivalent to {@code of(parent, capacity, true)}.
     *
     * @param parent the parent
     * @param capacity the capacity
     * @return the ring table
     * @see #of(Table, int, boolean)
     **/
    public static Table of(Table parent, int capacity) {
        return of(parent, capacity, true);
    }

    /**
     * Constructs a "ring" table, whereby the latest {@code capacity} rows from the {@code parent} are retained and
     * re-indexed by the resulting ring table. Latest is determined solely by the {@link TableUpdate#added()} updates,
     * {@link TableUpdate#removed()} are ignored; and {@link TableUpdate#modified()} / {@link TableUpdate#shifted()} are
     * not expected. In particular, this is a useful construction with {@link BlinkTableTools#isBlink(Table) blink
     * tables} which do not retain their own data for more than an update cycle.
     *
     * @param parent the parent
     * @param capacity the capacity
     * @param initialize if the resulting table should source initial data from the snapshot of {@code parent}
     * @return the ring table
     */
    public static Table of(Table parent, int capacity, boolean initialize) {
        Require.leq(capacity, "capacity", ArrayUtil.MAX_ARRAY_SIZE);
        return QueryPerformanceRecorder.withNugget("RingTableTools.of", () -> {
            final BaseTable<?> baseTable = (BaseTable<?>) parent.coalesce();
            final OperationSnapshotControl snapshotControl =
                    baseTable.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);
            return new RingTableSnapshotFunction(baseTable, capacity, initialize, snapshotControl).constructResults();
        });
    }

    /**
     * Constructs a "ring" table, where the next-power-of-2 {@code capacity} from the {@code parent} are retained and
     * re-indexed, with an additional {@link Table#tail(long)} to restructure for {@code capacity}.
     *
     * <p>
     * Logically equivalent to {@code of(parent, MathUtil.roundUpPowerOf2(capacity), initialize).tail(capacity)}.
     *
     * <p>
     * This setup may be useful when consumers need to maximize random access fill speed from a ring table.
     *
     * @param parent the parent
     * @param capacity the capacity
     * @param initialize if the resulting table should source initial data from the snapshot of {@code parent}
     * @return the ring table
     * @see #of(Table, int, boolean)
     * @see MathUtil#roundUpPowerOf2(int)
     */
    public static Table of2(Table parent, int capacity, boolean initialize) {
        Require.leq(capacity, "capacity", MathUtil.MAX_POWER_OF_2);
        return QueryPerformanceRecorder.withNugget("RingTableTools.of2", () -> {
            final int capacityPowerOf2 = MathUtil.roundUpPowerOf2(capacity);
            final BaseTable<?> baseTable = (BaseTable<?>) parent.coalesce();
            final OperationSnapshotControl snapshotControl =
                    baseTable.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);
            final Table tablePowerOf2 =
                    new RingTableSnapshotFunction(baseTable, capacityPowerOf2, initialize, snapshotControl)
                            .constructResults();
            return capacityPowerOf2 == capacity ? tablePowerOf2 : tablePowerOf2.tail(capacity);
        });
    }

    private static class RingTableSnapshotFunction implements SnapshotFunction {
        private final Table parent;
        private final int capacity;
        private final boolean initialize;
        private final OperationSnapshotControl snapshotControl;

        private Table results;

        public RingTableSnapshotFunction(
                Table parent, int capacity, boolean initialize, OperationSnapshotControl snapshotControl) {
            this.parent = Objects.requireNonNull(parent);
            this.capacity = capacity;
            this.initialize = initialize;
            this.snapshotControl = snapshotControl;
        }

        public Table constructResults() {
            try (final SafeCloseable ignored =
                    ExecutionContext.getContext().withUpdateGraph(parent.getUpdateGraph()).open()) {
                BaseTable.initializeWithSnapshot(
                        RingTableSnapshotFunction.class.getSimpleName(), snapshotControl, this);
            }
            return Objects.requireNonNull(results);
        }

        @Override
        public boolean call(boolean usePrev, long beforeClockValue) {
            final Init init = !initialize ? Init.NONE : usePrev ? Init.FROM_PREVIOUS : Init.FROM_CURRENT;
            results = AddsToRingsListener.of(snapshotControl, parent, capacity, init);
            return true;
        }
    }
}
