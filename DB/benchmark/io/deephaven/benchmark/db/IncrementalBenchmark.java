package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListenerAdapter;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.select.RollingReleaseFilter;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;

import java.util.function.BiFunction;
import java.util.function.Function;

class IncrementalBenchmark {
    static <R> R incrementalBenchmark(final Function<Table, R> function, final Table inputTable,
        final int steps) {
        final long sizePerStep = Math.max(inputTable.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
            new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        while (filtered.size() < inputTable.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        }

        return result;
    }

    static <R> R rollingBenchmark(final Function<Table, R> function, final Table inputTable,
        final int steps) {
        final long sizePerStep = Math.max(inputTable.size() / steps, 1);
        final RollingReleaseFilter incrementalReleaseFilter =
            new RollingReleaseFilter(sizePerStep * 2, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        for (int currentStep = 0; currentStep <= steps; currentStep++) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);
        }

        return result;
    }

    static <R> R incrementalBenchmark(final Function<Table, R> function, final Table inputTable) {
        return incrementalBenchmark(function, inputTable, 10);
    }

    static <R> R rollingBenchmark(final Function<Table, R> function, final Table inputTable) {
        return rollingBenchmark(function, inputTable, 10);
    }

    static <R> R incrementalBenchmark(final BiFunction<Table, Table, R> function,
        final Table inputTable1, final Table inputTable2) {
        return incrementalBenchmark(function, inputTable1, inputTable2, 0.1, 9);
    }

    static <R> R incrementalBenchmark(final BiFunction<Table, Table, R> function,
        final Table inputTable1, final Table inputTable2, double initialFraction, int steps) {
        final long initialSize1 = (long) (inputTable1.size() * initialFraction);
        final long initialSize2 = (long) (inputTable2.size() * initialFraction);

        final long sizePerStep1 = Math.max((inputTable1.size() - initialSize1) / steps, 1);
        final long sizePerStep2 = Math.max((inputTable2.size() - initialSize2) / steps, 1);

        final IncrementalReleaseFilter incrementalReleaseFilter1 =
            new IncrementalReleaseFilter(initialSize1, sizePerStep1);
        final IncrementalReleaseFilter incrementalReleaseFilter2 =
            new IncrementalReleaseFilter(initialSize2, sizePerStep2);
        final Table filtered1 = inputTable1.where(incrementalReleaseFilter1);
        final Table filtered2 = inputTable2.where(incrementalReleaseFilter2);

        final R result = function.apply(filtered1, filtered2);

        final InstrumentedShiftAwareListenerAdapter failureListener;
        if (result instanceof DynamicTable) {
            failureListener = new InstrumentedShiftAwareListenerAdapter("Failure Listener",
                (DynamicTable) result, false) {
                @Override
                public void onUpdate(Update upstream) {}

                @Override
                public void onFailureInternal(Throwable originalException,
                    UpdatePerformanceTracker.Entry sourceEntry) {
                    originalException.printStackTrace();
                    System.exit(1);
                }
            };
            ((DynamicTable) result).listenForUpdates(failureListener);
        } else {
            failureListener = null;
        }

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        while (filtered1.size() < inputTable1.size() || filtered2.size() < inputTable2.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter1::refresh);
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter2::refresh);
        }

        if (failureListener != null) {
            ((DynamicTable) result).removeUpdateListener(failureListener);
        }

        return result;
    }
}
