//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Prints the heap that each storage of {@link SparseStorage} retains for small results, as bytes per source, averaged
 * over many sources of each shape. The storages to measure may be given as a comma separated first argument. Shapes:
 * <ul>
 * <li>Dense: row keys 0 through n - 1</li>
 * <li>Stride64: every 64th row key from 0</li>
 * <li>High: n consecutive row keys from 3 * 2^23, in the upper quarter of a bottom page table level</li>
 * <li>Spread: every 8192nd row key from 0, one leaf in every eight</li>
 * </ul>
 */
public final class SparseSmallMemory {
    private static final String[] STORAGES = {"Sparse", "Redirected", "Adaptive"};
    private static final String[] SHAPES = {"Dense", "Stride64", "High", "Spread"};
    private static final int[] SIZES = {10, 100, 1000, 3000, 10000, 100000};
    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    private SparseSmallMemory() {}

    private static long usedAfterGc() throws InterruptedException {
        long last = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 20; ++attempt) {
            System.gc();
            Thread.sleep(100);
            final long used = MEMORY.getHeapMemoryUsage().getUsed();
            if (Math.abs(last - used) < 64 * 1024) {
                return used;
            }
            last = used;
        }
        return last;
    }

    static RowSet keys(final String shape, final int size) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        switch (shape) {
            case "Dense":
                return RowSetFactory.flat(size);
            case "Stride64":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii * 64);
                }
                return builder.build();
            case "Spread":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii * 8192);
                }
                return builder.build();
            case "High":
                return RowSetFactory.fromRange(3L << 23, (3L << 23) + size - 1);
            default:
                throw new IllegalArgumentException("Unknown shape " + shape);
        }
    }

    public static void main(final String[] args) throws InterruptedException {
        final String[] storages = args.length > 0 ? args[0].split(",") : STORAGES;
        try (final SafeCloseable ignored = TestExecutionContext.createForUnitTests().open()) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
            System.out.printf("%-14s %-10s %8s %8s %14s %10s%n", "storage", "shape", "rows", "sources",
                    "bytes/source", "bytes/row");
            for (final String shape : SHAPES) {
                for (final int size : SIZES) {
                    final int copies = Math.max(1, Math.min(500, 2_000_000 / size));
                    try (final RowSet keys = keys(shape, size)) {
                        for (final String storage : storages) {
                            final long before = usedAfterGc();
                            @SuppressWarnings("unchecked")
                            final WritableColumnSource<Double>[] sources = new WritableColumnSource[copies];
                            for (int ii = 0; ii < copies; ++ii) {
                                sources[ii] = SparseStorage.populate(storage, keys);
                            }
                            final long retained = usedAfterGc() - before;
                            System.out.printf("%-14s %-10s %8d %8d %14.0f %10.2f%n", storage, shape, size, copies,
                                    (double) retained / copies, (double) retained / copies / size);
                            // read the sources after measuring, so that they are retained through the measurement
                            for (final WritableColumnSource<Double> source : sources) {
                                if (source.getDouble(keys.firstRowKey()) != 0) {
                                    throw new IllegalStateException("Unexpected first value");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
