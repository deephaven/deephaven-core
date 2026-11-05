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
 * <li>Level2s: every 2^25th row key from 0, one in each bottom page table level</li>
 * <li>Regions: n / 8 consecutive row keys at the start of each of eight regions 2^43 row keys apart, as a regioned
 * source numbers its rows</li>
 * <li>FarRegions: n / 2 consecutive row keys at the start of region 0 and of region 100000, so that the top page table
 * level's children are far apart</li>
 * </ul>
 * Shapes over many page table levels retain far more in some storages, so fewer sources of them are measured.
 */
public final class SparseSmallMemory {
    private static final String[] STORAGES = {"Sparse", "Redirected", "Adaptive"};
    private static final String[] SHAPES = {"Dense", "Stride64", "High", "Spread", "Level2s", "Regions", "FarRegions"};
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
            case "Level2s":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii << 25);
                }
                return builder.build();
            case "FarRegions":
                builder.appendRange(0, (size + 1) / 2 - 1);
                if (size / 2 > 0) {
                    builder.appendRange(100_000L << 43, (100_000L << 43) + size / 2 - 1);
                }
                return builder.build();
            case "Regions":
                for (long region = 0; region < 8; ++region) {
                    final long first = region << 43;
                    final long count = size / 8 + (region < size % 8 ? 1 : 0);
                    if (count > 0) {
                        builder.appendRange(first, first + count - 1);
                    }
                }
                return builder.build();
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
                    final int maxCopies = shape.equals("Level2s") || shape.endsWith("Regions") ? 20 : 500;
                    final int copies = Math.max(1, Math.min(maxCopies, 1_000_000 / size));
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
