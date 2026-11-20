//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.SafeCloseable;

/**
 * Prints the heap that each storage of {@link SparseStorage} retains for small results, in the shapes of
 * {@link SparsePatterns#smallKeys(String, int)}, as bytes per source, averaged over many sources of each shape. The
 * storages to measure may be given as a comma separated first argument. Shapes over many page table levels retain far
 * more in some storages, so fewer sources of them are measured.
 */
public final class SparseSmallMemory {
    private static final String[] STORAGES = {"Sparse", "Redirected", "Adaptive", "AdaptiveFull"};
    private static final String[] SHAPES = {"Dense", "Stride64", "High", "Spread", "Level2s", "Regions", "FarRegions"};
    private static final int[] SIZES = {10, 100, 1000, 3000, 10000, 100000};

    private SparseSmallMemory() {}

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws InterruptedException {
        final String[] storages = args.length > 0 ? args[0].split(",") : STORAGES;
        try (final SafeCloseable ignored = SparseStorage.openContext()) {
            System.out.printf("%-14s %-10s %8s %8s %14s %10s%n", "storage", "shape", "rows", "sources",
                    "bytes/source", "bytes/row");
            for (final String shape : SHAPES) {
                for (final int size : SIZES) {
                    final int maxCopies = shape.equals("Level2s") || shape.endsWith("Regions") ? 20 : 500;
                    final int copies = Math.max(1, Math.min(maxCopies, 1_000_000 / size));
                    try (final RowSet keys = SparsePatterns.smallKeys(shape, size)) {
                        for (final String storage : storages) {
                            final long retained = SparseStorage.retainedHeap(() -> {
                                final WritableColumnSource<Double>[] sources = new WritableColumnSource[copies];
                                for (int ii = 0; ii < copies; ++ii) {
                                    sources[ii] = SparseStorage.populate(storage, keys);
                                }
                                return sources;
                            }, keys);
                            System.out.printf("%-14s %-10s %8d %8d %14.0f %10.2f%n", storage, shape, size, copies,
                                    (double) retained / copies, (double) retained / copies / size);
                        }
                    }
                }
            }
        }
    }
}
