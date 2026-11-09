//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.util.SafeCloseable;

/**
 * Prints the heap that each storage of {@link SparseStorage} retains after populating each pattern of
 * {@link SparsePatterns}, as bytes per row. "Redirected2" is two redirected columns that share one redirection,
 * reported per column. Arguments optionally restrict the storages, then the patterns, as comma-separated lists.
 */
public final class SparseStorageMemory {
    private static final String[] STORAGES = {"Sparse", "Redirected", "Redirected2", "Adaptive", "AdaptiveFull"};
    private static final String[] PATTERNS = {"Dense", "Stride8", "Stride64", "Stride512", "Runs64", "Random10",
            "Random1", "Clustered"};

    private SparseStorageMemory() {}

    /**
     * Populate two redirected columns that share one redirection, as the columns of one redirected update() do.
     */
    @SuppressWarnings("unchecked")
    private static WritableColumnSource<Double>[] populateShared(final RowSet keys) {
        final WritableRedirectedColumnSource<Double> first =
                (WritableRedirectedColumnSource<Double>) SparseStorage.populate("Redirected", keys);
        final DoubleArraySource secondDense = new DoubleArraySource();
        secondDense.ensureCapacity(keys.size());
        final WritableColumnSource<Double> second =
                WritableRedirectedColumnSource.maybeRedirect(first.getRowRedirection(), secondDense, keys.size());
        SparseStorage.fill(second, keys, null);
        return new WritableColumnSource[] {first, second};
    }

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws InterruptedException {
        final String[] storages = args.length > 0 ? args[0].split(",") : STORAGES;
        final String[] patterns = args.length > 1 ? args[1].split(",") : PATTERNS;
        try (final SafeCloseable ignored = SparseStorage.openContext()) {
            System.out.printf("%-16s %-10s %10s %12s %10s%n", "storage", "pattern", "rows", "bytes", "bytes/row");
            for (final String pattern : patterns) {
                try (final RowSet keys = SparsePatterns.keys(pattern)) {
                    for (final String storage : storages) {
                        final boolean shared = storage.equals("Redirected2");
                        final long retained = SparseStorage.retainedHeap(() -> shared
                                ? populateShared(keys)
                                : new WritableColumnSource[] {SparseStorage.populate(storage, keys)}, keys);
                        System.out.printf("%-16s %-10s %10d %12d %10.2f%n", storage, pattern, keys.size(), retained,
                                (double) retained / ((shared ? 2 : 1) * keys.size()));
                    }
                }
            }
        }
    }
}
