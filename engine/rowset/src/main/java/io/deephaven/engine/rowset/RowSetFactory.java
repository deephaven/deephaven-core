//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.AdaptiveRowSetBuilderRandom;
import io.deephaven.engine.rowset.impl.BasicRowSetBuilderSequential;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

/**
 * Repository of factory methods for constructing {@link WritableRowSet row sets}.
 */
public abstract class RowSetFactory {

    private RowSetFactory() {}

    /**
     * Get an empty {@link WritableRowSet}.
     *
     * @return A new {@link WritableRowSet} containing no rows
     */
    public static WritableRowSet empty() {
        return new WritableRowSetImpl();
    }

    /**
     * Get a {@link WritableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     *
     * @param rowKeys The row keys to include
     * @return A new {@link WritableRowSet} containing the specified row keys
     */
    public static WritableRowSet fromKeys(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return empty();
        }
        if (rowKeys.length == 1) {
            return fromKeys(rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = builderRandom();
        for (final long rowKey : rowKeys) {
            indexBuilder.addKey(rowKey);
        }
        return indexBuilder.build();
    }

    /**
     * Produce a {@link WritableRowSet} containing a single row key. Row keys must be nonnegative numbers.
     *
     * @param rowKey The row key to include
     * @return A new {@link WritableRowSet} containing the specified row key
     */
    public static WritableRowSet fromKeys(final long rowKey) {
        return fromRange(rowKey, rowKey);
    }

    /**
     * Create a {@link WritableRowSet} containing the continuous range [firstRowKey, lastRowKey], or an {@link #empty()
     * empty row set} if {@code lastRowKey < firstRowKey}.
     *
     * @param firstRowKey The first row key in the continuous range
     * @param lastRowKey The last row key in the continuous range
     * @return A new {@link WritableRowSet} containing the specified row key range
     */
    public static WritableRowSet fromRange(final long firstRowKey, final long lastRowKey) {
        if (lastRowKey < firstRowKey) {
            return empty();
        }
        return new WritableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    /**
     * Get a flat {@link WritableRowSet} containing the row key range {@code [0, size)}, or an {@link #empty() empty row
     * set} if {@code size <= 0}.
     *
     * @param size The size of the {@link WritableRowSet} to create
     * @return A flat {@link WritableRowSet} containing the row key range {@code [0, size)} or an {@link #empty() empty
     *         row set} if the {@code size <= 0}
     */
    public static WritableRowSet flat(final long size) {
        return size <= 0 ? empty() : fromRange(0, size - 1);
    }

    /**
     * @return A {@link RowSetBuilderRandom} suitable for inserting row keys and row key ranges in no particular order
     */
    public static RowSetBuilderRandom builderRandom() {
        return new AdaptiveRowSetBuilderRandom();
    }

    /**
     * @return A {@link RowSetBuilderRandom} optimized for inserting row keys and row key ranges sequentially in order
     */
    public static RowSetBuilderSequential builderSequential() {
        return new BasicRowSetBuilderSequential();
    }

    /**
     * Construct a new {@link WritableRowSet} from the union of {@code rowSets}, leaving the inputs untouched.
     *
     * <p>
     * The caller keeps ownership of {@code rowSets} and remains responsible for closing them. Considering only the
     * {@link RowSet#isNonempty() nonempty} inputs: if none exist {@link RowSetFactory#empty()} is returned; if exactly
     * one exists a {@link RowSet#copy() copy} of it is returned.
     *
     * @param rowSets The row sets to union
     * @return A new {@link WritableRowSet} containing every row key in {@code rowSets}
     */
    public static WritableRowSet union(final Collection<? extends RowSet> rowSets) {
        final RowSet[] input = rowSets.toArray(RowSet[]::new);
        return union(input, input.length);
    }

    /**
     * Construct a new {@link WritableRowSet} from the union of {@code rowSets}, leaving the inputs untouched.
     *
     * <p>
     * The array itself is not reordered. See {@link #union(Collection)} for the ownership and result contract.
     *
     * @param rowSets The row sets to union
     * @return A new {@link WritableRowSet} containing every row key in {@code rowSets}
     */
    public static WritableRowSet union(final RowSet... rowSets) {
        return union(rowSets.clone(), rowSets.length);
    }

    /**
     * Constructs a new combined {@link WritableRowSet} from the union of {@code rowSets}.
     *
     * @param rowSets the input row sets
     * @return the new row set
     * @deprecated Use {@link #union(Collection)}.
     */
    @Deprecated(forRemoval = true)
    public static WritableRowSet unionInsert(final Collection<RowSet> rowSets) {
        return union(rowSets);
    }

    /**
     * Union {@code rowSets[0, size)}, which this method owns and may reorder and clear.
     *
     * <p>
     * Row sets are merged in passes. Within a pass an accumulator keeps absorbing the next row set while that row set
     * only appends to it, and while the row set before it duplicated rows the accumulator already held, which means the
     * inputs are covering each other and further insertion stays cheap. A new accumulator is started as soon as the
     * next row set overlaps and the one before it brought nothing the accumulator already had, which is where inserting
     * everything into a single accumulator would become quadratic. Only the most recent insertion counts: a cumulative
     * count would let one early overlapping pair license absorbing an unbounded run of disjoint row sets afterwards.
     * Every accumulator takes at least one partner, so a pass at least halves the count and the merge terminates; where
     * nothing duplicates anything this is a balanced pairwise merge, and where the inputs are disjoint and ordered the
     * first pass consumes all of them by appending.
     *
     * <p>
     * Sorting by first row key is what makes the append case reachable regardless of the order the caller supplies.
     * Cardinality and endpoints are O(1) to query; range counts, which drive the real cost, are linear in the span
     * count and too expensive to consult per decision.
     */
    private static WritableRowSet union(final RowSet[] rowSets, final int size) {
        // Compact away the empty inputs so that first and last row key are meaningful for every remaining row set.
        int count = 0;
        for (int ii = 0; ii < size; ++ii) {
            final RowSet rowSet = rowSets[ii];
            rowSets[ii] = null;
            if (rowSet == null) {
                continue;
            }
            if (rowSet.isEmpty()) {
                continue;
            }
            rowSets[count++] = rowSet;
        }
        if (count == 0) {
            return empty();
        }
        Arrays.sort(rowSets, 0, count, Comparator.comparingLong(RowSet::firstRowKey));

        // Each group but the last takes at least two row sets.
        final WritableRowSet[] groups = new WritableRowSet[(count + 1) / 2];
        int groupCount = 0;
        try {
            int read = 0;
            while (read < count) {
                final WritableRowSet accumulator = rowSets[read].copy();
                rowSets[read++] = null;
                groups[groupCount++] = accumulator;
                long duplicates = 0;
                if (read < count) {
                    duplicates = absorb(accumulator, rowSets[read], appends(accumulator, rowSets[read]));
                    rowSets[read++] = null;
                }
                while (read < count) {
                    final RowSet next = rowSets[read];
                    final boolean appends = appends(accumulator, next);
                    if (!appends && duplicates == 0) {
                        break;
                    }
                    duplicates = absorb(accumulator, next, appends);
                    rowSets[read++] = null;
                }
            }
            while (groupCount > 1) {
                int write = 0;
                int read2 = 0;
                while (read2 < groupCount) {
                    final WritableRowSet accumulator = groups[read2];
                    if (write != read2) {
                        groups[write] = accumulator;
                        groups[read2] = null;
                    }
                    ++write;
                    ++read2;
                    long duplicates = 0;
                    if (read2 < groupCount) {
                        // Every row set in this pass is an accumulator the first pass created, so absorbing one hands
                        // this method the last reference to it. Insertion borrows its argument; closing is ours to do.
                        try (final WritableRowSet next = groups[read2]) {
                            groups[read2++] = null;
                            duplicates = absorb(accumulator, next, appends(accumulator, next));
                        }
                    }
                    while (read2 < groupCount) {
                        final WritableRowSet next = groups[read2];
                        final boolean appends = appends(accumulator, next);
                        if (!appends && duplicates == 0) {
                            break;
                        }
                        try (next) {
                            groups[read2++] = null;
                            duplicates = absorb(accumulator, next, appends);
                        }
                    }
                }
                groupCount = write;
            }
            return groups[0];
        } catch (final RuntimeException | Error e) {
            // Accumulators are parked in groups before anything is inserted into them, so everything this method
            // created is reachable from that array.
            closeAll(groups, groups.length);
            throw e;
        }
    }

    /**
     * Whether inserting {@code next} into {@code accumulator} only extends it past its last row key, which the row set
     * implementations satisfy by splicing rather than by merging range by range.
     */
    private static boolean appends(final RowSet accumulator, final RowSet next) {
        return next.firstRowKey() > accumulator.lastRowKey();
    }

    /**
     * Insert {@code next} into {@code accumulator}.
     *
     * @return The number of rows of {@code next} that {@code accumulator} already held. An append cannot duplicate
     *         anything, so it does not need to be measured.
     */
    private static long absorb(
            final WritableRowSet accumulator,
            final RowSet next,
            final boolean appends) {
        if (appends) {
            accumulator.insert(next);
            return 0;
        }
        final long accumulatorSize = accumulator.size();
        final long nextSize = next.size();
        accumulator.insert(next);
        return nextSize - (accumulator.size() - accumulatorSize);
    }

    private static void closeAll(final RowSet[] rowSets, final int size) {
        for (int ii = 0; ii < size; ++ii) {
            final RowSet rowSet = rowSets[ii];
            if (rowSet != null) {
                rowSets[ii] = null;
                rowSet.close();
            }
        }
    }
}
