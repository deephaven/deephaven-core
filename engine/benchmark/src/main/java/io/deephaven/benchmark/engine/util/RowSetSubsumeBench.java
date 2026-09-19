//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmark.engine.util.RowSetShapes.Representation;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

/**
 * {@link WritableRowSet#subsume} against {@link WritableRowSet#insert(RowSet)} over the same pair of sets.
 *
 * <p>
 * The two produce the same keys; what differs is which side may be edited. {@code insert} has to leave its argument
 * alone, so the target is always the receiver, whereas {@code subsume} may make the incoming set the receiver and hand
 * the result back. The cases where that is worth doing are the ones the parameters here lay out: the incoming set being
 * the larger side, so that the smaller one is the one placed entry by entry; and {@link Position#PREPEND}, where the
 * incoming keys all fall below the target's, so appending the target onto the incoming set replaces a walk into the
 * middle of the target. {@link Position#APPEND} is the case {@code insert} already gets right, and is here to show
 * {@code subsume} does not give it up.
 *
 * <p>
 * Both methods close the incoming set, because the pattern this replaces closes it:
 *
 * <pre>
 * try (final WritableRowSet incoming = ...) {
 *     target.insert(incoming);
 * }
 * </pre>
 *
 * Closing is where the difference would otherwise hide. After {@code insert} the close gives back a set that still
 * holds its keys; after {@code subsume} it gives back an empty one, the keys having gone to the target already. Timing
 * the insert alone would charge that release to {@code subsume} and not to {@code insert}, when it is the caller's
 * either way, and would make {@code subsume} look slower on the shapes where the two do the same work.
 *
 * <p>
 * Each side is a {@link Side}, a representation and an entry count. Every side holds single keys one to a block, so an
 * {@link RspBitmap} side holds one span per key and a {@link SortedRanges} side one array position per key, and the
 * entry count is what an insert's work is proportional to. The sizes run from 64 entries to 256K, and past
 * {@link SortedRanges#MAX_CAPACITY} only {@link RspBitmap} appears -- a set that outgrows the packed array becomes an
 * {@link RspBitmap}, so a large {@link SortedRanges} is not a shape the engine can be in.
 *
 * <p>
 * Both sides are rebuilt for every invocation, since both operations mutate: the copies are made in the invocation
 * setup and so are outside what is timed, but they are the same two copies on both paths either way.
 *
 * <p>
 * Two forks, because one fork's error bars only describe the scatter between iterations of a single JIT compilation of
 * a single JVM. On the shortest shapes here that understates the run-to-run spread by an order of magnitude, and a
 * single fork reports differences of a few tens of nanoseconds as though they were outside the error when re-measuring
 * shows them to be zero.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2)
public class RowSetSubsumeBench {

    /** Where the incoming set's keys lie relative to the target's. */
    public enum Position {
        /** Wholly above the target's last key, the case both operations can do as an append. */
        APPEND,
        /** Wholly below the target's first key, which only {@code subsume} can turn into an append. */
        PREPEND,
        /** Spread over the same key range as the target, a block off so the two never share one. */
        INTERLEAVED
    }

    /**
     * One side of the pair: how its keys are held and how many entries it holds.
     * <p>
     * {@link SortedRanges} appears only at the two smaller sizes, because outgrowing its packed array is what turns a
     * row set into an {@link RspBitmap}. That ceiling is {@link SortedRanges#INT_SPARSE_MAX_CAPACITY} while the keys
     * fit int offsets and {@link SortedRanges#LONG_SPARSE_MAX_CAPACITY} once they do not, and the widest shape here --
     * interleaved against 256K blocks -- spreads them past what an int holds, so the lower of the two is the size that
     * every shape can be built at.
     */
    public enum Side {
        SR_64(Representation.SORTED_RANGES, 64), SR_4K(Representation.SORTED_RANGES, 4096), RSP_64(Representation.RSP,
                64), RSP_4K(Representation.RSP, 4096), RSP_256K(Representation.RSP, 262144);

        private final Representation representation;
        private final int entries;

        Side(final Representation representation, final int entries) {
            this.representation = representation;
            this.entries = entries;
        }
    }

    /** The shape of the set that receives when {@code insert} is used, and may not when {@code subsume} is. */
    @Param
    private Side targetSide;

    /** The shape of the set whose keys are being folded in, and which is closed either way. */
    @Param
    private Side incomingSide;

    @Param
    private Position position;

    private OrderedLongSet targetTemplate;
    private OrderedLongSet incomingTemplate;

    private WritableRowSet target;
    private WritableRowSet incoming;

    @Setup(Level.Trial)
    public void setupTrial() {
        final int targetEntries = targetSide.entries;
        final int incomingEntries = incomingSide.entries;
        final long[] targetKeys;
        final long[] incomingKeys;
        switch (position) {
            case APPEND:
                targetKeys = spread(0, targetEntries, targetEntries);
                incomingKeys = spread(2L * targetEntries, incomingEntries, incomingEntries);
                break;
            case PREPEND:
                incomingKeys = spread(0, incomingEntries, incomingEntries);
                targetKeys = spread(2L * incomingEntries, targetEntries, targetEntries);
                break;
            case INTERLEAVED: {
                // Both sides cover the same blocks; the one with fewer keys leaves gaps rather than stopping short.
                final int steps = Math.max(targetEntries, incomingEntries);
                targetKeys = spread(0, steps, targetEntries);
                incomingKeys = spread(1, steps, incomingEntries);
                break;
            }
            default:
                throw new IllegalStateException("unhandled position " + position);
        }
        targetTemplate = RowSetShapes.impl(targetSide.representation, targetKeys);
        incomingTemplate = RowSetShapes.impl(incomingSide.representation, incomingKeys);

        final long expected;
        try (final WritableRowSet a = new WritableRowSetImpl(deepCopy(targetTemplate));
                final WritableRowSet b = new WritableRowSetImpl(deepCopy(incomingTemplate))) {
            a.insert(b);
            expected = a.size();
        }
        try (final WritableRowSet a = new WritableRowSetImpl(deepCopy(targetTemplate));
                final WritableRowSet b = new WritableRowSetImpl(deepCopy(incomingTemplate))) {
            a.subsume(b);
            if (a.size() != expected || !b.isEmpty()) {
                throw new IllegalStateException("subsume disagrees with insert for " + position);
            }
        }
        // Which direction subsume chose is not printed here: the estimator that decides it is package private to
        // the row set implementation, and the shape plus the entry counts below say what it had to work with.
        System.out.println(position + " target=" + targetSide + " incoming=" + incomingSide
                + " entries=" + targetTemplate.ixEntryCount() + "/" + incomingTemplate.ixEntryCount()
                + " union=" + expected);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        targetTemplate.ixRelease();
        incomingTemplate.ixRelease();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        target = new WritableRowSetImpl(deepCopy(targetTemplate));
        incoming = new WritableRowSetImpl(deepCopy(incomingTemplate));
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        // Only the target: closing the incoming set is part of the pattern each benchmark measures.
        target.close();
    }

    /**
     * {@code count} single keys, one to a block, spread evenly over {@code blocks} steps of two blocks each starting at
     * block {@code firstBlock}. Two blocks to a step leaves the odd blocks free, so a second set built at
     * {@code firstBlock + 1} interleaves with this one without ever sharing a block.
     */
    private static long[] spread(final long firstBlock, final int blocks, final int count) {
        final long[] ranges = new long[2 * count];
        for (int ri = 0; ri < count; ++ri) {
            final long block = firstBlock + 2L * ((long) ri * blocks / count);
            ranges[2 * ri] = ranges[2 * ri + 1] = block * RspBitmap.BLOCK_SIZE;
        }
        return ranges;
    }

    /** An unshared copy of a template, so that neither operation is measured copying one first. */
    private static OrderedLongSet deepCopy(final OrderedLongSet template) {
        if (template instanceof RspBitmap) {
            return ((RspBitmap) template).deepCopy();
        }
        return ((SortedRanges) template).deepCopy();
    }

    @Benchmark
    public WritableRowSet insert() {
        try {
            target.insert(incoming);
        } finally {
            incoming.close();
        }
        return target;
    }

    @Benchmark
    public WritableRowSet subsume() {
        try {
            target.subsume(incoming);
        } finally {
            incoming.close();
        }
        return target;
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetSubsumeBench.class);
    }
}
