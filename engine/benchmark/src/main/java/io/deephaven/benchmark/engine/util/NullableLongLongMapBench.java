//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.util.hash.HashMapLockFreeK1V1;
import io.deephaven.engine.table.impl.util.hash.HashMapLockFreeK2V2;
import io.deephaven.engine.table.impl.util.hash.HashMapLockFreeK4V4;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Benchmarks for the {@link NullableLongLongMap} implementations (the maps backing hashed row redirections), with
 * fastutil's {@link Long2LongOpenHashMap} as an external baseline.
 *
 * <p>
 * These benchmarks are the fixed yardstick for a series of staged changes to the map implementations. To keep
 * comparisons honest across that series, the measured code is written entirely in terms of chunks
 * ({@link LongChunk}&lt;? extends {@link Any}&gt;). When the series began the map API was per-element and small "glue"
 * methods bridged the gap; the maps now speak chunks natively and the glue is gone. Every benchmark method, workload
 * generator, and parameter has stayed identical along the way, so numbers remain comparable across the whole series.
 *
 * <p>
 * Workload notes:
 * <ul>
 * <li>{@code keyDist} controls the shape of the keys resident in the table. {@code pulsed} — runs of N consecutive keys
 * separated by gaps, N ~ U[100, 10000], G ~ U[100, 100000] — models real row keys, and is the shape the maps'
 * deliberately weak first probe hash exploits (adjacent keys land in adjacent buckets). {@code regioned} models the row
 * keys of a partitioned table: 64 regions, each a run of size/64 consecutive rows, region r starting at {@code r << 43}
 * (RegionedColumnSource's layout, 20 region bits above 43 row bits), so consecutive runs are a power of two apart.
 * {@code sequential} is one dense block of keys, as a sort's output positions; {@code random} is uniform 64-bit
 * keys.</li>
 * <li>Misses are absent keys from the table's own neighbourhood: for {@code pulsed}, keys drawn uniformly from the gaps
 * between the runs; for {@code regioned}, the rows just past each region's last row; for {@code sequential}, the keys
 * just past the table's end; for {@code random}, fresh random keys. They are ordered like the hits ({@code sorted},
 * {@code shuffled}); with {@code window} they are the contiguous run of keys just past the table's last key.</li>
 * <li>{@code lookups} decouples the probed-key count from the table size, modeling a large resident table read in
 * comparatively small batches (a redirection index typically dwarfs any single read). 0 means "probe every table key in
 * insertion order".</li>
 * <li>{@code lookupPattern} controls how probed keys are chosen when {@code lookups != 0}: a uniform random sample of
 * the table's keys — without replacement when {@code lookups <= size}, with replacement otherwise — in ascending
 * ({@code sorted}) or random ({@code shuffled}) order, or a contiguous ascending run ({@code window}, which needs
 * {@code lookups <= size} and one of the ordered key distributions; with {@code random} keys no run is ascending, so
 * the combination is rejected). {@code window} turns pulsed-table lookups into a dense streaming read and flatters the
 * weak-hash implementations enormously; it is retained as a labeled control, not a realistic workload.</li>
 * <li>With {@code presize=true} the map is constructed at full capacity, so the filled table sits at ~{@code
 * loadFactor} occupancy and {@code fill} measures pure insertion rather than growth. When comparing against FASTUTIL at
 * a specific occupancy, pick {@code size = loadFactor * 2^k}: fastutil rounds its table to a power of two, and other
 * sizes silently leave it at a lower occupancy than requested.</li>
 * </ul>
 *
 * <p>
 * Example invocations (the {@code --args} value is a JMH command line; the leading regex selects benchmarks):
 *
 * <pre>
 * ./gradlew :engine-benchmark:jmhRunNullableLongLongMap
 * ./gradlew :engine-benchmark:jmhRunNullableLongLongMap --args="NullableLongLongMapBench.getHit\$ -p keyDist=pulsed"
 * ./gradlew :engine-benchmark:jmhRunNullableLongLongMap --args="NullableLongLongMapBench -p size=120700000 \
 *     -p lookups=1000000 -p presize=true -p loadFactor=0.9 -rf json -rff results.json"
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class NullableLongLongMapBench {
    @FunctionalInterface
    public interface MapFactory {
        /**
         * @param desiredEntries entry-slot capacity, in the maps' desiredInitialCapacity convention
         * @param loadFactor the table load factor
         */
        NullableLongLongMap create(int desiredEntries, double loadFactor);
    }

    public enum Impl {
        // The third constructor argument is the noEntryValue; -1 is the maps' default.
        // @formatter:off
        K1V1((desiredEntries, loadFactor) -> new HashMapLockFreeK1V1(desiredEntries, loadFactor, -1)),
        K2V2((desiredEntries, loadFactor) -> new HashMapLockFreeK2V2(desiredEntries, loadFactor, -1)),
        K4V4((desiredEntries, loadFactor) -> new HashMapLockFreeK4V4(desiredEntries, loadFactor, -1)),
        FASTUTIL(FastutilAdapter::new);
        // @formatter:on

        final MapFactory factory;

        Impl(MapFactory factory) {
            this.factory = factory;
        }
    }

    @Param({"K1V1", "K2V2", "K4V4", "FASTUTIL"})
    public Impl impl;

    /**
     * Number of keys resident in the table.
     */
    @Param({"1000000"})
    public int size;

    /**
     * Batch size: keys are fed through the glue methods in chunks of this many elements.
     */
    @Param({"4096"})
    public int chunkSize;

    /**
     * "random": uniform random longs. "sequential": one contiguous block of small keys. "pulsed": pulses of N
     * consecutive keys separated by gaps. "regioned": a partitioned table's row keys, 64 runs a power of two apart (see
     * class javadoc). pulsed is the default: it is the workload the series is measured against.
     */
    @Param({"pulsed"})
    public String keyDist;

    /**
     * Number of keys probed per getHit/getMiss invocation; 0 means "all table keys, in insertion order".
     */
    @Param({"0"})
    public int lookups;

    /**
     * How the probed keys are chosen when lookups != 0: "sorted", "shuffled", or "window" (see class javadoc).
     */
    @Param({"sorted"})
    public String lookupPattern;

    /**
     * When true, maps are created at full capacity, so fill measures pure insertion rather than rehashing, and the
     * filled table sits at ~loadFactor occupancy for the lookup benchmarks.
     */
    @Param({"false"})
    public boolean presize;

    /**
     * Table load factor.
     */
    @Param({"0.5"})
    public double loadFactor;

    private LongChunk<Any>[] keyChunks;
    private LongChunk<Any>[] valueChunks;
    private LongChunk<Any>[] hitChunks;
    private LongChunk<Any>[] missChunks;
    private WritableLongChunk<Any> scratch;
    private NullableLongLongMap filledMap;

    /**
     * The regioned key distribution: RegionedColumnSource addresses a row as 20 region bits above 43 row bits, so
     * region r's rows start at {@code r << REGION_ROW_BITS}; the table's keys are spread over REGIONS such regions.
     */
    private static final int REGION_ROW_BITS = 43;
    private static final int REGIONS = 64;

    @Setup(Level.Trial)
    public void setupTrial() {
        final SplittableRandom rng = new SplittableRandom(20260831);
        final int nLookups = lookups == 0 ? size : lookups;
        final long[] keys;
        final long[] hits;
        final long[] misses;
        if ("sequential".equals(keyDist)) {
            keys = new long[size];
            final long start = 1_000_000;
            for (int ii = 0; ii < size; ++ii) {
                keys[ii] = start + ii;
            }
            hits = sampleHits(keys, nLookups, rng);
            // The absent keys nearest the table: the ones just past its end.
            misses = arrangeMisses(runOf(start + size, nLookups), keys, rng);
        } else if ("pulsed".equals(keyDist)) {
            keys = new long[size];
            final LongArrayList gapStarts = new LongArrayList();
            final LongArrayList gapLengths = new LongArrayList();
            long key = 1_000_000;
            int ii = 0;
            while (ii < size) {
                final int pulseLen = Math.min(rng.nextInt(100, 10_001), size - ii);
                for (int jj = 0; jj < pulseLen; ++jj) {
                    keys[ii++] = key++;
                }
                final int gap = rng.nextInt(100, 100_001);
                gapStarts.add(key);
                gapLengths.add(gap);
                key += gap;
            }
            hits = sampleHits(keys, nLookups, rng);
            // Misses live in the gaps between the runs (the gap after the last run included): absent keys from the
            // table's own neighbourhood, in the table's own key order, rather than the hits mirrored to negative keys.
            misses = arrangeMisses(sampleGaps(gapStarts, gapLengths, nLookups, rng), keys, rng);
        } else if ("regioned".equals(keyDist)) {
            // A partitioned table's row keys: region r's rows sit at r << REGION_ROW_BITS. REGIONS regions of
            // size/REGIONS
            // consecutive rows each (the last takes the remainder), so consecutive runs are a power of two apart.
            final int rowsPerRegion = Math.max(1, (size + REGIONS - 1) / REGIONS);
            final int regions = (size + rowsPerRegion - 1) / rowsPerRegion;
            keys = new long[size];
            for (int ii = 0; ii < size; ++ii) {
                keys[ii] = ((long) (ii / rowsPerRegion) << REGION_ROW_BITS) + ii % rowsPerRegion;
            }
            hits = sampleHits(keys, nLookups, rng);
            // Misses are rows just past each region's last row: absent keys of the same partitions.
            final long[] candidates = new long[nLookups];
            for (int mi = 0; mi < nLookups; ++mi) {
                final int region = rng.nextInt(regions);
                final int rows = Math.min(rowsPerRegion, size - region * rowsPerRegion);
                candidates[mi] = ((long) region << REGION_ROW_BITS) + rows + rng.nextInt(rows);
            }
            misses = arrangeMisses(candidates, keys, rng);
        } else if ("random".equals(keyDist)) {
            keys = distinctKeys(rng, size);
            hits = sampleHits(keys, nLookups, rng);
            // Fresh random keys: overlap with 'keys' is negligible in a 64-bit key space.
            misses = arrangeMisses(distinctKeys(rng, nLookups), keys, rng);
        } else {
            throw new IllegalArgumentException("unknown keyDist: " + keyDist);
        }
        final long[] values = new long[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        keyChunks = chunkify(keys, chunkSize);
        valueChunks = chunkify(values, chunkSize);
        hitChunks = chunkify(hits, chunkSize);
        missChunks = chunkify(misses, chunkSize);
        scratch = WritableLongChunk.makeWritableChunk(chunkSize);
        filledMap = createMap();
        fill(filledMap);
    }

    /**
     * Select the probed keys per {@link #lookupPattern}; lookups=0 means the whole table in insertion order.
     */
    private long[] sampleHits(final long[] tableKeys, final int nLookups, final SplittableRandom rng) {
        if (lookups == 0) {
            return tableKeys;
        }
        if ("window".equals(lookupPattern)) {
            if ("random".equals(keyDist)) {
                throw new IllegalArgumentException(
                        "lookupPattern=window needs an ordered key distribution (sequential, pulsed or regioned), not random");
            }
            if (nLookups > tableKeys.length) {
                throw new IllegalArgumentException(
                        "lookupPattern=window needs lookups <= size: lookups=" + nLookups + ", size="
                                + tableKeys.length);
            }
            final int start = rng.nextInt(tableKeys.length - nLookups + 1);
            return Arrays.copyOfRange(tableKeys, start, start + nLookups);
        }
        if (!"sorted".equals(lookupPattern) && !"shuffled".equals(lookupPattern)) {
            throw new IllegalArgumentException("unknown lookupPattern: " + lookupPattern);
        }
        final long[] result = new long[nLookups];
        if (nLookups <= tableKeys.length) {
            // Without replacement: Floyd's algorithm draws nLookups distinct indices in O(nLookups) space, which
            // matters when the table holds hundreds of millions of keys.
            final IntOpenHashSet chosen = new IntOpenHashSet(nLookups);
            int oi = 0;
            for (int ii = tableKeys.length - nLookups; ii < tableKeys.length; ++ii) {
                final int candidate = rng.nextInt(ii + 1);
                final int pick = chosen.add(candidate) ? candidate : ii;
                if (pick == ii) {
                    chosen.add(ii);
                }
                result[oi++] = tableKeys[pick];
            }
        } else {
            // More lookups than keys: with replacement, necessarily.
            for (int ii = 0; ii < nLookups; ++ii) {
                result[ii] = tableKeys[rng.nextInt(tableKeys.length)];
            }
        }
        if ("sorted".equals(lookupPattern)) {
            Arrays.sort(result);
        } else {
            shuffle(result, rng);
        }
        return result;
    }

    /**
     * Order the miss candidates like the hits: ascending for {@code sorted} (and for lookups=0, where the hits are the
     * table's keys in insertion order), random for {@code shuffled}; for {@code window} the misses are instead the
     * contiguous run of keys just past the table's last key, the miss counterpart of a dense streaming read.
     */
    private long[] arrangeMisses(final long[] candidates, final long[] tableKeys, final SplittableRandom rng) {
        if (lookups == 0) {
            if (!"random".equals(keyDist)) {
                Arrays.sort(candidates);
            }
            return candidates;
        }
        if ("window".equals(lookupPattern)) {
            return runOf(tableKeys[tableKeys.length - 1] + 1, candidates.length);
        }
        if ("shuffled".equals(lookupPattern)) {
            shuffle(candidates, rng);
            return candidates;
        }
        Arrays.sort(candidates);
        return candidates;
    }

    /**
     * nLookups keys drawn uniformly from the gaps between the table's runs; gap g covers {@code [gapStarts[g],
     * gapStarts[g] + gapLengths[g])}. Returned in random order.
     */
    private static long[] sampleGaps(final LongArrayList gapStarts, final LongArrayList gapLengths, final int nLookups,
            final SplittableRandom rng) {
        final int nGaps = gapStarts.size();
        final long[] cumulative = new long[nGaps + 1];
        for (int gi = 0; gi < nGaps; ++gi) {
            cumulative[gi + 1] = cumulative[gi] + gapLengths.getLong(gi);
        }
        final long[] result = new long[nLookups];
        for (int mi = 0; mi < nLookups; ++mi) {
            final long t = rng.nextLong(cumulative[nGaps]);
            int gi = Arrays.binarySearch(cumulative, t);
            if (gi < 0) {
                gi = -gi - 2; // the gap whose cumulative start is the largest at or below t
            }
            result[mi] = gapStarts.getLong(gi) + (t - cumulative[gi]);
        }
        return result;
    }

    private static long[] runOf(final long first, final int n) {
        final long[] result = new long[n];
        for (int ii = 0; ii < n; ++ii) {
            result[ii] = first + ii;
        }
        return result;
    }

    private static void shuffle(final long[] a, final SplittableRandom rng) {
        for (int ii = a.length - 1; ii > 0; --ii) {
            final int jj = rng.nextInt(ii + 1);
            final long t = a[ii];
            a[ii] = a[jj];
            a[jj] = t;
        }
    }

    private static long[] distinctKeys(final SplittableRandom rng, final int count) {
        final long[] result = new long[count];
        for (int ii = 0; ii < count; ++ii) {
            long key;
            do {
                key = rng.nextLong();
                // NULL_LONG is the null key; NULL_LONG + 1 and + 2 are reserved sentinels (see HashMapBase).
            } while (key >= NULL_LONG && key <= NULL_LONG + 2);
            result[ii] = key;
        }
        return result;
    }

    /**
     * Split a key array into zero-copy chunk views of at most chunkSize elements each.
     */
    @SuppressWarnings("unchecked")
    private static LongChunk<Any>[] chunkify(final long[] src, final int chunkSize) {
        final int numChunks = (src.length + chunkSize - 1) / chunkSize;
        final LongChunk<Any>[] result = new LongChunk[numChunks];
        for (int ci = 0; ci < numChunks; ++ci) {
            final int from = ci * chunkSize;
            result[ci] = LongChunk.chunkWrap(src, from, Math.min(chunkSize, src.length - from));
        }
        return result;
    }

    private NullableLongLongMap createMap() {
        return impl.factory.create(presize ? (int) (size / loadFactor) + 1 : 16, loadFactor);
    }

    private void fill(final NullableLongLongMap map) {
        for (int ci = 0; ci < keyChunks.length; ++ci) {
            map.put(keyChunks[ci], valueChunks[ci], scratch);
        }
    }

    /**
     * The one shared call site for lookups, so getHit and getMiss measure identical code.
     */
    private void sweep(final NullableLongLongMap map, final LongChunk<Any>[] chunks, final Blackhole bh) {
        for (final LongChunk<Any> chunk : chunks) {
            map.get(chunk, scratch);
            bh.consume(scratch);
        }
    }

    /**
     * Insert {@code size} distinct keys into a fresh map (includes growth cost unless presize=true).
     */
    @Benchmark
    public NullableLongLongMap fill() {
        final NullableLongLongMap map = createMap();
        fill(map);
        return map;
    }

    /**
     * Look up {@code lookups} present keys (all table keys, in insertion order, when lookups=0).
     */
    @Benchmark
    public void getHit(final Blackhole bh) {
        sweep(filledMap, hitChunks, bh);
    }

    /**
     * Look up {@code lookups} absent keys.
     */
    @Benchmark
    public void getMiss(final Blackhole bh) {
        sweep(filledMap, missChunks, bh);
    }

    /**
     * Remove every table key, then re-insert all of them (exercises deleted-slot handling).
     */
    @Benchmark
    public void removeThenReinsert() {
        final NullableLongLongMap map = filledMap;
        for (final LongChunk<Any> chunk : keyChunks) {
            map.remove(chunk, scratch);
        }
        fill(map);
    }

    /**
     * fastutil baseline behind the same interface. Only the operations these benchmarks exercise are implemented.
     */
    private static final class FastutilAdapter implements NullableLongLongMap {
        private final Long2LongOpenHashMap map;

        FastutilAdapter(final int desiredEntries, final double loadFactor) {
            // fastutil's first argument is the expected element count, not slot capacity; convert from ours exactly, so
            // that a non-presized fill starts both maps at the same handful of slots and grows them the same number
            // of times.
            map = new Long2LongOpenHashMap((int) (desiredEntries * loadFactor), (float) loadFactor);
            map.defaultReturnValue(NULL_LONG);
        }

        @Override
        public void put(final LongChunk<? extends Any> keys, final LongChunk<? extends Any> values,
                final WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.put(keys.get(ii), values.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public void putIfAbsent(final LongChunk<? extends Any> keys, final LongChunk<? extends Any> values,
                final WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.putIfAbsent(keys.get(ii), values.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public void put(final LongChunk<? extends Any> keys, final LongChunk<? extends Any> values) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                map.put(keys.get(ii), values.get(ii));
            }
        }

        @Override
        public void put(final LongChunk<? extends Any> keys, final long value) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                map.put(keys.get(ii), value);
            }
        }

        @Override
        public void get(final LongChunk<? extends Any> keys, final WritableLongChunk<? extends Any> result) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                result.set(ii, map.get(keys.get(ii)));
            }
            result.setSize(size);
        }

        @Override
        public void remove(final LongChunk<? extends Any> keys, final WritableLongChunk<? extends Any> oldValues) {
            final int size = keys.size();
            for (int ii = 0; ii < size; ++ii) {
                oldValues.set(ii, map.remove(keys.get(ii)));
            }
            oldValues.setSize(size);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public long defaultReturnValue() {
            return map.defaultReturnValue();
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public int capacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetToNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetToNullRetainingCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(final it.unimi.dsi.fastutil.longs.LongLongBiConsumer consumer) {
            throw new UnsupportedOperationException();
        }
    }
}
