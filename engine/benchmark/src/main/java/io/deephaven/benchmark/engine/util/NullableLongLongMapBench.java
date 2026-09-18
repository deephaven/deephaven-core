//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK1V1;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK2V2;
import io.deephaven.util.datastructures.hash.HashMapLockFreeK4V4;
import io.deephaven.util.datastructures.hash.NullableLongLongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
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
 * ({@link LongChunk}&lt;? extends {@link Any}&gt;), even though the current map API is per-element: the small "glue"
 * methods below iterate over each chunk and call the per-element API. When a later change converts the map API itself
 * to chunk-oriented operations, only the glue methods change; every benchmark method, workload generator, and parameter
 * stays byte-identical, so before/after numbers remain comparable.
 *
 * <p>
 * Workload notes:
 * <ul>
 * <li>{@code keyDist} controls the shape of the keys resident in the table. {@code pulsed} — runs of N consecutive keys
 * separated by gaps, N ~ U[100, 10000], G ~ U[100, 100000] — models real row keys, and is the shape the maps'
 * deliberately weak first probe hash exploits (adjacent keys land in adjacent buckets).</li>
 * <li>{@code lookups} decouples the probed-key count from the table size, modeling a large resident table read in
 * comparatively small batches (a redirection index typically dwarfs any single read). 0 means "probe every table key in
 * insertion order".</li>
 * <li>{@code lookupPattern} controls how probed keys are chosen when {@code lookups != 0}: a uniform random subset in
 * ascending ({@code sorted}) or random ({@code shuffled}) order, or a contiguous ascending run ({@code window}).
 * {@code window} turns pulsed-table lookups into a dense streaming read and flatters the weak-hash implementations
 * enormously; it is retained as a labeled control, not a realistic workload.</li>
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
     * consecutive keys separated by gaps (see class javadoc).
     */
    @Param({"random"})
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

    @Setup(Level.Trial)
    public void setupTrial() {
        final SplittableRandom rng = new SplittableRandom(20260831);
        final int nLookups = lookups == 0 ? size : lookups;
        final long[] keys;
        final long[] hits;
        final long[] misses;
        if ("sequential".equals(keyDist)) {
            keys = new long[size];
            misses = new long[nLookups];
            final long start = 1_000_000;
            for (int ii = 0; ii < size; ++ii) {
                keys[ii] = start + ii;
            }
            for (int ii = 0; ii < nLookups; ++ii) {
                misses[ii] = start + size + ii;
            }
            hits = sampleHits(keys, nLookups, rng);
        } else if ("pulsed".equals(keyDist)) {
            keys = new long[size];
            long key = 1_000_000;
            int ii = 0;
            while (ii < size) {
                final int pulseLen = Math.min(rng.nextInt(100, 10_001), size - ii);
                for (int jj = 0; jj < pulseLen; ++jj) {
                    keys[ii++] = key++;
                }
                key += rng.nextInt(100, 100_001);
            }
            hits = sampleHits(keys, nLookups, rng);
            // The mirror image of the hits has the same pulse structure but is disjoint from the (all-positive)
            // table keys.
            misses = new long[nLookups];
            for (int mi = 0; mi < nLookups; ++mi) {
                misses[mi] = -hits[mi];
            }
        } else if ("random".equals(keyDist)) {
            keys = distinctKeys(rng, size);
            misses = distinctKeys(rng, nLookups); // overlap with 'keys' is negligible in a 64-bit key space
            hits = sampleHits(keys, nLookups, rng);
            if ("sorted".equals(lookupPattern) && lookups != 0) {
                Arrays.sort(misses); // keep miss ordering consistent with hit ordering
            }
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
            final int start = nLookups >= tableKeys.length ? 0 : rng.nextInt(tableKeys.length - nLookups);
            return Arrays.copyOfRange(tableKeys, start, start + Math.min(nLookups, tableKeys.length));
        }
        final long[] result = new long[nLookups];
        for (int ii = 0; ii < nLookups; ++ii) {
            result[ii] = tableKeys[rng.nextInt(tableKeys.length)];
        }
        if ("sorted".equals(lookupPattern)) {
            Arrays.sort(result);
        } else if (!"shuffled".equals(lookupPattern)) {
            throw new IllegalArgumentException("unknown lookupPattern: " + lookupPattern);
        }
        return result;
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
            putAll(map, keyChunks[ci], valueChunks[ci], scratch);
        }
    }

    // region glue
    // Glue between the chunk-oriented benchmark code above/below and the map's current per-element API. When the map
    // API itself becomes chunk-oriented (a later change in this series), only these four methods change; everything
    // the benchmarks measure and generate stays identical. Each output chunk must have capacity >= keys.size();
    // element ii of the output corresponds to element ii of keys.

    private static <T extends Any> void putAll(final NullableLongLongMap map, final LongChunk<? extends Any> keys,
            final LongChunk<T> values, final WritableLongChunk<T> oldValues) {
        for (int ii = 0; ii < keys.size(); ++ii) {
            oldValues.set(ii, map.put(keys.get(ii), values.get(ii)));
        }
    }

    private static <T extends Any> void putAllIfAbsent(final NullableLongLongMap map,
            final LongChunk<? extends Any> keys, final LongChunk<T> values, final WritableLongChunk<T> oldValues) {
        for (int ii = 0; ii < keys.size(); ++ii) {
            oldValues.set(ii, map.putIfAbsent(keys.get(ii), values.get(ii)));
        }
    }

    private static void getAll(final NullableLongLongMap map, final LongChunk<? extends Any> keys,
            final WritableLongChunk<? extends Any> result) {
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, map.get(keys.get(ii)));
        }
    }

    private static void removeAll(final NullableLongLongMap map, final LongChunk<? extends Any> keys,
            final WritableLongChunk<? extends Any> oldValues) {
        for (int ii = 0; ii < keys.size(); ++ii) {
            oldValues.set(ii, map.remove(keys.get(ii)));
        }
    }
    // endregion glue

    /**
     * The one shared call site for lookups, so getHit and getMiss measure identical code.
     */
    private void sweep(final NullableLongLongMap map, final LongChunk<Any>[] chunks, final Blackhole bh) {
        for (final LongChunk<Any> chunk : chunks) {
            getAll(map, chunk, scratch);
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
            removeAll(map, chunk, scratch);
        }
        fill(map);
    }

    /**
     * fastutil baseline behind the same interface. Only the operations these benchmarks exercise are implemented.
     */
    private static final class FastutilAdapter implements NullableLongLongMap {
        private final Long2LongOpenHashMap map;

        FastutilAdapter(final int desiredEntries, final double loadFactor) {
            // fastutil's first argument is expected element count, not slot capacity; convert from ours.
            map = new Long2LongOpenHashMap(Math.max(16, (int) (desiredEntries * loadFactor)), (float) loadFactor);
            map.defaultReturnValue(NULL_LONG);
        }

        @Override
        public long put(final long key, final long value) {
            return map.put(key, value);
        }

        @Override
        public long putIfAbsent(final long key, final long value) {
            return map.putIfAbsent(key, value);
        }

        @Override
        public long get(final long key) {
            return map.get(key);
        }

        @Override
        public long remove(final long key) {
            return map.remove(key);
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
