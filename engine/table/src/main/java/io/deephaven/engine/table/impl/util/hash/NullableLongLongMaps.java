//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

/**
 * The one way to construct a {@link NullableLongLongMap}, and the one place where "which shape?" decisions live: a map
 * consults {@link #shapeForRebuild} whenever it builds an array, so no owner ever decides — or even notices — when a
 * map widens. Callers name a {@link Shape}, never a class: every implementation class in this package is
 * package-private, and the shape a map was built with is an implementation detail behind the interface.
 */
public final class NullableLongLongMaps {
    /**
     * The bucket width of a map: how many key/value pairs each hash bucket holds. Within a bucket the pairs are
     * interleaved, each key at an even offset with its value in the slot after it, so a bucket of width w spans 2w
     * longs. Wider buckets mean fewer cache lines per probe chain at density and more bytes per bucket when sparse.
     * K4V4 is one 64-byte cache line per bucket, and the only shape with an AMAC window kernel (see {@link ReadMode}).
     */
    public enum Shape {
        /** One key/value pair per bucket. */
        K1V1(1),
        /** Four key/value pairs per bucket, interleaved: one cache line. */
        K4V4(4);

        private final int bucketWidth;

        Shape(final int bucketWidth) {
            this.bucketWidth = bucketWidth;
        }

        /**
         * The number of keys (equally, values) per bucket.
         */
        public int bucketWidth() {
            return bucketWidth;
        }

        /**
         * The shape with the given bucket width, for configuration that speaks in widths (1 or 4). Width 2 was retired:
         * two campaigns on two machines found no scenario in which the two-key bucket was the fastest shape.
         *
         * @throws IllegalArgumentException for any other width
         */
        public static Shape forBucketWidth(final int bucketWidth) {
            for (final Shape shape : values()) {
                if (shape.bucketWidth == bucketWidth) {
                    return shape;
                }
            }
            throw new IllegalArgumentException("Unsupported bucket width " + bucketWidth
                    + " (supported: 1 and 4; width 2, K2V2, was retired: it was the fastest shape in no measured scenario)");
        }
    }

    /**
     * How a {@link Shape#K4V4} map's chunked gets choose between the serial probe loop and the AMAC window. Production
     * code uses {@link #ADAPTIVE}; the pinned modes exist so the yardstick can price the adaptive gate against each
     * pure strategy, and so tests can exercise the window kernel at sizes where the gate would choose serial. The
     * narrow shapes have no window kernel: their reads are serial whatever the mode, and {@link #WINDOW} is rejected
     * for them.
     */
    public enum ReadMode {
        /** The footprint gate decides per chunk (see {@link NullableLongLongMaps#wantWindowedReads}). */
        ADAPTIVE,
        /** Always the AMAC window, regardless of footprint. {@link Shape#K4V4} only. */
        WINDOW,
        /** Always the serial probe loop, regardless of footprint. */
        SERIAL
    }

    /**
     * Entry capacity at and above which a K4V4 map services chunked gets through the AMAC window (16 bytes per entry,
     * so 1M entries is a 16MB array). This is a FOOTPRINT threshold. The footprint sweep on a Ryzen 9 9950X3D2 (1MB L2,
     * ~96MB per-CCD L3; serial vs forced-window, 2M..16M entries, load factors 0.5 and 0.9, sorted and shuffled, three
     * forks) found the window winning or tying at EVERY size from 2M up — 13-38% faster on shuffled lookups and 12-20%
     * on sorted at load factor 0.9 — even though a 2M-entry array (32MB) fits in that L3: the window overlaps L3
     * latency, not just DRAM latency, so the real boundary is near L2, not the last-level cache. At 100K entries
     * (1.6MB) the two strategies measured as parity. The crossover therefore lies between 100K and 2M, and this value
     * sits inside that bracket, bounded below by parity and above by a measured win. Re-sweep before trusting it on
     * hardware with a materially larger L2. The same size is where a dense narrow map widens itself into the K4V4 shape
     * (see {@link #shapeForRebuild}): an array big enough to want the window is built wide.
     */
    public static final int DEFAULT_AMAC_THRESHOLD_ENTRIES = 1 << 20;

    /**
     * The narrowest chunk a K4V4 map services through the AMAC window: the window's own width,
     * {@code K4V4Kernel.GET_WINDOW}, so that retuning the window moves this gate with it. Below it a chunked get runs
     * serially even when the footprint says window, because the window only pays when it is full: a chunk narrower than
     * the window cannot overlap a window's worth of misses, while the window's fixed cost (the per-thread scratch,
     * per-job setup) is paid regardless. Measured at 10M entries on an i9-13900K (three forks, serial vs forced window,
     * window/serial time ratio): single-key chunks cost 1.6-2.3x under the window on every load factor and pattern;
     * four-key chunks lose on everything but dense shuffled lookups; sixteen-key chunks win 10% on sparse shuffled and
     * 2.4x on dense shuffled; sixty-four-key chunks win 23% and 3x there and tie dense sorted. Sixteen is the window
     * width: the smallest chunk that can fill it. (Sorted sparse lookups lose under the window at every chunk size on
     * that machine and tie or win on a Ryzen 9 9950X3D2 — a lookup-pattern question, which the gate's second stage
     * answers: see {@link #wantSerialForMonotoneKeys}.)
     */
    public static final int MIN_WINDOWED_CHUNK = K4V4Kernel.GET_WINDOW;

    /**
     * Second stage of the read-strategy gate, for a chunk whose keys walk memory in order: below this occupancy
     * (occupied slots over entry capacity, tombstones counting as occupied, since a probe chain runs past them exactly
     * as it runs past entries) such a chunk takes the serial loop even though the footprint gate would open the window.
     * The maps' first probe is deliberately weak — consecutive keys land in consecutive buckets — so monotone keys into
     * a sparse table are a straight walk through memory that almost always ends at the first bucket; the hardware
     * prefetcher already hides that walk, and the window's bookkeeping then costs 16 to 38% on an i9-13900K and 4 to 8%
     * on a Ryzen 9 9950X3D2 at 30 to 38% occupancy (measured at 1M to 10M entries). At 61% occupancy and above the
     * probe chains wander and the window wins by 8 to 19% even on monotone keys, so the threshold sits between the two,
     * at one half. A map's occupancy sawtooths within [loadFactor/2, loadFactor] as rehash doubles overshoot: a
     * load-factor-0.5 map never rises above it, so its monotone reads are always serial, and a load-factor-0.9 map dips
     * below it only in the first ninth of each doubling cycle, where the window's gain is smallest. Shuffled keys never
     * consult this: their misses are what the window exists to overlap.
     */
    public static final double MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY = 0.5;

    /**
     * The walk must also be local: a monotone chunk whose consecutive keys are, on average, more than this many keys
     * apart lands each lookup in its own far-off bucket, and the prefetcher has nothing to stream. In K4V4 buckets of
     * 64 bytes, 2048 keys is 128KB between consecutive first probes. Measured on the i9: 100,000 sorted lookups into
     * 10M entries (about 100 keys apart) are 14% faster serial; into 100M (about 1,000 apart) 6% faster serial; into
     * 500M (about 5,000 apart) 19% faster through the window. The threshold sits between the last two.
     */
    public static final int MONOTONE_KEYS_MAX_LOCAL_STEP = 2048;

    /**
     * The load factor at and above which a map that is big enough is rebuilt in the wide-bucket (K4V4) shape; below it
     * the shape does not pay, regardless of size. This gates the LAYOUT only: whether the wide map's reads then go
     * through the AMAC window is decided separately, by footprint ({@link #wantWindowedReads}). Measured at 10M
     * entries: at load factor 0.5 the wide shape loses (~30-40% slower — probe chains barely exist, so the window has
     * nothing to hide and wider buckets just cost more bytes per probe); at 0.75 it is a wash (sorted lookups trend
     * against it, shuffled trend for it, error bars overlapping); at 0.9 it wins 20% on sorted and 2.5x on shuffled.
     * The floor sits above the measured wash and below the measured win.
     */
    public static final double AMAC_LOAD_FACTOR_FLOOR = 0.8;

    private NullableLongLongMaps() {}

    /**
     * Creates a map of the given {@link Shape} with the given initial capacity, load factor, and noEntryValue (the
     * value returned by reads that find no mapping). A K4V4 map's reads adapt by footprint ({@link ReadMode#ADAPTIVE}).
     */
    public static NullableLongLongMap of(final Shape shape, final int desiredInitialCapacity, final double loadFactor,
            final long noEntryValue) {
        return of(shape, desiredInitialCapacity, loadFactor, noEntryValue, ReadMode.ADAPTIVE);
    }

    /**
     * As {@link #of(Shape, int, double, long)}, with the read strategy pinned. For pricing and tests; production code
     * should let the map adapt.
     *
     * @throws IllegalArgumentException for {@link ReadMode#WINDOW} with a shape other than {@link Shape#K4V4}
     */
    public static NullableLongLongMap of(final Shape shape, final int desiredInitialCapacity, final double loadFactor,
            final long noEntryValue, final ReadMode readMode) {
        if (readMode == ReadMode.WINDOW && shape != Shape.K4V4) {
            throw new IllegalArgumentException("ReadMode.WINDOW requires Shape.K4V4, not " + shape);
        }
        return new HashMapLockFreeKnVn(shape, desiredInitialCapacity, loadFactor, noEntryValue, readMode);
    }

    /**
     * Creates a map of the given {@link Shape}, presized so that {@code expectedSize} entries at {@code loadFactor} fit
     * without a rehash.
     */
    public static NullableLongLongMap ofExpectedSize(final Shape shape, final int expectedSize,
            final double loadFactor, final long noEntryValue) {
        final int desiredInitialCapacity = HashMapLockFreeKnVn.capacityForExpectedEntries(expectedSize, loadFactor);
        return of(shape, desiredInitialCapacity, loadFactor, noEntryValue);
    }

    /**
     * Should a K4V4-shaped map service this chunked get through the AMAC window? Yes exactly when its FOOTPRINT is past
     * the measured crossover — entry capacity at or above {@link #DEFAULT_AMAC_THRESHOLD_ENTRIES}, which the footprint
     * sweep above puts near L2, well inside the last-level cache — because the window's whole job is overlapping cache
     * misses, and a table that fits the near caches has none worth overlapping (there the window is pure bookkeeping,
     * measured as a tax); and when the chunk is at least {@link #MIN_WINDOWED_CHUNK} keys wide, because a chunk that
     * cannot fill the window pays its fixed cost for nothing (a single-key chunk — the scalar cursor's case — has
     * nothing to overlap at all). Footprint is the first-order predictor, and this gate is the first stage: it looks
     * only at the array and the chunk width, so its answer is stable between rehashes and flips exactly when the array
     * grows past the crossover. Occupancy on its own is second-order — at a fixed large footprint the window ties or
     * wins at every occupancy measured when keys arrive shuffled — and is not an input here. It matters in one
     * combination, monotone local keys into a sparse table, which the second stage handles: see
     * {@link #wantSerialForMonotoneKeys}, {@link #MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY} and {@link #isLocalWalk}.
     */
    public static boolean wantWindowedReads(final int entryCapacity, final int chunkSize) {
        return chunkSize >= MIN_WINDOWED_CHUNK && entryCapacity >= DEFAULT_AMAC_THRESHOLD_ENTRIES;
    }

    /**
     * Second stage of the read-strategy gate, consulted only after {@link #wantWindowedReads} has said yes: is this map
     * sparse enough that a chunk of monotone, local keys is better served by the serial loop? The map answers with its
     * occupied slot count — entries and tombstones alike, since a probe chain runs past both — and its entry capacity;
     * whether the chunk's keys are in fact a local monotone walk is the map's own check, made only when this says yes.
     * See {@link #MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY} for the evidence.
     */
    public static boolean wantSerialForMonotoneKeys(final int occupiedSlots, final int entryCapacity) {
        return occupiedSlots < entryCapacity * MONOTONE_KEYS_SERIAL_BELOW_OCCUPANCY;
    }

    /**
     * Is a monotone chunk running from {@code first} to {@code last} over {@code n} keys a local walk, at most
     * {@link #MONOTONE_KEYS_MAX_LOCAL_STEP} keys per step on average? A span a long cannot hold, or whose absolute
     * value it cannot, is not local.
     */
    public static boolean isLocalWalk(final long first, final long last, final int n) {
        if (n < 2) {
            return true;
        }
        final long span = last - first;
        // Overflow: first and last have opposite signs and the difference has the wrong sign — a huge span.
        if (((first ^ last) & (last ^ span)) < 0) {
            return false;
        }
        // A span of exactly Long.MIN_VALUE does not overflow, but Math.abs of it stays negative: also huge.
        if (span == Long.MIN_VALUE) {
            return false;
        }
        // Compare the whole span against the threshold scaled to the chunk, rather than dividing: an average step just
        // over the limit must not round down into "local". The product fits a long: the step is small and n is an int.
        return Math.abs(span) <= (long) MONOTONE_KEYS_MAX_LOCAL_STEP * (n - 1);
    }

    /**
     * When a map becomes K4V4. A map builds a new array in three situations — its first allocation, every rehash, and a
     * reset that retains capacity — and each time it asks this function which shape to build. The answer depends on
     * exactly three things: the shape it has now ({@code current}), its configured load factor, and the entry capacity
     * the new array will have ({@code newEntryCapacity}, counted after prime rounding), plus whether that array is the
     * largest its shape can build ({@code atCeiling}). The rule, in order:
     * <ol>
     * <li>A K4V4 map stays K4V4. Widening is a one-way door: a K4V4 map's reads adapt to its footprint on their own
     * (see {@link #wantWindowedReads}), so there is nothing to go back for.</li>
     * <li>Deliberately dense maps widen: a configured load factor at or above {@link #AMAC_LOAD_FACTOR_FLOOR} and a new
     * array with room for at least {@link #DEFAULT_AMAC_THRESHOLD_ENTRIES} entries is built K4V4. At load factor 0.9
     * that is the doubling triggered somewhere between about 470,000 and 940,000 entries, depending on where the
     * doubling sequence falls; a map presized that large is born K4V4.</li>
     * <li>Maps at the ceiling widen: a new array that is the largest the current shape can build (see
     * {@code HashMapLockFreeKnVn.getMaxBucketCapacity}) is built K4V4, because no further growth is possible and
     * occupancy will climb whatever the load factor says (the rehash threshold moves to the nearly-full load factor).
     * For the default map, K1V1 at 0.5, that is the doubling triggered at about 268 million entries; it comes out K4V4
     * with room for about 1.07 billion and holds at most {@code HashMapLockFreeKnVn.SIZE_LIMIT4} entries.</li>
     * <li>Otherwise the map keeps the shape it was born with.</li>
     * </ol>
     * What does not trigger widening: the current occupancy (below the ceiling a map at 0.5 never passes half full; it
     * doubles instead), the entry count on its own (a K1V1 map at 0.5 with 100 million entries is still K1V1), a load
     * factor below the floor such as 0.75, and the read pattern. The map asks this wherever it builds an array, where
     * the rebuild is free; no owner mediates, and the map's identity never changes.
     */
    static Shape shapeForRebuild(final Shape current, final double loadFactor, final int newEntryCapacity,
            final boolean atCeiling) {
        if (current == Shape.K4V4) {
            return Shape.K4V4;
        }
        final boolean deliberatelyDense =
                loadFactor >= AMAC_LOAD_FACTOR_FLOOR && newEntryCapacity >= DEFAULT_AMAC_THRESHOLD_ENTRIES;
        return deliberatelyDense || atCeiling ? Shape.K4V4 : current;
    }
}
