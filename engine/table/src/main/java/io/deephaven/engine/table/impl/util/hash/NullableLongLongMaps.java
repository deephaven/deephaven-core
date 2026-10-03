//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

/**
 * The one way to construct a {@link NullableLongLongMap}, and the one place where "which shape?" decisions live, so
 * that owners do not each re-derive them. Callers name a {@link Shape}, never a class: every implementation class in
 * this package is package-private, and the shape a map was built with is an implementation detail behind the interface.
 * (If shape-shifting ever moves inside the maps themselves — a rehash that changes the array's shape — this policy
 * moves with it.)
 */
public final class NullableLongLongMaps {
    /**
     * The bucket width of a map: how many keys (followed by that many values) each hash bucket holds. Wider buckets
     * mean fewer cache lines per probe chain at density and more bytes per bucket when sparse. K4V4 is one 64-byte
     * cache line per bucket, and the only shape with an AMAC window kernel (see {@link ReadMode}).
     */
    public enum Shape {
        /** One key and one value per bucket. */
        K1V1(1),
        /** Two keys followed by two values per bucket. */
        K2V2(2),
        /** Four keys followed by four values per bucket: one cache line. */
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
         * The shape with the given bucket width, for configuration that speaks in widths (1, 2 or 4).
         *
         * @throws IllegalArgumentException for any other width
         */
        public static Shape forBucketWidth(final int bucketWidth) {
            for (final Shape shape : values()) {
                if (shape.bucketWidth == bucketWidth) {
                    return shape;
                }
            }
            throw new IllegalArgumentException("Unsupported bucket width " + bucketWidth + " (supported: 1, 2, 4)");
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
     * hardware with a materially larger L2.
     */
    public static final int DEFAULT_AMAC_THRESHOLD_ENTRIES = 1 << 20;

    /**
     * The narrowest chunk a K4V4 map services through the AMAC window: the window's own width,
     * {@code HashMapK4V4.GET_WINDOW}, so that retuning the window moves this gate with it. Below it a chunked get runs
     * serially even when the footprint says window, because the window only pays when it is full: a chunk narrower than
     * the window cannot overlap a window's worth of misses, while the window's fixed cost (the per-thread scratch,
     * per-job setup) is paid regardless. Measured at 10M entries on an i9-13900K (three forks, serial vs forced window,
     * window/serial time ratio): single-key chunks cost 1.6-2.3x under the window on every load factor and pattern;
     * four-key chunks lose on everything but dense shuffled lookups; sixteen-key chunks win 10% on sparse shuffled and
     * 2.4x on dense shuffled; sixty-four-key chunks win 23% and 3x there and tie dense sorted. Sixteen is the window
     * width: the smallest chunk that can fill it. (Sorted sparse lookups lose under the window at every chunk size on
     * that machine and tie or win on a Ryzen 9 9950X3D2 — a lookup-pattern question, deliberately left out of this
     * gate.)
     */
    public static final int MIN_WINDOWED_CHUNK = HashMapK4V4.GET_WINDOW;

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

    /**
     * Entry count at and above which a map is within reach of the absolute capacity ceiling (~1.07 billion entries),
     * where the configured load factor stops mattering: once doubling is no longer possible (past ~537M entries for
     * K1V1), rehash clamps to the maximum table, the threshold jumps to the nearly-full load factor, and occupancy can
     * only climb until the hard size limit throws. At 750M entries occupancy is already ~0.70 — the measured
     * wash-to-win crossover for the windowed shape — and rising, so the upgrade fires here regardless of the configured
     * load factor.
     */
    public static final int DEFAULT_CEILING_CUTOVER_ENTRIES = 750_000_000;

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
        switch (shape) {
            case K1V1:
                return new HashMapLockFreeK1V1(desiredInitialCapacity, loadFactor, noEntryValue);
            case K2V2:
                return new HashMapLockFreeK2V2(desiredInitialCapacity, loadFactor, noEntryValue);
            case K4V4:
                return new HashMapLockFreeK4V4(desiredInitialCapacity, loadFactor, noEntryValue, readMode);
            default:
                throw new IllegalStateException("Unknown shape " + shape);
        }
    }

    /**
     * Creates a map of the given {@link Shape}, presized so that {@code expectedSize} entries at {@code loadFactor} fit
     * without a rehash.
     */
    public static NullableLongLongMap ofExpectedSize(final Shape shape, final int expectedSize,
            final double loadFactor, final long noEntryValue) {
        final int desiredInitialCapacity = HashMapBase.capacityForExpectedEntries(expectedSize, loadFactor);
        return of(shape, desiredInitialCapacity, loadFactor, noEntryValue);
    }

    /**
     * Should a K4V4-shaped map service this chunked get through the AMAC window? Yes exactly when its FOOTPRINT is past
     * the measured crossover — entry capacity at or above {@link #DEFAULT_AMAC_THRESHOLD_ENTRIES}, which the footprint
     * sweep above puts near L2, well inside the last-level cache — because the window's whole job is overlapping cache
     * misses, and a table that fits the near caches has none worth overlapping (there the window is pure bookkeeping,
     * measured as a tax); and when the chunk is at least {@link #MIN_WINDOWED_CHUNK} keys wide, because a chunk that
     * cannot fill the window pays its fixed cost for nothing (a single-key chunk — the scalar cursor's case — has
     * nothing to overlap at all). Footprint is the first-order predictor. Occupancy turned out to be second-order and
     * is deliberately NOT an input: at a fixed large footprint the window ties or wins at every occupancy measured, and
     * open-addressing occupancy sawtooths in [loadFactor/2, loadFactor] as rehash doubles overshoot, so it never sits
     * where a threshold calibrated on load factor expects it (a lesson learned the hard way). Capacity changes only at
     * rehash, so this answer is stable between rehashes and flips exactly when the array grows past the cache.
     */
    public static boolean wantWindowedReads(final int entryCapacity, final int chunkSize) {
        return chunkSize >= MIN_WINDOWED_CHUNK && entryCapacity >= DEFAULT_AMAC_THRESHOLD_ENTRIES;
    }

    /**
     * If {@code map} is not already a wide-bucket K4V4-shaped map and is dense — deliberately (grown to
     * {@code amacThresholdEntries} entries or more, to be rebuilt at a {@code loadFactor} at or above
     * {@link #AMAC_LOAD_FACTOR_FLOOR}) or forcibly (within reach of the absolute capacity ceiling,
     * {@link #DEFAULT_CEILING_CUTOVER_ENTRIES}, where the configured load factor no longer matters) — returns a
     * presized {@link Shape#K4V4} map holding the same mappings and the same noEntryValue; otherwise returns
     * {@code map} unchanged. This is a LAYOUT change only (four entries per bucket, one cache line, at density); that
     * map's reads then adapt to the AMAC window by footprint on their own (see {@link #wantWindowedReads}). The
     * replacement is presized, so the drain performs no rehashes.
     *
     * <p>
     * The caller owns the swap: it must be the map's single writer, it must publish the returned map through the same
     * field every reader loads per operation (a captured or aliased reference would keep serving the abandoned map),
     * and it must never mutate the old map again. The old map then stays internally consistent forever, so a concurrent
     * reader that loaded the field before the swap simply sees the pre-swap state, under the usual clock discipline.
     */
    public static NullableLongLongMap maybeUpgrade(final NullableLongLongMap map, final double loadFactor,
            final int amacThresholdEntries) {
        return maybeUpgrade(map, loadFactor, amacThresholdEntries, DEFAULT_CEILING_CUTOVER_ENTRIES);
    }

    // Package-visible so tests can exercise the ceiling trigger without building a 750M-entry map.
    static NullableLongLongMap maybeUpgrade(final NullableLongLongMap map, final double loadFactor,
            final int amacThresholdEntries, final int ceilingCutoverEntries) {
        if (map instanceof HashMapK4V4) {
            return map;
        }
        final boolean deliberatelyDense =
                map.size() >= amacThresholdEntries && loadFactor >= AMAC_LOAD_FACTOR_FLOOR;
        final boolean forcedDense = map.size() >= ceilingCutoverEntries;
        if (!deliberatelyDense && !forcedDense) {
            return map;
        }
        final NullableLongLongMap upgraded =
                ofExpectedSize(Shape.K4V4, map.size(), loadFactor, map.defaultReturnValue());
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(upgraded);
        map.forEach(cursor::put);
        return upgraded;
    }
}
