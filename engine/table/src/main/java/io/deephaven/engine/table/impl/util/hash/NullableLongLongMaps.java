//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

/**
 * Construction and upgrade policy for {@link NullableLongLongMap}s: the one place where "which shape?" decisions live,
 * so that owners do not each re-derive them. (If shape-shifting ever moves inside the maps themselves — a rehash that
 * changes the array's shape — this policy moves with it.)
 */
public final class NullableLongLongMaps {
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
     * Load factor at and below which the windowed shape does not pay, regardless of size. Measured at 10M entries: at
     * load factor 0.5 the windowed shape loses (~30-40% slower — probe chains barely exist, so the window has nothing
     * to hide and wider buckets just cost more bytes per probe); at 0.75 it is a wash (sorted lookups trend against it,
     * shuffled trend for it, error bars overlapping); at 0.9 it wins 20% on sorted and 2.5x on shuffled. The floor sits
     * above the measured wash and below the measured win.
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
     * Should a K4V4-shaped map service chunked gets through the AMAC window right now? Yes exactly when its FOOTPRINT
     * is beyond the last-level cache — entry capacity at or above {@link #DEFAULT_AMAC_THRESHOLD_ENTRIES} — because the
     * window's whole job is overlapping cache misses, and a cache-resident table has none to overlap (there the window
     * is pure bookkeeping, measured as a tax). Footprint is the first-order predictor. Occupancy turned out to be
     * second-order and is deliberately NOT an input: at a fixed large footprint the window ties or wins at every
     * occupancy measured, and open-addressing occupancy sawtooths in [loadFactor/2, loadFactor] as rehash doubles
     * overshoot, so it never sits where a threshold calibrated on load factor expects it (a lesson learned the hard
     * way). Capacity changes only at rehash, so this answer is stable between rehashes and flips exactly when the array
     * grows past the cache.
     */
    public static boolean wantWindowedReads(final int entryCapacity) {
        return entryCapacity >= DEFAULT_AMAC_THRESHOLD_ENTRIES;
    }

    /**
     * If {@code map} is not already a wide-bucket K4V4-shaped map and is dense — deliberately (grown to
     * {@code amacThresholdEntries} entries or more, to be rebuilt at a {@code loadFactor} at or above
     * {@link #AMAC_LOAD_FACTOR_FLOOR}) or forcibly (within reach of the absolute capacity ceiling,
     * {@link #DEFAULT_CEILING_CUTOVER_ENTRIES}, where the configured load factor no longer matters) — returns a
     * presized {@link HashMapLockFreeK4V4} holding the same mappings and the same noEntryValue; otherwise returns
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
                HashMapLockFreeK4V4.ofExpectedSize(map.size(), loadFactor, map.defaultReturnValue());
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(upgraded);
        map.forEach(cursor::put);
        return upgraded;
    }
}
