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
     * Entry count at and above which a map's working set is comfortably beyond any last-level cache, so chunked gets
     * are miss-dominated and the AMAC window pays for its bookkeeping (measured: parity at 100K entries, 2.5x on
     * shuffled lookups at 10M dense). Below it, the serial maps' simpler loop ties or wins.
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
     * Creates a map presized for {@code expectedSize} entries, choosing the shape by size and density: the windowed
     * {@link HashMapLockFreeK4V4WithAMAC} when the map will be both big (at or above {@code amacThresholdEntries}) and
     * dense — deliberately (load factor at or above {@link #AMAC_LOAD_FACTOR_FLOOR}) or forcibly (within reach of the
     * absolute capacity ceiling, {@link #DEFAULT_CEILING_CUTOVER_ENTRIES}); the serial {@link HashMapLockFreeK4V4}
     * otherwise.
     */
    public static NullableLongLongMap ofExpectedSize(final int expectedSize, final double loadFactor,
            final long noEntryValue, final int amacThresholdEntries) {
        final boolean deliberatelyDense =
                expectedSize >= amacThresholdEntries && loadFactor >= AMAC_LOAD_FACTOR_FLOOR;
        final boolean forcedDense = expectedSize >= DEFAULT_CEILING_CUTOVER_ENTRIES;
        return deliberatelyDense || forcedDense
                ? HashMapLockFreeK4V4WithAMAC.ofExpectedSize(expectedSize, loadFactor, noEntryValue)
                : HashMapLockFreeK4V4.ofExpectedSize(expectedSize, loadFactor, noEntryValue);
    }

    /**
     * If {@code map} is not already the windowed shape and is dense — deliberately (grown to
     * {@code amacThresholdEntries} entries or more, to be rebuilt at a {@code loadFactor} at or above
     * {@link #AMAC_LOAD_FACTOR_FLOOR}) or forcibly (within reach of the absolute capacity ceiling,
     * {@link #DEFAULT_CEILING_CUTOVER_ENTRIES}, where the configured load factor no longer matters) — returns a
     * presized {@link HashMapLockFreeK4V4WithAMAC} holding the same mappings and the same noEntryValue; otherwise
     * returns {@code map} unchanged. The replacement is presized, so the drain performs no rehashes.
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
        if (map instanceof HashMapLockFreeK4V4WithAMAC) {
            return map;
        }
        final boolean deliberatelyDense =
                map.size() >= amacThresholdEntries && loadFactor >= AMAC_LOAD_FACTOR_FLOOR;
        final boolean forcedDense = map.size() >= ceilingCutoverEntries;
        if (!deliberatelyDense && !forcedDense) {
            return map;
        }
        final NullableLongLongMap upgraded =
                HashMapLockFreeK4V4WithAMAC.ofExpectedSize(map.size(), loadFactor, map.defaultReturnValue());
        final NullableLongLongMap.ScalarAccess cursor = new NullableLongLongMap.ScalarAccess(upgraded);
        map.forEach(cursor::put);
        return upgraded;
    }
}
