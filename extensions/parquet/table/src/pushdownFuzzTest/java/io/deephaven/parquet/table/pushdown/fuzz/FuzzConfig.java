//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.configuration.Configuration;

/**
 * Run knobs for the pushdown fuzzer, read from configuration.
 *
 * <p>
 * A plain JVM {@code -D} wins over {@code dh-tests.prop}, because {@code Configuration.reloadProperties} does
 * {@code properties.putAll(System.getProperties())}. The {@code pushdownFuzzTest} Gradle task forwards every
 * {@code -DPushdownFuzzer.*} it was given into the test JVM.
 */
public final class FuzzConfig {

    /** Master seed. {@code -1} means "use the wall clock", for open-ended exploration. */
    public final long baseSeed;

    /** Cases to run. {@code -1} means unbounded, which is only useful together with {@link #maxMinutes}. */
    public final int cases;

    /** Wall-clock budget in minutes; {@code 0} means none. Checked between cases. */
    public final int maxMinutes;

    /** Index into the seed stream to start from, so a truncated run can be resumed. */
    public final int startCase;

    /** Upper bound on rows per table; the size axis draws at or below this. */
    public final int maxTableSize;

    /** Upper bound on columns per case. */
    public final int maxColumns;

    /** Filters (or filter groups) evaluated per case. */
    public final int filtersPerCase;

    /** Stop at the first mismatch, rather than collecting. */
    public final boolean failFast;

    /** When collecting, give up after this many failing cases. */
    public final int maxFailures;

    /** Fraction of cases that apply some rename. */
    public final double renameWeight;

    /** Fraction of cases with a sorted column. */
    public final double sortedWeight;

    /** Fraction of cases using a key-value partitioned layout. */
    public final double partitionedWeight;

    /** Fraction of cases that are empty in some form. */
    public final double emptyWeight;

    /** Fraction of filter groups that carry barrier/serial wrappers. */
    public final double barrierWeight;

    /**
     * Exact case seeds to run, instead of drawing a stream from {@link #baseSeed}.
     *
     * <p>
     * This is the correct way to replay one case. A case seed is <em>not</em> usable as a base seed: case seeds come
     * from {@code new Random(baseSeed).nextLong()}, and {@code new Random(s).nextLong() != s}, so passing a case seed
     * as {@code baseSeed} runs a different case entirely.
     */
    public final long[] seeds;

    private FuzzConfig(
            final long baseSeed,
            final int cases,
            final int maxMinutes,
            final int startCase,
            final int maxTableSize,
            final int maxColumns,
            final int filtersPerCase,
            final boolean failFast,
            final int maxFailures,
            final double renameWeight,
            final double sortedWeight,
            final double partitionedWeight,
            final double emptyWeight,
            final double barrierWeight,
            final long[] seeds) {
        this.baseSeed = baseSeed;
        this.cases = cases;
        this.maxMinutes = maxMinutes;
        this.startCase = startCase;
        this.maxTableSize = maxTableSize;
        this.maxColumns = maxColumns;
        this.filtersPerCase = filtersPerCase;
        this.failFast = failFast;
        this.maxFailures = maxFailures;
        this.renameWeight = renameWeight;
        this.sortedWeight = sortedWeight;
        this.partitionedWeight = partitionedWeight;
        this.emptyWeight = emptyWeight;
        this.barrierWeight = barrierWeight;
        this.seeds = seeds;
    }

    /** Read the knobs from configuration. */
    public static FuzzConfig fromConfiguration() {
        final Configuration config = Configuration.getInstance();
        final long configuredSeed = config.getLongWithDefault("PushdownFuzzer.baseSeed", 0L);
        return new FuzzConfig(
                configuredSeed == -1L ? System.nanoTime() : configuredSeed,
                config.getIntegerWithDefault("PushdownFuzzer.cases", 1000),
                config.getIntegerWithDefault("PushdownFuzzer.maxMinutes", 0),
                config.getIntegerWithDefault("PushdownFuzzer.startCase", 0),
                config.getIntegerWithDefault("PushdownFuzzer.maxTableSize", 100_000),
                config.getIntegerWithDefault("PushdownFuzzer.maxColumns", 4),
                config.getIntegerWithDefault("PushdownFuzzer.filtersPerCase", 8),
                config.getBooleanWithDefault("PushdownFuzzer.failFast", true),
                config.getIntegerWithDefault("PushdownFuzzer.maxFailures", 10),
                config.getDoubleWithDefault("PushdownFuzzer.renameWeight", 0.5),
                config.getDoubleWithDefault("PushdownFuzzer.sortedWeight", 0.5),
                config.getDoubleWithDefault("PushdownFuzzer.partitionedWeight", 0.3),
                config.getDoubleWithDefault("PushdownFuzzer.emptyWeight", 0.1),
                config.getDoubleWithDefault("PushdownFuzzer.barrierWeight", 0.2),
                parseSeeds(config.getStringWithDefault("PushdownFuzzer.seeds", "")));
    }

    /** A small deterministic configuration, for the harness's own self-test. */
    public static FuzzConfig forSmoke(final long baseSeed, final int cases) {
        return new FuzzConfig(baseSeed, cases, 0, 0, 2_000, 3, 6, true, 1,
                0.5, 0.5, 0.3, 0.1, 0.2, new long[0]);
    }

    private static long[] parseSeeds(final String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new long[0];
        }
        final String[] parts = raw.split("[,\\s]+");
        final java.util.List<Long> parsed = new java.util.ArrayList<>(parts.length);
        for (final String part : parts) {
            final String trimmed = part.trim().replaceAll("[lL]$", "");
            if (!trimmed.isEmpty()) {
                parsed.add(Long.parseLong(trimmed));
            }
        }
        final long[] seeds = new long[parsed.size()];
        for (int ii = 0; ii < seeds.length; ++ii) {
            seeds[ii] = parsed.get(ii);
        }
        return seeds;
    }

    @Override
    public String toString() {
        return "FuzzConfig{baseSeed=" + baseSeed + "L"
                + ", cases=" + cases
                + ", maxMinutes=" + maxMinutes
                + ", startCase=" + startCase
                + ", maxTableSize=" + maxTableSize
                + ", maxColumns=" + maxColumns
                + ", filtersPerCase=" + filtersPerCase
                + ", failFast=" + failFast
                + ", maxFailures=" + maxFailures
                + ", renameWeight=" + renameWeight
                + ", sortedWeight=" + sortedWeight
                + ", partitionedWeight=" + partitionedWeight
                + ", emptyWeight=" + emptyWeight
                + ", barrierWeight=" + barrierWeight
                + (seeds.length == 0 ? "" : ", seeds=" + java.util.Arrays.toString(seeds))
                + '}';
    }
}
