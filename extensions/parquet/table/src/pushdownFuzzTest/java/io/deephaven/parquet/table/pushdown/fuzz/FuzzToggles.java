//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.util.SafeCloseable;

import java.util.Random;

/**
 * A randomized profile over the {@code QueryTable} pushdown switches, applied for the duration of one case and then
 * restored.
 *
 * <p>
 * Flipping these changes <em>which</em> pushdown mechanism serves a filter without changing what the filter must
 * return, so every profile has to agree with the in-memory oracle. The all-disabled profile is included with real
 * weight because it is the control: if a case fails with pushdown on and passes with it off, the divergence is
 * attributable to pushdown rather than to the parquet read path.
 *
 * <p>
 * {@code QueryTable.FORCE_PARALLEL_WHERE}, {@code DISABLE_PARALLEL_WHERE}, {@code PARALLEL_WHERE_ROWS_PER_SEGMENT} and
 * {@code PARALLEL_WHERE_SEGMENTS} are package-private to {@code io.deephaven.engine.table.impl}, so they cannot be set
 * from here. They are driven per-JVM instead, through the {@code QueryTable.forceParallelWhere} /
 * {@code QueryTable.disableParallelWhere} / {@code QueryTable.parallelWhereRowsPerSegment} /
 * {@code QueryTable.parallelWhereSegments} configuration properties, which the {@code pushdownFuzzTest} Gradle task
 * sets from {@code -PfuzzParallelWhere}. What we <em>can</em> vary per case is the thread-local override,
 * {@link QueryTable#disableParallelWhereForThread()}.
 */
public final class FuzzToggles implements SafeCloseable {

    private final boolean useDataIndexForWhere;
    private final boolean disableMergedTables;
    private final boolean disableRowGroupMetadata;
    private final boolean disableDataIndex;
    private final boolean disableDictionary;
    private final boolean disableSortedColumn;
    private final double dictionaryThreshold;
    private final double dataIndexThreshold;
    private final boolean disableParallelWhereForThread;
    private final boolean allDisabled;

    // Saved previous values, restored by close().
    private boolean savedUseDataIndexForWhere;
    private boolean savedDisableMergedTables;
    private boolean savedDisableRowGroupMetadata;
    private boolean savedDisableDataIndex;
    private boolean savedDisableDictionary;
    private boolean savedDisableSortedColumn;
    private double savedDictionaryThreshold;
    private double savedDataIndexThreshold;
    private SafeCloseable threadLocalRestore;
    private boolean applied;

    private FuzzToggles(final Random random) {
        // One case in six is the all-disabled control.
        allDisabled = random.nextInt(6) == 0;
        if (allDisabled) {
            useDataIndexForWhere = false;
            disableMergedTables = true;
            disableRowGroupMetadata = true;
            disableDataIndex = true;
            disableDictionary = true;
            disableSortedColumn = true;
            dictionaryThreshold = 0.25;
            dataIndexThreshold = 0.25;
            disableParallelWhereForThread = random.nextBoolean();
            return;
        }
        useDataIndexForWhere = random.nextInt(4) != 0;
        disableMergedTables = random.nextInt(5) == 0;
        disableRowGroupMetadata = random.nextInt(5) == 0;
        disableDataIndex = random.nextInt(5) == 0;
        disableDictionary = random.nextInt(5) == 0;
        disableSortedColumn = random.nextInt(5) == 0;
        // Double.MAX_VALUE makes the dictionary action always engage; 0 makes it always decline.
        final double[] thresholds = {0.0, 0.25, 1.0, Double.MAX_VALUE};
        dictionaryThreshold = thresholds[random.nextInt(thresholds.length)];
        dataIndexThreshold = thresholds[random.nextInt(thresholds.length)];
        disableParallelWhereForThread = random.nextInt(4) == 0;
    }

    /** Draw a profile. Nothing is applied until {@link #apply()}. */
    public static FuzzToggles generate(final Random random) {
        return new FuzzToggles(random);
    }

    /**
     * Names of pushdown switches to force off for every case, from {@code PushdownFuzzer.forceDisable}.
     *
     * <p>
     * A diagnostic, not part of the sampling: replaying one failing seed while forcing a single action off says which
     * action is responsible, which is otherwise guesswork. Accepts a comma-separated list of {@code merged},
     * {@code stats}, {@code dataIndex}, {@code dictionary}, {@code sorted}, or {@code all}.
     */
    private static final java.util.Set<String> FORCE_DISABLED = java.util.Arrays.stream(
            io.deephaven.configuration.Configuration.getInstance()
                    .getStringWithDefault("PushdownFuzzer.forceDisable", "")
                    .split("[,\\s]+"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(java.util.stream.Collectors.toUnmodifiableSet());

    private boolean forced(final String name, final boolean drawn) {
        return drawn || FORCE_DISABLED.contains(name) || FORCE_DISABLED.contains("all");
    }

    /** Whether this is the all-pushdown-disabled control profile. */
    public boolean allDisabled() {
        return allDisabled;
    }

    /** Whether the data index is reachable at all under this profile. */
    public boolean dataIndexUsable() {
        return useDataIndexForWhere && !disableDataIndex;
    }

    /** Install this profile, saving the previous values. */
    public FuzzToggles apply() {
        if (applied) {
            throw new IllegalStateException("FuzzToggles already applied");
        }
        savedUseDataIndexForWhere = QueryTable.USE_DATA_INDEX_FOR_WHERE;
        savedDisableMergedTables = QueryTable.DISABLE_WHERE_PUSHDOWN_MERGED_TABLES;
        savedDisableRowGroupMetadata = QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA;
        savedDisableDataIndex = QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX;
        savedDisableDictionary = QueryTable.DISABLE_WHERE_PUSHDOWN_DICTIONARY;
        savedDisableSortedColumn = QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION;
        savedDictionaryThreshold = QueryTable.DICTIONARY_FOR_WHERE_THRESHOLD;
        savedDataIndexThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
        applied = true;

        QueryTable.USE_DATA_INDEX_FOR_WHERE = useDataIndexForWhere && !FORCE_DISABLED.contains("dataIndex")
                && !FORCE_DISABLED.contains("all");
        QueryTable.DISABLE_WHERE_PUSHDOWN_MERGED_TABLES = forced("merged", disableMergedTables);
        QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA = forced("stats", disableRowGroupMetadata);
        QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX = forced("dataIndex", disableDataIndex);
        QueryTable.DISABLE_WHERE_PUSHDOWN_DICTIONARY = forced("dictionary", disableDictionary);
        QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION = forced("sorted", disableSortedColumn);
        QueryTable.DICTIONARY_FOR_WHERE_THRESHOLD = dictionaryThreshold;
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = dataIndexThreshold;
        if (disableParallelWhereForThread) {
            threadLocalRestore = QueryTable.disableParallelWhereForThread();
        }
        return this;
    }

    @Override
    public void close() {
        if (!applied) {
            return;
        }
        if (threadLocalRestore != null) {
            threadLocalRestore.close();
            threadLocalRestore = null;
        }
        QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndexForWhere;
        QueryTable.DISABLE_WHERE_PUSHDOWN_MERGED_TABLES = savedDisableMergedTables;
        QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA = savedDisableRowGroupMetadata;
        QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX = savedDisableDataIndex;
        QueryTable.DISABLE_WHERE_PUSHDOWN_DICTIONARY = savedDisableDictionary;
        QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION = savedDisableSortedColumn;
        QueryTable.DICTIONARY_FOR_WHERE_THRESHOLD = savedDictionaryThreshold;
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedDataIndexThreshold;
        applied = false;
    }

    @Override
    public String toString() {
        if (allDisabled) {
            return "toggles=ALL_PUSHDOWN_DISABLED"
                    + (disableParallelWhereForThread ? " serialWhereThread" : "");
        }
        final StringBuilder sb = new StringBuilder("toggles=");
        sb.append("useDataIndex=").append(useDataIndexForWhere);
        if (disableMergedTables) {
            sb.append(" noMerged");
        }
        if (disableRowGroupMetadata) {
            sb.append(" noStats");
        }
        if (disableDataIndex) {
            sb.append(" noDataIndex");
        }
        if (disableDictionary) {
            sb.append(" noDictionary");
        }
        if (disableSortedColumn) {
            sb.append(" noSorted");
        }
        sb.append(" dictThreshold=").append(dictionaryThreshold);
        sb.append(" indexThreshold=").append(dataIndexThreshold);
        if (disableParallelWhereForThread) {
            sb.append(" serialWhereThread");
        }
        return sb.toString();
    }
}
