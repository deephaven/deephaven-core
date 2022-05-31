package io.deephaven.web.client.api;


import elemental2.core.JsMap;
import io.deephaven.web.shared.data.ColumnStatistics;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Javascript wrapper for {@link ColumnStatistics}
 */
public class JsColumnStatistics {

    public enum StatType {
        // Note that a null format means default to columns formatting
        COUNT("COUNT", "long"), SIZE("SIZE", "long"), UNIQUE_VALUES("UNIQUE VALUES", "long"), SUM("SUM", null), SUM_ABS(
                "SUM (ABS)", null), AVG("AVG", "double"), AVG_ABS("AVG (ABS)", "double"), MIN("MIN",
                        null), MIN_ABS("MIN (ABS)", null), MAX("MAX", null), MAX_ABS("MAX (ABS)", null);

        private final String displayName;
        private final String formatType;

        StatType(String displayName, String formatType) {
            this.displayName = displayName;
            this.formatType = formatType;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getFormatType() {
            return formatType;
        }
    }

    private static final Map<String, String> STAT_TYPE_MAP = new HashMap<>();
    static {
        Arrays.stream(StatType.values())
                .forEach(type -> STAT_TYPE_MAP.put(type.getDisplayName(), type.getFormatType()));
    }

    private final JsMap<String, Object> statisticsMap;
    private final JsMap<String, Double> uniqueValues;

    @JsIgnore
    public JsColumnStatistics(ColumnStatistics statistics) {
        statisticsMap = new JsMap<>();
        statisticsMap.set(StatType.SIZE.getDisplayName(), (double) statistics.getSize());
        if (statistics.getType() == ColumnStatistics.ColumnType.NUMERIC) {
            // Mimics io.deephaven.console.engine.GenerateNumericalStatsFunction.statsResultToString
            if (statistics.getCount() > 0) {
                statisticsMap.set(StatType.SUM.getDisplayName(), statistics.getSum());
                statisticsMap.set(StatType.SUM_ABS.getDisplayName(), statistics.getAbsSum());
            }
            statisticsMap.set(StatType.COUNT.getDisplayName(), (double) statistics.getCount());
            if (statistics.getCount() > 0) {
                statisticsMap.set(StatType.AVG.getDisplayName(), statistics.getSum() / (double) statistics.getCount());
                statisticsMap.set(StatType.AVG_ABS.getDisplayName(),
                        statistics.getAbsSum() / (double) statistics.getCount());
                statisticsMap.set(StatType.MIN.getDisplayName(), statistics.getMin());
                statisticsMap.set(StatType.MAX.getDisplayName(), statistics.getMax());
                statisticsMap.set(StatType.MIN_ABS.getDisplayName(), statistics.getAbsMin());
                statisticsMap.set(StatType.MAX_ABS.getDisplayName(), statistics.getAbsMax());
            }
        } else if (statistics.getType() == ColumnStatistics.ColumnType.DATETIME) {
            statisticsMap.set(StatType.COUNT.getDisplayName(), (double) statistics.getCount());
            if (statistics.getCount() > 0) {
                statisticsMap.set(StatType.MIN.getDisplayName(), new DateWrapper(statistics.getMinDateTime()));
                statisticsMap.set(StatType.MAX.getDisplayName(), new DateWrapper(statistics.getMaxDateTime()));
            }
        } else {
            statisticsMap.set(StatType.COUNT.getDisplayName(), (double) statistics.getCount());
            if (statistics.getType() == ColumnStatistics.ColumnType.COMPARABLE) {
                statisticsMap.set(StatType.UNIQUE_VALUES.getDisplayName(), (double) statistics.getNumUnique());
            }
        }

        uniqueValues = new JsMap<>();
        if (statistics.getType() == ColumnStatistics.ColumnType.COMPARABLE) {
            final String[] keys = statistics.getUniqueKeys();
            final long[] values = statistics.getUniqueValues();
            assert keys.length == values.length : "Table Statistics Unique Value Count does not have the same" +
                    "number of keys and values.  Keys = " + keys.length + ", Values = " + values.length;
            for (int i = 0; i < keys.length; i++) {
                uniqueValues.set(keys[i], (double) values[i]);
            }
        }
    }

    /**
     * Gets the type of formatting that should be used for given statistic.
     *
     * @param name the display name of the statistic
     * @return the format type, null to use column formatting
     */
    @JsMethod
    public String getType(String name) {
        return STAT_TYPE_MAP.get(name);
    }

    /**
     * Gets a map with the display name of statistics as keys and the numeric stat as a value.
     *
     * @return the statistics map
     */
    @JsProperty
    public JsMap<String, Object> getStatisticsMap() {
        return statisticsMap;
    }


    /**
     * Gets a map with the name of each unique value as key and the count a the value.
     *
     * @return the unique values map
     */
    @JsProperty
    public JsMap<String, Double> getUniqueValues() {
        return uniqueValues;
    }
}
