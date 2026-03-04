//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;


import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsMap;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents statistics for a given table column.
 *
 * Statistics are exposed via {@link #getStatisticsMap()}, keyed by display name (for example, "COUNT" or "AVG"). For
 * each statistic name, {@link #getType(String)} returns the expected type name for formatting purposes, or {@code null}
 * to indicate that the column's formatting should be used.
 *
 * If present, {@link #getUniqueValues()} returns a map of unique values (as strings) to their occurrence counts.
 */
@TsInterface
@TsName(name = "ColumnStatistics", namespace = "dh")
public class JsColumnStatistics {

    public enum StatType {
        // Note that a null format means default to columns formatting
        /**
         * The number of non-null values in the column.
         */
        COUNT("COUNT", "long"),
        /**
         * The total number of values in the column.
         */
        SIZE("SIZE", "long"),
        /**
         * The number of unique values in the column.
         */
        UNIQUE_VALUES("UNIQUE VALUES", "int"),
        /**
         * The sum of all data in the column.
         */
        SUM("SUM", null),
        /**
         * The sum of the absolute value of all data in the column.
         */
        SUM_ABS("SUM (ABS)", null),
        /**
         * The average of all data in the column.
         */
        AVG("AVG", "double"),
        /**
         * The average of the absolute value of all data in the column.
         */
        AVG_ABS("AVG (ABS)", "double"),
        /**
         * The minimum value found in the column.
         */
        MIN("MIN", null),
        /**
         * The minimum absolute value found in the column.
         */
        MIN_ABS("MIN (ABS)", null),
        /**
         * The maximum value found in the column.
         */
        MAX("MAX", null),
        /**
         * The maximum absolute value found in the column.
         */
        MAX_ABS("MAX (ABS)", null),
        /**
         * The sample standard deviation of the values in the column.
         *
         * Sample standard deviation is computed using Bessel's correction
         * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an
         * unbiased estimator of population variance.
         */
        STD_DEV("STD DEV", "double"),
        /**
         * The sum of the square of all values in the column.
         */
        SUM_SQRD("SUM (SQRD)", null);

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
    public JsColumnStatistics(TableData data) {
        statisticsMap = new JsMap<>();

        TableData.Row r = data.get(0);
        Column uniqueKeys = null;
        Column uniqueCounts = null;
        for (Column column : data.getColumns().asList()) {
            if (column.getName().equals("UNIQUE_KEYS")) {
                uniqueKeys = column;
                continue;
            } else if (column.getName().equals("UNIQUE_COUNTS")) {
                uniqueCounts = column;
                continue;
            }
            try {
                StatType type = StatType.valueOf(column.getName());
                statisticsMap.set(type.getDisplayName(), r.get(column));
            } catch (IllegalArgumentException e) {
                // ignore, can't be used as a generic statistic
            }
        }

        uniqueValues = new JsMap<>();
        if (uniqueCounts == null || uniqueKeys == null) {
            return;
        }
        JsArray<String> keys = (JsArray<String>) r.get(uniqueKeys);
        JsArray<LongWrapper> counts = (JsArray<LongWrapper>) r.get(uniqueCounts);
        for (int i = 0; i < keys.length; i++) {
            uniqueValues.set(keys.getAt(i), counts.getAt(i).asNumber());
        }
    }

    /**
     * Gets the type of formatting that should be used for given statistic. A null return value means that the column
     * formatting should be used.
     *
     * @param name the display name of the statistic
     * @return String
     */
    @JsMethod
    public String getType(String name) {
        return STAT_TYPE_MAP.get(name);
    }

    /**
     * Gets a map of each statistic's display name to its value.
     *
     * @return Map of String and Object
     */
    @JsProperty
    public JsMap<String, Object> getStatisticsMap() {
        return statisticsMap;
    }

    /**
     * Gets a map of each unique value's name to the count of how many times it occurred in the column. This map will be
     * empty for tables containing more than 19 unique values.
     *
     * @return Map of String double
     *
     */
    @JsProperty
    public JsMap<String, Double> getUniqueValues() {
        return uniqueValues;
    }
}
