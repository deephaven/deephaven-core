//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsMap;
import elemental2.promise.Promise;

public class ColumnStatisticsTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("staticTable", "empty_table(3).update([\"A = i % 2\", \"B = `` + A\"])");

    public void testStatsWithUniqueCounts() {
        connect(tables)
                .then(table("staticTable"))
                .then(table -> Promise.all(
                        table.getColumnStatistics(table.findColumn("A"))
                                .then(stats -> {
                                    JsMap<String, Double> uniqueValues = stats.getUniqueValues();
                                    assertEquals(0, uniqueValues.size);

                                    return null;
                                }),
                        table.getColumnStatistics(table.findColumn("B"))
                                .then(stats -> {
                                    JsMap<String, Double> uniqueValues = stats.getUniqueValues();
                                    assertEquals(2, uniqueValues.size);
                                    assertEquals(2.0, uniqueValues.get("0"));
                                    assertEquals(1.0, uniqueValues.get("1"));

                                    return null;
                                })))
                .then(this::finish).catch_(this::report);
    }

    public void testStats() {
        connect(tables)
                .then(table("staticTable"))
                .then(table -> Promise.all(
                        table.getColumnStatistics(table.findColumn("A"))
                                .then(stats -> {
                                    JsMap<String, Object> values = stats.getStatisticsMap();
                                    assertEquals(LongWrapper.of(3),
                                            values.get(JsColumnStatistics.StatType.COUNT.getDisplayName()));
                                    assertEquals(LongWrapper.of(3),
                                            values.get(JsColumnStatistics.StatType.SIZE.getDisplayName()));
                                    assertEquals(null,
                                            values.get(JsColumnStatistics.StatType.UNIQUE_VALUES.getDisplayName()));
                                    assertEquals(LongWrapper.of(1),
                                            values.get(JsColumnStatistics.StatType.SUM.getDisplayName()));
                                    assertEquals(LongWrapper.of(1),
                                            values.get(JsColumnStatistics.StatType.SUM_ABS.getDisplayName()));
                                    assertEquals(1. / 3., values.get(JsColumnStatistics.StatType.AVG.getDisplayName()));
                                    assertEquals(1. / 3.,
                                            values.get(JsColumnStatistics.StatType.AVG_ABS.getDisplayName()));
                                    assertEquals(0.0, values.get(JsColumnStatistics.StatType.MIN.getDisplayName()));
                                    assertEquals(0.0, values.get(JsColumnStatistics.StatType.MIN_ABS.getDisplayName()));
                                    assertEquals(1.0, values.get(JsColumnStatistics.StatType.MAX.getDisplayName()));
                                    assertEquals(1.0, values.get(JsColumnStatistics.StatType.MAX_ABS.getDisplayName()));
                                    assertEquals(0.5774,
                                            (Double) values.get(JsColumnStatistics.StatType.STD_DEV.getDisplayName()),
                                            0.0001);
                                    assertEquals(LongWrapper.of(1),
                                            values.get(JsColumnStatistics.StatType.SUM_SQRD.getDisplayName()));

                                    return null;
                                }),
                        table.getColumnStatistics(table.findColumn("B"))
                                .then(stats -> {
                                    JsMap<String, Object> values = stats.getStatisticsMap();
                                    assertEquals(LongWrapper.of(3),
                                            values.get(JsColumnStatistics.StatType.COUNT.getDisplayName()));
                                    assertEquals(LongWrapper.of(3),
                                            values.get(JsColumnStatistics.StatType.SIZE.getDisplayName()));
                                    assertEquals(2.0,
                                            values.get(JsColumnStatistics.StatType.UNIQUE_VALUES.getDisplayName()));

                                    assertEquals(null, values.get(JsColumnStatistics.StatType.SUM.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.SUM_ABS.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.AVG.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.AVG_ABS.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.MIN.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.MIN_ABS.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.MAX.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.MAX_ABS.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.STD_DEV.name()));
                                    assertEquals(null, values.get(JsColumnStatistics.StatType.SUM_SQRD.name()));
                                    return null;
                                })))
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

}
