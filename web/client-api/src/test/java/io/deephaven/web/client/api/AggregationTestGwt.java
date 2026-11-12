//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.agg.AggAllByUnion;
import io.deephaven.web.client.api.agg.AggByOptions;
import io.deephaven.web.client.api.agg.Aggregation;
import io.deephaven.web.client.api.agg.AggregationUnion;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.ide.IdeSession;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs every aggregation type through {@code aggBy}, both as built by the {@link Aggregation} factories and as the
 * plain objects JS code writes, and checks each output against the engine's answer.
 */
public class AggregationTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            // Two groups of three rows: G = 0 holds X = 0, 2, 4 and G = 1 holds X = 1, 3, 5
            .script("source", "empty_table(6).update([\"G = i % 2\", \"X = i\", \"W = 1.0 + i\", \"K = G * 10\"])");

    /** An aggregation in both forms, the column it writes, and that column's value in each group; null for no check. */
    private static final class Case {
        final AggregationUnion factory;
        final JsPropertyMap<Object> literal;
        final String column;
        final boolean checked;
        final Double group0;
        final Double group1;
        final double tolerance;

        Case(AggregationUnion factory, JsPropertyMap<Object> literal, String column, boolean checked, Double group0,
                Double group1, double tolerance) {
            this.factory = factory;
            this.literal = literal;
            this.column = column;
            this.checked = checked;
            this.group0 = group0;
            this.group1 = group1;
            this.tolerance = tolerance;
        }
    }

    private static Case value(Object factory, JsPropertyMap<Object> literal, String column, double group0,
            double group1) {
        return new Case(Js.uncheckedCast(factory), literal, column, true, group0, group1, 1e-9);
    }

    private static Case nullValue(Object factory, JsPropertyMap<Object> literal, String column) {
        return new Case(Js.uncheckedCast(factory), literal, column, true, null, null, 0);
    }

    /** Array- or object-valued outputs: only that the server accepts the aggregation and makes the column. */
    private static Case present(Object factory, JsPropertyMap<Object> literal, String column) {
        return new Case(Js.uncheckedCast(factory), literal, column, false, null, null, 0);
    }

    private static List<Case> cases() {
        List<Case> cases = new ArrayList<>();
        cases.add(value(Aggregation.AggSum(arr("SumX = X")),
                JsPropertyMap.of("type", "Sum", "columns", arr("SumX = X")), "SumX", 6, 9));
        cases.add(value(Aggregation.AggAbsSum(arr("AbsSumX = X")),
                JsPropertyMap.of("type", "AbsSum", "columns", arr("AbsSumX = X")), "AbsSumX", 6, 9));
        cases.add(value(Aggregation.AggAvg(arr("AvgX = X")),
                JsPropertyMap.of("type", "Avg", "columns", arr("AvgX = X")), "AvgX", 2, 3));
        cases.add(value(Aggregation.AggFirst(arr("FirstX = X")),
                JsPropertyMap.of("type", "First", "columns", arr("FirstX = X")), "FirstX", 0, 1));
        cases.add(value(Aggregation.AggLast(arr("LastX = X")),
                JsPropertyMap.of("type", "Last", "columns", arr("LastX = X")), "LastX", 4, 5));
        cases.add(value(Aggregation.AggMax(arr("MaxX = X")),
                JsPropertyMap.of("type", "Max", "columns", arr("MaxX = X")), "MaxX", 4, 5));
        cases.add(value(Aggregation.AggMin(arr("MinX = X")),
                JsPropertyMap.of("type", "Min", "columns", arr("MinX = X")), "MinX", 0, 1));
        cases.add(value(Aggregation.AggMed(null, arr("MedX = X")),
                JsPropertyMap.of("type", "Median", "columns", arr("MedX = X")), "MedX", 2, 3));
        // Sample standard deviation and variance of 0, 2, 4 (and of 1, 3, 5)
        cases.add(value(Aggregation.AggStd(arr("StdX = X")),
                JsPropertyMap.of("type", "Std", "columns", arr("StdX = X")), "StdX", 2, 2));
        cases.add(value(Aggregation.AggVar(arr("VarX = X")),
                JsPropertyMap.of("type", "Var", "columns", arr("VarX = X")), "VarX", 4, 4));
        cases.add(value(Aggregation.AggPct(0.5, null, arr("PctX = X")),
                JsPropertyMap.of("type", "Percentile", "percentile", 0.5, "columns", arr("PctX = X")), "PctX", 2, 3));
        // An approximation, so only close to the exact median
        cases.add(new Case(Js.uncheckedCast(Aggregation.AggApproxPct(0.5, null, arr("ApproxX = X"))),
                JsPropertyMap.of("type", "ApproxPercentile", "percentile", 0.5, "columns", arr("ApproxX = X")),
                "ApproxX", true, 2.0, 3.0, 1));
        cases.add(value(Aggregation.AggCountDistinct(null, arr("CountDistinctX = X")),
                JsPropertyMap.of("type", "CountDistinct", "columns", arr("CountDistinctX = X")), "CountDistinctX", 3,
                3));
        // K is constant within each group, so each has a unique value; X never does, giving the null sentinel
        cases.add(value(Aggregation.AggUnique(null, arr("UniqueK = K"), null),
                JsPropertyMap.of("type", "Unique", "columns", arr("UniqueK = K")), "UniqueK", 0, 10));
        cases.add(nullValue(Aggregation.AggUnique(null, arr("UniqueX = X"), null),
                JsPropertyMap.of("type", "Unique", "columns", arr("UniqueX = X")), "UniqueX"));
        // Weights W = 1, 3, 5 and W = 2, 4, 6
        cases.add(value(Aggregation.AggWAvg(Js.uncheckedCast("W"), arr("WAvgX = X")),
                JsPropertyMap.of("type", "WAvg", "weightColumn", "W", "columns", arr("WAvgX = X")), "WAvgX",
                26.0 / 9, 44.0 / 12));
        cases.add(value(Aggregation.AggWSum(Js.uncheckedCast("W"), arr("WSumX = X")),
                JsPropertyMap.of("type", "WSum", "weightColumn", "W", "columns", arr("WSumX = X")), "WSumX", 26,
                44));
        cases.add(value(Aggregation.AggSortedFirst(arr("W"), arr("SortedFirstX = X")),
                JsPropertyMap.of("type", "SortedFirst", "sortedColumns", arr("W"), "columns",
                        arr("SortedFirstX = X")),
                "SortedFirstX", 0, 1));
        cases.add(value(Aggregation.AggSortedLast(arr("W"), arr("SortedLastX = X")),
                JsPropertyMap.of("type", "SortedLast", "sortedColumns", arr("W"), "columns", arr("SortedLastX = X")),
                "SortedLastX", 4, 5));
        cases.add(value(Aggregation.AggCount("N"),
                JsPropertyMap.of("type", "Count", "col", "N"), "N", 3, 3));
        cases.add(value(Aggregation.AggCountWhere("NBig", arr("X > 1")),
                JsPropertyMap.of("type", "CountWhere", "col", "NBig", "filters", arr("X > 1")), "NBig", 2, 2));
        cases.add(value(Aggregation.AggFirstRowKey("FirstKey"),
                JsPropertyMap.of("type", "FirstRowKey", "col", "FirstKey"), "FirstKey", 0, 1));
        cases.add(value(Aggregation.AggLastRowKey("LastKey"),
                JsPropertyMap.of("type", "LastRowKey", "col", "LastKey"), "LastKey", 4, 5));
        cases.add(value(Aggregation.AggFormula("MaxW = max(W)"),
                JsPropertyMap.of("type", "Formula", "formula", "MaxW = max(W)"), "MaxW", 5, 6));
        cases.add(present(Aggregation.AggGroup(arr("GroupX = X")),
                JsPropertyMap.of("type", "Group", "columns", arr("GroupX = X")), "GroupX"));
        cases.add(present(Aggregation.AggDistinct(null, arr("DistinctX = X")),
                JsPropertyMap.of("type", "Distinct", "columns", arr("DistinctX = X")), "DistinctX"));
        cases.add(present(Aggregation.AggTDigest(null, arr("DigestX = X")),
                JsPropertyMap.of("type", "TDigest", "columns", arr("DigestX = X")), "DigestX"));
        cases.add(present(Aggregation.AggPartition("Parts", null),
                JsPropertyMap.of("type", "Partition", "col", "Parts"), "Parts"));
        return cases;
    }

    public void testEveryAggregationFromFactories() {
        List<Case> cases = cases();
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_001);
                    return aggregateAndCheck(session, "aggBy with factory-built aggregations",
                            arr(cases.stream().map(c -> c.factory).toArray()), cases);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testEveryAggregationAsPlainObjects() {
        List<Case> cases = cases();
        connect(tables)
                .then(session -> {
                    delayTestFinish(15_002);
                    return aggregateAndCheck(session, "aggBy with plain-object aggregations",
                            arr(cases.stream().map(c -> c.literal).toArray()), cases);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testFreezeOneRowPerGroup() {
        // Freeze needs at most one row per group, so this groups by X
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_003);
                    AggByOptions options = new AggByOptions();
                    options.aggregations = arr(Aggregation.AggFreeze(arr("FrozenW = W")),
                            JsPropertyMap.of("type", "Freeze", "columns", arr("FrozenW2 = W")));
                    options.groupByColumns = arr("X");
                    JsPendingTable<JsResolvedTable> frozen;
                    try {
                        frozen = session.getTable("source", null).aggBy(options);
                    } catch (RuntimeException e) {
                        return Promise.reject("aggBy with Freeze, from a factory and as a plain object, threw: " + e);
                    }
                    return frozen.resolve().then(table -> snapshot(table,
                            arr("FrozenW", "FrozenW2")).then(data -> {
                                assertEquals(1.0, number(data.getData(0, table.findColumn("FrozenW"))), 1e-9);
                                assertEquals(2.0, number(data.getData(1, table.findColumn("FrozenW2"))), 1e-9);
                                table.close();
                                return Promise.resolve((Object) null);
                            }), err -> Promise.reject("aggBy with Freeze rejected: " + err));
                })
                .then(this::finish).catch_(this::report);
    }

    public void testAggAllByFromFactoryAndPlainObject() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(10_004);
                    JsPendingTable<JsResolvedTable> source = session.getTable("source", null).view(arr("G", "X"));
                    AggAllByUnion fromFactory = Js.uncheckedCast(Aggregation.AggSum(null));
                    AggAllByUnion plain = Js.uncheckedCast(JsPropertyMap.of("type", "Max"));
                    Promise<JsResolvedTable> sums = source.aggAllBy(fromFactory, arr("G")).resolve();
                    Promise<JsResolvedTable> maxes = source.aggAllBy(plain, arr("G")).resolve();
                    return sums.then(sum -> maxes.then(max -> snapshot(sum, arr("X"))
                            .then(sumData -> snapshot(max, arr("X")).then(maxData -> {
                                assertEquals(6, number(sumData.getData(0, sum.findColumn("X"))), 1e-9);
                                assertEquals(9, number(sumData.getData(1, sum.findColumn("X"))), 1e-9);
                                assertEquals(4, number(maxData.getData(0, max.findColumn("X"))), 1e-9);
                                assertEquals(5, number(maxData.getData(1, max.findColumn("X"))), 1e-9);
                                sum.close();
                                max.close();
                                return Promise.resolve((Object) null);
                            }))));
                })
                .then(this::finish).catch_(this::report);
    }

    /** Aggregates {@code source} by G and checks every case's column; fails naming {@code what} on any error. */
    private Promise<Object> aggregateAndCheck(IdeSession session, String what,
            ReadonlyArray<AggregationUnion> aggregations, List<Case> cases) {
        AggByOptions options = new AggByOptions();
        options.aggregations = aggregations;
        options.groupByColumns = arr("G");
        JsPendingTable<JsResolvedTable> aggregated;
        try {
            aggregated = session.getTable("source", null).aggBy(options);
        } catch (RuntimeException e) {
            return Promise.reject(what + " threw: " + e);
        }
        List<Case> checked = cases.stream().filter(c -> c.checked).collect(Collectors.toList());
        return aggregated.resolve().then(table -> {
            for (Case c : cases) {
                if (!table.getColumns().asList().stream().anyMatch(col -> col.getName().equals(c.column))) {
                    return Promise.reject(what + " made no column " + c.column);
                }
            }
            return snapshot(table, arr(checked.stream().map(c -> c.column).toArray())).then(data -> {
                for (Case c : checked) {
                    Column column = table.findColumn(c.column);
                    for (int row = 0; row < 2; row++) {
                        Double expected = row == 0 ? c.group0 : c.group1;
                        Any actual = data.getData(row, column);
                        if (expected == null) {
                            assertNull(what + ": " + c.column + " in group " + row, actual);
                        } else {
                            assertEquals(what + ": " + c.column + " in group " + row, expected, number(actual),
                                    c.tolerance);
                        }
                    }
                }
                table.close();
                return Promise.resolve((Object) null);
            });
        }, err -> Promise.reject(what + " rejected: " + err));
    }

    private static Promise<TableData> snapshot(JsResolvedTable table, ReadonlyArray<Column.ColumnOrName> columns) {
        DataOptions.SnapshotOptions options = new DataOptions.SnapshotOptions();
        options.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 1));
        options.columns = columns;
        return table.createSnapshot(options);
    }

    /** A numeric cell as a double, whether it arrives as a JS number or, for a long column, a LongWrapper. */
    private static double number(Any cell) {
        Object value = cell;
        if (value instanceof LongWrapper) {
            return ((LongWrapper) value).asNumber();
        }
        return Js.asDouble(cell);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
