//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.agg.AggByOptions;
import io.deephaven.web.client.api.agg.Aggregation;
import io.deephaven.web.client.api.agg.AggregationUnion;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.updateby.UpdateByCumulativeSum;
import io.deephaven.web.client.api.updateby.UpdateByOperation;
import io.deephaven.web.client.api.updateby.UpdateByOptions;
import io.deephaven.web.client.api.updateby.UpdateByRollingSum;
import io.deephaven.web.client.api.updateby.UpdateByWindowTicks;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.List;

/**
 * Calls every operation on {@link JsTableOperations} once with trivial arguments and checks that each one resolves to a
 * table. Not a test of the operations' results, only that each call reaches the server and comes back.
 */
public class TableOperationsSmokeTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("source", "empty_table(5).update([\"X = i\", \"Y = i * 2\", \"W = i + 1\"])")
            .script("right", "empty_table(5).update([\"X = i\", \"Z = i * 3\"])")
            .script("grouped", "source.group_by()");

    /** How to call one operation, given the fixture tables. */
    private interface Call {
        JsPendingTable<JsResolvedTable> apply(JsTableOperations source, JsTable right, JsTable grouped);
    }

    private static final class Case {
        final String name;
        final Call call;

        Case(String name, Call call) {
            this.name = name;
            this.call = call;
        }
    }

    private static TableData.RowPositionUnion row(double position) {
        return Js.uncheckedCast(position);
    }

    private static <T> T lit(String literal) {
        return Js.uncheckedCast(literal);
    }

    private static List<Case> cases() {
        FilterCondition xIsOne = FilterValue.ofColumn("X").eq(FilterValue.ofNumber(1.0));

        // The trigger's stamp columns are added to the result, so they must not collide with the base table's
        JsTableOperations.SnapshotWhenOptions snapshotWhen = new JsTableOperations.SnapshotWhenOptions();
        snapshotWhen.stampColumns = arr("Z");

        JsTableOperations.RangeJoinMatch range = new JsTableOperations.RangeJoinMatch();
        range.leftStartColumn = lit("X");
        range.rangeStartRule = lit("LESS_THAN_OR_EQUAL");
        range.rightRangeColumn = lit("X");
        range.rangeEndRule = lit("GREATER_THAN_OR_EQUAL");
        range.leftEndColumn = lit("Y");

        JsTableOperations.DownsampleTableOptions downsample = new JsTableOperations.DownsampleTableOptions();
        downsample.xColumn = lit("L");
        downsample.yColumns = arr("Y");
        downsample.width = 100;

        AggByOptions aggBy = new AggByOptions();
        aggBy.aggregations = arr(Aggregation.AggSum(arr("Y")));
        aggBy.groupByColumns = arr("X");

        UpdateByOperation cumSum = new UpdateByOperation();
        cumSum.spec = Js.uncheckedCast(new UpdateByCumulativeSum());
        cumSum.matchPairs = arr("CS = X");
        UpdateByOptions updateBy = new UpdateByOptions();
        updateBy.operations = arr(cumSum);
        updateBy.groupByColumns = arr();

        // A rolling operation with no forward window, which then defaults to zero ticks
        UpdateByWindowTicks threeTicks = new UpdateByWindowTicks();
        threeTicks.ticks = 3;
        UpdateByRollingSum rollingSumSpec = new UpdateByRollingSum();
        rollingSumSpec.reverseWindowScale = Js.uncheckedCast(threeTicks);
        UpdateByOperation rollingSum = new UpdateByOperation();
        rollingSum.spec = Js.uncheckedCast(rollingSumSpec);
        rollingSum.matchPairs = arr("RS = X");
        UpdateByOptions rollingUpdateBy = new UpdateByOptions();
        rollingUpdateBy.operations = arr(rollingSum);

        List<Case> cases = new ArrayList<>();
        cases.add(new Case("head", (s, r, g) -> s.head(row(2))));
        cases.add(new Case("tail", (s, r, g) -> s.tail(row(2))));
        cases.add(new Case("reverse", (s, r, g) -> s.reverse()));
        cases.add(new Case("snapshot", (s, r, g) -> s.snapshot()));
        cases.add(new Case("snapshotWhen", (s, r, g) -> s.snapshotWhen(JsTableReference.of(r), snapshotWhen)));
        cases.add(new Case("sort", (s, r, g) -> s.sort(arr("X"))));
        cases.add(new Case("where", (s, r, g) -> s.where(arr(xIsOne))));
        cases.add(new Case("whereIn", (s, r, g) -> s.whereIn(JsTableReference.of(r), arr("X"))));
        cases.add(new Case("whereNotIn", (s, r, g) -> s.whereNotIn(JsTableReference.of(r), arr("X"))));
        cases.add(new Case("slice", (s, r, g) -> s.slice(row(1), row(3))));
        cases.add(new Case("view", (s, r, g) -> s.view(arr("Z = X + Y"))));
        cases.add(new Case("updateView", (s, r, g) -> s.updateView(arr("Z = X + Y"))));
        cases.add(new Case("update", (s, r, g) -> s.update(arr("Z = X + Y"))));
        cases.add(new Case("lazyUpdate", (s, r, g) -> s.lazyUpdate(arr("Z = X + Y"))));
        cases.add(new Case("select", (s, r, g) -> s.select(arr("Z = X + Y"))));
        cases.add(new Case("naturalJoin",
                (s, r, g) -> s.naturalJoin(JsTableReference.of(r), arr("X"), arr("Z"), null)));
        cases.add(new Case("exactJoin", (s, r, g) -> s.exactJoin(JsTableReference.of(r), arr("X"), arr("Z"))));
        cases.add(new Case("join", (s, r, g) -> s.join(JsTableReference.of(r), arr("X"), arr("Z"), null)));
        cases.add(new Case("aj", (s, r, g) -> s.aj(JsTableReference.of(r), arr("X"), arr("Z"))));
        cases.add(new Case("raj", (s, r, g) -> s.raj(JsTableReference.of(r), arr("X"), arr("Z"))));
        cases.add(new Case("rangeJoin", (s, r, g) -> s.rangeJoin(JsTableReference.of(r), arr(), range,
                TableOperationsSmokeTestGwt.<AggregationUnion>arr(Aggregation.AggGroup(arr("Z"))))));
        cases.add(new Case("groupBy", (s, r, g) -> s.groupBy(arr("X"))));
        cases.add(new Case("aggAllBy", (s, r, g) -> s.aggAllBy(Js.uncheckedCast(Aggregation.AggSum(null)), arr("X"))));
        cases.add(new Case("aggBy", (s, r, g) -> s.aggBy(aggBy)));
        cases.add(new Case("updateBy", (s, r, g) -> s.updateBy(updateBy)));
        cases.add(new Case("updateBy, rolling without a forward window", (s, r, g) -> s.updateBy(rollingUpdateBy)));
        // The X axis must be an Instant or long column, spanning more nanoseconds than the plot has bins
        cases.add(new Case("downsample",
                (s, r, g) -> s.update(arr("L = (long) X * 1_000_000_000L")).downsample(downsample)));
        cases.add(new Case("selectDistinct", (s, r, g) -> s.selectDistinct(arr("X"))));
        cases.add(new Case("countBy", (s, r, g) -> s.countBy("N", arr("X"))));
        cases.add(new Case("firstBy", (s, r, g) -> s.firstBy(arr("X"))));
        cases.add(new Case("lastBy", (s, r, g) -> s.lastBy(arr("X"))));
        cases.add(new Case("minBy", (s, r, g) -> s.minBy(arr("X"))));
        cases.add(new Case("maxBy", (s, r, g) -> s.maxBy(arr("X"))));
        cases.add(new Case("sumBy", (s, r, g) -> s.sumBy(arr("X"))));
        cases.add(new Case("avgBy", (s, r, g) -> s.avgBy(arr("X"))));
        cases.add(new Case("medianBy", (s, r, g) -> s.medianBy(arr("X"))));
        cases.add(new Case("stdBy", (s, r, g) -> s.stdBy(arr("X"))));
        cases.add(new Case("varBy", (s, r, g) -> s.varBy(arr("X"))));
        cases.add(new Case("absSumBy", (s, r, g) -> s.absSumBy(arr("X"))));
        cases.add(new Case("wsumBy", (s, r, g) -> s.wsumBy(lit("W"), arr("X"))));
        cases.add(new Case("wavgBy", (s, r, g) -> s.wavgBy(lit("W"), arr("X"))));
        cases.add(new Case("ungroup", (s, r, g) -> g.operations().ungroup(null)));
        cases.add(new Case("dropColumns", (s, r, g) -> s.dropColumns(arr("Y"))));
        cases.add(new Case("flatten", (s, r, g) -> s.flatten()));
        return cases;
    }

    public void testEveryOperationResolves() {
        connect(tables)
                .then(session -> {
                    delayTestFinish(20_099);
                    return session.getTable("source", null).asPromise()
                            .then(source -> session.getTable("right", null).asPromise()
                                    .then(right -> session.getTable("grouped", null).asPromise()
                                            .then(grouped -> {
                                                List<String> failures = new ArrayList<>();
                                                return runFrom(cases(), 0, source.operations(), right,
                                                        grouped, failures)
                                                        .then(ignore -> {
                                                            source.close();
                                                            right.close();
                                                            grouped.close();
                                                            if (!failures.isEmpty()) {
                                                                fail(String.join("\n", failures));
                                                            }
                                                            return Promise.resolve(session);
                                                        });
                                            })));
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Runs the cases one at a time, so a hang is attributed to a case by its timeout value (20100 plus its index), and
     * collects failures rather than stopping at the first.
     */
    private Promise<Object> runFrom(List<Case> cases, int index, JsTableOperations source, JsTable right,
            JsTable grouped, List<String> failures) {
        if (index == cases.size()) {
            return Promise.resolve((Object) null);
        }
        Case c = cases.get(index);
        delayTestFinish(20_100 + index);
        Promise<JsResolvedTable> resolved;
        try {
            resolved = c.call.apply(source, right, grouped).resolve();
        } catch (Exception e) {
            failures.add(c.name + ": threw " + e);
            return runFrom(cases, index + 1, source, right, grouped, failures);
        }
        return resolved.then(table -> {
            assertTrue(c.name, table.getSize() >= 0);
            assertTrue(c.name, table.getColumns().getLength() > 0);
            table.close();
            return Promise.resolve((Object) null);
        }, err -> {
            failures.add(c.name + ": " + err);
            return Promise.resolve((Object) null);
        }).then(ignore -> runFrom(cases, index + 1, source, right, grouped, failures));
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
