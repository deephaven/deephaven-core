//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.agg.AggByOptions;
import io.deephaven.web.client.api.agg.Aggregation;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.updateby.UpdateByOptions;
import io.deephaven.web.client.ide.IdeSession;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks that the JS API produces the same tables as the server's Python API. Each case pairs a Python expression on
 * the {@code src} fixture with the JS calls a client would write for it; both results are snapshotted in full and
 * compared cell by cell.
 */
public class ParityTestGwt extends AbstractAsyncGwtTestCase {
    /** A thousand rows: a few keys, every common column type, nulls in each value column, NaN in D. */
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, agg, merge")
            .script("from deephaven import updateby as ub")
            .script("src", "empty_table(1000).update(["
                    + "\"Key = i % 7\","
                    + "\"Sym = (i % 5 == 0) ? null : `S` + (i % 13)\","
                    + "\"I = (i % 11 == 0) ? NULL_INT : (int) ((i * 37) % 1000)\","
                    + "\"L = (i % 17 == 0) ? NULL_LONG : (long) i * 1_000_003L\","
                    + "\"D = (i % 19 == 0) ? NULL_DOUBLE : (i % 23 == 0) ? Double.NaN : i * 1.5 - 300\","
                    + "\"W = (double) (i % 4 + 1)\","
                    + "\"B = (i % 13 == 0) ? null : (i % 2 == 0)\","
                    + "\"T = '2024-01-01T00:00:00 UTC' + i * MINUTE\"])")
            // Operands for the joins: one row per key, a time series per key, some keys, ranges of I, a trigger
            .script("keys", "empty_table(7).update([\"Key = i\", \"Name = `K` + i\"])")
            .script("quotes", "empty_table(200).update([\"Key = i % 7\","
                    + "\"T = '2024-01-01T00:00:00 UTC' + i * 5 * MINUTE\", \"Px = i * 0.25\"])")
            .script("some_keys", "empty_table(3).update([\"Key = i * 2\"])")
            .script("windows", "empty_table(20).update([\"Key = i % 7\", \"Lo = i * 40\", \"Hi = Lo + 30\"])")
            .script("tick", "empty_table(1).update([\"Tick = 1\"])");

    private static final String[] FIXTURE_TABLES = {"src", "keys", "quotes", "some_keys", "windows", "tick"};

    /** The fixture tables, fetched once per test, for cases that join or merge. */
    private static final class Fixture {
        final IdeSession session;
        final Map<String, JsTable> tables = new HashMap<>();

        Fixture(IdeSession session) {
            this.session = session;
        }

        JsTableReference ref(String name) {
            return JsTableReference.of(tables.get(name));
        }

        JsTableOperations ops(String name) {
            return tables.get(name).operations();
        }
    }

    /** The JS calls for one case, made on a fresh operations view of {@code src}. */
    private interface JsCall {
        JsPendingTable<?> on(JsTableOperations src);
    }

    /** As {@link JsCall}, for a case that also uses the other fixture tables or the session. */
    private interface JsFixtureCall {
        JsPendingTable<?> on(JsTableOperations src, Fixture fixture);
    }

    private static final class Case {
        final String python;
        final JsFixtureCall js;

        Case(String python, JsFixtureCall js) {
            this.python = python;
            this.js = js;
        }
    }

    /** A case: the Python expression, which names the case in failures, and the JS calls that should match it. */
    private static Case parity(String python, JsCall js) {
        return new Case(python, (src, fixture) -> js.on(src));
    }

    private static Case parity(String python, JsFixtureCall js) {
        return new Case(python, js);
    }

    private static TableData.RowPositionUnion rows(double count) {
        return Js.uncheckedCast(count);
    }

    public void testFiltersSortsAndProjections() {
        runAll(20_001,
                parity("src.where('I > 500')",
                        src -> src.where(arr(FilterValue.ofColumn("I").greaterThan(FilterValue.ofNumber(500))))),
                parity("src.where('Sym = `S3`')",
                        src -> src.where(arr(FilterValue.ofColumn("Sym").eq(FilterValue.ofString("S3"))))),
                parity("src.where('isNull(D)')",
                        src -> src.where(arr(FilterValue.ofColumn("D").isNull()))),
                parity("src.where_one_of(['I < 100', 'Key = 3'])",
                        src -> src.where(arr(FilterValue.ofColumn("I").lessThan(FilterValue.ofNumber(100))
                                .or(FilterValue.ofColumn("Key").eq(FilterValue.ofNumber(3)))))),
                parity("src.sort('I')",
                        src -> src.sort(arr("I"))),
                parity("src.sort_descending('D')",
                        src -> src.sort(arr(JsPropertyMap.of("column", "D", "direction", Sort.SortDirection.DESC)))),
                parity("src.sort(['Key', 'Sym'])",
                        src -> src.sort(arr("Key", "Sym"))),
                parity("src.update(['X = I * 2', 'Y = D / W'])",
                        src -> src.update(arr("X = I * 2", "Y = D / W"))),
                parity("src.view(['Key', 'Z = L + I'])",
                        src -> src.view(arr("Key", "Z = L + I"))),
                parity("src.update_view(['U = Sym + `_` + Key'])",
                        src -> src.updateView(arr("U = Sym + `_` + Key"))),
                parity("src.select(['Key', 'D'])",
                        src -> src.select(arr("Key", "D"))),
                parity("src.lazy_update(['Q = W * 10'])",
                        src -> src.lazyUpdate(arr("Q = W * 10"))),
                parity("src.drop_columns(['B', 'T'])",
                        src -> src.dropColumns(arr("B", "T"))),
                parity("src.head(10)",
                        src -> src.head(rows(10))),
                parity("src.tail(10)",
                        src -> src.tail(rows(10))),
                parity("src.slice(5, 20)",
                        src -> src.slice(rows(5), rows(20))),
                parity("src.reverse()",
                        JsTableOperations::reverse),
                parity("src.select_distinct(['Key', 'Sym'])",
                        src -> src.selectDistinct(arr("Key", "Sym"))),
                parity("src.where('Key = 2').flatten()",
                        src -> src.where(arr(FilterValue.ofColumn("Key").eq(FilterValue.ofNumber(2)))).flatten()),
                parity("src.snapshot()",
                        JsTableOperations::snapshot));
    }

    /** The numeric columns, for the aggregations that only take numbers. */
    private static final String NUMERIC = "src.view(['Key', 'I', 'L', 'D', 'W'])";

    private static JsPendingTable<JsResolvedTable> numeric(JsTableOperations src) {
        return src.view(arr("Key", "I", "L", "D", "W"));
    }

    public void testAggregations() {
        runAll(20_002,
                parity(NUMERIC + ".sum_by('Key')", src -> numeric(src).sumBy(arr("Key"))),
                parity(NUMERIC + ".abs_sum_by('Key')", src -> numeric(src).absSumBy(arr("Key"))),
                parity(NUMERIC + ".avg_by('Key')", src -> numeric(src).avgBy(arr("Key"))),
                parity(NUMERIC + ".median_by('Key')", src -> numeric(src).medianBy(arr("Key"))),
                parity(NUMERIC + ".std_by('Key')", src -> numeric(src).stdBy(arr("Key"))),
                parity(NUMERIC + ".var_by('Key')", src -> numeric(src).varBy(arr("Key"))),
                parity(NUMERIC + ".min_by('Key')", src -> numeric(src).minBy(arr("Key"))),
                parity(NUMERIC + ".max_by('Key')", src -> numeric(src).maxBy(arr("Key"))),
                parity(NUMERIC + ".weighted_sum_by('W', 'Key')", src -> numeric(src).wsumBy(lit("W"), arr("Key"))),
                parity(NUMERIC + ".weighted_avg_by('W', 'Key')", src -> numeric(src).wavgBy(lit("W"), arr("Key"))),
                parity("src.first_by('Key')", src -> src.firstBy(arr("Key"))),
                parity("src.last_by(['Key', 'B'])", src -> src.lastBy(arr("Key", "B"))),
                parity("src.count_by('N', 'Sym')", src -> src.countBy("N", arr("Sym"))),
                parity(NUMERIC + ".sum_by()", src -> numeric(src).sumBy(null)),

                parity("src.agg_by([agg.sum_(['SumI = I', 'SumD = D'])], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggSum(arr("SumI = I", "SumD = D")))),
                parity("src.agg_by([agg.abs_sum('AbsD = D')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggAbsSum(arr("AbsD = D")))),
                parity("src.agg_by([agg.avg(['AvgI = I', 'AvgL = L'])], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggAvg(arr("AvgI = I", "AvgL = L")))),
                parity("src.agg_by([agg.count_('N')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggCount("N"))),
                parity("src.agg_by([agg.count_where('Big', 'I > 500')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggCountWhere("Big", arr("I > 500")))),
                parity("src.agg_by([agg.count_distinct('CD = Sym')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggCountDistinct(null, arr("CD = Sym")))),
                parity("src.agg_by([agg.count_distinct('CD = Sym', count_nulls=True)], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggCountDistinct(true, arr("CD = Sym")))),
                parity("src.agg_by([agg.distinct('DW = W')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggDistinct(null, arr("DW = W")))),
                parity("src.agg_by([agg.first(['FI = I', 'FS = Sym']), agg.last('LT = T')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggFirst(arr("FI = I", "FS = Sym")),
                                Aggregation.AggLast(arr("LT = T")))),
                parity("src.agg_by([agg.min_('MinD = D'), agg.max_('MaxT = T')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggMin(arr("MinD = D")),
                                Aggregation.AggMax(arr("MaxT = T")))),
                parity("src.agg_by([agg.median('MedI = I')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggMed(null, arr("MedI = I")))),
                parity("src.agg_by([agg.median('MedI = I', average_evenly_divided=False)], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggMed(false, arr("MedI = I")))),
                parity("src.agg_by([agg.pct(0.25, 'P25 = D')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggPct(0.25, null, arr("P25 = D")))),
                parity("src.agg_by([agg.pct(0.25, 'P25 = I', average_evenly_divided=True)], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggPct(0.25, true, arr("P25 = I")))),
                parity("src.agg_by([agg.std('StdD = D'), agg.var('VarI = I')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggStd(arr("StdD = D")),
                                Aggregation.AggVar(arr("VarI = I")))),
                parity("src.agg_by([agg.weighted_avg('W', 'WAvgD = D'), agg.weighted_sum('W', 'WSumI = I')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggWAvg(lit("W"), arr("WAvgD = D")),
                                Aggregation.AggWSum(lit("W"), arr("WSumI = I")))),
                // As in the weighted average example in types/examples/use-cases.ts, factories and a literal mixed
                parity("src.agg_by([agg.weighted_avg('W', 'WAvgX = I'), agg.avg('AvgX = I'), agg.count_('N')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggWAvg(lit("W"), arr("WAvgX = I")),
                                Aggregation.AggAvg(arr("AvgX = I")), JsPropertyMap.of("type", "Count", "col", "N"))),
                parity("src.agg_by([agg.sorted_first('D', 'SF = I'), agg.sorted_last('D', 'SL = I')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggSortedFirst(arr("D"), arr("SF = I")),
                                Aggregation.AggSortedLast(arr("D"), arr("SL = I")))),
                parity("src.agg_by([agg.unique('UW = W', non_unique_sentinel=-1.0)], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggUnique(null,
                                Js.uncheckedCast(JsPropertyMap.of("type", "double", "value", -1.0)), arr("UW = W")))),
                parity("src.agg_by([agg.unique('UI = I', non_unique_sentinel=-1)], by=['Key', 'L'])",
                        src -> aggBy(src, by("Key", "L"), Aggregation.AggUnique(null,
                                Js.uncheckedCast(JsPropertyMap.of("type", "int", "value", -1.0)), arr("UI = I")))),
                parity("src.agg_by([agg.unique('US = Sym', include_nulls=True, non_unique_sentinel='many')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggUnique(true,
                                Js.uncheckedCast(JsPropertyMap.of("type", "string", "value", "many")),
                                arr("US = Sym")))),
                parity("src.agg_by([agg.group('GI = I')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggGroup(arr("GI = I")))),
                // Freeze has no Python agg; AggregationTestGwt covers it
                parity("src.agg_by([agg.formula('MaxW = max(W)')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggFormula("MaxW = max(W)"))),
                // As in the Formula docs: two vector functions, then arithmetic on their results
                parity("src.agg_by([agg.formula('Spread = max(W) - min(W)')], by='Key')",
                        src -> aggBy(src, by("Key"), Aggregation.AggFormula("Spread = max(W) - min(W)"))),
                parity("src.agg_by([agg.sum_('S = I'), agg.count_('N')])",
                        src -> aggBy(src, by(), Aggregation.AggSum(arr("S = I")), Aggregation.AggCount("N"))),

                parity(NUMERIC + ".agg_all_by(agg.sum_(), by='Key')",
                        src -> numeric(src).aggAllBy(Js.uncheckedCast(Aggregation.AggSum(null)), arr("Key"))),
                parity("src.agg_all_by(agg.first(), by=['Key', 'B'])",
                        src -> src.aggAllBy(Js.uncheckedCast(Aggregation.AggFirst(null)), arr("Key", "B"))),
                parity("src.view(['Key', 'Sym', 'B']).agg_all_by(agg.count_distinct(), by='Key')",
                        src -> src.view(arr("Key", "Sym", "B"))
                                .aggAllBy(Js.uncheckedCast(Aggregation.AggCountDistinct(null, null)), arr("Key"))));
    }

    /** aggBy over the given aggregations, grouped by the {@link #by} columns. */
    private static JsPendingTable<JsResolvedTable> aggBy(JsTableOperations src, String[] by, Object... aggregations) {
        AggByOptions options = new AggByOptions();
        options.aggregations = Js.uncheckedCast(aggregations);
        options.groupByColumns = Js.uncheckedCast(by);
        return src.aggBy(options);
    }

    /** The group-by columns, as Python's {@code by=}; none for one group of all rows. */
    private static String[] by(String... columns) {
        return columns;
    }

    private static <T> T lit(String literal) {
        return Js.uncheckedCast(literal);
    }

    public void testUpdateBy() {
        runAll(20_003,
                parity("src.update_by(ub.cum_sum('CS = I'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByCumulativeSum"), "CS = I")),
                parity("src.update_by(ub.cum_prod('CP = W'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByCumulativeProduct"), "CP = W")),
                parity("src.update_by([ub.cum_min('Lo = D'), ub.cum_max('Hi = D')], by='Key')",
                        src -> src.updateBy(options("Key", op(spec("UpdateByCumulativeMin"), "Lo = D"),
                                op(spec("UpdateByCumulativeMax"), "Hi = D")))),
                parity("src.update_by(ub.forward_fill('FS = Sym'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByFill"), "FS = Sym")),
                parity("src.update_by(ub.delta('DL = L'))",
                        src -> updateBy(src, null, spec("UpdateByDelta"), "DL = L")),
                parity("src.update_by(ub.delta('DL = L', delta_control=ub.DeltaControl.VALUE_DOMINATES))",
                        src -> updateBy(src, null, spec("UpdateByDelta", "nullBehavior", "VALUE_DOMINATES"),
                                "DL = L")),
                parity("src.update_by(ub.ema_tick(10, 'E = D'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEma", "windowScale", ticks(10)), "E = D")),
                parity("src.update_by(ub.ema_time('T', 'PT10M', 'E = D'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEma", "windowScale", time("PT10M")), "E = D")),
                parity("src.update_by(ub.ema_tick(10, 'E = D', op_control=ub.OperationControl("
                        + "on_null=ub.BadDataBehavior.RESET, on_nan=ub.BadDataBehavior.RESET)), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEma", "windowScale", ticks(10), "emOptions",
                                JsPropertyMap.of("onNullValue", "RESET", "onNaNValue", "RESET")), "E = D")),
                parity("src.update_by(ub.ems_tick(10, 'E = I'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEms", "windowScale", ticks(10)), "E = I")),
                parity("src.update_by(ub.emmin_tick(10, 'E = D'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEmMin", "windowScale", ticks(10)), "E = D")),
                parity("src.update_by(ub.emmax_time('T', 'PT10M', 'E = D'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEmMax", "windowScale", time("PT10M")), "E = D")),
                parity("src.update_by(ub.emstd_tick(10, 'E = D'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByEmStd", "windowScale", ticks(10)), "E = D")),
                parity("src.update_by(ub.rolling_sum_tick('RS = I', rev_ticks=5, fwd_ticks=2), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingSum", "reverseWindowScale", ticks(5),
                                "forwardWindowScale", ticks(2)), "RS = I")),
                parity("src.update_by(ub.rolling_sum_time('T', 'RS = I', rev_time='PT5M', fwd_time='PT1M'))",
                        src -> updateBy(src, null, spec("UpdateByRollingSum", "reverseWindowScale", time("PT5M"),
                                "forwardWindowScale", time("PT1M")), "RS = I")),
                parity("src.update_by(ub.rolling_group_tick('RG = I', rev_ticks=3), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingGroup", "reverseWindowScale", ticks(3)),
                                "RG = I")),
                parity("src.update_by(ub.rolling_avg_tick('RA = D', rev_ticks=4), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingAvg", "reverseWindowScale", ticks(4)),
                                "RA = D")),
                parity("src.update_by(ub.rolling_min_tick('RMin = I', rev_ticks=4), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingMin", "reverseWindowScale", ticks(4)),
                                "RMin = I")),
                parity("src.update_by(ub.rolling_max_time('T', 'RMax = D', rev_time='PT20M'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingMax", "reverseWindowScale", time("PT20M")),
                                "RMax = D")),
                parity("src.update_by(ub.rolling_prod_tick('RP = W', rev_ticks=3), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingProduct", "reverseWindowScale", ticks(3)),
                                "RP = W")),
                parity("src.update_by(ub.rolling_count_tick('RC = Sym', rev_ticks=6), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingCount", "reverseWindowScale", ticks(6)),
                                "RC = Sym")),
                parity("src.update_by(ub.rolling_std_tick('RStd = D', rev_ticks=6), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingStd", "reverseWindowScale", ticks(6)),
                                "RStd = D")),
                parity("src.update_by(ub.rolling_wavg_tick('W', 'RW = D', rev_ticks=3), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingWAvg", "weightColumn", "W",
                                "reverseWindowScale", ticks(3)), "RW = D")),
                parity("src.update_by(ub.rolling_wavg_time('T', 'W', 'RW = I', rev_time='PT10M'))",
                        src -> updateBy(src, null, spec("UpdateByRollingWAvg", "weightColumn", "W",
                                "reverseWindowScale", time("PT10M")), "RW = I")),
                parity("src.update_by(ub.rolling_formula_tick('RF = sum(I)', rev_ticks=3), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingFormula", "formula", "RF = sum(I)",
                                "reverseWindowScale", ticks(3)))),
                parity("src.update_by(ub.cum_count_where('CW', 'I > 500'), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByCumulativeCountWhere", "resultColumn", "CW",
                                "filters", arr("I > 500")))),
                parity("src.update_by(ub.rolling_count_where_tick('RCW', 'I > 500', rev_ticks=5), by='Key')",
                        src -> updateBy(src, "Key", spec("UpdateByRollingCountWhere", "resultColumn", "RCW",
                                "filters", arr("I > 500"), "reverseWindowScale", ticks(5)))));
    }

    public void testJoinsGroupingAndMerge() {
        runAll(20_004,
                parity("src.natural_join(keys, on='Key', joins='Name')",
                        (src, f) -> src.naturalJoin(f.ref("keys"), arr("Key"), arr("Name"), null)),
                parity("src.exact_join(keys, on='Key', joins='Name')",
                        (src, f) -> src.exactJoin(f.ref("keys"), arr("Key"), arr("Name"))),
                parity("src.where('I < 100').join(keys, on='Key', joins='Name')",
                        (src, f) -> src.where(arr(FilterValue.ofColumn("I").lessThan(FilterValue.ofNumber(100))))
                                .join(f.ref("keys"), arr("Key"), arr("Name"), null)),
                parity("src.where('I < 20').join(some_keys, joins='Picked = Key')",
                        (src, f) -> src.where(arr(FilterValue.ofColumn("I").lessThan(FilterValue.ofNumber(20))))
                                .join(f.ref("some_keys"), arr(), arr("Picked = Key"), null)),
                parity("src.aj(quotes, on=['Key', 'T'], joins='Px')",
                        (src, f) -> src.aj(f.ref("quotes"), arr("Key", "T"), arr("Px"))),
                parity("src.aj(quotes, on=['Key', 'T > T'], joins=['QT = T', 'Px'])",
                        (src, f) -> src.aj(f.ref("quotes"), arr("Key", "T > T"), arr("QT = T", "Px"))),
                parity("src.raj(quotes, on=['Key', 'T'], joins='Px')",
                        (src, f) -> src.raj(f.ref("quotes"), arr("Key", "T"), arr("Px"))),
                parity("src.where_in(some_keys, 'Key')",
                        (src, f) -> src.whereIn(f.ref("some_keys"), arr("Key"))),
                parity("src.where_not_in(some_keys, 'Key')",
                        (src, f) -> src.whereNotIn(f.ref("some_keys"), arr("Key"))),
                // A range join needs its right table ordered by the range column
                parity("windows.range_join(src.sort('I'), on=['Key', 'Lo <= I <= Hi'], aggs=agg.group('Is = I'))",
                        (src, f) -> f.ops("windows").rangeJoin(JsTableReference.of(src.sort(arr("I"))), arr("Key"),
                                loToHi(), arr(Aggregation.AggGroup(arr("Is = I"))))),
                parity("src.view(['Key', 'I']).group_by('Key')",
                        src -> src.view(arr("Key", "I")).groupBy(arr("Key"))),
                parity("src.view(['Key', 'I', 'D']).group_by('Key').ungroup()",
                        src -> src.view(arr("Key", "I", "D")).groupBy(arr("Key")).ungroup(null)),
                parity("src.view(['Key', 'I', 'D']).group_by('Key').ungroup('I')",
                        src -> src.view(arr("Key", "I", "D")).groupBy(arr("Key")).ungroup(Js.uncheckedCast(
                                JsPropertyMap.of("columnsToUngroup", arr("I"))))),
                parity("merge([src.head(5), src.tail(5)])",
                        (src, f) -> f.session.mergeTables(arr(src.head(rows(5)), src.tail(rows(5))))),
                // tick is static and never ticks, so only the initial snapshot has rows
                parity("src.snapshot_when(tick, initial=True)",
                        (src, f) -> src.snapshotWhen(f.ref("tick"),
                                Js.uncheckedCast(JsPropertyMap.of("initial", true)))));
    }

    /** Lo &lt;= I &lt;= Hi, for a range join from the windows table onto src. */
    private static JsTableOperations.RangeJoinMatch loToHi() {
        JsTableOperations.RangeJoinMatch range = new JsTableOperations.RangeJoinMatch();
        range.leftStartColumn = lit("Lo");
        range.rangeStartRule = lit("LESS_THAN_OR_EQUAL");
        range.rightRangeColumn = lit("I");
        range.rangeEndRule = lit("GREATER_THAN_OR_EQUAL");
        range.leftEndColumn = lit("Hi");
        return range;
    }

    /** One updateBy operation applying {@code spec} to {@code pairs}, grouped by {@code key} or not at all. */
    private static JsPendingTable<JsResolvedTable> updateBy(JsTableOperations src, String key, Object spec,
            String... pairs) {
        return src.updateBy(options(key, op(spec, pairs)));
    }

    /** updateBy options for the given operations, grouped by {@code key} or not at all. */
    private static UpdateByOptions options(String key, Object... operations) {
        JsPropertyMap<Object> options = JsPropertyMap.of("operations", Js.uncheckedCast(operations));
        if (key != null) {
            options.set("groupByColumns", arr(key));
        }
        return Js.uncheckedCast(options);
    }

    /** One updateBy operation: a spec applied to the given match pairs, omitted when there are none. */
    private static Object op(Object spec, String... pairs) {
        JsPropertyMap<Object> op = JsPropertyMap.of("spec", spec);
        if (pairs.length > 0) {
            op.set("matchPairs", Js.uncheckedCast(pairs));
        }
        return op;
    }

    /** An updateBy spec object literal: its type, then property names and values in turn. */
    private static Object spec(String type, Object... properties) {
        JsPropertyMap<Object> spec = JsPropertyMap.of("type", type);
        for (int i = 0; i < properties.length; i += 2) {
            spec.set((String) properties[i], properties[i + 1]);
        }
        return spec;
    }

    private static Object ticks(double ticks) {
        return JsPropertyMap.of("type", "ticks", "ticks", ticks);
    }

    /** A window of {@code duration} over the timestamp column T. */
    private static Object time(String duration) {
        return JsPropertyMap.of("type", "time", "column", "T", "duration", duration);
    }

    /**
     * Runs each case in turn and fails once at the end, listing every case whose tables differ. The Python result is
     * held in one variable, {@code result}, reassigned per case and cleared when done.
     */
    private void runAll(int timeout, Case... cases) {
        connect(tables)
                .then(session -> {
                    delayTestFinish(timeout * TIMEOUT_SCALE);
                    Fixture fixture = new Fixture(session);
                    List<String> failures = new ArrayList<>();
                    return fetchFixtures(fixture, 0)
                            .then(ignore -> runFrom(fixture, cases, 0, timeout, failures))
                            .then(ignore -> session.runCode("result = None"))
                            .then(ignore -> {
                                fixture.tables.values().forEach(JsTable::close);
                                if (!failures.isEmpty()) {
                                    fail(failures.size() + " of " + cases.length + " cases differ:\n"
                                            + String.join("\n", failures));
                                }
                                return Promise.resolve(session);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    private static Promise<Object> fetchFixtures(Fixture fixture, int index) {
        if (index == FIXTURE_TABLES.length) {
            return Promise.resolve((Object) null);
        }
        String name = FIXTURE_TABLES[index];
        return fixture.session.getTable(name, null).asPromise().then(table -> {
            fixture.tables.put(name, table);
            return fetchFixtures(fixture, index + 1);
        });
    }

    private Promise<Object> runFrom(Fixture fixture, Case[] cases, int index, int timeout, List<String> failures) {
        if (index == cases.length) {
            return Promise.resolve((Object) null);
        }
        Case c = cases[index];
        IdeSession session = fixture.session;
        delayTestFinish(timeout * TIMEOUT_SCALE);
        boolean[] settled = {false};
        Promise<Object> attempt = session.runCode("result = " + c.python).then(run -> {
            if (run.getError() != null) {
                failures.add(c.python + ": Python failed: " + run.getError());
                return Promise.resolve((Object) null);
            }
            // Both are made in this task: the JS chain on a fresh view of src, and the fetch of the Python result
            Promise<JsResolvedTable> js;
            try {
                js = c.js.on(fixture.ops("src"), fixture).resolve();
            } catch (RuntimeException e) {
                failures.add(c.python + ": JS threw " + e);
                return Promise.resolve((Object) null);
            }
            Promise<JsTable> py = session.getTable("result", null).asPromise();
            return js.then(jsTable -> py.then(pyTable -> compareTables(c.python, pyTable, jsTable)
                    .then(differences -> {
                        pyTable.close();
                        jsTable.close();
                        if (differences != null) {
                            failures.add(differences);
                        }
                        return Promise.resolve((Object) null);
                    }), pyErr -> {
                        jsTable.close();
                        failures.add(c.python + ": fetching the Python result failed: " + pyErr);
                        return Promise.resolve((Object) null);
                    }), jsErr -> {
                        failures.add(c.python + ": JS rejected: " + jsErr);
                        return py.then(pyTable -> {
                            pyTable.close();
                            return Promise.resolve((Object) null);
                        }, pyErr -> Promise.resolve((Object) null));
                    });
        }).catch_(err -> {
            // Anything not recorded above, so that one case cannot stop the rest or lose their results
            failures.add(c.python + ": " + err);
            return Promise.resolve((Object) null);
        }).then(ignore -> {
            settled[0] = true;
            return Promise.resolve((Object) null);
        });
        // A case that never settles is reported by name, well before the test's own timeout. The run stops there: the
        // case may still be running, and the next would reassign the result it reads.
        Promise<Object> deadline = delay(CASE_TIMEOUT_MS * TIMEOUT_SCALE)
                .then(ignore -> Promise.resolve(settled[0] ? null : TIMED_OUT));
        return Promise.race(attempt, deadline).then(winner -> {
            if (winner == TIMED_OUT) {
                failures.add(c.python + ": no result after " + CASE_TIMEOUT_MS + " ms; the "
                        + (cases.length - index - 1) + " cases after it were not run");
                return Promise.resolve((Object) null);
            }
            return runFrom(fixture, cases, index + 1, timeout, failures);
        });
    }

    /** How long one case may take; less than each test's timeout, which every case resets. */
    private static final int CASE_TIMEOUT_MS = 15_000;
    private static final Object TIMED_OUT = new Object();

    /** As {@link #compare}, with a failure to read either table reported as a difference. */
    private static Promise<String> compareTables(String name, JsTable py, JsResolvedTable js) {
        Promise<String> comparison;
        try {
            comparison = compare(name, py.getColumns(), py.getSize(), py::createSnapshot, js.getColumns(),
                    js.getSize(), js::createSnapshot);
        } catch (RuntimeException e) {
            return Promise.resolve(name + ": comparing failed: " + e);
        }
        return comparison.then(differences -> Promise.resolve(differences),
                err -> Promise.resolve(name + ": comparing failed: " + err));
    }

    private interface Snapshotter {
        Promise<TableData> snapshot(Object options);
    }

    /** Resolves with a description of how the two tables differ, or null if they match. */
    private static Promise<String> compare(String name, JsArray<Column> pyColumns, double pySize, Snapshotter py,
            elemental2.core.ReadonlyArray<Column> jsColumns, double jsSize, Snapshotter js) {
        List<String> pyShape = new ArrayList<>();
        List<String> jsShape = new ArrayList<>();
        for (int i = 0; i < pyColumns.length; i++) {
            pyShape.add(pyColumns.getAt(i).getName() + ":" + pyColumns.getAt(i).getType());
        }
        for (int i = 0; i < jsColumns.getLength(); i++) {
            jsShape.add(jsColumns.getAt(i).getName() + ":" + jsColumns.getAt(i).getType());
        }
        if (!pyShape.equals(jsShape)) {
            return Promise.resolve(name + ": columns differ, Python " + pyShape + " vs JS " + jsShape);
        }
        if (pySize != jsSize) {
            return Promise.resolve(name + ": size differs, Python " + pySize + " vs JS " + jsSize);
        }
        if (pySize == 0) {
            return Promise.resolve(name + ": both results are empty, so no values were compared");
        }
        // Not Promise.all: its result is typed as a Java array, which the plain JS array it resolves with is not
        return py.snapshot(allRows(pyColumns, pySize)).then(pyData -> js.snapshot(allRows(jsColumns, jsSize))
                .then(jsData -> {
                    List<String> cells = new ArrayList<>();
                    int differing = 0;
                    for (int row = 0; row < pySize; row++) {
                        for (int col = 0; col < pyColumns.length; col++) {
                            String expected = cell(pyData.getData(row, pyColumns.getAt(col)));
                            String actual = cell(jsData.getData(row, jsColumns.getAt(col)));
                            if (!expected.equals(actual)) {
                                differing++;
                                if (cells.size() < 5) {
                                    cells.add("row " + row + " " + pyColumns.getAt(col).getName() + ": Python "
                                            + expected + " vs JS " + actual);
                                }
                            }
                        }
                    }
                    return Promise.resolve(differing == 0 ? null
                            : name + ": " + differing + " cells differ, e.g. " + String.join("; ", cells));
                }));
    }

    private static DataOptions.SnapshotOptions allRows(elemental2.core.ReadonlyArray<Column> columns, double size) {
        DataOptions.SnapshotOptions options = new DataOptions.SnapshotOptions();
        options.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, size - 1));
        options.columns = Js.uncheckedCast(columns);
        return options;
    }

    /** A cell's value as comparable text: wrappers by their value, NaN and null spelled out, strings quoted. */
    private static String cell(Any value) {
        Object v = value;
        if (v == null) {
            return "null";
        }
        String type = Js.typeof(v);
        if ("number".equals(type)) {
            double d = Js.asDouble(v);
            return Double.isNaN(d) ? "NaN" : String.valueOf(d);
        }
        if ("string".equals(type)) {
            return "'" + v + "'";
        }
        if ("boolean".equals(type)) {
            return String.valueOf(Js.asBoolean(v));
        }
        if (JsArray.isArray(v)) {
            // Element by element: JSON.stringify would write NaN and null alike
            JsArray<Any> array = Js.uncheckedCast(v);
            List<String> items = new ArrayList<>();
            for (int i = 0; i < array.length; i++) {
                items.add(cell(array.getAt(i)));
            }
            return "[" + String.join(", ", items) + "]";
        }
        return String.valueOf(v);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
