//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.object.UnionObject;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.table.EmptyTable;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;

import static io.deephaven.api.agg.Aggregation.*;

/**
 * Aggregations of add-only and blink inputs, and of static inputs, select operators that keep only the state additions
 * need. Each test compares those results with the same aggregation of a refreshing table that carries no add-only
 * attribute, after every update.
 */
public class AddOnlyAggregationTest {

    private static final long INPUT_SIZE = 20_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 2_000;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private Table source;

    /**
     * Buckets 0-4 hold only null values. In {@code Double1}, buckets 5-9 see NaN, 10-14 see positive infinity, 15-19
     * see negative infinity, and 20-24 see both infinities; buckets 25-29 hold only false and null {@code Bool1}
     * values. Buckets below 40 hold a single distinct {@code Uniq*} value. Finite floating point values are multiples
     * of 1/8, so their sums are exact in any order.
     */
    @Before
    public void setUp() {
        source = TableCreatorImpl.create(EmptyTable.of(INPUT_SIZE).update(
                "Row = ii",
                "Bucket = (int) (ii % 97)",
                "Sym = `S` + Bucket",
                "Long1 = Bucket < 5 || ii % 11 == 0 ? NULL_LONG : (ii % 1000) - 500",
                "Int1 = isNull(Long1) ? NULL_INT : (int) Long1",
                "Short1 = isNull(Long1) ? NULL_SHORT : (short) Long1",
                "Byte1 = isNull(Long1) ? NULL_BYTE : (byte) (Long1 % 100)",
                "Char1 = isNull(Long1) ? NULL_CHAR : (char) ('a' + (ii % 26))",
                "Double1 = Bucket < 5 || ii % 11 == 0 ? NULL_DOUBLE"
                        + " : Bucket < 10 && ii % 7 == 0 ? Double.NaN"
                        + " : Bucket >= 10 && Bucket < 15 && ii % 13 == 0 ? Double.POSITIVE_INFINITY"
                        + " : Bucket >= 15 && Bucket < 20 && ii % 13 == 0 ? Double.NEGATIVE_INFINITY"
                        + " : Bucket >= 20 && Bucket < 25 && ii % 13 == 0 ? Double.POSITIVE_INFINITY"
                        + " : Bucket >= 20 && Bucket < 25 && ii % 17 == 0 ? Double.NEGATIVE_INFINITY"
                        + " : ((ii % 1000) - 500) / 8.0",
                "Float1 = isNull(Double1) ? NULL_FLOAT : (float) Double1",
                "Bool1 = Bucket < 5 || ii % 3 == 0 ? (Boolean) null"
                        + " : Bucket >= 25 && Bucket < 30 ? Boolean.FALSE : Boolean.valueOf(ii % 3 == 1)",
                "BigInt1 = isNull(Long1) ? (java.math.BigInteger) null : java.math.BigInteger.valueOf(Long1)",
                "BigDec1 = isNull(Long1) ? (java.math.BigDecimal) null : java.math.BigDecimal.valueOf(Long1, 1)",
                "Weight = ii % 19 == 0 ? NULL_INT : (int) (ii % 7) - 3",
                "LongWeight = isNull(Weight) ? NULL_LONG : (long) Weight",
                "DWeight = isNull(Weight) ? NULL_DOUBLE : ii % 23 == 0 ? Double.NaN : Weight / 2.0",
                "Uniq = Bucket < 40 ? (ii % 31 == 0 ? NULL_LONG : (long) Bucket) : ii % 5",
                "UniqNull = Bucket < 5 ? NULL_LONG : Uniq",
                "UniqInstant = epochNanosToInstant(Uniq)",
                "UniqD = Bucket < 40 ? (Bucket == 7 ? Double.NaN : Bucket == 8 ? (ii % 2 == 0 ? -0.0 : 0.0)"
                        + " : (double) Bucket) : (double) (ii % 5)",
                "UniqS = Bucket < 40 ? Sym : Long.toString(ii % 5)"));
    }

    /**
     * The order in which a test adds the source's rows.
     */
    private enum InsertionOrder {
        /**
         * Each step adds rows after every row already added, skipping some row keys, so rows arrive in row key order.
         */
        INCREASING,
        /**
         * Each step adds rows from anywhere among the row keys not yet added, so a step may add rows before rows that
         * an earlier step added; some row keys are never added.
         */
        SCATTERED
    }

    /**
     * Supplies the rows each step adds, in an {@link InsertionOrder}.
     */
    private static final class RowSupplier {
        private final InsertionOrder order;
        private final Random random = new Random(1);
        /**
         * For {@link InsertionOrder#SCATTERED}, the row keys to add, in the order they are added.
         */
        private final long[] scatteredKeys;
        private int scatteredUsed;
        /**
         * For {@link InsertionOrder#INCREASING}, the next row key to consider.
         */
        private long nextKey;

        private RowSupplier(@NotNull final InsertionOrder order) {
            this.order = order;
            if (order == InsertionOrder.SCATTERED) {
                final List<Long> keys = new ArrayList<>();
                for (long key = 0; key < INPUT_SIZE; ++key) {
                    if (random.nextDouble() < 0.9) {
                        keys.add(key);
                    }
                }
                Collections.shuffle(keys, random);
                scatteredKeys = keys.stream().mapToLong(Long::longValue).toArray();
            } else {
                scatteredKeys = null;
            }
        }

        private boolean hasMore() {
            return order == InsertionOrder.INCREASING ? nextKey < INPUT_SIZE : scatteredUsed < scatteredKeys.length;
        }

        private RowSet next(final long size) {
            if (order == InsertionOrder.INCREASING) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                long added = 0;
                while (added < size && nextKey < INPUT_SIZE) {
                    if (random.nextDouble() < 0.8) {
                        builder.appendKey(nextKey);
                        ++added;
                    }
                    ++nextKey;
                }
                return builder.build();
            }
            final int end = (int) Math.min(scatteredKeys.length, scatteredUsed + size);
            final long[] keys = Arrays.copyOfRange(scatteredKeys, scatteredUsed, end);
            scatteredUsed = end;
            Arrays.sort(keys);
            return RowSetFactory.fromKeys(keys);
        }
    }

    /**
     * Apply {@code operator} to a refreshing table without an add-only attribute and to an add-only table, then add
     * rows a step at a time in each {@link InsertionOrder} and compare the results after every step. When rows are
     * added in increasing order, the results are also compared with the operation on a blink table holding each step's
     * rows, and on a static table of the rows added so far; when rows are added out of order, a blink table aggregates
     * them in arrival order and a static table in row key order, so neither is compared.
     *
     * @param operator the operation to test
     * @param testBlink whether the operation supports blink tables
     * @param testStatic whether to compare with the operation on a static table
     */
    private void doOperatorTest(
            @NotNull final UnaryOperator<Table> operator,
            final boolean testBlink,
            final boolean testStatic) {
        for (final InsertionOrder order : InsertionOrder.values()) {
            final boolean increasing = order == InsertionOrder.INCREASING;
            doOperatorTest(operator, order, testBlink && increasing, testStatic && increasing);
        }
    }

    private void doOperatorTest(
            @NotNull final UnaryOperator<Table> operator,
            @NotNull final InsertionOrder order,
            final boolean testBlink,
            final boolean testStatic) {
        final QueryTable normal = new QueryTable(RowSetFactory.empty().toTracking(), source.getColumnSourceMap());
        normal.setRefreshing(true);

        final Table addOnly = normal.assertAddOnly();

        final QueryTable blink = new QueryTable(RowSetFactory.empty().toTracking(), source.getColumnSourceMap());
        blink.setRefreshing(true);
        blink.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        final Table expected = operator.apply(normal);
        final Table addOnlyResult = operator.apply(addOnly);
        final Table blinkResult = testBlink ? operator.apply(blink) : null;
        compare(testStatic ? operator : null, normal, expected, addOnlyResult, blinkResult);

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random(0).longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();
        final RowSupplier rows = new RowSupplier(order);

        int step = 0;
        RowSet blinkLastInserted = RowSetFactory.empty();
        while (rows.hasMore()) {
            final RowSet stepInserted = rows.next(refreshSizes.nextLong());
            final RowSet finalBlinkLastInserted = blinkLastInserted;

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                if (stepInserted.isNonempty()) {
                    normal.getRowSet().writableCast().insert(stepInserted);
                    normal.notifyListeners(new TableUpdateImpl(stepInserted.copy(), RowSetFactory.empty(),
                            RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                }
                if (stepInserted.isNonempty() || finalBlinkLastInserted.isNonempty()) {
                    blink.getRowSet().writableCast().update(stepInserted, finalBlinkLastInserted);
                    blink.notifyListeners(new TableUpdateImpl(stepInserted.copy(), finalBlinkLastInserted.copy(),
                            RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                }
            });

            try {
                compare(testStatic ? operator : null, normal, expected, addOnlyResult, blinkResult);
            } catch (ComparisonFailure e) {
                System.err.printf("FAILURE: %s order, step %d, %d rows added, %d this step%n", order, step,
                        normal.size(), stepInserted.size());
                throw e;
            }

            ++step;
            blinkLastInserted = stepInserted;
        }
    }

    private void compare(
            final UnaryOperator<Table> operator,
            @NotNull final QueryTable normal,
            @NotNull final Table expected,
            @NotNull final Table addOnlyResult,
            final Table blinkResult) {
        TstUtils.assertTableEquals("add-only", expected, addOnlyResult);
        if (blinkResult != null) {
            TstUtils.assertTableEquals("blink", expected, blinkResult);
        }
        if (operator != null) {
            final Table staticInput =
                    new QueryTable(normal.getRowSet().copy().toTracking(), source.getColumnSourceMap());
            TstUtils.assertTableEquals("static", expected, operator.apply(staticInput));
        }
    }

    private void doOperatorTest(@NotNull final UnaryOperator<Table> operator, final boolean testBlink) {
        doOperatorTest(operator, testBlink, true);
    }

    private void testKeyedAndUnkeyed(@NotNull final List<? extends Aggregation> aggregations) {
        doOperatorTest(table -> table.aggBy(aggregations, "Sym"), true);
        doOperatorTest(table -> table.aggBy(aggregations), true);
    }

    @Test
    public void testStateChangeRecording() {
        testKeyedAndUnkeyed(List.of(AggSum("Long1"), AggMax("Int1")));
    }

    @Test
    public void testStateChangeRecordingWithCount() {
        testKeyedAndUnkeyed(List.of(AggCount("N"), AggSum("Long1")));
    }

    @Test
    public void testStateChangeRecordingInitialGroups() {
        final Table initialGroups = TableTools.newTable(TableTools.stringCol("Sym", "S3", "Missing", "S50"));
        // a refreshing aggregation leaves the key null for an initial group that is empty when it is created and
        // filled later, so only preserveEmpty is compared with the static result
        for (final boolean preserveEmpty : new boolean[] {false, true}) {
            doOperatorTest(table -> table.aggBy(List.of(AggSum("Long1")), preserveEmpty, initialGroups,
                    List.of(ColumnName.of("Sym"))), true, preserveEmpty);
        }
    }

    @Test
    public void testStateChangeRecordingPreserveEmpty() {
        doOperatorTest(table -> table.aggBy(List.of(AggSum("Long1")), true, null, List.of(ColumnName.of("Sym"))),
                true);
    }

    @Test
    public void testIntegralSum() {
        testKeyedAndUnkeyed(List.of(
                AggSum("SL=Long1", "SI=Int1", "SS=Short1", "SB=Byte1", "SC=Char1"),
                AggAbsSum("AL=Long1", "AI=Int1", "AS=Short1", "AB=Byte1", "AC=Char1")));
    }

    @Test
    public void testBigIntegerSum() {
        testKeyedAndUnkeyed(List.of(AggSum("S=BigInt1"), AggAbsSum("A=BigInt1")));
    }

    @Test
    public void testBigDecimalSum() {
        testKeyedAndUnkeyed(List.of(AggSum("S=BigDec1"), AggAbsSum("A=BigDec1")));
    }

    @Test
    public void testBooleanSum() {
        testKeyedAndUnkeyed(List.of(AggSum("S=Bool1"), AggAbsSum("A=Bool1")));
    }

    @Test
    public void testFloatingPointSum() {
        testKeyedAndUnkeyed(List.of(AggSum("SD=Double1", "SF=Float1"), AggAbsSum("AD=Double1", "AF=Float1")));
    }

    @Test
    public void testIntegralWeightedSum() {
        testKeyedAndUnkeyed(List.of(AggWSum("LongWeight", "WL=Long1")));
    }

    @Test
    public void testFloatingPointWeightedSum() {
        testKeyedAndUnkeyed(List.of(AggWSum("DWeight", "WD=Double1", "WF=Float1", "WL=Long1")));
    }

    @Test
    public void testWeightedAverage() {
        testKeyedAndUnkeyed(List.of(
                AggWAvg("DWeight", "AD=Double1", "AF=Float1", "AL=Long1"),
                AggWAvg("LongWeight", "BD=Double1", "BL=Long1")));
    }

    @Test
    public void testUnique() {
        final String[] pairs = {"U=Uniq", "UN=UniqNull", "UI=UniqInstant", "UD=UniqD", "US=UniqS", "UL=Long1",
                "UB=Byte1", "UC=Char1", "UF=Float1", "UBool=Bool1"};
        testKeyedAndUnkeyed(List.of(AggUnique(pairs)));
        testKeyedAndUnkeyed(List.of(AggUnique(true, pairs)));
        testKeyedAndUnkeyed(List.of(
                AggUnique(false, UnionObject.of(-1L), "U=Uniq", "UN=UniqNull"),
                AggUnique(true, UnionObject.of(-1.0), "UD=UniqD"),
                AggUnique(false, UnionObject.from("Many"), "US=UniqS")));
    }

    @Test
    public void testAverage() {
        testKeyedAndUnkeyed(List.of(AggAvg("AD=Double1", "AF=Float1", "AL=Long1", "AI=Int1", "ABI=BigInt1",
                "ABD=BigDec1")));
    }

    @Test
    public void testVarianceAndStd() {
        testKeyedAndUnkeyed(List.of(
                AggVar("VD=Double1", "VF=Float1", "VL=Long1", "VBD=BigDec1"),
                AggStd("SD=Double1", "SF=Float1", "SL=Long1")));
    }

    @Test
    public void testPercentile() {
        testKeyedAndUnkeyed(List.of(
                AggMed("MD=Double1", "MF=Float1", "ML=Long1", "MS=UniqS", "MI=UniqInstant"),
                AggMed(false, "NL=Long1", "NI=Int1"),
                AggPct(0.25, "PD=Double1", "PL=Long1", "PB=Bool1")));
    }
}
