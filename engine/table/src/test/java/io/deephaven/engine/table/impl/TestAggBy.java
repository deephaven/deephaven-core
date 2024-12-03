//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import gnu.trove.set.TDoubleSet;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TDoubleHashSet;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.object.UnionObject;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestAggBy extends RefreshingTableTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testBy() {
        ColumnHolder<?> aHolder = col("A", 0, 0, 1, 1, 0, 0, 1, 1, 0, 0);
        ColumnHolder<?> bHolder = col("B", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        ColumnHolder<?> cHolder = col("C", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        Table table = TableTools.newTable(aHolder, bHolder, cHolder);
        show(table);
        assertEquals(10, table.size());
        assertEquals(2, table.groupBy("A").size());

        Table minMax = table.aggBy(
                List.of(
                        AggFormula("min(each)", "each", "Min=B"),
                        AggFormula("max(each)", "each", "Max=B"),
                        AggFormula("sum(each)", "each", "Sum=B"),
                        AggFormula("f_const=6.0 + 3"),
                        AggFormula("f_min=min(B)"),
                        AggFormula("f_max=max(B)"),
                        AggFormula("f_sum=sum(B)"),
                        AggFormula("f_sum_two_col=sum(B) + sum(C)"),
                        AggFormula("f_custom_sum=A * (sum(B) + sum(C))"),
                        AggFormula("f_weighted_avg=wavg(B, C)")),
                "A");
        show(minMax);

        assertEquals(2, minMax.size());

        DoubleVector consts = ColumnVectors.ofDouble(minMax, "f_const");
        assertEquals(9.0, consts.get(0));
        assertEquals(9.0, consts.get(1));

        IntVector mins = ColumnVectors.ofInt(minMax, "Min");
        assertEquals(1, mins.get(0));
        assertEquals(3, mins.get(1));
        mins = ColumnVectors.ofInt(minMax, "f_min");
        assertEquals(1, mins.get(0));
        assertEquals(3, mins.get(1));

        IntVector maxes = ColumnVectors.ofInt(minMax, "Max");
        assertEquals(10, maxes.get(0));
        assertEquals(8, maxes.get(1));
        maxes = ColumnVectors.ofInt(minMax, "f_max");
        assertEquals(10, maxes.get(0));
        assertEquals(8, maxes.get(1));

        LongVector sums = ColumnVectors.ofLong(minMax, "Sum");
        assertEquals(33, sums.get(0));
        assertEquals(22, sums.get(1));
        sums = ColumnVectors.ofLong(minMax, "f_sum");
        assertEquals(33, sums.get(0));
        assertEquals(22, sums.get(1));

        sums = ColumnVectors.ofLong(minMax, "f_sum_two_col");
        assertEquals(33 + 6, sums.get(0));
        assertEquals(22 + 4, sums.get(1));

        sums = ColumnVectors.ofLong(minMax, "f_custom_sum");
        assertEquals(0, sums.get(0));
        assertEquals(22 + 4, sums.get(1));

        Table doubleCounted = table.aggBy(List.of(AggCount("Count1"), AggCount("Count2")), "A");
        show(doubleCounted);
        assertEquals(2, doubleCounted.size());

        LongVector counts = ColumnVectors.ofLong(doubleCounted, "Count1");
        assertEquals(6L, counts.get(0));
        assertEquals(4L, counts.get(1));
        counts = ColumnVectors.ofLong(doubleCounted, "Count2");
        assertEquals(6L, counts.get(0));
        assertEquals(4L, counts.get(1));

        // Lets do some interesting incremental computations, as this is the use case that I'm really aiming at. For
        // example, getting the count, and average on each update.
        // It would be nice to do a min and a max as well,
        // which can often be efficient (but sometimes could also require linear work). That isn't related to this test
        // but more related to the underlying min and max.

        // Interestingly, the factories appear to be single use. If you try to reuse a factory it fails with an NPE.
        // minFactory = new AggregationFormulaSpec("min(each)", "each");
        // maxFactory = new AggregationFormulaSpec("max(each)", "each");

        Collection<? extends Aggregation> summaryStatistics = List.of(
                AggCount("Count"),
                AggMin("MinB=B", "MinC=C"),
                AggMed("MedB=B", "MedC=C"),
                AggMax("MaxB=B", "MaxC=C"),
                AggAvg("AvgB=B", "AvgC=C"),
                AggStd("StdB=B", "StdC=C"),
                AggSum("SumB=B", "SumC=C"),
                AggCountDistinct("DistinctA=A"),
                AggCountDistinct("DistinctB=B"));

        Collection<? extends Aggregation> percentiles = List.of(
                AggPct(0.25, "Pct01B=B", "Pct01C=C"),
                AggPct(0.25, "Pct25B=B", "Pct25C=C"),
                AggPct(0.75, "Pct75B=B", "Pct75C=C"),
                AggPct(0.75, true, "Pct75T_B=B", "Pct75T_C=C"),
                AggPct(0.75, false, "Pct75F_B=B", "Pct75F_C=C"),
                AggPct(0.99, "Pct99B=B", "Pct99C=C"),
                AggPct(0.50, "Pct50B=B", "Pct50C=C"),
                AggPct(0.50, true, "Pct50T_B=B", "Pct50T_C=C"),
                AggPct(0.50, false, "Pct50F_B=B", "Pct50F_C=C"));

        Double[] doubles = new Double[10];
        final IntChunk<Values> bChunk = bHolder.getChunk().asIntChunk();
        for (int ii = 0; ii < bChunk.size(); ++ii) {
            doubles[ii] = 1.1 * bChunk.get(ii);
        }
        ColumnHolder<?> dHolder = col("D", doubles);
        table = TableTools.newTable(aHolder, bHolder, cHolder, dHolder);
        show(table);
        Table summary = table.aggBy(summaryStatistics, "A");
        show(summary);

        System.out.println("\nPercentiles (overall):");
        Table percentilesAll = table.aggBy(percentiles);
        show(percentilesAll);
    }

    @Test
    public void testComboByMinMaxTypes() {
        final Random random = new Random(0);
        final int size = 10;
        final ColumnInfo[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "shortCol", "byteCol", "longCol", "charCol", "doubleCol",
                                "floatCol", "Instant", "BoolCol", "bigI", "bigD"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new ShortGenerator(),
                        new ByteGenerator(),
                        new LongGenerator(),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 10.0f),
                        new UnsortedInstantGenerator(DateTimeUtils.parseInstant("2020-03-17T12:00:00 NY"),
                                DateTimeUtils.parseInstant("2020-03-18T12:00:00 NY")),
                        new BooleanGenerator(),
                        new BigIntegerGenerator(),
                        new BigDecimalGenerator()));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.aggAllBy(AggSpec.min())),
                EvalNugget.from(() -> queryTable.aggAllBy(AggSpec.max())),
                new QueryTableTest.TableComparator(
                        queryTable.aggAllBy(AggSpec.min()), "aggAllBy",
                        queryTable.minBy(), "minBy"),
                EvalNugget.Sorted.from(() -> queryTable.aggAllBy(AggSpec.min(), "Sym"), "Sym"),
                new QueryTableTest.TableComparator(
                        queryTable.aggAllBy(AggSpec.min(), "Sym").sort("Sym"), "aggAllBy",
                        queryTable.minBy("Sym").sort("Sym"), "minBy"),
        };
        final int steps = 100; // 8;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep("step == " + step, size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testComboByIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testComboByIncremental("size-" + size, size);
        }
    }

    private void testComboByIncremental(final String ctxt, final int size) {
        Random random = new Random(0);
        ColumnInfo[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo =
                        initColumnInfos(new String[] {"Sym", "intCol", "intColNulls", "doubleCol", "doubleColNulls"},
                                new SetGenerator<>("a", "b", "c", "d"),
                                new IntGenerator(10, 100),
                                new IntGenerator(10, 100, .1),
                                new SetGenerator<>(10.1, 20.1, 30.1),
                                new SetGenerator<>(10.1, 20.1, 30.1, QueryConstants.NULL_DOUBLE)));

        ExecutionContext.getContext().getQueryLibrary().importClass(TestAggBy.class);

        String[] groupByColumns = new String[0];
        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.aggBy(List.of(
                                AggAvg("MeanI=intCol", "MeanD=doubleCol"),
                                AggStd("StdI=intCol", "StdD=doubleCol")), "Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.aggBy(List.of(
                                AggFormula("min(each)", "each", "MinI=intCol", "MinD=doubleCol"),
                                AggFormula("max(each)", "each", "MaxI=intCol"),
                                AggFormula("f_const=6.0 / 3"),
                                AggFormula("f_min=min(intColNulls)"),
                                AggFormula("f_max=max(doubleColNulls)"),
                                AggFormula("f_sum=sum(intColNulls + doubleColNulls)"),
                                AggFormula("f_key=Sym.equals(\"a\") ? \"a\" : \"not a\""),
                                AggFormula("f_key_sum=Sym + ':' + sum(intColNulls + doubleColNulls)"),
                                AggFormula(
                                        "f_custom_sum=sum(intColNulls) + sum(doubleCol) + min(doubleColNulls)")),
                                "Sym").sort("Sym");
                    }
                },
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinI=min(intCol)", "MinD=min(doubleCol)").sort("Sym"),
                        "view",
                        queryTable.aggBy(AggMin("MinI=intCol", "MinD=doubleCol"), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MaxI=max(intCol)", "MaxD=max(doubleCol)").sort("Sym"),
                        "view",
                        queryTable.aggBy(AggMax("MaxI=intCol", "MaxD=doubleCol"), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinI=min(intCol)", "MaxI=max(intCol)").sort("Sym"),
                        "view",
                        queryTable.aggBy(List.of(AggMin("MinI=intCol"), AggMax("MaxI=intCol")), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinD=min(doubleCol)", "MaxD=max(doubleCol)").sort("Sym"),
                        "view",
                        queryTable.aggBy(List.of(AggMin("MinD=doubleCol"), AggMax("MaxD=doubleCol")), "Sym")
                                .sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "MinD=min(doubleCol)", "MaxI=max(intCol)", "FirstD=first(doubleCol)",
                                        "LastI=last(intCol)")
                                .sort("Sym"),
                        "view",
                        queryTable.aggBy(List.of(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxI=intCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggLast("LastI=intCol")), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "MinD=min(doubleCol)", "MaxD=max(doubleCol)", "MinI=min(intCol)",
                                        "MaxI=max(intCol)", "LastD=last(doubleCol)", "FirstD=first(doubleCol)",
                                        "FirstI=first(intCol)", "LastI=last(intCol)")
                                .sort("Sym"),
                        "view",
                        queryTable.aggBy(List.of(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxD=doubleCol"),
                                AggMin("MinI=intCol"),
                                AggMax("MaxI=intCol"),
                                AggLast("LastD=doubleCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggFirst("FirstI=intCol"),
                                AggLast("LastI=intCol")), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy().view("MinD=min(doubleCol)", "MaxI=max(intCol)", "MaxD=max(doubleCol)",
                                "MinI=min(intCol)",
                                "FirstD=first(doubleCol)", "LastI=last(intCol)", "LastD=last(doubleCol)",
                                "FirstI=first(intCol)"),
                        "view",
                        queryTable.aggBy(List.of(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxI=intCol"),
                                AggMax("MaxD=doubleCol"),
                                AggMin("MinI=intCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggLast("LastI=intCol"),
                                AggLast("LastD=doubleCol"),
                                AggFirst("FirstI=intCol")), groupByColumns),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "AvgD=avg(doubleCol)", "SumD=sum(doubleCol)", "VarD=var(doubleCol)",
                                        "StdD=std(doubleCol)", "intCol")
                                .sort("Sym"),
                        "view",
                        queryTable.aggBy(List.of(
                                AggAvg("AvgD=doubleCol"),
                                AggSum("SumD=doubleCol"),
                                AggVar("VarD=doubleCol"),
                                AggStd("StdD=doubleCol"),
                                AggGroup("intCol")), "Sym").sort("Sym"),
                        "aggBy"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym",
                                "MedD=median(doubleCol)",
                                "Pct01D=percentile(0.01, doubleCol)",
                                "Pct01I=(int)TestAggBy.percentile(intCol, 0.01)",
                                "Pct05D=percentile(0.05, doubleCol)",
                                "Pct05I=(int)TestAggBy.percentile(intCol, 0.05)",
                                "Pct25D=percentile(0.25, doubleCol)",
                                "Pct25I=(int)TestAggBy.percentile(intCol, 0.25)",
                                "Pct50D=percentile(0.50, doubleCol)",
                                "Pct50I=(int)TestAggBy.percentile(intCol, 0.50)",
                                "Pct65D=percentile(0.65, doubleCol)",
                                "Pct65I=(int)TestAggBy.percentile(intCol, 0.65)",
                                "Pct90D=percentile(0.90, doubleCol)",
                                "Pct90I=(int)TestAggBy.percentile(intCol, 0.90)",
                                "Pct99D=percentile(0.99, doubleCol)",
                                "Pct99I=(int)TestAggBy.percentile(intCol, 0.99)").sort("Sym"),
                        queryTable.aggBy(List.of(
                                AggMed("MedD=doubleCol"),
                                AggPct(0.01, "Pct01D=doubleCol", "Pct01I=intCol"),
                                AggPct(0.05, "Pct05D=doubleCol", "Pct05I=intCol"),
                                AggPct(0.25, "Pct25D=doubleCol", "Pct25I=intCol"),
                                AggPct(0.50, "Pct50D=doubleCol", "Pct50I=intCol"),
                                AggPct(0.65, "Pct65D=doubleCol", "Pct65I=intCol"),
                                AggPct(0.90, "Pct90D=doubleCol", "Pct90I=intCol"),
                                AggPct(0.99, "Pct99D=doubleCol", "Pct99I=intCol")), "Sym").sort("Sym")),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").wavgBy("doubleCol", "Sym")
                                .renameColumns("WAvg=intCol"),
                        "WAvgBy",
                        queryTable.aggBy(List.of(
                                AggWAvg("doubleCol", "WAvg=intCol")), "Sym"),
                        "AggWAvg"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        queryTable.aggBy(AggCount("Count")), "AggCount"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        queryTable.aggBy(AggCount("Count")), "AggCount"),
                new QueryTableTestBase.TableComparator(
                        queryTable.groupBy("Sym").view("Sym",
                                "cdi=countDistinct(intCol)",
                                "ddi=countDistinct(doubleCol)",
                                "cdiN=countDistinct(intColNulls, true)",
                                "ddiN=countDistinct(doubleColNulls, true)",
                                "dic=vec(sort(distinct(intCol, false)))",
                                "did=vec(sort(distinct(doubleCol, false)))",
                                "dicN=vec(sort(distinct(intColNulls, true)))",
                                "didN=vec(sort(distinct(doubleColNulls, true)))",
                                "uic=TestAggBy.uniqueValue(intCol, false)",
                                "uid=TestAggBy.uniqueValue(doubleCol, false)",
                                "uicN=TestAggBy.uniqueValue(intColNulls, true)",
                                "uidN=TestAggBy.uniqueValue(doubleColNulls, true)")
                                .sort("Sym"),
                        "countDistinctView",
                        queryTable.aggBy(List.of(AggCountDistinct("cdi=intCol", "ddi=doubleCol"),
                                AggCountDistinct(true, "cdiN=intColNulls", "ddiN=doubleColNulls"),
                                AggDistinct("dic=intCol", "did=doubleCol"),
                                AggDistinct(true, "dicN=intColNulls", "didN=doubleColNulls"),
                                AggUnique("uic=intCol", "uid=doubleCol"),
                                AggUnique(true, "uicN=intColNulls", "uidN=doubleColNulls")), "Sym")
                                .sort("Sym"),
                        "AggCountDistinct")
        };
        final int steps = 100; // 8;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep(ctxt + " step == " + step, size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testComboByDoubleClaim() {
        final int size = 10;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        queryTable.aggBy(AggCount("Count")), "AggCount"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        queryTable.aggBy(AggCount("Count")), "AggCount")
        };
        final int steps = 100; // 8;
        for (int i = 0; i < steps; i++) {
            System.out.println("Abstract Table:");
            show(queryTable);
            simulateShiftAwareStep("double Claim" + " step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testComboByDistinct() {
        QueryTable dataTable = TstUtils.testRefreshingTable(
                intCol("Grp", 1, 2, 3, 4),
                charCol("Let", 'a', 'b', 'c', 'd'));

        final Table tail = dataTable.tail(10);
        final Table result = tail.aggBy(AggDistinct("Let"), "Grp");

        final ColumnSource<CharVector> cs = result.getColumnSource("Let");
        assertEquals(4, result.size());
        assertArrayEquals(new char[] {'a'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'b'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d'}, cs.get(3).toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet toAdd2 = i(4, 5, 6, 7);
            addToTable(dataTable, toAdd2,
                    intCol("Grp", 1, 2, 3, 4),
                    charCol("Let", 'e', 'f', 'g', 'h'));
            dataTable.notifyListeners(toAdd2, i(), i());
        });
        assertEquals(4, result.size());
        assertArrayEquals(new char[] {'a', 'e'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'b', 'f'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c', 'g'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d', 'h'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet toAdd1 = i(8, 9, 10, 11);
            addToTable(dataTable, toAdd1,
                    intCol("Grp", 1, 2, 3, 4),
                    charCol("Let", 'i', 'j', 'k', 'l'));
            dataTable.notifyListeners(toAdd1, i(), i());
        });
        assertArrayEquals(new char[] {'e', 'i'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'f', 'j'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c', 'g', 'k'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d', 'h', 'l'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet toAdd = i(12, 13, 14, 15);
            addToTable(dataTable, toAdd,
                    intCol("Grp", 1, 2, 3, 4),
                    charCol("Let", 'm', 'n', 'o', 'p'));
            dataTable.notifyListeners(toAdd, i(), i());
        });
        assertArrayEquals(new char[] {'i', 'm'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'g', 'k', 'o'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'h', 'l', 'p'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(16), intCol("Grp", 1), charCol("Let", 'q'));
            dataTable.notifyListeners(i(16), i(), i());
        });
        assertArrayEquals(new char[] {'i', 'm', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'h', 'l', 'p'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(17), intCol("Grp", 2), charCol("Let", 'r'));
            dataTable.notifyListeners(i(17), i(), i());
        });
        assertArrayEquals(new char[] {'i', 'm', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(18), intCol("Grp", 3), charCol("Let", 's'));
            dataTable.notifyListeners(i(18), i(), i());
        });
        assertArrayEquals(new char[] {'m', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o', 's'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p'}, cs.get(3).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(19), intCol("Grp", 4), charCol("Let", 't'));
            dataTable.notifyListeners(i(19), i(), i());
        });
        assertArrayEquals(new char[] {'m', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o', 's'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p', 't'}, cs.get(3).toArray());
    }

    @Test
    public void testComboByCountDistinct() {
        QueryTable dataTable = TstUtils.testRefreshingTable(
                col("USym", "AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "SPY", "SPY", "SPY", "SPY", "VXX"),
                longCol("Account", 1, 1, 2, 1, 3, 2, 4, 2, 5, 5),
                intCol("Qty", 100, 100, 200, 300, 50, 100, 150, 200, 50, 50));

        Table result = dataTable.aggBy(AggCountDistinct("Account", "Qty"), "USym").sort("USym");
        Table countNulls = dataTable.aggBy(AggCountDistinct(true, "Account", "Qty"), "USym").sort("USym");
        assertEquals(4, result.size());
        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"GOOG", 2L, 2L}, getRowData(result, 1));
        assertArrayEquals(new Object[] {"SPY", 3L, 4L}, getRowData(result, 2));
        assertArrayEquals(new Object[] {"VXX", 1L, 1L}, getRowData(result, 3));
        assertTableEquals(result, countNulls);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(1, 10),
                    col("USym", "AAPL", "VXX"),
                    longCol("Account", QueryConstants.NULL_LONG, 1),
                    intCol("Qty", 100, QueryConstants.NULL_INT));
            dataTable.notifyListeners(i(10), i(), i(1));
        });

        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"VXX", 2L, 1L}, getRowData(result, 3));

        assertArrayEquals(new Object[] {"AAPL", 3L, 2L}, getRowData(countNulls, 0));
        assertArrayEquals(new Object[] {"VXX", 2L, 2L}, getRowData(countNulls, 3));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(2),
                    col("USym", "AAPL"),
                    longCol("Account", QueryConstants.NULL_LONG),
                    intCol("Qty", 200));
            dataTable.notifyListeners(i(), i(), i(2));
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 2L}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, getRowData(countNulls, 0));

        TableTools.showWithRowSet(dataTable, dataTable.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(1, 2, 11),
                    col("USym", "AAPL", "AAPL", "SPY"),
                    longCol("Account", 1, 2, QueryConstants.NULL_LONG),
                    intCol("Qty", 100, 200, 200));

            removeRows(dataTable, i(6));
            dataTable.notifyListeners(i(11), i(6), i(1, 2));
        });

        TableTools.showWithRowSet(dataTable, dataTable.size());

        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"SPY", 3L, 3L}, getRowData(countNulls, 2));
    }

    @Test
    public void testComboByAggUnique() {
        final Instant dtDefault = DateTimeUtils.parseInstant("1987-10-20T07:45:00.000 NY");
        final Instant dt1 = DateTimeUtils.parseInstant("2021-01-01T00:00:01.000 NY");
        final Instant dt2 = DateTimeUtils.parseInstant("2021-01-01T00:00:02.000 NY");

        QueryTable dataTable = TstUtils.testRefreshingTable(
                col("USym", "AAPL", "AAPL", "AAPL", /**/ "GOOG", "GOOG", /**/ "SPY", "SPY", "SPY", "SPY", /**/ "VXX"),
                longCol("Account", 1, 1, 2, /**/ 1, 3, /**/ 2, 4, 2, 5, /**/ 5),
                intCol("Qty", 100, 100, 100, /**/ 300, 50, /**/ 100, 150, 200, 50, /**/ 50),
                col("Whee", dt1, dt1, dt1, /**/ dt1, dt2, /**/ dt2, dt2, dt2, dt2, /**/ null));

        Table result = dataTable.aggBy(List.of(
                AggUnique(false, UnionObject.of(-1), "Account", "Qty"),
                AggUnique(false, UnionObject.of(dtDefault), "Whee")), "USym").sort("USym");

        Table countNulls = dataTable.aggBy(List.of(
                AggUnique(true, UnionObject.of(-1), "Account", "Qty"),
                AggUnique(true, UnionObject.of(dtDefault), "Whee")), "USym").sort("USym");

        assertEquals(4, result.size());
        assertArrayEquals(new Object[] {"AAPL", -1L, 100, dt1}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtDefault}, getRowData(result, 1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, getRowData(result, 2));
        assertArrayEquals(new Object[] {"VXX", 5L, 50, null}, getRowData(result, 3));
        assertTableEquals(result, countNulls);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(2, 10),
                    col("USym", "AAPL", "VXX"),
                    longCol("Account", 1, 5),
                    intCol("Qty", 100, QueryConstants.NULL_INT),
                    col("Whee", null, (Instant) null));
            dataTable.notifyListeners(i(10), i(), i(2));
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dt1}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtDefault}, getRowData(result, 1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, getRowData(result, 2));
        assertArrayEquals(new Object[] {"VXX", 5L, 50, null}, getRowData(result, 3));

        // Check the nulls table
        assertArrayEquals(new Object[] {"VXX", 5L, -1, null}, getRowData(countNulls, 3));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(11),
                    col("USym", "USO"),
                    longCol("Account", 2),
                    intCol("Qty", 200),
                    col("Whee", dt1));
            removeRows(dataTable, i(9, 10));
            dataTable.notifyListeners(i(11), i(9, 10), i());
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dt1}, getRowData(result, 0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtDefault}, getRowData(result, 1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, getRowData(result, 2));
        assertArrayEquals(new Object[] {"USO", 2L, 200, dt1}, getRowData(result, 3));

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dtDefault}, getRowData(countNulls, 0));

        //
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(11),
                    col("USym", "USO"),
                    longCol("Account", QueryConstants.NULL_LONG),
                    intCol("Qty", QueryConstants.NULL_INT),
                    col("Whee", dt2));
            dataTable.notifyListeners(i(), i(), i(11));
        });
        assertArrayEquals(new Object[] {"USO", null, null, dt2}, getRowData(result, 3));

        //
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(3, 4, 9, 10),
                    col("USym", "GOOG", "GOOG", "VXX", "VXX"),
                    longCol("Account", 2L, 2L, QueryConstants.NULL_LONG, 99),
                    intCol("Qty", 350, 350, 50, 50),
                    col("Whee", dt2, dt2, null, dt1));
            dataTable.notifyListeners(i(9, 10), i(), i(3, 4));
        });

        assertArrayEquals(new Object[] {"GOOG", 2L, 350, dt2}, getRowData(result, 1));
        assertArrayEquals(new Object[] {"VXX", 99L, 50, dt1}, getRowData(result, 4));
        assertArrayEquals(new Object[] {"VXX", -1L, 50, dtDefault}, getRowData(countNulls, 4));
    }

    @Test
    public void testAggUniqueDefaultValues() {
        final Instant dt1 = DateTimeUtils.parseInstant("2021-01-01T00:01:02.000 NY");
        final Instant dt2 = DateTimeUtils.parseInstant("2021-02-02T00:02:03.000 NY");

        QueryTable dataTable = TstUtils.testRefreshingTable(
                col("USym", "NoKey", "SingleVal", "NonUnique", "NonUnique"),
                col("StringCol", null, "Apple", "Bacon", "Pancake"),
                col("BoolCol", null, true, true, false),
                col("Instant", null, dt1, dt1, dt2),
                charCol("CharCol", NULL_CHAR, 'a', 'b', 'c'),
                byteCol("ByteCol", NULL_BYTE, (byte) 100, (byte) 110, (byte) 120),
                shortCol("ShortCol", NULL_SHORT, (short) 1234, (short) 4321, (short) 1324),
                intCol("IntCol", NULL_INT, 99999, 100000, 200000),
                longCol("LongCol", NULL_LONG, 44444444L, 55555555L, 66666666L),
                floatCol("FloatCol", NULL_FLOAT, 1.2345f, 2.3456f, 3.4567f),
                doubleCol("DoubleCol", NULL_DOUBLE, 1.1E22d, 2.2E22d, 3.3E22d),
                col("BigIntCol", null,
                        BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(2)),
                        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(1)),
                        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(2))),
                col("BigDecCol", null,
                        BigDecimal.valueOf(MAX_FINITE_DOUBLE).add(BigDecimal.valueOf(2)),
                        BigDecimal.valueOf(MIN_FINITE_DOUBLE).subtract(BigDecimal.valueOf(1)),
                        BigDecimal.valueOf(MIN_FINITE_DOUBLE).subtract(BigDecimal.valueOf(2))));

        // First try mixing column types and values
        expectException(IllegalArgumentException.class,
                "Attempted to use no key/non unique values of incorrect types for aggregated columns!",
                () -> dataTable
                        .aggBy(AggUnique(false, UnionObject.of(2), "StringCol", "BoolCol", "DatTime", "CharCol",
                                "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol", "DoubleCol", "BigIntCol",
                                "BigDecCol"), "USym")
                        .sort("USym"));

        dataTable.aggBy(AggUnique(false, UnionObject.of(-2), "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol",
                "DoubleCol", "BigIntCol", "BigDecCol"), "USym").sort("USym");

        dataTable.aggBy(AggUnique(false, UnionObject.of(BigInteger.valueOf(-2)),
                "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol",
                "DoubleCol", "BigIntCol", "BigDecCol"), "USym").sort("USym");

        dataTable.aggBy(AggUnique(false, UnionObject.of(BigDecimal.valueOf(-2)),
                "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol",
                "DoubleCol", "BigIntCol", "BigDecCol"), "USym").sort("USym");

        // Byte out of range
        testUniqueOutOfRangeParams(Byte.class, dataTable, ((short) Byte.MIN_VALUE - 1), Byte.MIN_VALUE,
                ((short) Byte.MAX_VALUE + 1), Byte.MAX_VALUE, "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol",
                "DoubleCol");
        testUniqueOutOfRangeParams(Short.class, dataTable, ((int) Short.MIN_VALUE - 1), Short.MIN_VALUE,
                ((int) Short.MAX_VALUE + 1), Short.MAX_VALUE, "ShortCol", "IntCol", "LongCol", "FloatCol", "DoubleCol");
        testUniqueOutOfRangeParams(Integer.class, dataTable, ((long) Integer.MIN_VALUE - 1), Integer.MIN_VALUE,
                ((long) Integer.MAX_VALUE + 1), Integer.MAX_VALUE, "IntCol", "LongCol", "FloatCol", "DoubleCol");
        testUniqueOutOfRangeParams(Long.class, dataTable, BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                Long.MIN_VALUE, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), Long.MAX_VALUE, "LongCol",
                "FloatCol", "DoubleCol");

        testUniqueOutOfRangeParams(Long.class, dataTable, -2.2,
                Long.MIN_VALUE, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), Long.MAX_VALUE, "LongCol",
                "FloatCol", "DoubleCol");
    }

    private void testUniqueOutOfRangeParams(Class<?> type, Table dataTable, Number invalidLow, Number validLow,
            Number invalidHigh, Number validHigh, String... aggCols) {
        // Byte out of range
        expectException(IllegalArgumentException.class,
                "Attempted to non unique values too small for " + type.getName() + "!",
                () -> dataTable.aggBy(AggUnique(false, UnionObject.from(invalidLow), aggCols), "USym")
                        .sort("USym"));

        expectException(IllegalArgumentException.class,
                "Attempted to use non unique values too large for " + type.getName() + "!",
                () -> dataTable.aggBy(AggUnique(false, UnionObject.from(invalidHigh), aggCols), "USym")
                        .sort("USym"));

        dataTable.aggBy(AggUnique(false, UnionObject.from(validLow), aggCols), "USym").sort("USym");
        dataTable.aggBy(AggUnique(false, UnionObject.from(validHigh), aggCols), "USym").sort("USym");
    }

    private static <T extends Throwable> void expectException(@SuppressWarnings("SameParameterValue") Class<T> excType,
            String failMessage, Runnable action) {
        try {
            action.run();
            fail(failMessage);
        } catch (Throwable error) {
            if (error.getClass() != excType) {
                fail("Unexpected exception type `" + error.getClass().getName() + "' expected '" + excType.getName()
                        + "'");
            }
        }
    }

    @SuppressWarnings("unused") // used in a query test
    public static double percentile(int[] a, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new RuntimeException("Invalid percentile = " + percentile);
        } else if (a.length == 0) {
            return Double.NaN;
        } else {
            int n = a.length;
            int[] copy = new int[n];
            System.arraycopy(a, 0, copy, 0, n);
            Arrays.sort(copy);

            int idx = (int) Math.round(percentile * (a.length - 1));
            return copy[idx];
        }
    }

    /**
     * Get the single unique value in the array, or null if there are none, or there are more than 1 distinct values.
     *
     * @param arr the array
     * @param countNull if nulls should count as values
     * @return the single unique value in the array, or null.
     */
    public static int uniqueValue(final IntVector arr, boolean countNull) {
        if (arr == null || arr.isEmpty()) {
            return NULL_INT;
        }

        if (arr.size() == 1) {
            return arr.get(0);
        }

        final TIntSet keys = new TIntHashSet();
        for (int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if (!countNull) {
            keys.remove(NULL_INT);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_INT;
    }

    /**
     * Get the single unique value in the array, or null if there are none, or there are more than 1 distinct values.
     *
     * @param arr the array
     * @param countNull if nulls should count as values
     * @return the single unique value in the array, or null.
     */
    public static double uniqueValue(final DoubleVector arr, boolean countNull) {
        if (arr == null || arr.isEmpty()) {
            return NULL_DOUBLE;
        }

        if (arr.size() == 1) {
            return arr.get(0);
        }

        final TDoubleSet keys = new TDoubleHashSet();
        for (int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if (!countNull) {
            keys.remove(NULL_DOUBLE);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_DOUBLE;
    }

    @Test
    public void testAggAllByWithFormatColumn() {
        String doubleColName = "Doubles";
        String intColName = "Integers";
        QueryTable dataTable = TstUtils.testRefreshingTable(
                doubleCol("Doubles", 3.1, 5.45, 4.2),
                intCol("Integers", 1, 2, 3));

        Table result = dataTable.formatColumns("Doubles=Decimal(`##0.00%`)").aggAllBy(AggSpec.median());
        assertEquals(1, result.size());
        ColumnSource<?> cs = result.getColumnSource("Doubles");
        assertEquals(4.2, cs.get(0));
        cs = result.getColumnSource("Integers");
        assertEquals(2.0, cs.get(0));

        result = dataTable.formatColumns("Doubles=Decimal(`##0.00%`)").headBy(1);
        List<String> columnNames = result.getDefinition().getColumnNames();
        assertEquals(3, columnNames.size()); // Additional column for formatting information of "Doubles"
        for (String colName : columnNames) {
            if (!colName.equalsIgnoreCase(doubleColName) && !colName.equalsIgnoreCase(intColName) &&
                    !ColumnFormatting.isFormattingColumn(colName)) {
                TestCase.fail("Result table should have two original columns and one formatting column");
            }
        }
        assertEquals(1, result.size());
        cs = result.getColumnSource("Doubles");
        assertEquals(3.1, cs.get(0));
        cs = result.getColumnSource("Integers");
        assertEquals(1, cs.get(0));
    }

    @Test
    public void testFormulaUpdatePropagation() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6).toTracking(),
                col("Key", "A", "B", "A"),
                doubleCol("Double", 1.0, 2.0, 2.0),
                shortCol("Short", (short) 10, (short) 20, (short) 30),
                intCol("Int", 2, 4, 6));
        final QueryTable result = (QueryTable) table.aggBy(
                List.of(AggFormula("sumInt=sum(Int)"),
                        AggFormula("sumDouble=sum(Double)"),
                        AggFormula("sumCustom=sum(Int) + sum(Double)")),
                "Key");

        // Add to "B" bucket
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final SimpleListener resultListener = new SimpleListener(result);
        result.addUpdateListener(resultListener);
        int lastResultListenerCount = 0;

        final ModifiedColumnSet sumIntMCS = result.newModifiedColumnSet("sumInt");
        final ModifiedColumnSet sumDoubleMCS = result.newModifiedColumnSet("sumDouble");
        final ModifiedColumnSet sumCustomMCS = result.newModifiedColumnSet("sumCustom");
        final ModifiedColumnSet allFormulaMCS = result.newModifiedColumnSet("sumInt", "sumDouble", "sumCustom");

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(8),
                    col("Key", "B"),
                    doubleCol("Double", 3.0),
                    shortCol("Short", (short) 40),
                    intCol("Int", 8)); // Add to "B" bucket
            table.notifyListeners(i(8), i(), i());
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().isEmpty(), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().equals(i(1)), "modified() matches expected");

        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAll(allFormulaMCS), "MCS matches expected");
        lastResultListenerCount = resultListener.count;

        // Change a row in the "B" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(8),
                    col("Key", "B"),
                    doubleCol("Double", 3.0),
                    shortCol("Short", (short) 40),
                    intCol("Int", 9)); // Change Int

            table.notifyListeners(new TableUpdateImpl(
                    i(), i(), i(8),
                    RowSetShiftData.EMPTY,
                    table.newModifiedColumnSet("Int")));
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().isEmpty(), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().equals(i(1)), "modified() matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAny(sumIntMCS), "MCS matches expected");
        Assert.eqFalse(resultListener.update.modifiedColumnSet().containsAny(sumDoubleMCS), "MCS matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAny(sumCustomMCS), "MCS matches expected");
        lastResultListenerCount = resultListener.count;

        // Change a row in the "B" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(8),
                    col("Key", "B"),
                    doubleCol("Double", 5.0), // Change Double
                    shortCol("Short", (short) 40),
                    intCol("Int", 9));

            table.notifyListeners(new TableUpdateImpl(
                    i(), i(), i(8),
                    RowSetShiftData.EMPTY,
                    table.newModifiedColumnSet("Double")));
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().isEmpty(), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().equals(i(1)), "modified() matches expected");
        Assert.eqFalse(resultListener.update.modifiedColumnSet().containsAny(sumIntMCS), "MCS matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAny(sumDoubleMCS), "MCS matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAny(sumCustomMCS), "MCS matches expected");
        lastResultListenerCount = resultListener.count;

        // Change a row in the "B" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(8),
                    col("Key", "B"),
                    doubleCol("Double", 5.0),
                    shortCol("Short", (short) 50), // Change Short
                    intCol("Int", 9));

            table.notifyListeners(new TableUpdateImpl(
                    i(), i(), i(8),
                    RowSetShiftData.EMPTY,
                    table.newModifiedColumnSet("Short")));
        });
        // No update expected from the result table,
        Assert.eq(lastResultListenerCount, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");

        // New "C" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(9),
                    col("Key", "C"),
                    doubleCol("Double", 4.0),
                    shortCol("Short", (short) 40),
                    intCol("Int", 10)); // New "C" bucket in isolation
            table.notifyListeners(i(9), i(), i());
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().equals(i(2)), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().isEmpty(), "modified() matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().empty(), "MCS matches expected");
        lastResultListenerCount = resultListener.count;

        // Row from "B" bucket to "C" bucket
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(8),
                    col("Key", "C"),
                    doubleCol("Double", 3.0),
                    shortCol("Short", (short) 40),
                    intCol("Int", 11)); // Row from "B" bucket to "C" bucket
            table.notifyListeners(new TableUpdateImpl(
                    i(), i(), i(8),
                    RowSetShiftData.EMPTY,
                    table.newModifiedColumnSet("Key")));
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().isEmpty(), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().equals(i(1, 2)), "modified() matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAll(allFormulaMCS), "MCS matches expected");
        lastResultListenerCount = resultListener.count;

        // New "D" bucket, and new row in "C"
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table,
                    i(10, 11),
                    col("Key", "D", "C"),
                    doubleCol("Double", 5.0, 6.0),
                    shortCol("Short", (short) 40, (short) 50),
                    intCol("Int", 10, 11)); // New "D" bucket
            table.notifyListeners(i(10, 11), i(), i());
        });
        Assert.eq(lastResultListenerCount + 1, "lastResultListenerCount + 1",
                resultListener.count, "resultListener.count");
        Assert.eqTrue(resultListener.update.added().equals(i(3)), "added() matches expected");
        Assert.eqTrue(resultListener.update.removed().isEmpty(), "removed() matches expected");
        Assert.eqTrue(resultListener.update.shifted().empty(), "shifted() matches expected");
        Assert.eqTrue(resultListener.update.modified().equals(i(2)), "modified() matches expected");
        Assert.eqTrue(resultListener.update.modifiedColumnSet().containsAll(allFormulaMCS), "MCS matches expected");

        TableTools.show(result);
    }
}
