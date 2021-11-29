/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.vector.CharVector;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.by.AggregationFormulaSpec;
import io.deephaven.engine.table.impl.by.MinMaxBySpecImpl;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.util.ColumnHolder;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.time.DateTimeUtils.convertDateTime;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.table.impl.by.AggregationFactory.*;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestAggBy extends RefreshingTableTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    private static Table by(Table table, AggregationSpec spec, String... groupByColumns) {
        return ((QueryTable) table).by(spec, SelectColumn.from(Selectable.from(groupByColumns)));
    }

    public void testBy() {
        ColumnHolder aHolder = c("A", 0, 0, 1, 1, 0, 0, 1, 1, 0, 0);
        ColumnHolder bHolder = c("B", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Table table = TableTools.newTable(aHolder, bHolder);
        show(table);
        assertEquals(10, table.size());
        assertEquals(2, table.groupBy("A").size());

        AggregationFormulaSpec minFactory = new AggregationFormulaSpec("min(each)", "each");
        AggregationFormulaSpec maxFactory = new AggregationFormulaSpec("max(each)", "each");

        AggregationFactory minMaxFactory =
                new AggregationFactory(new AggregationElementImpl(minFactory, "Min=B"),
                        new AggregationElementImpl(maxFactory, "Max=B"));

        Table minMax = by(table, minMaxFactory, "A");
        show(minMax);
        assertEquals(2, minMax.size());
        DataColumn dc = minMax.getColumn("Min");
        assertEquals(1, dc.get(0));
        assertEquals(3, dc.get(1));
        dc = minMax.getColumn("Max");
        assertEquals(10, dc.get(0));
        assertEquals(8, dc.get(1));

        AggregationFactory doubleCountFactory = new AggregationFactory(
                new CountAggregationElement("Count1"), new CountAggregationElement("Count2"));
        Table doubleCounted = by(table, doubleCountFactory, "A");
        show(doubleCounted);
        assertEquals(2, doubleCounted.size());

        dc = doubleCounted.getColumn("Count1");
        assertEquals(6L, dc.get(0));
        assertEquals(4L, dc.get(1));
        dc = doubleCounted.getColumn("Count2");
        assertEquals(6L, dc.get(0));
        assertEquals(4L, dc.get(1));

        // Lets do some interesting incremental computations, as this is the use case that I'm really aiming at. For
        // example, getting the count, and average on each update.
        // It would be nice to do a min and a max as well,
        // which can often be efficient (but sometimes could also require linear work). That isn't related to this test
        // but more related to the underlying min and max.

        // Interestingly, the factories appear to be single use. If you try to reuse a factory it fails with an NPE.
        // minFactory = new AggregationFormulaSpec("min(each)", "each");
        // maxFactory = new AggregationFormulaSpec("max(each)", "each");

        AggregationFactory summaryStatisticsFactory = AggCombo(
                AggCount("Count"),
                AggMin("MinB=B", "MinC=C"),
                AggMed("MedB=B", "MedC=C"),
                AggMax("MaxB=B", "MaxC=C"),
                AggAvg("AvgB=B", "AvgC=C"),
                AggStd("StdB=B", "StdC=C"),
                AggSum("SumB=B", "SumC=C"),
                AggCountDistinct("DistinctA=A"),
                AggCountDistinct("DistinctB=B"));

        AggregationFactory percentilesFactory = AggCombo(
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
        int bLength = Array.getLength(bHolder.data);
        for (int ii = 0; ii < bLength; ++ii) {
            doubles[ii] = 1.1 * Array.getInt(bHolder.data, ii);
        }
        ColumnHolder cHolder = c("C", doubles);
        table = TableTools.newTable(aHolder, bHolder, cHolder);
        show(table);
        Table summary = by(table, summaryStatisticsFactory, "A");
        show(summary);

        // System.out.println("Percentiles (keyed):");
        // Table percentiles = table.groupBy(percentilesFactory, "A");
        // show(percentiles);

        System.out.println("\nPercentiles (overall):");
        Table percentilesAll = by(table, percentilesFactory);
        show(percentilesAll);
    }

    public void testComboByMinMaxTypes() {
        final Random random = new Random(0);
        final int size = 10;
        final ColumnInfo[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "shortCol", "byteCol", "longCol", "charCol", "doubleCol",
                                "floatCol", "DateTime", "BoolCol", "bigI", "bigD"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.ShortGenerator(),
                        new TstUtils.ByteGenerator(),
                        new TstUtils.LongGenerator(),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                        new TstUtils.FloatGenerator(0, 10.0f),
                        new TstUtils.UnsortedDateTimeGenerator(convertDateTime("2020-03-17T12:00:00 NY"),
                                convertDateTime("2020-03-18T12:00:00 NY")),
                        new TstUtils.BooleanGenerator(),
                        new TstUtils.BigIntegerGenerator(),
                        new TstUtils.BigDecimalGenerator()));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget
                        .from(() -> by(queryTable, AggCombo(AggMin(queryTable.getDefinition().getColumnNamesArray())))),
                EvalNugget
                        .from(() -> by(queryTable, AggCombo(AggMax(queryTable.getDefinition().getColumnNamesArray())))),
                new QueryTableTest.TableComparator(
                        by(queryTable, AggCombo(AggMin(queryTable.getDefinition().getColumnNamesArray()))),
                        "AggCombo",
                        queryTable.minBy(),
                        "MinBy"),
                EvalNugget.Sorted.from(
                        () -> by(queryTable, AggCombo(AggMin(queryTable.getDefinition().getColumnNamesArray())), "Sym"),
                        "Sym"),
                new QueryTableTest.TableComparator(
                        by(queryTable, AggCombo(AggMin(queryTable.getDefinition().getColumnNamesArray())), "Sym")
                                .sort("Sym"),
                        "AggCombo",
                        queryTable.minBy("Sym").sort("Sym"),
                        "MinBy"),
        };
        final int steps = 100; // 8;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep("step == " + step, size, random, queryTable, columnInfo, en);
        }
    }

    public void testComboByIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testComboByIncremental("size-" + size, size);
        }
    }

    private void testComboByIncremental(final String ctxt, final int size) {
        Random random = new Random(0);
        ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(size, random,
                columnInfo =
                        initColumnInfos(new String[] {"Sym", "intCol", "intColNulls", "doubleCol", "doubleColNulls"},
                                new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                                new TstUtils.IntGenerator(10, 100),
                                new TstUtils.IntGenerator(10, 100, .1),
                                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1, QueryConstants.NULL_DOUBLE)));

        QueryLibrary.importClass(TestAggBy.class);

        final AggregationElement reusedCount = AggCount("Count");
        EvalNuggetInterface en[] = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return by(queryTable, AggCombo(Agg(AggType.Avg, "MeanI=intCol", "MeanD=doubleCol"),
                                Agg(AggType.Std, "StdI=intCol", "StdD=doubleCol")), "Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return by(queryTable, AggCombo(AggFormula("min(each)", "each", "MinI=intCol", "MinD=doubleCol"),
                                AggFormula("max(each)", "each", "MaxI=intCol")), "Sym").sort("Sym");
                    }
                },
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinI=min(intCol)", "MinD=min(doubleCol)").sort("Sym"),
                        "UpdateView",
                        by(queryTable,
                                new AggregationFactory(
                                        Agg(new MinMaxBySpecImpl(true), "MinI=intCol", "MinD=doubleCol")),
                                "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MaxI=max(intCol)", "MaxD=max(doubleCol)").sort("Sym"),
                        "UpdateView",
                        by(queryTable, AggCombo(Agg(AggType.Max, "MaxI=intCol", "MaxD=doubleCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinI=min(intCol)", "MaxI=max(intCol)").sort("Sym"),
                        "UpdateView",
                        by(queryTable, new AggregationFactory(Agg(new MinMaxBySpecImpl(true), "MinI=intCol"),
                                Agg(new MinMaxBySpecImpl(false), "MaxI=intCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym", "MinD=min(doubleCol)", "MaxD=max(doubleCol)").sort("Sym"),
                        "UpdateView",
                        by(queryTable, AggCombo(Agg(new MinMaxBySpecImpl(true), "MinD=doubleCol"),
                                Agg(new MinMaxBySpecImpl(false), "MaxD=doubleCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "MinD=min(doubleCol)", "MaxI=max(intCol)", "FirstD=first(doubleCol)",
                                        "LastI=last(intCol)")
                                .sort("Sym"),
                        "UpdateView",
                        by(queryTable, AggCombo(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxI=intCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggLast("LastI=intCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "MinD=min(doubleCol)", "MaxD=max(doubleCol)", "MinI=min(intCol)",
                                        "MaxI=max(intCol)", "LastD=last(doubleCol)", "FirstD=first(doubleCol)",
                                        "FirstI=first(intCol)", "LastI=last(intCol)")
                                .sort("Sym"),
                        "UpdateView",
                        by(queryTable, AggCombo(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxD=doubleCol"),
                                AggMin("MinI=intCol"),
                                AggMax("MaxI=intCol"),
                                AggLast("LastD=doubleCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggFirst("FirstI=intCol"),
                                AggLast("LastI=intCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy().view("MinD=min(doubleCol)", "MaxI=max(intCol)", "MaxD=max(doubleCol)",
                                "MinI=min(intCol)",
                                "FirstD=first(doubleCol)", "LastI=last(intCol)", "LastD=last(doubleCol)",
                                "FirstI=first(intCol)"),
                        "UpdateView",
                        by(queryTable, AggCombo(
                                AggMin("MinD=doubleCol"),
                                AggMax("MaxI=intCol"),
                                AggMax("MaxD=doubleCol"),
                                AggMin("MinI=intCol"),
                                AggFirst("FirstD=doubleCol"),
                                AggLast("LastI=intCol"),
                                AggLast("LastD=doubleCol"),
                                AggFirst("FirstI=intCol"))),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym")
                                .view("Sym", "AvgD=avg(doubleCol)", "SumD=sum(doubleCol)", "VarD=var(doubleCol)",
                                        "StdD=std(doubleCol)", "intCol")
                                .sort("Sym"),
                        "UpdateView",
                        by(queryTable, new AggregationFactory(
                                AggAvg("AvgD=doubleCol"),
                                AggSum("SumD=doubleCol"),
                                AggVar("VarD=doubleCol"),
                                AggStd("StdD=doubleCol"),
                                AggGroup("intCol")), "Sym").sort("Sym"),
                        "AggregationElement"),
                new QueryTableTest.TableComparator(
                        queryTable.groupBy("Sym").view("Sym",
                                "MedD=median(doubleCol)",
                                "Pct01D=percentile(doubleCol, 0.01)",
                                "Pct01I=(int)TestAggBy.percentile(intCol, 0.01)",
                                "Pct05D=percentile(doubleCol, 0.05)",
                                "Pct05I=(int)TestAggBy.percentile(intCol, 0.05)",
                                "Pct25D=percentile(doubleCol, 0.25)",
                                "Pct25I=(int)TestAggBy.percentile(intCol, 0.25)",
                                "Pct50D=percentile(doubleCol, 0.50)",
                                "Pct50I=(int)TestAggBy.percentile(intCol, 0.50)",
                                "Pct65D=percentile(doubleCol, 0.65)",
                                "Pct65I=(int)TestAggBy.percentile(intCol, 0.65)",
                                "Pct90D=percentile(doubleCol, 0.90)",
                                "Pct90I=(int)TestAggBy.percentile(intCol, 0.90)",
                                "Pct99D=percentile(doubleCol, 0.99)",
                                "Pct99I=(int)TestAggBy.percentile(intCol, 0.99)").sort("Sym"),
                        by(queryTable, AggCombo(
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
                        by(queryTable, AggCombo(
                                AggWAvg("doubleCol", "WAvg=intCol")), "Sym"),
                        "AggWAvg"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        by(queryTable, AggCombo(reusedCount), CollectionUtil.ZERO_LENGTH_STRING_ARRAY), "AggCount"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        by(queryTable, AggCombo(reusedCount), CollectionUtil.ZERO_LENGTH_STRING_ARRAY), "AggCount"),
                new QueryTableTestBase.TableComparator(
                        queryTable.groupBy("Sym").view("Sym",
                                "cdi=countDistinct(intCol)",
                                "ddi=countDistinct(doubleCol)",
                                "cdiN=countDistinct(intColNulls, true)",
                                "ddiN=countDistinct(doubleColNulls, true)",
                                "dic=distinct(intCol, false, true)",
                                "did=distinct(doubleCol, false, true)",
                                "dicN=distinct(intColNulls, true, true)",
                                "didN=distinct(doubleColNulls, true, true)",
                                "uic=uniqueValue(intCol, false)",
                                "uid=uniqueValue(doubleCol, false)",
                                "uicN=uniqueValue(intColNulls, true)",
                                "uidN=uniqueValue(doubleColNulls, true)")
                                .sort("Sym"),
                        "countDistinctView",
                        by(queryTable, AggCombo(AggCountDistinct("cdi=intCol", "ddi=doubleCol"),
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

    public void testComboByDoubleClaim() throws IOException {
        final int size = 10;
        final Random random = new Random(0);
        final ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1)));

        final AggregationElement reusedCount = AggCount("Count");
        final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        by(queryTable, AggCombo(reusedCount), CollectionUtil.ZERO_LENGTH_STRING_ARRAY), "AggCount"),
                new QueryTableTest.TableComparator(
                        queryTable.view("Sym", "intCol", "doubleCol").countBy("Count"), "Count",
                        by(queryTable, AggCombo(reusedCount), CollectionUtil.ZERO_LENGTH_STRING_ARRAY), "AggCount")
        };
        final int steps = 100; // 8;
        for (int i = 0; i < steps; i++) {
            System.out.println("Abstract Table:");
            show(queryTable);
            simulateShiftAwareStep("double Claim" + " step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    public void testComboByDistinct() {
        QueryTable dataTable = TstUtils.testRefreshingTable(
                intCol("Grp", 1, 2, 3, 4),
                charCol("Let", 'a', 'b', 'c', 'd'));

        final Table tail = dataTable.tail(10);
        final Table result = by(tail, AggCombo(AggDistinct("Let")), "Grp");

        // noinspection unchecked
        final ColumnSource<CharVector> cs = result.getColumnSource("Let");
        assertEquals(4, result.size());
        assertArrayEquals(new char[] {'a'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'b'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final RowSet toAdd = i(4, 5, 6, 7);
            addToTable(dataTable, toAdd,
                    intCol("Grp", 1, 2, 3, 4),
                    charCol("Let", 'e', 'f', 'g', 'h'));
            dataTable.notifyListeners(toAdd, i(), i());
        });
        assertEquals(4, result.size());
        assertArrayEquals(new char[] {'a', 'e'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'b', 'f'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c', 'g'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d', 'h'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final RowSet toAdd = i(8, 9, 10, 11);
            addToTable(dataTable, toAdd,
                    intCol("Grp", 1, 2, 3, 4),
                    charCol("Let", 'i', 'j', 'k', 'l'));
            dataTable.notifyListeners(toAdd, i(), i());
        });
        assertArrayEquals(new char[] {'e', 'i'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'f', 'j'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'c', 'g', 'k'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'d', 'h', 'l'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(16), intCol("Grp", 1), charCol("Let", 'q'));
            dataTable.notifyListeners(i(16), i(), i());
        });
        assertArrayEquals(new char[] {'i', 'm', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'h', 'l', 'p'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(17), intCol("Grp", 2), charCol("Let", 'r'));
            dataTable.notifyListeners(i(17), i(), i());
        });
        assertArrayEquals(new char[] {'i', 'm', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(18), intCol("Grp", 3), charCol("Let", 's'));
            dataTable.notifyListeners(i(18), i(), i());
        });
        assertArrayEquals(new char[] {'m', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'j', 'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o', 's'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p'}, cs.get(3).toArray());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(19), intCol("Grp", 4), charCol("Let", 't'));
            dataTable.notifyListeners(i(19), i(), i());
        });
        assertArrayEquals(new char[] {'m', 'q'}, cs.get(0).toArray());
        assertArrayEquals(new char[] {'n', 'r'}, cs.get(1).toArray());
        assertArrayEquals(new char[] {'k', 'o', 's'}, cs.get(2).toArray());
        assertArrayEquals(new char[] {'l', 'p', 't'}, cs.get(3).toArray());
    }

    public void testComboByCountDistinct() {
        QueryTable dataTable = TstUtils.testRefreshingTable(
                c("USym", "AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "SPY", "SPY", "SPY", "SPY", "VXX"),
                longCol("Account", 1, 1, 2, 1, 3, 2, 4, 2, 5, 5),
                intCol("Qty", 100, 100, 200, 300, 50, 100, 150, 200, 50, 50));

        Table result = by(dataTable, AggCombo(AggCountDistinct("Account", "Qty")), "USym").sort("USym");
        Table countNulls = by(dataTable, AggCombo(AggCountDistinct(true, "Account", "Qty")), "USym").sort("USym");
        assertEquals(4, result.size());
        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, result.getRecord(0));
        assertArrayEquals(new Object[] {"GOOG", 2L, 2L}, result.getRecord(1));
        assertArrayEquals(new Object[] {"SPY", 3L, 4L}, result.getRecord(2));
        assertArrayEquals(new Object[] {"VXX", 1L, 1L}, result.getRecord(3));
        assertTableEquals(result, countNulls);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(1, 10),
                    c("USym", "AAPL", "VXX"),
                    longCol("Account", QueryConstants.NULL_LONG, 1),
                    intCol("Qty", 100, QueryConstants.NULL_INT));
            dataTable.notifyListeners(i(10), i(), i(1));
        });

        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, result.getRecord(0));
        assertArrayEquals(new Object[] {"VXX", 2L, 1L}, result.getRecord(3));

        assertArrayEquals(new Object[] {"AAPL", 3L, 2L}, countNulls.getRecord(0));
        assertArrayEquals(new Object[] {"VXX", 2L, 2L}, countNulls.getRecord(3));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(2),
                    c("USym", "AAPL"),
                    longCol("Account", QueryConstants.NULL_LONG),
                    intCol("Qty", 200));
            dataTable.notifyListeners(i(), i(), i(2));
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 2L}, result.getRecord(0));
        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, countNulls.getRecord(0));

        TableTools.showWithRowSet(dataTable, dataTable.size());

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(1, 2, 11),
                    c("USym", "AAPL", "AAPL", "SPY"),
                    longCol("Account", 1, 2, QueryConstants.NULL_LONG),
                    intCol("Qty", 100, 200, 200));

            removeRows(dataTable, i(6));
            dataTable.notifyListeners(i(11), i(6), i(1, 2));
        });

        TableTools.showWithRowSet(dataTable, dataTable.size());

        assertArrayEquals(new Object[] {"AAPL", 2L, 2L}, result.getRecord(0));
        assertArrayEquals(new Object[] {"SPY", 3L, 3L}, countNulls.getRecord(2));
    }

    public void testComboByAggUnique() {
        final DateTime dtdefault = convertDateTime("1987-10-20T07:45:00.000 NY");
        final DateTime dt1 = convertDateTime("2021-01-01T00:00:01.000 NY");
        final DateTime dt2 = convertDateTime("2021-01-01T00:00:02.000 NY");
        final DateTime dt3 = convertDateTime("2021-01-01T00:00:03.000 NY");

        QueryTable dataTable = TstUtils.testRefreshingTable(
                c("USym", "AAPL", "AAPL", "AAPL", /**/ "GOOG", "GOOG", /**/ "SPY", "SPY", "SPY", "SPY", /**/ "VXX"),
                longCol("Account", 1, 1, 2, /**/ 1, 3, /**/ 2, 4, 2, 5, /**/ 5),
                intCol("Qty", 100, 100, 100, /**/ 300, 50, /**/ 100, 150, 200, 50, /**/ 50),
                c("Whee", dt1, dt1, dt1, /**/ dt1, dt2, /**/ dt2, dt2, dt2, dt2, /**/ null));

        Table result = by(dataTable, AggCombo(AggUnique(false, null, -1, "Account", "Qty"),
                AggUnique(false, null, dtdefault, "Whee")), "USym").sort("USym");

        Table countNulls = by(dataTable, AggCombo(AggUnique(true, null, -1, "Account", "Qty"),
                AggUnique(true, null, dtdefault, "Whee")), "USym").sort("USym");

        assertEquals(4, result.size());
        assertArrayEquals(new Object[] {"AAPL", -1L, 100, dt1}, result.getRecord(0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtdefault}, result.getRecord(1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, result.getRecord(2));
        assertArrayEquals(new Object[] {"VXX", 5L, 50, null}, result.getRecord(3));
        assertTableEquals(result, countNulls);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(2, 10),
                    c("USym", "AAPL", "VXX"),
                    longCol("Account", 1, 5),
                    intCol("Qty", 100, QueryConstants.NULL_INT),
                    c("Whee", (DateTime) null, (DateTime) null));
            dataTable.notifyListeners(i(10), i(), i(2));
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dt1}, result.getRecord(0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtdefault}, result.getRecord(1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, result.getRecord(2));
        assertArrayEquals(new Object[] {"VXX", 5L, 50, null}, result.getRecord(3));

        // Check the nulls table
        assertArrayEquals(new Object[] {"VXX", 5L, -1, null}, countNulls.getRecord(3));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(11),
                    c("USym", "USO"),
                    longCol("Account", 2),
                    intCol("Qty", 200),
                    c("Whee", dt1));
            removeRows(dataTable, i(9, 10));
            dataTable.notifyListeners(i(11), i(9, 10), i());
        });

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dt1}, result.getRecord(0));
        assertArrayEquals(new Object[] {"GOOG", -1L, -1, dtdefault}, result.getRecord(1));
        assertArrayEquals(new Object[] {"SPY", -1L, -1, dt2}, result.getRecord(2));
        assertArrayEquals(new Object[] {"USO", 2L, 200, dt1}, result.getRecord(3));

        assertArrayEquals(new Object[] {"AAPL", 1L, 100, dtdefault}, countNulls.getRecord(0));

        //
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(11),
                    c("USym", "USO"),
                    longCol("Account", QueryConstants.NULL_LONG),
                    intCol("Qty", QueryConstants.NULL_INT),
                    c("Whee", dt2));
            dataTable.notifyListeners(i(), i(), i(11));
        });
        assertArrayEquals(new Object[] {"USO", null, null, dt2}, result.getRecord(3));

        //
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(dataTable, i(3, 4, 9, 10),
                    c("USym", "GOOG", "GOOG", "VXX", "VXX"),
                    longCol("Account", 2L, 2L, QueryConstants.NULL_LONG, 99),
                    intCol("Qty", 350, 350, 50, 50),
                    c("Whee", dt2, dt2, (DateTime) null, dt1));
            dataTable.notifyListeners(i(9, 10), i(), i(3, 4));
        });

        assertArrayEquals(new Object[] {"GOOG", 2L, 350, dt2}, result.getRecord(1));
        assertArrayEquals(new Object[] {"VXX", 99L, 50, dt1}, result.getRecord(4));
        assertArrayEquals(new Object[] {"VXX", -1L, 50, dtdefault}, countNulls.getRecord(4));
    }

    public void testAggUniqueDefaultValues() {
        final DateTime dt1 = convertDateTime("2021-01-01T00:01:02.000 NY");
        final DateTime dt2 = convertDateTime("2021-02-02T00:02:03.000 NY");

        QueryTable dataTable = TstUtils.testRefreshingTable(
                c("USym", "NoKey", "SingleVal", "NonUnique", "NonUnique"),
                c("StringCol", null, "Apple", "Bacon", "Pancake"),
                c("BoolCol", null, true, true, false),
                c("DateTime", null, dt1, dt1, dt2),
                charCol("CharCol", NULL_CHAR, 'a', 'b', 'c'),
                byteCol("ByteCol", NULL_BYTE, (byte) 100, (byte) 110, (byte) 120),
                shortCol("ShortCol", NULL_SHORT, (short) 1234, (short) 4321, (short) 1324),
                intCol("IntCol", NULL_INT, 99999, 100000, 200000),
                longCol("LongCol", NULL_LONG, 44444444L, 55555555L, 66666666L),
                floatCol("FloatCol", NULL_FLOAT, 1.2345f, 2.3456f, 3.4567f),
                doubleCol("DoubleCol", NULL_DOUBLE, 1.1E22d, 2.2E22d, 3.3E22d));

        // First try mixing column types and values
        Table result;
        expectException(IllegalArgumentException.class,
                "Attempted to use no key/non unique values of incorrect types for aggregated columns!",
                () -> by(dataTable, AggCombo(AggUnique(false, -1, -2, "StringCol", "BoolCol", "DatTime", "CharCol",
                        "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol", "DoubleCol")), "USym").sort("USym"));

        result = by(dataTable,
                AggCombo(AggUnique(false, -1, -2, "ByteCol", "ShortCol", "IntCol", "LongCol", "FloatCol", "DoubleCol")),
                "USym").sort("USym");

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

    }

    private void testUniqueOutOfRangeParams(Class<?> type, Table dataTable, Number invalidLow, Number validLow,
            Number invalidHigh, Number validHigh, String... aggCols) {
        // Byte out of range
        expectException(IllegalArgumentException.class,
                "Attempted to use no key values too small for " + type.getName() + "!",
                () -> by(dataTable, AggCombo(AggUnique(false, invalidLow, -1, aggCols)), "USym").sort("USym"));

        expectException(IllegalArgumentException.class,
                "Attempted to use no key values too large for " + type.getName() + "!",
                () -> by(dataTable, AggCombo(AggUnique(false, invalidHigh, -1, aggCols)), "USym").sort("USym"));

        expectException(IllegalArgumentException.class,
                "Attempted to non unique values too small for " + type.getName() + "!",
                () -> by(dataTable, AggCombo(AggUnique(false, -1, invalidLow, aggCols)), "USym").sort("USym"));

        expectException(IllegalArgumentException.class,
                "Attempted to use non unique values too large for " + type.getName() + "!",
                () -> by(dataTable, AggCombo(AggUnique(false, -1, invalidHigh, aggCols)), "USym").sort("USym"));

        by(dataTable, AggCombo(AggUnique(false, validLow, validLow, aggCols)), "USym").sort("USym");
        by(dataTable, AggCombo(AggUnique(false, validHigh, validHigh, aggCols)), "USym").sort("USym");
    }

    private static <T extends Throwable> void expectException(Class<T> excType, String failMessage, Runnable action) {
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

    // used in a query test
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

}
