//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.ConditionalExpr;
import com.github.javaparser.ast.expr.Expression;
import com.google.common.collect.Sets;
import io.deephaven.base.Pair;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.JavaExpressionParser;
import io.deephaven.engine.table.impl.select.ShiftedColumnDefinition;
import io.deephaven.engine.table.impl.sources.ShiftedColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.TimeLiteralReplacedExpression;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.MemoizedOperationKey.SelectUpdateViewOrUpdateView.*;
import static io.deephaven.engine.table.impl.ShiftedColumnOperationTest.shiftColName;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class TestFormulaArrayEvaluation {
    @Rule
    public EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testViewIncrementalSimpleTest() {

        final QueryTable queryTable = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30),
                intCol("Value2", 202, 222, 242, 262, 282, 302),
                intCol("Value3", 2020, 2220, 2420, 2620, 2820, 3020));


        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.view("newCol=Value / 2", "newCol2=newCol_[i-1] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=Value / 2", "newCol2=newCol_[i+1] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=Value / 2", "newCol2=newCol_[i-2] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=Value / 2", "newCol2=newCol_[i+2] * 4")),
        };
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable,
                    i(10, 12, 18),
                    intCol("Sentinel", 56, 57, 510),
                    intCol("Value", 420, 422, 428),
                    intCol("Value2", 302, 322, 382),
                    intCol("Value3", 4020, 4220, 4820));

            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(10, 12, 18), shiftData,
                    queryTable.newModifiedColumnSet("Sentinel", "Value", "Value2", "Value3"));
            System.out.println("published update in Test: " + update);
            queryTable.notifyListeners(update);
        });
        TstUtils.validate("", en);
    }

    @Test
    public void testViewIncrementalRandomizedTest() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.updateView("intCol=intCol * 2")),
                EvalNugget.from(() -> queryTable.updateView("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2").updateView("newCol2=newCol * 4")),
                EvalNugget.from(() -> queryTable.view("intCol=intCol * 2")),
                EvalNugget.from(() -> queryTable.view("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2").updateView("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.updateView("intCol=intCol * 2")),
                EvalNugget.from(() -> sortedTable.updateView("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2").updateView("newCol2=newCol * 4")),
                EvalNugget.from(() -> sortedTable.view("intCol=intCol * 2")),
                EvalNugget.from(() -> sortedTable.view("intCol=intCol + doubleCol")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2").updateView("newCol2=newCol * 4")),
                // i-1
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-1] * 4")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-1] * newCol")),
                EvalNugget.from(() -> queryTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-1] * repeatedCol")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i-1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol_[i-1]")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-1] * 4")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-1] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-1] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i-1] + 7")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol_[i-1]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol_[i-1]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i-1] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i-1] * newCol")),
                EvalNugget.from(() -> queryTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-1] * repeatedCol")),
                EvalNugget.from(() -> queryTable.view("newCol2=intCol / 2", "newCol=newCol2_[i-1] + 7")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol_[i-1]")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i-1] * 4")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i-1] * newCol")),
                EvalNugget.from(() -> sortedTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-1] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.view("newCol2=intCol / 2", "newCol=newCol2_[i-1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2",
                        "newCol=newCol_[i-1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol=newCol_[i-1] + 7")),
                // i+1
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+1] * 4")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+1] * newCol")),
                EvalNugget.from(() -> queryTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+1] * repeatedCol")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i+1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol_[i+1]")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+1] * 4")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+1] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+1] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i+1] + 7")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol_[i+1]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol_[i+1]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i+1] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i+1] * newCol")),
                EvalNugget.from(() -> queryTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+1] * repeatedCol")),
                EvalNugget.from(() -> queryTable.view("newCol2=intCol / 2", "newCol=newCol2_[i+1] + 7")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol_[i+1]")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i+1] * 4")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i+1] * newCol")),
                EvalNugget.from(() -> sortedTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+1] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.view("newCol2=intCol / 2", "newCol=newCol2_[i+1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2",
                        "newCol=newCol_[i+1] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol=newCol_[i+1] + 7")),
                // i-2
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-2] * 4")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-2] * newCol")),
                EvalNugget.from(() -> queryTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-2] * repeatedCol")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i-2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol_[i-2]")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-2] * 4")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-2] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-2] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i-2] + 7")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol_[i-2]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol_[i-2]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i-2] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i-2] * newCol")),
                EvalNugget.from(() -> queryTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-2] * repeatedCol")),
                EvalNugget.from(() -> queryTable.view("newCol2=intCol / 2", "newCol=newCol2_[i-2] + 7")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol_[i-2]")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i-2] * 4")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i-2] * newCol")),
                EvalNugget.from(() -> sortedTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i-2] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.view("newCol2=intCol / 2", "newCol=newCol2_[i-2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2",
                        "newCol=newCol_[i-2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol=newCol_[i-2] + 7")),
                // i+2
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+2] * 4")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+2] * newCol")),
                EvalNugget.from(() -> queryTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+2] * repeatedCol")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i+2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol_[i+2]")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+2] * 4")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+2] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+2] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol2=intCol / 2", "newCol=newCol2_[i+2] + 7")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol_[i+2]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol_[i+2]")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i+2] * 4")),
                EvalNugget.from(() -> queryTable.view("newCol=intCol / 2", "newCol2=newCol_[i+2] * newCol")),
                EvalNugget.from(() -> queryTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+2] * repeatedCol")),
                EvalNugget.from(() -> queryTable.view("newCol2=intCol / 2", "newCol=newCol2_[i+2] + 7")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol_[i+2]")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i+2] * 4")),
                EvalNugget.from(() -> sortedTable.view("newCol=intCol / 2", "newCol2=newCol_[i+2] * newCol")),
                EvalNugget.from(() -> sortedTable.view("repeatedCol=doubleCol - 0.5", "newCol=intCol / 2",
                        "repeatedCol=newCol_[i+2] * repeatedCol")),
                EvalNugget.from(() -> sortedTable.view("newCol2=intCol / 2", "newCol=newCol2_[i+2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol2=intCol / 2", "newCol=newCol2",
                        "newCol=newCol_[i+2] + 7")),
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol=newCol_[i+2] + 7")),
        };

        for (int i = 0; i < 10; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testViewIncrementalRandomizedPerformanceTest() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final int size = 50000;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table sortedTable = queryTable.sort("intCol");

        final EvalNugget[] en = new EvalNugget[] {
                // i-500
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-500] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i-500] * newCol")),
                // i+500
                EvalNugget.from(() -> queryTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+500] * newCol")),
                EvalNugget.from(() -> sortedTable.updateView("newCol=intCol / 2", "newCol2=newCol_[i+500] * newCol")),
        };

        // original implementation of ShiftedColumnOperation took ~30s; current impl takes ~8s
        final long startTm = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
        final long duration = System.currentTimeMillis() - startTm;
        Assert.assertTrue(
                String.format("Test finishes in less than 15 seconds, instead duration was %d ms", duration),
                duration < 15_000);
    }

    @Test
    public void updateTableStableMultiShiftedColValuesTest() {
        stableMultiShiftedColValuesTest(Flavor.Update, 7, 20);
    }

    @Test
    public void selectTableStableMultiShiftedColValuesTest() {
        stableMultiShiftedColValuesTest(Flavor.Select, 7, 20);
    }

    @Test
    public void viewTableStableMultiShiftedColValuesTest() {
        stableMultiShiftedColValuesTest(Flavor.View, 7, 20);
    }

    @Test
    public void updateViewTableStableMultiShiftedColValuesTest() {
        stableMultiShiftedColValuesTest(Flavor.UpdateView, 7, 20);
    }

    @SuppressWarnings("SameParameterValue")
    private void stableMultiShiftedColValuesTest(Flavor flavor, final int tableSize, final int displayTableSize) {
        String[] formulas = new String[] {
                "A=k * 10",
                "B=i * 2",
                "Z=A_[i-1]",
                "D=A_[i + 1] * B_[ii -2]",
                "C=true"
        };

        final ShiftedColumnDefinition d0 = new ShiftedColumnDefinition("A", -1);
        final ShiftedColumnDefinition d1 = new ShiftedColumnDefinition("A", 1);
        final ShiftedColumnDefinition d2 = new ShiftedColumnDefinition("B", -2);
        Table source = emptyTable(tableSize);
        Table shiftedTable = emptyTable(tableSize);
        switch (flavor) {
            case Update: {
                source = source.update(formulas);
                shiftedTable = shiftedTable.update("A=k * 10", "B=i * 2");
                break;
            }
            case UpdateView: {
                source = source.updateView(formulas);
                shiftedTable = shiftedTable.updateView("A=k * 10", "B=i * 2");
                break;
            }
            case Select: {
                source = source.select(formulas);
                shiftedTable = shiftedTable.select("A=k * 10", "B=i * 2");
                break;
            }
            case View: {
                source = source.view(formulas);
                shiftedTable = shiftedTable.view("A=k * 10", "B=i * 2");
                break;
            }
        }

        shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, d0);
        shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, d1);
        shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, d2);
        shiftedTable = shiftedTable.view("A", "B", "Z=__A_Shifted_Minus_1__",
                "D = __A_Shifted_Plus_1__ * __B_Shifted_Minus_2__", "C=true");

        showTableWithRowSet(source, Math.min(tableSize, displayTableSize));
        showTableWithRowSet(shiftedTable, Math.min(tableSize, displayTableSize));

        String[] columns = source.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, formulas.length, columns.length);

        String[] shiftedColumns = shiftedTable.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, formulas.length, shiftedColumns.length);

        for (int i = 0; i < formulas.length; i++) {
            ColumnSource<?> cs = source.getColumnSource(columns[i]);
            ColumnSource<?> scs = shiftedTable.getColumnSource(shiftedColumns[i]);
            Assert.assertEquals("ColumnSources' names are equal", columns[i], shiftedColumns[i]);

            if (columns[i].equals("Z")) {
                Assert.assertTrue("ColumnSource is a ShiftedColumnSource ", cs instanceof ShiftedColumnSource);
                Assert.assertTrue("ColumnSource is a ShiftedColumnSource ", scs instanceof ShiftedColumnSource);
            } else {
                Assert.assertFalse("ColumnSource is NOT a ShiftedColumnSource ", cs instanceof ShiftedColumnSource);
                Assert.assertFalse("ColumnSource is NOT a ShiftedColumnSource ", scs instanceof ShiftedColumnSource);
            }
        }

        AtomicLong atomicLong = new AtomicLong(0);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        final RowSet index = source.getRowSet();
        final int aMultiplier = 10;
        final int bMultiplier = 2;
        Long prevAValue = null;
        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            final int currIntValue = atomicInteger.getAndIncrement();
            final long currLongValue = atomicLong.getAndIncrement();
            final long expectedColAValue = currLongValue * aMultiplier;
            final int expectedColBValue = currIntValue * bMultiplier;
            final Long arrayAccessValue = prevAValue;
            prevAValue = expectedColAValue;
            final Long expectedColDS1Value = key >= tableSize - 1 ? null : (currLongValue + 1) * aMultiplier;
            final Integer expectedColDS2Value = key < 2 ? null : (currIntValue - 2) * bMultiplier;
            final Long expectedColDValue = (expectedColDS1Value == null || expectedColDS2Value == null) ? null
                    : expectedColDS1Value * expectedColDS2Value.longValue();

            Assert.assertEquals("A=k * 10 verification", expectedColAValue,
                    source.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("B=i * 2 verification", expectedColBValue, source.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("Z=A_[i-1] verification", arrayAccessValue,
                    source.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=A_[i + 1] * B_[ii -2] verification", expectedColDValue,
                    source.getColumnSource(columns[3]).get(key));
            Assert.assertEquals("C=true verification", true, source.getColumnSource(columns[4]).get(key));

            Assert.assertEquals("A=k * 10 verification", expectedColAValue,
                    shiftedTable.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("B=i * 2 verification", expectedColBValue,
                    shiftedTable.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("Z=A_[i-1] verification", arrayAccessValue,
                    shiftedTable.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=A_[i + 1] * B_[ii -2] verification", expectedColDValue,
                    shiftedTable.getColumnSource(columns[3]).get(key));
            Assert.assertEquals("C=true verification", true, shiftedTable.getColumnSource(columns[4]).get(key));
        }
    }

    @Test
    public void updateTableStableColValuesTest() {
        stableColValuesTest(Flavor.Update, 7, 20);
    }

    @Test
    public void selectTableStableColValuesTest() {
        stableColValuesTest(Flavor.Select, 7, 20);
    }

    @Test
    public void viewTableStableColValuesTest() {
        stableColValuesTest(Flavor.View, 7, 20);
    }

    @Test
    public void updateViewTableStableColValuesTest() {
        stableColValuesTest(Flavor.UpdateView, 7, 20);
    }

    @SuppressWarnings("SameParameterValue")
    private void stableColValuesTest(Flavor flavor, final int tableSize, final int displayTableSize) {
        String[] formulas = new String[] {
                "A=k * 10",
                "B=i * 2",
                "Z=A_[i-1]",
                "D=i",
                "C=true"
        };

        Table source = emptyTable(tableSize);
        Table shiftedTable = emptyTable(tableSize);

        final ShiftedColumnDefinition d0 = new ShiftedColumnDefinition("A", -1);

        switch (flavor) {
            case Update: {
                source = source.update(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.update("A=k * 10", "B=i * 2"), d0)
                        .renameColumns("Z = __A_Shifted_Minus_1__")
                        .update("D=i", "C=true");
                break;
            }
            case UpdateView: {
                source = source.updateView(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.updateView("A=k * 10", "B=i * 2"), d0)
                        .renameColumns("Z = __A_Shifted_Minus_1__")
                        .updateView("D=i", "C=true");
                break;
            }
            case Select: {
                source = source.select(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.select("A=k * 10", "B=i * 2"), d0)
                        .renameColumns("Z = __A_Shifted_Minus_1__")
                        .view("A", "B", "Z", "D=i", "C=true");
                break;
            }
            case View: {
                source = source.view(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.view("A=k * 10", "B=i * 2"), d0)
                        .renameColumns("Z = __A_Shifted_Minus_1__")
                        .view("A", "B", "Z", "D=i", "C=true");
                break;
            }
        }

        showTableWithRowSet(source, Math.min(tableSize, displayTableSize));
        showTableWithRowSet(shiftedTable, Math.min(tableSize, displayTableSize));

        String[] columns = source.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, formulas.length, columns.length);

        String[] shiftedColumns = shiftedTable.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, formulas.length, shiftedColumns.length);

        for (int i = 0; i < formulas.length; i++) {
            ColumnSource<?> cs = source.getColumnSource(columns[i]);
            ColumnSource<?> scs = shiftedTable.getColumnSource(shiftedColumns[i]);
            Assert.assertEquals("ColumnSources' names are equal", columns[i], shiftedColumns[i]);

            if (columns[i].equals("Z")) {
                Assert.assertTrue("ColumnSource is a ShiftedColumnSource ", cs instanceof ShiftedColumnSource);
                Assert.assertTrue("ColumnSource is a ShiftedColumnSource ", scs instanceof ShiftedColumnSource);
            } else {
                Assert.assertFalse("ColumnSource is NOT a ShiftedColumnSource ", cs instanceof ShiftedColumnSource);
                Assert.assertFalse("ColumnSource is NOT a ShiftedColumnSource ", scs instanceof ShiftedColumnSource);
            }
        }

        AtomicLong atomicLong = new AtomicLong(0);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        final RowSet index = source.getRowSet();
        final int aMultiplier = 10;
        final int bMultiplier = 2;
        Long prevAValue = null;
        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            final int currIntValue = atomicInteger.getAndIncrement();
            final long currLongValue = atomicLong.getAndIncrement();
            final long expectedColAValue = currLongValue * aMultiplier;
            final int expectedColBValue = currIntValue * bMultiplier;
            final Long arrayAccessValue = prevAValue;
            prevAValue = expectedColAValue;

            Assert.assertEquals("A=k * 10 verification", expectedColAValue,
                    source.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("B=i * 2 verification", expectedColBValue, source.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("Z=A_[i-1] verification", arrayAccessValue,
                    source.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=i verification", currIntValue, source.getColumnSource(columns[3]).get(key));
            Assert.assertEquals("C=true verification", true, source.getColumnSource(columns[4]).get(key));

            Assert.assertEquals("A=k * 10 verification", expectedColAValue,
                    shiftedTable.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("B=i * 2 verification", expectedColBValue,
                    shiftedTable.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("Z=A_[i-1] verification", arrayAccessValue,
                    shiftedTable.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=i verification", currIntValue, shiftedTable.getColumnSource(columns[3]).get(key));
            Assert.assertEquals("C=true verification", true, shiftedTable.getColumnSource(columns[4]).get(key));
        }
    }

    @Test
    public void updateTableTest() {
        validateColSourceAndEntriesTest(Flavor.Update, 7, 20);
    }

    @Test
    public void selectTableTest() {
        validateColSourceAndEntriesTest(Flavor.Select, 7, 20);
    }

    @Test
    public void viewTableTest() {
        validateColSourceAndEntriesTest(Flavor.View, 7, 20);
    }

    @Test
    public void updateViewTableTest() {
        validateColSourceAndEntriesTest(Flavor.UpdateView, 7, 20);
    }

    @SuppressWarnings("SameParameterValue")
    private void validateColSourceAndEntriesTest(Flavor flavor, final int tableSize, final int displayTableSize) {

        final AtomicInteger atomicValue = new AtomicInteger(1);
        QueryScope.addParam("atomicValue", atomicValue);
        String[] formulas = new String[] {
                "X=10", // constant integer value of 10
                "Y=atomicValue.getAndIncrement()", // Predictable QueryScope Usage in Formula
                "Z=Y_[i - 1] * X", // Formula using two prev columns as dependent variable
                "D=(Z_[i+1]) * (Y_[i-1])", // Formula using two prev columns as dependent variable
                "E=(Z_[i+1]) = (Y_[i-1])", // Test single equals operator as equality
                "C=true" // constant bool value
        };

        Table source = emptyTable(tableSize);

        switch (flavor) {
            case Update: {
                source = source.update(formulas);
                break;
            }
            case UpdateView: {
                source = source.updateView(formulas);
                break;
            }
            case Select: {
                source = source.select(formulas);
                break;
            }
            case View: {
                source = source.view(formulas);
                break;
            }
        }

        showTableWithRowSet(source, Math.min(tableSize, displayTableSize));

        String[] columns = source.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, formulas.length, columns.length);

        for (int i = 0; i < formulas.length; i++) {
            ColumnSource<?> cs = source.getColumnSource(columns[i]);

            if ((columns[i].equals("Z") || columns[i].equals("D") || columns[i].equals("E")
                    || columns[i].equals("Y")) && (flavor == Flavor.UpdateView || flavor == Flavor.View)) {
                Assert.assertTrue(columns[i] + " ColumnSource is a ViewColumnSource ", cs instanceof ViewColumnSource);
            } else if (columns[i].equals("X") || columns[i].equals("C")) {
                Assert.assertTrue(columns[i] + " ColumnSource is a SingleValueColumnSource ",
                        cs instanceof SingleValueColumnSource);
            } else {
                Assert.assertFalse(columns[i] + " ColumnSource is NOT a ViewColumnSource ",
                        cs instanceof ViewColumnSource);
            }
        }

        if (flavor == Flavor.UpdateView || flavor == Flavor.View) {
            return;
        }

        AtomicInteger atomicInteger = new AtomicInteger(1);
        final RowSet index = source.getRowSet();
        final int constantXValue = 10;
        Integer yPrevValue = null;
        Integer zForwardValue = 10;
        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            final int expectedAtomicIntColValue = atomicInteger.getAndIncrement();
            final Integer expectedCalculatedColValue =
                    expectedAtomicIntColValue == 1 ? null : constantXValue * (expectedAtomicIntColValue - 1);
            final Integer dCurrValue = yPrevValue == null || zForwardValue == null ? null : zForwardValue * yPrevValue;
            final Boolean eCurrValue = zForwardValue != null && zForwardValue.equals(yPrevValue);
            yPrevValue = expectedAtomicIntColValue;
            zForwardValue = expectedAtomicIntColValue == (tableSize - 1) ? null
                    : constantXValue * (expectedAtomicIntColValue + 1);
            Assert.assertEquals("X=10 verification", constantXValue, source.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("Y=atomicValue.getAndIncrement() verification", expectedAtomicIntColValue,
                    source.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("Z=Y_[i - 1] * X verification", expectedCalculatedColValue,
                    source.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=(Z_[i+1]) * (Y_[i-1]) verification", dCurrValue,
                    source.getColumnSource(columns[3]).get(key));
            Assert.assertEquals("E=(Z_[i+1]) == (Y_[i-1]) verification", eCurrValue,
                    source.getColumnSource(columns[4]).get(key));
            Assert.assertEquals("C=true verification", true, source.getColumnSource(columns[5]).get(key));
        }

    }

    @Test
    public void parseForConstantArrayAccessTest() {

        String shiftedPrefix = ShiftedColumnsFactory.SHIFTED_COL_PREFIX;
        // singleResultColumnTest
        String[] expressions = new String[] {"Y_[i-1]", "Y_[ii-1]", "Y_[i - 1]", "Y_[ii - 1]", "Y_[ i - 1 ]",
                "Y_[ ii - 1 ]"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"(Y_[i-1]) * B", "(Y_[ii-1]) * B", "(Y_[i - 1]) * B", "(Y_[ii - 1]) * B",
                "(Y_[ i - 1]) * B", "(Y_[ ii - 1 ]) * B"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i+1]", "Y_[ii+1]", "Y_[i + 1]", "Y_[ii + 1]", "Y_[ i + 1 ]", "Y_[ ii + 1 ]"};
        singleResultColumnTest(expressions, 1L, "Y");

        expressions = new String[] {"(Y_[i+1]) * B", "(Y_[ii+1]) * B", "(Y_[i + 1]) * B", "(Y_[ii + 1]) * B",
                "(Y_[ i + 1]) * B", "(Y_[ ii + 1 ]) * B"};
        singleResultColumnTest(expressions, 1L, "Y");

        expressions = new String[] {"Y_[i-2]", "Y_[ii-2]", "Y_[i - 2]", "Y_[ii - 2]", "Y_[ i - 2 ]", "Y_[ ii - 2 ]"};
        singleResultColumnTest(expressions, -2L, "Y");

        expressions = new String[] {"(Y_[i-2]) * B", "(Y_[ii-2]) * B", "(Y_[i - 2]) * B", "(Y_[ii - 2]) * B",
                "(Y_[ i - 2]) * B", "(Y_[ ii - 2 ]) * B"};
        singleResultColumnTest(expressions, -2L, "Y");

        expressions = new String[] {"Y_[i+2]", "Y_[ii+2]", "Y_[i + 2]", "Y_[ii + 2]", "Y_[ i + 2 ]", "Y_[ ii + 2 ]"};
        singleResultColumnTest(expressions, 2L, "Y");

        expressions = new String[] {"(Y_[i+2]) * B", "(Y_[ii+2]) * B", "(Y_[i + 2]) * B", "(Y_[ii + 2]) * B",
                "(Y_[ i + 2]) * B", "(Y_[ ii + 2 ]) * B"};
        singleResultColumnTest(expressions, 2L, "Y");

        // singleShiftMultiColumnTest
        String[] sourceColumns = new String[] {"Y", "B"};
        expressions = new String[] {"(Y_[i-1]) * (B_[i-1])", "(Y_[ii-1]) * (B_[ii-1])", "(Y_[i - 1]) * (B_[i - 1])",
                "(Y_[ii - 1]) * (B_[ii - 1])", "(Y_[ i - 1]) * (B_[ i - 1 ])", "(Y_[ ii - 1 ]) * (B_[ ii - 1 ])"};
        singleShiftMultiColumnTest(expressions, -1L, sourceColumns);

        expressions = new String[] {"Y_[i-1] * B_[i-1]", "Y_[ii-1] * B_[ii-1]", "Y_[i - 1] * B_[i - 1]",
                "Y_[ii - 1] * B_[ii - 1]", "Y_[ i - 1] * B_[ i - 1 ]", "Y_[ ii - 1 ] * B_[ ii - 1 ]"};
        singleShiftMultiColumnTest(expressions, -1L, sourceColumns);

        expressions = new String[] {"(Y_[i+1]) * (B_[i+1])", "(Y_[ii+1]) * (B_[ii+1])", "(Y_[i + 1]) * (B_[i + 1])",
                "(Y_[ii + 1]) * (B_[ii + 1])", "(Y_[ i + 1]) * (B_[ i + 1 ])", "(Y_[ ii + 1 ]) * (B_[ ii + 1 ])"};
        singleShiftMultiColumnTest(expressions, 1L, sourceColumns);

        expressions = new String[] {"Y_[i+1] * B_[i+1]", "Y_[ii+1] * B_[ii+1]", "Y_[i + 1] * B_[i + 1]",
                "Y_[ii + 1] * B_[ii + 1]", "Y_[ i + 1] * B_[ i + 1 ]", "Y_[ ii + 1 ] * B_[ ii + 1 ]"};
        singleShiftMultiColumnTest(expressions, 1L, sourceColumns);

        expressions = new String[] {"(Y_[i-2]) * (B_[i-2])", "(Y_[ii-2]) * (B_[ii-2])", "(Y_[i - 2]) * (B_[i - 2])",
                "(Y_[ii - 2]) * (B_[ii - 2])", "(Y_[ i - 2]) * (B_[ i - 2 ])", "(Y_[ ii - 2 ]) * (B_[ ii - 2 ])"};
        singleShiftMultiColumnTest(expressions, -2L, sourceColumns);

        expressions = new String[] {"Y_[i-2] * B_[i-2]", "Y_[ii-2] * B_[ii-2]", "Y_[i - 2] * B_[i - 2]",
                "Y_[ii - 2] * B_[ii - 2]", "Y_[ i - 2] * B_[ i - 2 ]", "Y_[ ii - 2 ] * B_[ ii - 2 ]"};
        singleShiftMultiColumnTest(expressions, -2L, sourceColumns);

        expressions = new String[] {"(Y_[i+2]) * (B_[i+2])", "(Y_[ii+2]) * (B_[ii+2])", "(Y_[i + 2]) * (B_[i + 2])",
                "(Y_[ii + 2]) * (B_[ii + 2])", "(Y_[ i + 2]) * (B_[ i + 2 ])", "(Y_[ ii + 2 ]) * (B_[ ii + 2 ])"};
        singleShiftMultiColumnTest(expressions, 2L, sourceColumns);

        expressions = new String[] {"Y_[i+2] * B_[i+2]", "Y_[ii+2] * B_[ii+2]", "Y_[i + 2] * B_[i + 2]",
                "Y_[ii + 2] * B_[ii + 2]", "Y_[ i + 2] * B_[ i + 2 ]", "Y_[ ii + 2 ] * B_[ ii + 2 ]"};
        singleShiftMultiColumnTest(expressions, 2L, sourceColumns);

        // multiShiftSingleColumnTest
        long[] shift = new long[] {-1L, 1L};
        expressions = new String[] {"(Y_[i-1]) * (Y_[i+1])", "(Y_[ii-1]) * (Y_[ii+1])", "(Y_[i - 1]) * (Y_[i + 1])",
                "(Y_[ii - 1]) * (Y_[ii + 1])", "(Y_[ i - 1]) * (Y_[ i + 1 ])", "(Y_[ ii - 1 ]) * (Y_[ ii + 1 ])"};
        multiShiftSingleColumnTest(expressions, shift, "Y");

        shift = new long[] {2L, -2L};
        expressions = new String[] {"(Y_[i+2]) * (Y_[i-2])", "(Y_[ii+2]) * (Y_[ii-2])", "(Y_[i + 2]) * (Y_[i - 2])",
                "(Y_[ii + 2]) * (Y_[ii - 2])", "(Y_[ i + 2]) * (Y_[ i - 2 ])", "(Y_[ ii + 2 ]) * (Y_[ ii - 2 ])"};
        multiShiftSingleColumnTest(expressions, shift, "Y");

        shift = new long[] {1L, -2L};
        expressions = new String[] {"Y_[i+1] * Y_[i-2]", "Y_[ii+1] * Y_[ii-2]", "Y_[i + 1] * Y_[i - 2]",
                "Y_[ii + 1] * Y_[ii - 2]", "Y_[ i + 1] * Y_[ i - 2 ]", "Y_[ ii + 1 ] * Y_[ ii - 2 ]"};
        multiShiftSingleColumnTest(expressions, shift, "Y");

        // multiShiftMultiColumnTest
        shift = new long[] {-1L, 1L, -2L, 2L};
        Set<ShiftedColumnDefinition> expectedColPairs = Sets.newHashSet(
                new ShiftedColumnDefinition("Y", 1),
                new ShiftedColumnDefinition("Y", -1),
                new ShiftedColumnDefinition("B", 2),
                new ShiftedColumnDefinition("B", 2));
        expressions = new String[] {"(Y_[i+1] * B_[ i - 2] ) + (Y_[ii - 1] * B_[ii + 2]) + (Y_[i + 1] * B_[i - 2])"};
        multiShiftMultiColumnTest(expressions, shift, new String[] {"Y", "Y", "B", "B"});

        // singleResultColumnTest - different expressions
        expressions = new String[] {"Y_[i-1] * true"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] * 0.5f"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + \"test\""};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + 10"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + 10L"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + 'C'"};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + " + null};
        singleResultColumnTest(expressions, -1L, "Y");

        expressions = new String[] {"Y_[i-1] + " + Integer.MIN_VALUE};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests UnaryExpr
        expressions = new String[] {"Y_[ii-1] + " + Long.MIN_VALUE};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests LongLiteralMinValueExpr
        expressions = new String[] {"Y_[ii-1] + " + Long.MIN_VALUE + "L"};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests MethodReferenceExpr
        expressions = new String[] {"Y_[ii-1] + random.nextInt(1000000)"};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests FieldAccessExpression
        expressions = new String[] {"Y_[ii-1] + expectedColPairs.length"};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests CastExpr
        expressions = new String[] {"Y_[ii-1] + ((long)random.nextInt(1000000))"};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests ArrayCreationExpr
        expressions = new String[] {"Y_[ii-1] + new long[] { 1L, 2L}"};
        singleResultColumnTest(expressions, -1L, "Y");

        // tests ObjectCreationExpr
        expressions = new String[] {"Y_[ii-1] + new Boolean(true)"};
        // ObjectCreationExpression toString has two spaces between new and expression
        singleResultColumnTest(expressions, -1L, "Y");
    }

    @SuppressWarnings("SameParameterValue")
    private void singleResultColumnTest(String[] singleResultColumn, long shift, String sourceCol) {
        try {
            for (String expression : singleResultColumn) {
                final Expression expr = JavaExpressionParser.parseExpression(expression);
                final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                        ShiftedColumnsFactory.getShiftedColumnDefinitions(expr);
                Assert.assertNotNull(shifted);

                final Set<ShiftedColumnDefinition> definitions = shifted.getSecond();
                Assert.assertEquals(1, definitions.size());
                final ShiftedColumnDefinition definition = definitions.iterator().next();
                Assert.assertNotNull(definition);
                Assert.assertEquals(shift, definition.getShiftAmount());
                Assert.assertEquals(sourceCol, definition.getColumnName());
            }

        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail(exception.getMessage());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void singleShiftMultiColumnTest(String[] expressions, long shift, String[] sourceCol) {
        try {
            final Set<String> sourceSet = Arrays.stream(sourceCol).collect(Collectors.toSet());
            for (String expression : expressions) {
                final Expression expr = JavaExpressionParser.parseExpression(expression);
                final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                        ShiftedColumnsFactory.getShiftedColumnDefinitions(expr);
                Assert.assertNotNull(shifted);
                Assert.assertEquals(sourceCol.length, shifted.getSecond().size());
                Assert.assertTrue(
                        shifted.getSecond().stream().allMatch(col -> sourceSet.contains(col.getColumnName())));
                Assert.assertTrue(shifted.getSecond().stream().allMatch(col -> col.getShiftAmount() == shift));
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail(exception.getMessage());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void multiShiftSingleColumnTest(String[] expressions, long[] shift, String sourceCol) {
        try {
            final Set<Long> shiftSet = Arrays.stream(shift).boxed().collect(Collectors.toSet());
            for (String expression : expressions) {
                final Expression expr = JavaExpressionParser.parseExpression(expression);
                final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                        ShiftedColumnsFactory.getShiftedColumnDefinitions(expr);
                Assert.assertNotNull(shifted);
                Assert.assertEquals(shift.length, shifted.getSecond().size());
                Assert.assertTrue(
                        shifted.getSecond().stream().allMatch(col -> shiftSet.contains(col.getShiftAmount())));
                Assert.assertTrue(shifted.getSecond().stream().allMatch(col -> col.getColumnName().equals(sourceCol)));
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail(exception.getMessage());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void multiShiftMultiColumnTest(String[] expressions, long[] shift, String[] sourceCol) {
        try {
            Assert.assertEquals(shift.length, sourceCol.length);
            final ShiftedColumnDefinition[] expectedDefinitions = new ShiftedColumnDefinition[shift.length];
            for (int i = 0; i < shift.length; ++i) {
                expectedDefinitions[i] = new ShiftedColumnDefinition(sourceCol[i], shift[i]);
            }
            for (String expression : expressions) {
                final Expression expr = JavaExpressionParser.parseExpression(expression);
                final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                        ShiftedColumnsFactory.getShiftedColumnDefinitions(expr);
                assertShiftedColumnDefinitionsMatch(shifted.getSecond(), expectedDefinitions);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail(exception.getMessage());
        }
    }

    @Test
    public void arrayInitializerTest() {
        String[] expressions = new String[] {
                "Y_[i-1]", "Y_[i-2]", "Y_[i+1]", "Y_[i+2]",
                "random.nextInt(1000000)", "((4 + 3) - (7 -95) + (3 * (5 + (g - 8))))",
                "((4 + 3) - (7 -95) + (3 * (5 + (8 - g))))"
        };

        ShiftedColumnDefinition[] expectedDefinitions = new ShiftedColumnDefinition[] {
                new ShiftedColumnDefinition("Y", -1),
                new ShiftedColumnDefinition("Y", -2),
                new ShiftedColumnDefinition("Y", 1),
                new ShiftedColumnDefinition("Y", 2),
        };

        evaluateArrayInitializer(expressions, expectedDefinitions);

        evaluateArrayInitializer(new String[0], new ShiftedColumnDefinition[0]);
    }

    private void evaluateArrayInitializer(
            final String[] expressions,
            final ShiftedColumnDefinition[] expectedDefinitions) {
        NodeList<Expression> expressionList = new NodeList<>();
        for (String expression : expressions) {
            Expression expr = JavaExpressionParser.parseExpression(expression);
            expressionList.add(expr);
        }
        ArrayInitializerExpr arrayInitializerExpr = new ArrayInitializerExpr(expressionList);

        final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                ShiftedColumnsFactory.getShiftedColumnDefinitions(arrayInitializerExpr);
        Assert.assertEquals(shifted == null ? 0 : shifted.getSecond().size(), expectedDefinitions.length);
        if (expectedDefinitions.length > 0) {
            assertShiftedColumnDefinitionsMatch(shifted.getSecond(), expectedDefinitions);
        } else {
            Assert.assertNull(shifted);
        }
    }

    private void assertShiftedColumnDefinitionsMatch(
            final Set<ShiftedColumnDefinition> shifted,
            final ShiftedColumnDefinition[] expectedDefinitions) {
        Assert.assertNotNull(shifted);
        Assert.assertEquals("compare number of shifts", expectedDefinitions.length, shifted.size());

        for (int i = 0; i < expectedDefinitions.length; i++) {
            final boolean[] used = new boolean[expectedDefinitions.length];
            Assert.assertTrue(shifted.stream().allMatch(col -> {
                for (int ii = 0; ii < expectedDefinitions.length; ++ii) {
                    if (used[ii] || !col.equals(expectedDefinitions[ii])) {
                        continue;
                    }
                    used[ii] = true;
                    return true;
                }
                return false;
            }));
        }
    }

    @Test
    public void conditionExprTest() {
        // noinspection unchecked
        Triple<String, String, String>[] conditionalTriples = new Triple[] {
                new ImmutableTriple<>("A == D", "B_[i-1]", "C_[i-2]"),
                new ImmutableTriple<>("A == D", "B", "C_[i-2]"),
                new ImmutableTriple<>("A == D", "B * Y", "C_[2-i]")
        };

        evaluateConditionExpressions(conditionalTriples[0], Set.of(
                new ShiftedColumnDefinition("B", -1),
                new ShiftedColumnDefinition("C", -2)));

        evaluateConditionExpressions(conditionalTriples[1], Set.of(
                new ShiftedColumnDefinition("C", -2)));

        evaluateConditionExpressions(conditionalTriples[2], Set.of());
    }

    private void evaluateConditionExpressions(
            Triple<String, String, String> conditionalTriple,
            Set<ShiftedColumnDefinition> expectedShifts) {
        Expression condition = JavaExpressionParser.parseExpression(conditionalTriple.getLeft());
        Expression then = JavaExpressionParser.parseExpression(conditionalTriple.getMiddle());
        Expression elseExpr = JavaExpressionParser.parseExpression(conditionalTriple.getRight());
        ConditionalExpr conditionalExpr = new ConditionalExpr(condition, then, elseExpr);
        final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                ShiftedColumnsFactory.getShiftedColumnDefinitions(conditionalExpr);
        if (!expectedShifts.isEmpty()) {
            Assert.assertNotNull(shifted);
            Assert.assertEquals("compare map sizes", expectedShifts.size(), shifted.getSecond().size());
            assertShiftedColumnDefinitionsMatch(
                    shifted.getSecond(), expectedShifts.toArray(new ShiftedColumnDefinition[0]));
        } else {
            Assert.assertNull(shifted);
        }
    }

    @Test
    public void validConstantArrayAccessTest() {
        String[] expressions = new String[] {
                "Z=Y_[i-1]", "Z=Y_[i-2]", "Z=Y_[i+1]", "Z=Y_[i+2]",
                "Z=(Y_[i-1]) * B", "Z=(Y_[i-2]) * B", "Z=(Y_[i+1]) * B", "Z=(Y_[i+2]) * B",
                "Z=(Y_[i-1]) * (B_[i-1])", "Z=(Y_[i-2]) * (B_[i-2])", "Z=(Y_[i+1]) * (B_[i+1])",
                "Z=(Y_[i+2]) * (B_[i+2])",
                "Z=(Y_[i-1]) * (B_[i * 1])", "Z=(Y_[i-2]) * (B_[i / 2])", "Z=(Y_[i+1]) * (B_[1 - i])",
                "Z=(Y_[i+2]) * (B_[i + x])",

                "Z=Y_[ii-1]", "Z=Y_[ii-2]", "Z=Y_[ii+1]", "Z=Y_[ii+2]",
                "Z=(Y_[ii-1]) * B", "Z=(Y_[ii-2]) * B", "Z=(Y_[ii+1]) * B", "Z=(Y_[ii+2]) * B",
                "Z=(Y_[ii-1]) * (B_[ii-1])", "Z=(Y_[ii-2]) * (B_[ii-2])", "Z=(Y_[ii+1]) * (B_[ii+1])",
                "Z=(Y_[ii+2]) * (B_[ii+2])",
                "Z=(Y_[ii-1]) * (B_[ii * 1])", "Z=(Y_[ii-2]) * (B_[ii / 2])", "Z=(Y_[ii+1]) * (B_[1 - ii])",
                "Z=(Y_[ii+2]) * (B_[ii + x])",
                "X.getClass() == Long.class ? Y_[ii-1] : B_[i - 1]"
        };

        constantArrayAccessTest(expressions, true);
    }

    @Test
    public void inValidConstantArrayAccessTest() {
        String[] expressions = new String[] {
                "W=k", "M=ii", "N=i", "P=4 + a", "Y=random.nextInt(1000000)",
                "e=((4 + 3) - (7 -95) + (3 * (5 + (g - 8))))", "f=((4 + 3) - (7 -95) + (3 * (5 + (8 - g))))",
                "Z=Y_[i-x]", "Z=Y_[i+x]", "Z=Y_[i * 1]", "Z=Y_[i / 2]",
                "Z=Y_[2+2]", "Z=Y_[2+i]", "Z=Y_[2+ii]", "Z=Y_[2-i]", "Z=Y_[2-ii]",
                "X.getClass() == String.class"
        };

        constantArrayAccessTest(expressions, false);
    }

    @Test
    public void allAssignExprTest() {
        String[] assignExprValues = new String[] {
                "Y_[i-x]", "Y_[i+x]", "Y_[i * 1]", "Y_[i / 2]",
                "Y_[2+2]", "Y_[2+i]", "Y_[2+ii]", "Y_[2-i]", "Y_[2-ii]"
        };
        Random random = new Random(0);
        List<String> expressions = new LinkedList<>();
        for (AssignExpr.Operator op : AssignExpr.Operator.values()) {
            String expression = "Z " + op.asString() + " "
                    + assignExprValues[random.nextInt(assignExprValues.length - 1)];
            expressions.add(expression);
        }
        constantArrayAccessTest(expressions.toArray(new String[0]), false);
    }

    private void constantArrayAccessTest(String[] expressions, boolean assertTrue) {
        for (String expression : expressions) {
            final Expression expr = JavaExpressionParser.parseExpression(expression);
            final Pair<String, Set<ShiftedColumnDefinition>> shifted =
                    ShiftedColumnsFactory.getShiftedColumnDefinitions(expr);
            final boolean hasConstantArrayAccess = shifted != null && !shifted.getSecond().isEmpty();

            if (assertTrue) {
                Assert.assertTrue("\"" + expression + "\" has Constant ArrayAccess Expression",
                        hasConstantArrayAccess);
            } else {
                Assert.assertFalse("\"" + expression + "\" is not a Constant ArrayAccess Expression",
                        hasConstantArrayAccess);
            }
        }
    }

    private void showTableWithRowSet(Table table, int tableSize) {
        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(table, tableSize);
        }
    }

    // tests for Where Clause

    @Test
    public void dh12273_convertToShiftedFormula() {

        String[][] formulas = new String[][] {
                {"Y=Y_[i-1] && A=A_[i-1]",
                        "Y == " + shiftColName("Y", -1) + " && A == "
                                + shiftColName("A", -1)},
                {"Y_[i-1]==A_[i-1]",
                        shiftColName("Y", -1) + " == "
                                + shiftColName("A", -1)},
                {"Y_[1-i]==A_[i-1]", "Y_[1 - i] == " + shiftColName("A", -1)},
                {"Y=Y_[i-1] && A=A_[ii-1]",
                        "Y == " + shiftColName("Y", -1) + " && A == "
                                + shiftColName("A", -1)},
                {"(Y==Y_[i-1]) && (A==A_[ii-1])",
                        "(Y == " + shiftColName("Y", -1) + ") && (A == "
                                + shiftColName("A", -1) + ")"},
                {"(Y==Y_[i-k]) && (A==A_[2-ii])", "(Y == Y_[i - k]) && (A == A_[2 - ii])"},
                {"(1 <= Y_[i - 1] && Y_[i - 1] > 10)",
                        "(1 <= " + shiftColName("Y", -1) + " && "
                                + shiftColName("Y", -1) + " > 10)"}
        };


        for (String[] formulaPair : formulas) {
            try {
                final TimeLiteralReplacedExpression timeConversionResult =
                        TimeLiteralReplacedExpression.convertExpression(formulaPair[0]);
                final String convertedFilterFormula = timeConversionResult.getConvertedFormula();
                final String shiftedFilterFormula = ShiftedColumnsFactory.convertToShiftedFormula(formulaPair[0]);

                Assert.assertEquals("Verifying ShiftConverted Formulas", formulaPair[1], shiftedFilterFormula);
                Assert.assertNotEquals("Verifying time conversion formula not equal to ShiftConvertedFormula ",
                        convertedFilterFormula, shiftedFilterFormula);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void dh12273_verifyFilterTypesTestCase() {
        String[] formulas = new String[] {"A=atomicInteger.getAndIncrement()", "B=2", "C=20", "D=true"};

        String[] filters = new String[] {"(1 <= A_[i - 1] && A_[i - 1] <= 10)"};
        verifyFilterTypes(formulas, filters, 20, 10);

        filters = new String[] {"1 <= A_[i - 1] ", "A_[i - 1] <= 10"};
        verifyFilterTypes(formulas, filters, 20, 10);

        // TODO add different types of filters currently this is only conditional filter
    }

    @SuppressWarnings("SameParameterValue")
    private void verifyFilterTypes(String[] formulas, String[] filters, int sourceTableSize, int expectedRowCount) {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        QueryScope.addParam("atomicInteger", atomicInteger);

        final Table et = emptyTable(sourceTableSize).update(formulas);
        final Table arrayAccess = et.where(filters);

        Assert.assertEquals("verify expected filter table size", expectedRowCount, arrayAccess.size());
    }

    @Test
    public void dh12273SimpleConstantWhereTestCase() {
        String[] filters = new String[] {"Y=Y_[i-1] && A=A_[i-1]", "Y_[i-1]==A_[i-1]"};
        verifySimpleConstantTableWhere(filters, 7);

        filters = new String[] {"Y=Y_[i-1]", "A=A_[i-1]"};
        verifySimpleConstantTableWhere(filters, 7);

        filters = new String[] {"Y=Y_[i-1]", "A=A_[i-1]", "Y_[i-1]==A_[i-1]"};
        verifySimpleConstantTableWhere(filters, 7);

        filters = new String[] {"Y=Y_[i-1]", "A=A_[i-1]", "D==(Y_[i-1]==A_[i-1])"};
        verifySimpleConstantTableWhere(filters, 7);

        filters = new String[] {"Y==Y_[i-1] && A==A_[i-1]", "D==(Y_[i-1]==A_[i-1])"};
        verifySimpleConstantTableWhere(filters, 7);
    }

    @SuppressWarnings("SameParameterValue")
    private void verifySimpleConstantTableWhere(String[] filterExpressions, int tableSize) {
        String[] colFormulas = new String[] {"Y=10", "A=10", "B=20", "D=true"};
        final Table source = emptyTable(tableSize).update(colFormulas);
        final Table arrayAccess = source.where(filterExpressions);

        final String[] columns = arrayAccess.getDefinition().getColumnNamesArray();
        Assert.assertEquals("source table and arrayAccess table columns are equal / same", colFormulas.length,
                columns.length);

        final RowSet index = arrayAccess.getRowSet();
        final int constantYColValue = 10;
        final int constantAColValue = 10;
        final int constantBColValue = 20;
        final boolean constantDColValue = true;

        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            Assert.assertEquals("Y=10 verification", constantYColValue,
                    arrayAccess.getColumnSource(columns[0]).get(key));
            Assert.assertEquals("A=10 verification", constantAColValue,
                    arrayAccess.getColumnSource(columns[1]).get(key));
            Assert.assertEquals("B=20 verification", constantBColValue,
                    arrayAccess.getColumnSource(columns[2]).get(key));
            Assert.assertEquals("D=true verification", constantDColValue,
                    arrayAccess.getColumnSource(columns[3]).get(key));
        }
    }

    @Test
    public void dh12273_whereForNegativeShiftTestCase() {
        String[] formulas = new String[] {"Y=i", "YMod2=Y%2", "A=i", "AMod4=A%4"};

        String[] filters = new String[] {"YMod2=YMod2_[i-2] && AMod4=AMod4_[i-4]"};
        verifyWhereForNegativeShifts(filters, formulas, 8, -2, -4, 0, 0);

        filters = new String[] {"YMod2=YMod2_[i-2]", "AMod4=AMod4_[i-4]"};
        verifyWhereForNegativeShifts(filters, formulas, 8, -2, -4, 0, 0);

        formulas = new String[] {"Y=i", "YMod2=Y%2", "A=i", "AMod3=A%3"};
        filters = new String[] {"YMod2=YMod2_[i-2] && AMod3=AMod3_[i-3]"};
        verifyWhereForNegativeShifts(filters, formulas, 9, -2, -3, 1, 0);

        filters = new String[] {"YMod2=YMod2_[i-2]", "AMod3=AMod3_[i-3]"};
        verifyWhereForNegativeShifts(filters, formulas, 9, -2, -3, 1, 0);

        formulas = new String[] {"Y=i", "YMod1=Y%1", "A=i", "AMod3=A%3"};
        filters = new String[] {"YMod1=YMod1_[i-1] && AMod3=AMod3_[i-3]"};
        verifyWhereForNegativeShifts(filters, formulas, 9, -1, -3, 0, 0);

        filters = new String[] {"YMod1=YMod1_[i-1]", "AMod3=AMod3_[i-3]"};
        verifyWhereForNegativeShifts(filters, formulas, 9, -1, -3, 0, 0);
    }

    @SuppressWarnings("SameParameterValue")
    private void verifyWhereForNegativeShifts(String[] filterExpressions, String[] formulas, int tableSize, long yShift,
            long aShift, int expectedYShiftStartValue, int expectedAShiftStartValue) {
        final Table source = emptyTable(tableSize).update(formulas);
        final Table arrayAccess = source.where(filterExpressions);
        long absMaxShift = Math.max(Math.abs(yShift), Math.abs(aShift));

        final String[] columns = arrayAccess.getDefinition().getColumnNamesArray();
        Assert.assertEquals("verify expected filtered table row count tableSize[" + tableSize + "], absMaxShift["
                + absMaxShift + "]", (tableSize - absMaxShift), arrayAccess.intSize());

        final RowSet index = arrayAccess.getRowSet();

        int yModColValue = expectedYShiftStartValue;
        int aModColValue = expectedAShiftStartValue;
        int minStartCounterValue = 0;
        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            Assert.assertEquals(columns[1] + " verification", yModColValue,
                    arrayAccess.getColumnSource(columns[1]).get(key));
            Assert.assertEquals(columns[3] + " verification", aModColValue,
                    arrayAccess.getColumnSource(columns[3]).get(key));

            minStartCounterValue++;
            yModColValue = (int) ((minStartCounterValue + expectedYShiftStartValue) % yShift);
            aModColValue = (int) ((minStartCounterValue + expectedAShiftStartValue) % aShift);
        }
    }

    @Test
    public void dh12273_whereWithPositiveShiftsTestCase() {

        String[] formulas = new String[] {"Y=i", "A=ii", "YMod=Y_[i+1]", "AMod=A_[i + 2]"};
        String[] filters = new String[] {"Y_[i+1] % 2 == 0", "(A_[i + 2] + 1) % 2 == 0"};
        verifyWhereForPositiveShifts(filters, formulas, 10, 1, 2, 2, 3);

        formulas = new String[] {"Y=i", "A=ii", "YMod=Y_[i+2]", "AMod=A_[i + 3]"};
        filters = new String[] {"Y_[i+2] % 2 == 0", "(A_[i + 3] + 1) % 2 == 0"};
        verifyWhereForPositiveShifts(filters, formulas, 10, 2, 3, 2, 3);

        formulas = new String[] {"Y=i", "A=ii", "YMod=Y_[i+3]", "AMod=A_[i + 4]"};
        filters = new String[] {"Y_[i+3] % 2 == 0", "(A_[i + 4] + 1) % 2 == 0"};
        verifyWhereForPositiveShifts(filters, formulas, 10, 3, 4, 4, 5);

        formulas = new String[] {"Y=i", "A=ii", "YMod=Y_[i+2]", "AMod=A_[i + 3]"};
        filters = new String[] {"Y_[i+2] % 2 == 0", "(A_[i + 3] + 3) % 2 == 0"};
        verifyWhereForPositiveShifts(filters, formulas, 10, 2, 3, 2, 3);
    }

    @SuppressWarnings("SameParameterValue")
    private void verifyWhereForPositiveShifts(String[] filterExpressions, String[] formulas, int tableSize, long yShift,
            long aShift, int expectedYShiftStartValue, long expectedAShiftStartValue) {
        final Table source = emptyTable(tableSize).update(formulas);
        final Table arrayAccess = source.where(filterExpressions);
        long absMaxShift = Math.max(yShift, aShift);

        final String[] columns = arrayAccess.getDefinition().getColumnNamesArray();
        Assert.assertEquals("verify expected filtered table row count tableSize[" + tableSize + "], absMaxShift["
                + absMaxShift + "]", Math.round((double) (tableSize - absMaxShift) / 2), arrayAccess.intSize());

        final RowSet index = arrayAccess.getRowSet();

        int yModColValue = expectedYShiftStartValue;
        long aModColValue = expectedAShiftStartValue;
        for (final RowSet.Iterator indexIterator = index.iterator(); indexIterator.hasNext();) {
            final long key = indexIterator.nextLong();
            Assert.assertEquals(columns[2] + " verification", yModColValue,
                    arrayAccess.getColumnSource(columns[2]).get(key));
            Assert.assertEquals(columns[3] + " verification", aModColValue,
                    arrayAccess.getColumnSource(columns[3]).get(key));

            yModColValue = yModColValue + 2;
            aModColValue = aModColValue + 2;
        }
    }

    @Test
    public void dh12273_simpleIncrementalRefreshingTableWhereTest() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30),
                intCol("Value2", 202, 222, 242, 262, 282, 302),
                intCol("Value3", 2020, 2220, 2420, 2620, 2820, 3020),
                intCol("Value4", 2020, 2020, 2420, 2420, 2820, 2820));


        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.where("Value % 2 == 0", "Sentinel % 2 != 0")),
                EvalNugget.from(() -> queryTable.where("Value % 2 == 0", "Value4==Value4_[i-1]", "Sentinel % 2 != 0"))
        };

        TstUtils.validate("", en);

        /* ModifiedColumnSet.ALL */
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable,
                    i(10, 12, 18),
                    intCol("Sentinel", 56, 57, 510),
                    intCol("Value", 421, 422, 425),
                    intCol("Value2", 302, 322, 382),
                    intCol("Value3", 4020, 4220, 4820),
                    intCol("Value4", 4020, 4020, 4820));

            final ModifiedColumnSet queryTableColSetForUpdate = queryTable.getModifiedColumnSetForUpdates();
            queryTableColSetForUpdate.clear();
            queryTableColSetForUpdate.setAll("Sentinel", "Value", "Value2", "Value3", "Value4");
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(10, 12, 18), RowSetShiftData.EMPTY,
                    queryTableColSetForUpdate /* ModifiedColumnSet.ALL */);
            System.out.println("published update in Test: " + update);
            queryTable.notifyListeners(update);
        });

        TstUtils.validate("", en);
    }
}
