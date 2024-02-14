/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.ConditionalExpr;
import com.github.javaparser.ast.expr.Expression;
import io.deephaven.base.Pair;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.JavaExpressionParser;
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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.deephaven.engine.table.impl.MemoizedOperationKey.SelectUpdateViewOrUpdateView.*;
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

        Table source = emptyTable(tableSize);
        Table shiftedTable = emptyTable(tableSize);

        switch (flavor) {
            case Update: {
                source = source.update(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.update("A=k * 10", "B=i * 2"), -1, "Z=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, 1, "DS1=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, -2, "DS2=B")
                        .view("A", "B", "Z", "D=DS1 * DS2", "C=true");
                break;
            }
            case UpdateView: {
                source = source.updateView(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.updateView("A=k * 10", "B=i * 2"), -1, "Z=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, 1, "DS1=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, -2, "DS2=B")
                        .view("A", "B", "Z", "D=DS1 * DS2", "C=true");
                break;
            }
            case Select: {
                source = source.select(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.updateView("A=k * 10", "B=i * 2"), -1, "Z=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, 1, "DS1=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, -2, "DS2=B")
                        .view("A", "B", "Z", "D=DS1 * DS2", "C=true");
                break;
            }
            case View: {
                source = source.view(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.view("A=k * 10", "B=i * 2"), -1, "Z=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, 1, "DS1=A");
                shiftedTable = ShiftedColumnOperation.addShiftedColumns(shiftedTable, -2, "DS2=B")
                        .view("A", "B", "Z", "D=DS1 * DS2", "C=true");
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

        switch (flavor) {
            case Update: {
                source = source.update(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.update("A=k * 10", "B=i * 2"), -1, "Z=A")
                        .update("D=i", "C=true");
                break;
            }
            case UpdateView: {
                source = source.updateView(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.updateView("A=k * 10", "B=i * 2"), -1, "Z=A")
                        .updateView("D=i", "C=true");
                break;
            }
            case Select: {
                source = source.select(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.select("A=k * 10", "B=i * 2"), -1, "Z=A")
                        .view("A", "B", "Z", "D=i", "C=true");
                break;
            }
            case View: {
                source = source.view(formulas);
                shiftedTable = ShiftedColumnOperation
                        .addShiftedColumns(shiftedTable.view("A=k * 10", "B=i * 2"), -1, "Z=A")
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
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1");

        expressions = new String[] {"(Y_[i-1]) * B", "(Y_[ii-1]) * B", "(Y_[i - 1]) * B", "(Y_[ii - 1]) * B",
                "(Y_[ i - 1]) * B", "(Y_[ ii - 1 ]) * B"};
        singleResultColumnTest(expressions, -1L, "Y", "(" + shiftedPrefix + "1) * B");

        expressions = new String[] {"Y_[i+1]", "Y_[ii+1]", "Y_[i + 1]", "Y_[ii + 1]", "Y_[ i + 1 ]", "Y_[ ii + 1 ]"};
        singleResultColumnTest(expressions, 1L, "Y", shiftedPrefix + "1");

        expressions = new String[] {"(Y_[i+1]) * B", "(Y_[ii+1]) * B", "(Y_[i + 1]) * B", "(Y_[ii + 1]) * B",
                "(Y_[ i + 1]) * B", "(Y_[ ii + 1 ]) * B"};
        singleResultColumnTest(expressions, 1L, "Y", "(" + shiftedPrefix + "1) * B");

        expressions = new String[] {"Y_[i-2]", "Y_[ii-2]", "Y_[i - 2]", "Y_[ii - 2]", "Y_[ i - 2 ]", "Y_[ ii - 2 ]"};
        singleResultColumnTest(expressions, -2L, "Y", shiftedPrefix + "1");

        expressions = new String[] {"(Y_[i-2]) * B", "(Y_[ii-2]) * B", "(Y_[i - 2]) * B", "(Y_[ii - 2]) * B",
                "(Y_[ i - 2]) * B", "(Y_[ ii - 2 ]) * B"};
        singleResultColumnTest(expressions, -2L, "Y", "(" + shiftedPrefix + "1) * B");

        expressions = new String[] {"Y_[i+2]", "Y_[ii+2]", "Y_[i + 2]", "Y_[ii + 2]", "Y_[ i + 2 ]", "Y_[ ii + 2 ]"};
        singleResultColumnTest(expressions, 2L, "Y", shiftedPrefix + "1");

        expressions = new String[] {"(Y_[i+2]) * B", "(Y_[ii+2]) * B", "(Y_[i + 2]) * B", "(Y_[ii + 2]) * B",
                "(Y_[ i + 2]) * B", "(Y_[ ii + 2 ]) * B"};
        singleResultColumnTest(expressions, 2L, "Y", "(" + shiftedPrefix + "1) * B");

        // singleShiftMultiColumnTest
        String[] sourceColumns = new String[] {"Y", "B"};
        expressions = new String[] {"(Y_[i-1]) * (B_[i-1])", "(Y_[ii-1]) * (B_[ii-1])", "(Y_[i - 1]) * (B_[i - 1])",
                "(Y_[ii - 1]) * (B_[ii - 1])", "(Y_[ i - 1]) * (B_[ i - 1 ])", "(Y_[ ii - 1 ]) * (B_[ ii - 1 ])"};
        singleShiftMultiColumnTest(expressions, -1L, sourceColumns,
                "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        expressions = new String[] {"Y_[i-1] * B_[i-1]", "Y_[ii-1] * B_[ii-1]", "Y_[i - 1] * B_[i - 1]",
                "Y_[ii - 1] * B_[ii - 1]", "Y_[ i - 1] * B_[ i - 1 ]", "Y_[ ii - 1 ] * B_[ ii - 1 ]"};
        singleShiftMultiColumnTest(expressions, -1L, sourceColumns, shiftedPrefix + "1 * " + shiftedPrefix + "2");

        expressions = new String[] {"(Y_[i+1]) * (B_[i+1])", "(Y_[ii+1]) * (B_[ii+1])", "(Y_[i + 1]) * (B_[i + 1])",
                "(Y_[ii + 1]) * (B_[ii + 1])", "(Y_[ i + 1]) * (B_[ i + 1 ])", "(Y_[ ii + 1 ]) * (B_[ ii + 1 ])"};
        singleShiftMultiColumnTest(expressions, 1L, sourceColumns,
                "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        expressions = new String[] {"Y_[i+1] * B_[i+1]", "Y_[ii+1] * B_[ii+1]", "Y_[i + 1] * B_[i + 1]",
                "Y_[ii + 1] * B_[ii + 1]", "Y_[ i + 1] * B_[ i + 1 ]", "Y_[ ii + 1 ] * B_[ ii + 1 ]"};
        singleShiftMultiColumnTest(expressions, 1L, sourceColumns, shiftedPrefix + "1 * " + shiftedPrefix + "2");

        expressions = new String[] {"(Y_[i-2]) * (B_[i-2])", "(Y_[ii-2]) * (B_[ii-2])", "(Y_[i - 2]) * (B_[i - 2])",
                "(Y_[ii - 2]) * (B_[ii - 2])", "(Y_[ i - 2]) * (B_[ i - 2 ])", "(Y_[ ii - 2 ]) * (B_[ ii - 2 ])"};
        singleShiftMultiColumnTest(expressions, -2L, sourceColumns,
                "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        expressions = new String[] {"Y_[i-2] * B_[i-2]", "Y_[ii-2] * B_[ii-2]", "Y_[i - 2] * B_[i - 2]",
                "Y_[ii - 2] * B_[ii - 2]", "Y_[ i - 2] * B_[ i - 2 ]", "Y_[ ii - 2 ] * B_[ ii - 2 ]"};
        singleShiftMultiColumnTest(expressions, -2L, sourceColumns, shiftedPrefix + "1 * " + shiftedPrefix + "2");

        expressions = new String[] {"(Y_[i+2]) * (B_[i+2])", "(Y_[ii+2]) * (B_[ii+2])", "(Y_[i + 2]) * (B_[i + 2])",
                "(Y_[ii + 2]) * (B_[ii + 2])", "(Y_[ i + 2]) * (B_[ i + 2 ])", "(Y_[ ii + 2 ]) * (B_[ ii + 2 ])"};
        singleShiftMultiColumnTest(expressions, 2L, sourceColumns,
                "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        expressions = new String[] {"Y_[i+2] * B_[i+2]", "Y_[ii+2] * B_[ii+2]", "Y_[i + 2] * B_[i + 2]",
                "Y_[ii + 2] * B_[ii + 2]", "Y_[ i + 2] * B_[ i + 2 ]", "Y_[ ii + 2 ] * B_[ ii + 2 ]"};
        singleShiftMultiColumnTest(expressions, 2L, sourceColumns, shiftedPrefix + "1 * " + shiftedPrefix + "2");

        // multiShiftSingleColumnTest
        long[] shift = new long[] {-1L, 1L};
        expressions = new String[] {"(Y_[i-1]) * (Y_[i+1])", "(Y_[ii-1]) * (Y_[ii+1])", "(Y_[i - 1]) * (Y_[i + 1])",
                "(Y_[ii - 1]) * (Y_[ii + 1])", "(Y_[ i - 1]) * (Y_[ i + 1 ])", "(Y_[ ii - 1 ]) * (Y_[ ii + 1 ])"};
        multiShiftSingleColumnTest(expressions, shift, "Y", "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        shift = new long[] {2L, -2L};
        expressions = new String[] {"(Y_[i+2]) * (Y_[i-2])", "(Y_[ii+2]) * (Y_[ii-2])", "(Y_[i + 2]) * (Y_[i - 2])",
                "(Y_[ii + 2]) * (Y_[ii - 2])", "(Y_[ i + 2]) * (Y_[ i - 2 ])", "(Y_[ ii + 2 ]) * (Y_[ ii - 2 ])"};
        multiShiftSingleColumnTest(expressions, shift, "Y", "(" + shiftedPrefix + "1) * (" + shiftedPrefix + "2)");

        shift = new long[] {1L, -2L};
        expressions = new String[] {"Y_[i+1] * Y_[i-2]", "Y_[ii+1] * Y_[ii-2]", "Y_[i + 1] * Y_[i - 2]",
                "Y_[ii + 1] * Y_[ii - 2]", "Y_[ i + 1] * Y_[ i - 2 ]", "Y_[ ii + 1 ] * Y_[ ii - 2 ]"};
        multiShiftSingleColumnTest(expressions, shift, "Y", shiftedPrefix + "1 * " + shiftedPrefix + "2");

        // multiShiftMultiColumnTest
        shift = new long[] {-1L, 1L, -2L, 2L};
        MatchPair[] expectedColPairs = new MatchPair[] {
                new MatchPair(shiftedPrefix + "1", "Y"),
                new MatchPair(shiftedPrefix + "2", "B"),
                new MatchPair(shiftedPrefix + "3", "Y"),
                new MatchPair(shiftedPrefix + "4", "B")};
        expressions = new String[] {"(Y_[i+1] * B_[ i - 2] ) + (Y_[ii - 1] * B_[ii + 2]) + (Y_[i + 1] * B_[i - 2])"};
        multiShiftMultiColumnTest(expressions, shift, expectedColPairs,
                "(" + shiftedPrefix + "1 * " + shiftedPrefix + "2) + (" + shiftedPrefix + "3 * " + shiftedPrefix
                        + "4) + (" + shiftedPrefix + "1 * " + shiftedPrefix + "2)");

        // singleResultColumnTest - different expressions
        expressions = new String[] {"Y_[i-1] * true"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 * true");

        expressions = new String[] {"Y_[i-1] * 0.5f"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 * 0.5f");

        expressions = new String[] {"Y_[i-1] + \"test\""};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + \"test\"");

        expressions = new String[] {"Y_[i-1] + 10"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + 10");

        expressions = new String[] {"Y_[i-1] + 10L"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + 10L");

        expressions = new String[] {"Y_[i-1] + 'C'"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + 'C'");

        expressions = new String[] {"Y_[i-1] + " + null};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + null");

        expressions = new String[] {"Y_[i-1] + " + Integer.MIN_VALUE};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + " + Integer.MIN_VALUE);

        // tests UnaryExpr
        expressions = new String[] {"Y_[ii-1] + " + Long.MIN_VALUE};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + " + Long.MIN_VALUE);

        // tests LongLiteralMinValueExpr
        expressions = new String[] {"Y_[ii-1] + " + Long.MIN_VALUE + "L"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + " + Long.MIN_VALUE + "L");

        // tests MethodReferenceExpr
        expressions = new String[] {"Y_[ii-1] + random.nextInt(1000000)"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + random.nextInt(1000000)");

        // tests FieldAccessExpression
        expressions = new String[] {"Y_[ii-1] + expectedColPairs.length"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + expectedColPairs.length");

        // tests CastExpr
        expressions = new String[] {"Y_[ii-1] + ((long)random.nextInt(1000000))"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + ((long) random.nextInt(1000000))");

        // tests ArrayCreationExpr
        expressions = new String[] {"Y_[ii-1] + new long[] { 1L, 2L}"};
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + new long[] { 1L, 2L }");

        // tests ObjectCreationExpr
        expressions = new String[] {"Y_[ii-1] + new Boolean(true)"};
        // ObjectCreationExpression toString has two spaces between new and expression
        singleResultColumnTest(expressions, -1L, "Y", shiftedPrefix + "1 + new Boolean(true)");
    }

    @SuppressWarnings("SameParameterValue")
    private void singleResultColumnTest(String[] singleResultColumn, long shift, String sourceCol,
            String expectedFormula) {
        try {
            MatchPair expectedColPair = new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1, sourceCol);
            for (String expression : singleResultColumn) {
                Expression expr = JavaExpressionParser.parseExpression(expression);
                Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(expr);

                Assert.assertNotNull(pair);
                Assert.assertEquals(expectedFormula, pair.getFirst());
                Map<Long, List<MatchPair>> map = pair.getSecond();
                Assert.assertNotNull(map);
                Assert.assertEquals(1, map.size());
                List<MatchPair> colPairs = map.get(shift);
                Assert.assertNotNull(colPairs);
                Assert.assertEquals(1, colPairs.size());
                Assert.assertEquals(expectedColPair, colPairs.get(0));
            }

        } catch (Exception exception) {
            Assert.fail(exception.getMessage());
            exception.printStackTrace();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void singleShiftMultiColumnTest(String[] expressions, long shift, String[] sourceCol,
            String expectedFormula) {
        try {
            for (String expression : expressions) {
                Expression expr = JavaExpressionParser.parseExpression(expression);
                Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(expr);
                Assert.assertNotNull(pair);
                Assert.assertEquals(expectedFormula, pair.getFirst());
                Assert.assertEquals(1, pair.getSecond().size());
                Assert.assertNotNull(pair.getSecond().get(shift));
                Assert.assertEquals(sourceCol.length, pair.getSecond().get(shift).size());
                for (int i = 1; i <= sourceCol.length; i++) {
                    MatchPair matchPair = pair.getSecond().get(shift).get(i - 1);
                    Assert.assertEquals("verify shifted column name suffix",
                            ShiftedColumnsFactory.SHIFTED_COL_PREFIX + i, matchPair.leftColumn);
                    Assert.assertEquals("verify source column name", sourceCol[i - 1], matchPair.rightColumn);
                }
            }
        } catch (Exception exception) {
            Assert.fail(exception.getMessage());
            exception.printStackTrace();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void multiShiftSingleColumnTest(String[] expressions, long[] shift, String sourceCol,
            String expectedFormula) {
        try {
            for (String expression : expressions) {
                Expression expr = JavaExpressionParser.parseExpression(expression);
                Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(expr);
                Assert.assertNotNull(pair);
                Assert.assertEquals(expectedFormula, pair.getFirst());
                Assert.assertNotNull(pair.getSecond());
                Assert.assertEquals(shift.length, pair.getSecond().size());
                for (int i = 0; i < shift.length; i++) {
                    Assert.assertNotNull(pair.getSecond().get(shift[i]));
                    Assert.assertEquals(1, pair.getSecond().get(shift[i]).size());
                    MatchPair matchPair = pair.getSecond().get(shift[i]).get(0);
                    Assert.assertEquals("verify shifted column name suffix",
                            ShiftedColumnsFactory.SHIFTED_COL_PREFIX + (i + 1), matchPair.leftColumn);
                    Assert.assertEquals("verify source column name", sourceCol, matchPair.rightColumn);
                }
            }
        } catch (Exception exception) {
            Assert.fail(exception.getMessage());
            exception.printStackTrace();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void multiShiftMultiColumnTest(String[] expressions, long[] shift, MatchPair[] expectedColPairs,
            String expectedFormula) {
        try {
            for (String expression : expressions) {
                Expression expr = JavaExpressionParser.parseExpression(expression);
                Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(expr);
                Assert.assertNotNull(pair);
                Assert.assertEquals(expectedFormula, pair.getFirst());
                Assert.assertEquals(shift.length, pair.getSecond().size());
                Set<MatchPair> allResultColumns = new HashSet<>();
                for (long l : shift) {
                    Assert.assertNotNull(pair.getSecond().get(l));
                    allResultColumns.addAll(pair.getSecond().get(l));
                }
                Assert.assertEquals(expectedColPairs.length, allResultColumns.size());
                for (MatchPair expectedCol : expectedColPairs) {
                    Assert.assertTrue(allResultColumns.contains(expectedCol));
                }
            }
        } catch (Exception exception) {
            Assert.fail(exception.getMessage());
            exception.printStackTrace();
        }
    }

    @Test
    public void arrayInitializerTest() {
        String[] expressions = new String[] {
                "Y_[i-1]", "Y_[i-2]", "Y_[i+1]", "Y_[i+2]",
                "random.nextInt(1000000)", "((4 + 3) - (7 -95) + (3 * (5 + (g - 8))))",
                "((4 + 3) - (7 -95) + (3 * (5 + (8 - g))))"
        };

        String[] resultFormulaArray = new String[] {
                ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1,
                ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2,
                ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 3,
                ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 4,
                "random.nextInt(1000000)", "((4 + 3) - (7 - 95) + (3 * (5 + (g - 8))))",
                "((4 + 3) - (7 - 95) + (3 * (5 + (8 - g))))"
        };

        long[] expectedShift = new long[] {-1L, -2L, 1L, 2L};
        MatchPair[] expectedColPairs = new MatchPair[] {
                new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1, "Y"),
                new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2, "Y"),
                new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 3, "Y"),
                new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 4, "Y"),
        };

        evaluateArrayInitializer(expressions, resultFormulaArray, expectedShift, expectedColPairs);

        evaluateArrayInitializer(new String[0], new String[0], new long[0], MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    private void evaluateArrayInitializer(
            final String[] expressions,
            final String[] resultFormulaArray,
            final long[] expectedShift,
            final MatchPair[] expectedColPairs) {
        String expectedResult = buildArrayInitializerResult(resultFormulaArray);
        NodeList<Expression> expressionList = new NodeList<>();
        for (String expression : expressions) {
            Expression expr = JavaExpressionParser.parseExpression(expression);
            expressionList.add(expr);
        }
        ArrayInitializerExpr arrayInitializerExpr = new ArrayInitializerExpr(expressionList);

        Pair<String, Map<Long, List<MatchPair>>> pair =
                ShiftedColumnsFactory.getShiftToColPairsMap(arrayInitializerExpr);
        if (expectedShift.length > 0) {
            Assert.assertNotNull(pair);
            Assert.assertEquals("formula comparison", expectedResult, pair.getFirst());
            Assert.assertEquals("compare number of shifts", expectedShift.length, pair.getSecond().size());
            for (int i = 0; i < expectedShift.length; i++) {
                Assert.assertNotNull(pair.getSecond().get(expectedShift[i]));
                Assert.assertEquals("verify expected col pair size", 1,
                        pair.getSecond().get(expectedShift[i]).size());
                Assert.assertEquals("compare expected col pair", expectedColPairs[i],
                        pair.getSecond().get(expectedShift[i]).get(0));
            }
        } else {
            Assert.assertNull(pair);
        }
    }

    private String buildArrayInitializerResult(String[] resultFormulaArray) {
        if (resultFormulaArray == null || resultFormulaArray.length == 0) {
            return "{ }";
        }
        StringBuilder builder = new StringBuilder();
        for (String str : resultFormulaArray) {
            if (builder.length() == 0) {
                builder.append('{');
            } else {
                builder.append(',');
            }
            builder.append(' ').append(str);
        }
        builder.append(' ').append('}');
        return builder.toString();
    }

    @Test
    public void conditionExprTest() {
        // noinspection unchecked
        Triple<String, String, String>[] conditionalTriples = new Triple[] {
                new ImmutableTriple<>("A == D", "B_[i-1]", "C_[i-2]"),
                new ImmutableTriple<>("A == D", "B", "C_[i-2]"),
                new ImmutableTriple<>("A == D", "B * Y", "C_[2-i]")
        };

        // noinspection unchecked
        Pair<String, Boolean>[] expectedFormulas = new Pair[] {
                new Pair<>("A == D ? " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " : "
                        + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2, true),
                new Pair<>("A == D ? B : " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1, true),
                new Pair<>("A == D ? B * Y : ", false)
        };

        Map<Long, List<MatchPair>> shiftToMatchPair = new LinkedHashMap<>();
        List<MatchPair> matchPairList = new LinkedList<>();
        matchPairList.add(new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1, "B"));
        shiftToMatchPair.put(-1L, matchPairList);

        List<MatchPair> matchPairList2 = new LinkedList<>();
        matchPairList2.add(new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2, "C"));
        shiftToMatchPair.put(-2L, matchPairList2);

        evaluateConditionExpressions(conditionalTriples[0], expectedFormulas[0], shiftToMatchPair);

        shiftToMatchPair = new LinkedHashMap<>();
        matchPairList = new LinkedList<>();
        matchPairList.add(new MatchPair(ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1, "C"));
        shiftToMatchPair.put(-2L, matchPairList);
        evaluateConditionExpressions(conditionalTriples[1], expectedFormulas[1], shiftToMatchPair);

        evaluateConditionExpressions(conditionalTriples[2], expectedFormulas[2], null);
    }

    private void evaluateConditionExpressions(Triple<String, String, String> conditionalTriple,
            Pair<String, Boolean> expectedFormula, Map<Long, List<MatchPair>> shiftToMatchPair) {
        Expression condition = JavaExpressionParser.parseExpression(conditionalTriple.getLeft());
        Expression then = JavaExpressionParser.parseExpression(conditionalTriple.getMiddle());
        Expression elseExpr = JavaExpressionParser.parseExpression(conditionalTriple.getRight());
        ConditionalExpr conditionalExpr = new ConditionalExpr(condition, then, elseExpr);
        Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(conditionalExpr);
        if (expectedFormula.getSecond()) {
            Assert.assertNotNull(pair);
            Assert.assertEquals("compare formula", expectedFormula.getFirst(), pair.getFirst());
            Assert.assertNotNull(pair.getSecond());
            Assert.assertEquals("compare map sizes", shiftToMatchPair.size(), pair.getSecond().size());
            for (Map.Entry<Long, List<MatchPair>> entry : shiftToMatchPair.entrySet()) {
                Assert.assertNotNull(pair.getSecond().get(entry.getKey()));
                Assert.assertEquals("compare expected shifted cols for same shift", entry.getValue().size(),
                        pair.getSecond().get(entry.getKey()).size());
                ListIterator<MatchPair> expectedIt = entry.getValue().listIterator();
                ListIterator<MatchPair> actualIt = pair.getSecond().get(entry.getKey()).listIterator();
                while (expectedIt.hasNext() && actualIt.hasNext()) {
                    MatchPair expectedMp = expectedIt.next();
                    MatchPair actualMp = actualIt.next();
                    Assert.assertEquals("compare match pairs left column", expectedMp.leftColumn, actualMp.leftColumn);
                    Assert.assertEquals("compare match pairs right column", expectedMp.rightColumn,
                            actualMp.rightColumn);
                }
                if (expectedIt.hasNext()) {
                    Assert.fail("expected match pair list still has elements");
                }
                if (actualIt.hasNext()) {
                    Assert.fail("actual match pair list still has elements");
                }
            }
        } else {
            Assert.assertNull(pair);
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
            final Pair<String, Map<Long, List<MatchPair>>> pair = ShiftedColumnsFactory.getShiftToColPairsMap(expr);
            final boolean hasConstantArrayAccess = pair != null;

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
                        "Y == " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " && A == "
                                + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2},
                {"Y_[i-1]==A_[i-1]",
                        ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " == "
                                + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2},
                {"Y_[1-i]==A_[i-1]", "Y_[1 - i] == " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1},
                {"Y=Y_[i-1] && A=A_[ii-1]",
                        "Y == " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " && A == "
                                + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2},
                {"(Y==Y_[i-1]) && (A==A_[ii-1])",
                        "(Y == " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + ") && (A == "
                                + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 2 + ")"},
                {"(Y==Y_[i-k]) && (A==A_[2-ii])", "(Y == Y_[i - k]) && (A == A_[2 - ii])"},
                {"(1 <= Y_[i - 1] && Y_[i - 1] > 10)",
                        "(1 <= " + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " && "
                                + ShiftedColumnsFactory.SHIFTED_COL_PREFIX + 1 + " > 10)"}
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
