/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import com.google.common.primitives.Ints;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.UpdateGraphConflictException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.table.impl.locations.GroupingProvider;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.remote.InitialSnapshotTable;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.select.MatchFilter.CaseSensitivity;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.table.impl.sources.DeferredGroupingColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateGraphLock;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.log.LogEntry;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.vector.*;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.groovy.util.Maps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import java.util.stream.LongStream;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test of QueryTable functionality.
 * <p>
 * This test used to be a catch all, but at over 7,000 lines became unwieldy. It is still somewhat of a catch-all, but
 * some specific classes of tests have been broken out.
 * <p>
 * See also {@link QueryTableAggregationTest}, {@link QueryTableJoinTest}, {@link QueryTableSelectUpdateTest},
 * {@link QueryTableFlattenTest}, and {@link QueryTableSortTest}.
 */
@Category(OutOfBandTest.class)
public class QueryTableTest extends QueryTableTestBase {
    public void testStupidCast() {
        QueryTable table = testRefreshingTable(i(2, 4, 6).toTracking());
        // noinspection UnusedAssignment
        table = (QueryTable) table.select("x =1.2", "x=(int)x");
    }

    /**
     * Test Table.validate() on a few positive and negative cases
     */
    public void testFormulaValidation() {
        final String[][] positives = new String[][] {
                new String[] {"X = 12"},
                new String[] {"X = 12", "Y = X + 12"},
                new String[] {"X = 12", "Y = X + 12", "Z = Math.min(X_[i - 1], Y_[i - 1])"}
        };

        final String[][] negatives = new String[][] {
                new String[] {"X = twelve"},
                new String[] {"X = 12", "Y = WEIRD + 12"},
                new String[] {"X = 12", "Y = X + 12", "Z = Math.blob(X_[i - 1], Y_[i - 1])"}
        };

        for (String[] columns : positives) {
            ((QueryTable) TableTools.emptyTable(10)).validateSelect(SelectColumn.from(Selectable.from(columns)));
        }

        for (String[] columns : negatives) {
            try {
                ((QueryTable) TableTools.emptyTable(10)).validateSelect(SelectColumn.from(Selectable.from(columns)));
                TestCase.fail("validation should have failed for: " + Arrays.toString(columns));
            } catch (FormulaCompilationException fce) {
                // Expected.
            }
        }
    }

    /**
     * Test that the formula can see the internal variable that DateTimeUtils introduces here. (Prior to IDS-6532 this
     * threw an exception).
     */
    public void testIds6532() {
        final Table empty = emptyTable(5);
        final SelectColumn sc = SelectColumnFactory.getExpression("Result = '2020-03-15T09:45:00.000000000 UTC'");
        // First time ok
        // noinspection unused
        final Table t1 = empty.select(List.of(sc));
        // Second time throws exception
        // noinspection unused
        final Table t2 = empty.select(List.of(sc));
    }

    /**
     * Test that the formula behaves correctly when there are are two initDefs() without an intervening request to
     * compile the formula. Prior to the second update to IDS-6532, this threw an exception, although typically only by
     * OpenAPI code (because that code uses validateSelect() whereas other code tends not to). The issue is that this
     * sequence of operations works:
     *
     * initDef() get compiled formula initDef() get compiled formula
     *
     * But (prior to this change) this sequence of operations would not work: initDef() initDef() get compiled formula
     *
     * The reason the second one breaks is that (prior to this change), when using certain Time literals, the second
     * initDef() changes Formula state in such a way that a subsequent compilation would not succeed. The reason this
     * break was not observed in practice is that most usages are like the first example: there is a compilation request
     * interposed between initDefs() and, thanks to formula caching, the second compilation uses the cached Formula
     * object from the first compilation and doesn't actually invoke the compiler again.
     */
    public void testIds6532_part2() {
        final Table empty = emptyTable(5);
        final SelectColumn sc = SelectColumnFactory.getExpression("Result = '2020-03-15T09:45:00.000000000 UTC'");
        ((QueryTable) empty).validateSelect(sc);
        empty.select(List.of(sc));
    }

    /**
     * The formula generation code used to create internal variables called "__chunk" + columnName; it also created an
     * internal variable called "__chunkPos". Prior to the change that fixed this, a formula compilation error can
     * happen if the customer names their column "Pos".
     */
    public void testIds6614() {
        final Table empty = emptyTable(5);
        final Table table1 = empty.select("Pos = 1");
        table1.select("Result = Pos + 2");
    }

    /**
     * Confirm that the system behaves correctly with select validation and the new "flatten" code QueryTable#select().
     * Prior to the change that fixed this, "validateSelect" would cause the SelectColumn to get associated with one
     * RowSet, but then select() would want to flatten that RowSet, so a later initDef would try to associate it with a
     * different RowSet, and then the assertion would fail at AbstractFormulaColumn.java:86. The simple fix is that
     * validateSelect() should copy its select columns before using them and then throw away the copies.
     */
    public void testIds6760() {
        final Table t = emptyTable(10).select("II = ii").where("II > 5");
        final SelectColumn sc = SelectColumnFactory.getExpression("XX = II + 1000");
        ((QueryTable) t).validateSelect(sc);
        final Table result = t.select(List.of(sc));
    }

    public void testIds1822() {
        // formula column preserving types
        final Table t = emptyTable(5).select("I=i", "X=i=0?null:42");
        final Table t2 = t.update("X=isNull(X) ? 0 : X", "Y=(X)");
        final Table t3 = t.update("X=isNull(X) ? 0 : X", "Y=X");
        final Table t4 = t.update("X=isNull(X) ? 0 : X").update("Y=X");

        assertTableEquals(t2, t4);
        assertTableEquals(t3, t4);

        // formula column changing types
        final Table u = emptyTable(5).select("I=i", "X=i=0?null:42");
        final Table u2 = u.update("X=isNull(X) ? 0.1 : (double)X", "Y=(X)");
        final Table u3 = u.update("X=isNull(X) ? 0.1 : (double)X", "Y=X");
        final Table u4 = u.update("X=isNull(X) ? 0.1 : (double)X").update("Y=X");

        assertTableEquals(u2, u4);
        assertTableEquals(u3, u4);

        // source column
        final Table v = emptyTable(5).select("I=i", "X=i=0?null:42");
        final Table v3 = v.update("Z=isNull(X) ? 0 : X", "Y=Z");
        final Table v4 = v.update("Z=isNull(X) ? 0 : X").update("Y=Z");

        assertTableEquals(v3, v4);
    }

    public void testViewIncremental() {
        final Random random = new Random(0);
        final ColumnInfo[] columnInfo;
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
        };

        for (int i = 0; i < 10; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testView() {
        QueryScope.addParam("indexMinEdge", 2.0);
        QueryScope.addParam("IsIndex", true);
        QueryScope.addParam("MEF", 1.0);
        QueryScope.addParam("LnRatioStd", 1.0);
        QueryScope.addParam("VegaPer", 1.0);
        TableTools.emptyTable(3).updateView("MinEdge = (IsIndex ? indexMinEdge : MEF * LnRatioStd) * VegaPer");

        final QueryTable table0 = (QueryTable) TableTools.emptyTable(3).view("x = i*2", "y = \"\" + x");
        assertEquals(Arrays.asList(0, 2, 4),
                Arrays.asList(DataAccessHelpers.getColumn(table0, "x").get(0, table0.size())));
        assertEquals(Arrays.asList("0", "2", "4"),
                Arrays.asList(DataAccessHelpers.getColumn(table0, "y").get(0, table0.size())));

        final QueryTable table = (QueryTable) table0.updateView("z = x + 1", "x = z + 1", "t = x - 3");
        assertEquals(Arrays.asList(1, 3, 5),
                Arrays.asList(DataAccessHelpers.getColumn(table, "z").get(0, table.size())));
        assertEquals(Arrays.asList(2, 4, 6),
                Arrays.asList(DataAccessHelpers.getColumn(table, "x").get(0, table.size())));
        assertEquals(Arrays.asList(-1, 1, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table, "t").get(0, table.size())));

        final QueryTable table1 = testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));
        final QueryTable table2 = (QueryTable) table1.updateView("z = x", "x = z + 1", "t = x - 3");
        final ShiftObliviousListener table2Listener = newListenerWithGlobals(table2);
        table2.addUpdateListener(table2Listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table1, i(7, 9), col("x", 4, 5), col("y", 'd', 'e'));
            table1.notifyListeners(i(7, 9), i(), i());
        });
        assertEquals(5, table1.size());
        assertEquals(5, table2.size());
        assertTableEquals(table2, table1.update("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i(7, 9));
        assertEquals(modified, i());
        assertEquals(removed, i());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table1, i(7, 9), col("x", 3, 10), col("y", 'e', 'd'));
            table1.notifyListeners(i(), i(), i(7, 9));
        });
        assertEquals(5, table1.size());
        assertEquals(5, table2.size());
        assertTableEquals(table2, table1.update("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i());
        assertEquals(modified, i(7, 9));
        assertEquals(removed, i());

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table1, i(2, 6, 7));
            table1.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertEquals(2, table1.size());
        assertEquals(2, table2.size());
        assertTableEquals(table2, table1.update("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i());
        assertEquals(removed, i(2, 6, 7));
        assertEquals(modified, i());

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table1, i(9));
            addToTable(table1, i(2, 4, 6), col("x", 1, 22, 3), col("y", 'a', 'x', 'c'));
            table1.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertEquals(3, table1.size());
        assertEquals(3, table2.size());
        assertTableEquals(table2, table1.update("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i(2, 6));
        assertEquals(removed, i(9));
        assertEquals(modified, i(4));

        final QueryTable table3 = testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));
        final QueryTable table4 = (QueryTable) table3.view("z = x", "x = z + 1", "t = x - 3");
        final ShiftObliviousListener table4Listener = newListenerWithGlobals(table4);
        table4.addUpdateListener(table4Listener);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table3, i(7, 9), col("x", 4, 5), col("y", 'd', 'e'));
            table3.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals(5, table3.size());
        assertEquals(5, table4.size());
        assertTableEquals(table4, table3.select("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i(7, 9));
        assertEquals(modified, i());
        assertEquals(removed, i());


        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table3, i(7, 9), col("x", 3, 10), col("y", 'e', 'd'));
            table3.notifyListeners(i(), i(), i(7, 9));
        });

        assertEquals(5, table3.size());
        assertEquals(5, table4.size());
        assertTableEquals(table4, table3.select("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i());
        assertEquals(modified, i(7, 9));
        assertEquals(removed, i());

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table3, i(2, 6, 7));
            table3.notifyListeners(i(), i(2, 6, 7), i());
        });

        assertEquals(2, table4.size());
        assertEquals(2, table3.size());
        assertTableEquals(table4, table3.select("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i());
        assertEquals(removed, i(2, 6, 7));
        assertEquals(modified, i());


        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table3, i(9));
            addToTable(table3, i(2, 4, 6), col("x", 1, 22, 3), col("y", 'a', 'x', 'c'));
            table3.notifyListeners(i(2, 6), i(9), i(4));
        });

        assertEquals(3, table1.size());
        assertEquals(3, table3.size());
        assertTableEquals(table4, table3.select("z = x", "x = z + 1", "t = x - 3"));
        assertEquals(added, i(2, 6));
        assertEquals(removed, i(9));
        assertEquals(modified, i(4));
    }

    public void testView1() {
        final Table t = testRefreshingTable(col("x", true, false, true));
        final Table t1 = t.select("y=x && true");
        final Table t2 = t.view("y=x && true");
        TableTools.merge(t1, t2);
        TableTools.merge(t2, t1);
        assertNull(DataAccessHelpers.getColumn(t.updateView("nullD = NULL_DOUBLE + 0"), "nullD").get(0));

        assertEquals(
                Arrays.asList(DataAccessHelpers
                        .getColumn(emptyTable(4).updateView("b1 = (i%2 = 0)?null:true").updateView("x = b1 == null?1:2")
                                .select("x"), "x")
                        .get(0, 4)),
                Arrays.asList(1, 2, 1, 2));

        Table table = newTable(3, Arrays.asList("String", "Int"),
                Arrays.asList(TableTools.objColSource("c", "e", "g"), TableTools.colSource(2, 4, 6)));
        assertEquals(2, table.view(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).numColumns());
        assertEquals(table.getDefinition().getColumns().get(0).getName(),
                table.view(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getDefinition().getColumns().get(0).getName());
        assertEquals(table.getDefinition().getColumns().get(1).getName(),
                table.view(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getDefinition().getColumns().get(1).getName());

        assertEquals(2, table.view("String", "Int").numColumns());
        assertEquals(table.getDefinition().getColumns().get(0).getName(),
                table.view("String", "Int").getDefinition().getColumns().get(0).getName());
        assertEquals(table.getDefinition().getColumns().get(1).getName(),
                table.view("String", "Int").getDefinition().getColumns().get(1).getName());

        assertEquals(2, table.view("Int", "String").numColumns());
        assertEquals(table.getDefinition().getColumns().get(0).getName(),
                table.view("Int", "String").getDefinition().getColumns().get(1).getName());
        assertEquals(table.getDefinition().getColumns().get(1).getName(),
                table.view("Int", "String").getDefinition().getColumns().get(0).getName());

        assertEquals(2, table.view("Int1=Int", "String1=String").numColumns());
        assertSame(table.getDefinition().getColumns().get(0).getDataType(),
                table.view("Int1=Int", "String1=String").getDefinition().getColumns().get(1).getDataType());
        assertSame(table.getDefinition().getColumns().get(1).getDataType(),
                table.view("Int1=Int", "String1=String").getDefinition().getColumns().get(0).getDataType());
        assertEquals("Int1", table.view("Int1=Int", "String1=String").getDefinition().getColumns().get(0).getName());
        assertEquals("String1", table.view("Int1=Int", "String1=String").getDefinition().getColumns().get(1).getName());

        table = TableTools.emptyTable(3);
        table = table.view("x = i*2", "y = \"\" + x");
        assertEquals(Arrays.asList(0, 2, 4), Arrays.asList(DataAccessHelpers.getColumn(table, "x").get(0, 3)));
        assertEquals(Arrays.asList("0", "2", "4"), Arrays.asList(DataAccessHelpers.getColumn(table, "y").get(0, 3)));

        table = table.updateView("z = x + 1", "x = z + 1", "t = x - 3");
        assertEquals(Arrays.asList(1, 3, 5), Arrays.asList(DataAccessHelpers.getColumn(table, "z").get(0, 3)));
        assertEquals(Arrays.asList(2, 4, 6), Arrays.asList(DataAccessHelpers.getColumn(table, "x").get(0, 3)));
        assertEquals(Arrays.asList(-1, 1, 3), Arrays.asList(DataAccessHelpers.getColumn(table, "t").get(0, 3)));
    }

    public void testReinterpret() {
        final Table source = emptyTable(5).select("dt = epochNanosToInstant(ii)", "n = ii");
        final Table result = source.updateView(List.of(
                new ReinterpretedColumn<>("dt", Instant.class, "dt", long.class)));
        assertEquals((long[]) DataAccessHelpers.getColumn(result, 0).getDirect(), LongStream.range(0, 5).toArray());
        final Table reflexive = result.updateView(List.of(
                new ReinterpretedColumn<>("dt", long.class, "dt", Instant.class)));
        assertTableEquals(reflexive, source);
        final Table sortedSource = source.sortDescending("dt").dropColumns("dt");
        final Table sortedResult = result.sortDescending("dt").dropColumns("dt");
        assertTableEquals(sortedResult, sortedSource);
    }

    public void testStaticSelectIntermediateColumn() {
        final Table et = emptyTable(3);
        final Table result = et.select("A = i").join(et).select("B = A", "C = A + B");
        assertTrue(result.isFlat());

        final List<String> exNames = Arrays.asList("B", "C");
        final List<ColumnSource<?>> exSources = Arrays.asList(
                TableTools.colSource(0, 0, 0, 1, 1, 1, 2, 2, 2),
                TableTools.colSource(0, 0, 0, 2, 2, 2, 4, 4, 4));
        final Table expected = newTable(9, exNames, exSources);
        assertTableEquals(expected, result);
    }

    public void testDropColumns() {
        final List<String> colNames = Arrays.asList("String", "Int", "Double");
        final List<ColumnSource<?>> colSources =
                Arrays.asList(TableTools.objColSource("c", "e", "g"), colSource(2, 4, 6), colSource(1.0, 2.0, 3.0));
        final Table table = newTable(3, colNames, colSources);
        assertEquals(3, table.dropColumns(Collections.emptyList()).getColumnSources().size());
        Collection<? extends ColumnSource<?>> columnSourcesAfterDrop = table.getColumnSources();
        ColumnSource<?>[] columnsAfterDrop =
                columnSourcesAfterDrop.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        Collection<? extends ColumnSource<?>> columnSources =
                table.dropColumns(Collections.emptyList()).getColumnSources();
        ColumnSource<?>[] columns = columnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        assertSame(columns[0], columnsAfterDrop[0]);
        assertSame(columns[1], columnsAfterDrop[1]);
        assertSame(columns[2], columnsAfterDrop[2]);
        assertSame(table.getColumnSource("String"),
                table.dropColumns(Collections.emptyList()).getColumnSource("String"));
        assertSame(table.getColumnSource("Int"), table.dropColumns(Collections.emptyList()).getColumnSource("Int"));
        assertSame(table.getColumnSource("Double"),
                table.dropColumns(Collections.emptyList()).getColumnSource("Double"));

        assertEquals(2, table.dropColumns("Int").getColumnSources().size());
        columnSourcesAfterDrop = table.dropColumns("Int").getColumnSources();
        columnsAfterDrop = columnSourcesAfterDrop.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        columnSources = table.getColumnSources();
        columns = columnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        assertSame(columns[0], columnsAfterDrop[0]);
        assertSame(columns[2], columnsAfterDrop[1]);
        assertSame(table.getColumnSource("String"), table.dropColumns("Int").getColumnSource("String"));
        assertSame(table.getColumnSource("Double"), table.dropColumns("Int").getColumnSource("Double"));
        try {
            table.dropColumns("Int").getColumnSource("Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }

        columnSourcesAfterDrop = table.dropColumns("String", "Int").getColumnSources();
        columnsAfterDrop = columnSourcesAfterDrop.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        columnSources = table.getColumnSources();
        columns = columnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        assertEquals(1, table.dropColumns("String", "Int").getColumnSources().size());
        assertSame(columns[2], columnsAfterDrop[0]);
        assertSame(table.getColumnSource("Double"), table.dropColumns("String", "Int").getColumnSource("Double"));
        try {
            table.dropColumns("String", "Int").getColumnSource("String");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }
        try {
            DataAccessHelpers.getColumn(table.dropColumns("String", "Int"), "Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }

        assertEquals(1, table.dropColumns("String").dropColumns("Int").numColumns());
        columnSourcesAfterDrop = table.dropColumns("String").dropColumns("Int").getColumnSources();
        columnsAfterDrop = columnSourcesAfterDrop.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        columnSources = table.getColumnSources();
        columns = columnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        assertSame(columnsAfterDrop[0], columns[2]);
        assertSame(table.getColumnSource("Double"),
                table.dropColumns("String").dropColumns("Int").getColumnSource("Double"));
        try {
            table.dropColumns("String").dropColumns("Int").getColumnSource("String");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }
        try {
            table.dropColumns("String").dropColumns("Int").getColumnSource("Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }

        try {
            table.dropColumns(Collections.singletonList("DoesNotExist"));
            fail("Expected NoSuchColumnException");
        } catch (NoSuchColumnException e) {
            assertEquals("Unknown column names [DoesNotExist], available column names are [String, Int, Double]",
                    e.getMessage());
        }
        try {
            table.dropColumns(Arrays.asList("Int", "DoesNotExist"));
            fail("Expected NoSuchColumnException");
        } catch (NoSuchColumnException e) {
            assertEquals("Unknown column names [DoesNotExist], available column names are [String, Int, Double]",
                    e.getMessage());
        }
    }

    public void testRenameColumns() {
        final Table table = newTable(3,
                Arrays.asList("String", "Int", "Double"),
                Arrays.asList(TableTools.objColSource("c", "e", "g"), colSource(2, 4, 6), colSource(1.0, 2.0, 3.0)));
        assertEquals(3, table.renameColumns(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getColumnSources().size());
        final Collection<? extends ColumnSource<?>> columnSources = table.getColumnSources();
        final ColumnSource<?>[] columns = columnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        final Collection<? extends ColumnSource<?>> renamedColumnSources =
                table.renameColumns(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getColumnSources();
        final ColumnSource<?>[] renamedColumns =
                renamedColumnSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        assertSame(columns[0], renamedColumns[0]);
        assertSame(columns[1], renamedColumns[1]);
        assertSame(columns[2], renamedColumns[2]);
        assertSame(table.getColumnSource("String"),
                table.renameColumns(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getColumnSource("String"));
        assertSame(table.getColumnSource("Int"),
                table.renameColumns(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getColumnSource("Int"));
        assertSame(table.getColumnSource("Double"),
                table.renameColumns(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getColumnSource("Double"));

        assertEquals(3, table.renameColumns("NewInt=Int").numColumns());
        assertEquals(table.getColumnSources().toArray()[0],
                table.renameColumns("NewInt=Int").getColumnSources().toArray()[0]);
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, 1).getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), 1).getDirect());
        assertEquals(table.getColumnSources().toArray()[2],
                table.renameColumns("NewInt=Int").getColumnSources().toArray()[2]);
        assertEquals(table.getColumnSource("String"), table.renameColumns("NewInt=Int").getColumnSource("String"));
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, "Int").getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), "NewInt").getDirect());
        assertEquals(table.getColumnSource("Double"), table.renameColumns("NewInt=Int").getColumnSource("Double"));
        try {
            DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), "Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }

        assertEquals(3, table.renameColumns("NewInt=Int", "NewString=String").numColumns());
        assertArrayEquals((String[]) DataAccessHelpers.getColumn(table, 0).getDirect(),
                (String[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int", "NewString=String"), 0)
                        .getDirect());
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, 1).getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), 1).getDirect());
        assertEquals(table.getColumnSources().toArray()[2],
                table.renameColumns("NewInt=Int", "NewString=String").getColumnSources().toArray()[2]);
        assertArrayEquals((String[]) DataAccessHelpers.getColumn(table, "String").getDirect(),
                (String[]) DataAccessHelpers
                        .getColumn(table.renameColumns("NewInt=Int", "NewString=String"), "NewString").getDirect());
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, "Int").getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), "NewInt").getDirect());
        assertEquals(table.getColumnSource("Double"),
                table.renameColumns("NewInt=Int", "NewString=String").getColumnSource("Double"));
        try {
            DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int", "NewString=String"), "Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }
        try {
            DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int", "NewString=String"), "String");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }

        assertEquals(3, table.renameColumns("NewInt=Int").renameColumns("NewString=String").numColumns());
        assertArrayEquals((String[]) DataAccessHelpers.getColumn(table, 0).getDirect(),
                (String[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int", "NewString=String"), 0)
                        .getDirect());
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, 1).getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), 1).getDirect());
        assertEquals(table.getColumnSources().toArray()[2],
                table.renameColumns("NewInt=Int").renameColumns("NewString=String").getColumnSources().toArray()[2]);
        assertArrayEquals((String[]) DataAccessHelpers.getColumn(table, "String").getDirect(),
                (String[]) DataAccessHelpers
                        .getColumn(table.renameColumns("NewInt=Int", "NewString=String"), "NewString").getDirect());
        assertArrayEquals((int[]) DataAccessHelpers.getColumn(table, "Int").getDirect(),
                (int[]) DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int"), "NewInt").getDirect());
        assertEquals(table.getColumnSource("Double"),
                table.renameColumns("NewInt=Int").renameColumns("NewString=String").getColumnSource("Double"));
        try {
            DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int").renameColumns("NewString=String"), "Int");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }
        try {
            DataAccessHelpers.getColumn(table.renameColumns("NewInt=Int").renameColumns("NewString=String"), "String");
            fail("Expected exception");
        } catch (RuntimeException ignored) {
        }
    }

    public void testRenameColumnsIncremental() {
        final Random random = new Random(0);

        final int size = 100;
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.renameColumns(List.of())),
                EvalNugget.from(() -> queryTable.renameColumns("Symbol=Sym")),
                EvalNugget.from(() -> queryTable.renameColumns("Symbol=Sym", "Symbols=Sym")),
                EvalNugget.from(() -> queryTable.renameColumns("Sym2=Sym", "intCol2=intCol", "doubleCol2=doubleCol")),
        };

        // Verify our assumption that columns can be renamed at most once.
        Assert.assertTrue(queryTable.renameColumns("Symbol=Sym", "Symbols=Sym").hasColumns("Symbols"));
        Assert.assertFalse(queryTable.renameColumns("Symbol=Sym", "Symbols=Sym").hasColumns("Symbol"));

        final int steps = 100;
        for (int i = 0; i < steps; i++) {
            if (printTableUpdates) {
                System.out.println("\n == Simple Step i = " + i);
            }
            simulateShiftAwareStep("step == " + i, size, random, queryTable, columnInfo, en);
        }
    }

    public static WhereFilter stringContainsFilter(
            String columnName,
            String... values) {
        return stringContainsFilter(MatchType.Regular, columnName, values);
    }

    public static WhereFilter stringContainsFilter(
            MatchType matchType,
            String columnName,
            String... values) {
        return stringContainsFilter(CaseSensitivity.MatchCase, matchType, columnName, values);
    }

    public static WhereFilter stringContainsFilter(
            CaseSensitivity sensitivity,
            MatchType matchType,
            @NotNull String columnName,
            String... values) {
        return WhereFilterFactory.stringContainsFilter(sensitivity, matchType, columnName, true, false, values);
    }

    public void testStringContainsFilter() {
        Function<String, WhereFilter> filter = ConditionFilter::createConditionFilter;
        final Random random = new Random(0);

        final int size = 500;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"S1", "S2"},
                new StringGenerator(),
                new StringGenerator()));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(
                        table.where(filter.apply("S1.contains(`aab`)")),
                        table.where(stringContainsFilter("S1", "aab"))),
                new TableComparator(
                        table.where(filter.apply("S2.contains(`m`)")),
                        table.where(stringContainsFilter("S2", "m"))),
                new TableComparator(
                        table.where(filter.apply("!S2.contains(`ma`)")),
                        table.where(stringContainsFilter(MatchFilter.MatchType.Inverted, "S2", "ma"))),
                new TableComparator(
                        table.where(filter.apply("S2.toLowerCase().contains(`ma`)")),
                        table.where(stringContainsFilter(MatchFilter.CaseSensitivity.IgnoreCase,
                                MatchFilter.MatchType.Regular, "S2", "mA"))),
                new TableComparator(
                        table.where(filter.apply("S2.contains(`mA`)")),
                        table.where(stringContainsFilter("S2", "mA"))),
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testDoubleRangeFilterSimple() {
        final Table t = TableTools.newTable(doubleCol("DV", 1.0, 2.0, -3.0, Double.NaN, QueryConstants.NULL_DOUBLE, 6.0,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 9.0)).update("IV=i+1");
        Table leq1 = t.where("DV <= 1.0");
        Table leq1b = t.where("DV <= 1.0 && true");

        assertTableEquals(leq1b, leq1);
        assertTableEquals(
                TableTools.newTable(doubleCol("DV", 1.0, -3.0, QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY),
                        intCol("IV", 1, 3, 5, 8)),
                leq1);

        Table geq1 = t.where("DV >= 1.0");
        Table geq1b = t.where("DV >= 1.0 && true");
        TableTools.showWithRowSet(geq1);
        TableTools.showWithRowSet(geq1b);

        assertTableEquals(geq1b, geq1);
        assertTableEquals(TableTools.newTable(doubleCol("DV", 1.0, 2.0, Double.NaN, 6.0, Double.POSITIVE_INFINITY, 9.0),
                intCol("IV", 1, 2, 4, 6, 7, 9)), geq1);
    }

    public void testLongRangeFilterSimple() {
        final Table t = TableTools.newTable(longCol("LV", 1, 2, -3, Long.MAX_VALUE, QueryConstants.NULL_LONG, 6))
                .update("IV=i+1");
        Table leq1 = t.where("LV <= 1");
        Table leq1b = t.where("LV <= 1 && true");

        assertTableEquals(leq1b, leq1);
        assertTableEquals(TableTools.newTable(longCol("LV", 1, -3, QueryConstants.NULL_LONG), intCol("IV", 1, 3, 5)),
                leq1);

        Table geq1 = t.where("LV >= 1");
        Table geq1b = t.where("LV >= 1 && true");
        TableTools.showWithRowSet(geq1);
        TableTools.showWithRowSet(geq1b);

        assertTableEquals(geq1b, geq1);
        assertTableEquals(TableTools.newTable(longCol("LV", 1, 2, Long.MAX_VALUE, 6), intCol("IV", 1, 2, 4, 6)), geq1);
    }

    public void testComparableRangeFilterSimple() {
        final Table t = TableTools.newTable(longCol("LV", 1, 2, -3, Long.MAX_VALUE, QueryConstants.NULL_LONG, 6))
                .update("IV=i+1", "LV=LV==null ? null : java.math.BigInteger.valueOf(LV)");
        Table leq1 = t.where("LV <= 1");
        Table leq1b = t.where("io.deephaven.util.compare.ObjectComparisons.leq(LV, java.math.BigInteger.ONE)");
        Table leq1c = t.where("LV <= java.math.BigInteger.ONE");

        assertTableEquals(leq1b, leq1);
        assertTableEquals(leq1c, leq1);
        assertTableEquals(TableTools.newTable(intCol("IV", 1, 3, 5)), leq1.dropColumns("LV"));

        Table geq1 = t.where("LV >= 1");
        Table geq1b = t.where("io.deephaven.util.compare.ObjectComparisons.geq(LV, java.math.BigInteger.ONE)");
        Table geq1c = t.where("LV >= java.math.BigInteger.ONE");
        TableTools.showWithRowSet(geq1);
        TableTools.showWithRowSet(geq1b);

        assertTableEquals(geq1b, geq1);
        assertTableEquals(geq1c, geq1);
        assertTableEquals(TableTools.newTable(intCol("IV", 1, 2, 4, 6)), geq1.dropColumns("LV"));
    }

    public void testDoubleRangeFilter() {
        Function<String, WhereFilter> filter = ConditionFilter::createConditionFilter;
        final Random random = new Random(0);

        final int size = 500;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"D1", "D2", "F1", "F2"},
                        new DoubleGenerator(),
                        new DoubleGenerator(0, 1000, 0.1, 0.1),
                        new FloatGenerator(),
                        new FloatGenerator(0, 1000, 0.1, 0.1)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(table.where(filter.apply("D1 > 500 && D1 <= 501")),
                        table.where(new DoubleRangeFilter("D1", 500, 501, false, true))),
                new TableComparator(table.where(filter.apply("D1 > 500.7 && D1 <= 500.8")),
                        table.where(new DoubleRangeFilter("D1", 500.7, 500.8, false, true))),
                new TableComparator(table.where(filter.apply("D2 >= 250.02 && D2 < 250.03")),
                        table.where(DoubleRangeFilter.makeRange("D2", "250.02"))),
                new TableComparator(table.where(filter.apply("F1 > 500 && F1 <= 501")),
                        table.where(new FloatRangeFilter("F1", 500, 501, false, true))),
                new TableComparator(table.where(filter.apply("F1 > 500.7 && F1 <= 500.8")),
                        table.where(new FloatRangeFilter("F1", 500.7f, 500.8f, false, true))),
                new TableComparator(table.where(filter.apply("F2 >= 250.02 && F2 < 250.03")),
                        table.where(FloatRangeFilter.makeRange("F2", "250.02"))),

                new TableComparator(table.where(filter.apply("F1 <= -250.02 && F1 > -250.03")),
                        table.where(FloatRangeFilter.makeRange("F1", "-250.02"))),
                new TableComparator(table.where(filter.apply("F1 <= -37.0002 && F1 > -37.0003")),
                        table.where(FloatRangeFilter.makeRange("F1", "-37.0002"))),
                new TableComparator(table.where(filter.apply("D1 <= -250.02 && D1 > -250.03")),
                        table.where(DoubleRangeFilter.makeRange("D1", "-250.02"))),
                new TableComparator(table.where(filter.apply("D1 <= -37.0002 && D1 > -37.0003")),
                        table.where(DoubleRangeFilter.makeRange("D1", "-37.0002")))
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testInstantRangeFilter() {
        Function<String, WhereFilter> filter = ConditionFilter::createConditionFilter;
        final Random random = new Random(0);

        final int size = 500;

        final Instant startTime = DateTimeUtils.parseInstant("2019-04-30T16:00:00 NY");
        final Instant endTime = DateTimeUtils.parseInstant("2019-04-30T16:01:00 NY");

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Timestamp", "Ts2", "Sentinel"},
                        new UnsortedInstantGenerator(startTime, endTime),
                        new UnsortedInstantLongGenerator(startTime, endTime),
                        new IntGenerator(0, 1000)));

        final Instant lower = DateTimeUtils.plus(startTime, DateTimeUtils.SECOND);
        final Instant upper = DateTimeUtils.plus(startTime, DateTimeUtils.SECOND * 2);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp >= '" + lower.toString() + "' && Timestamp <= '" + upper.toString() + "'")),
                        "Condition", table.where(new InstantRangeFilter("Timestamp", lower, upper, true, true)),
                        "Range"),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp >= '" + lower.toString() + "' && Timestamp < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, true, false))),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp > '" + lower.toString() + "' && Timestamp <= '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, true, true))),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp > '" + lower.toString() + "' && Timestamp < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, false, false))),

                new TableComparator(
                        table.where(
                                filter.apply("Ts2 >= '" + lower.toString() + "' && Ts2 <= '" + upper.toString() + "'")),
                        "Condition", table.where(new InstantRangeFilter("Ts2", lower, upper, true, true)), "Range"),
                new TableComparator(
                        table.where(
                                filter.apply("Ts2 >= '" + lower.toString() + "' && Ts2 < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Ts2", lower, upper, true, false))),
                new TableComparator(
                        table.where(
                                filter.apply("Ts2 > '" + lower.toString() + "' && Ts2 <= '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Ts2", lower, upper, true, true))),
                new TableComparator(
                        table.where(
                                filter.apply("Ts2 > '" + lower.toString() + "' && Ts2 < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Ts2", lower, upper, false, false)))
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testInstantRangeFilterNulls() {
        final Function<String, WhereFilter> filter = ConditionFilter::createConditionFilter;
        final Random random = new Random(0);

        final int size = 500;

        final Instant startTime = DateTimeUtils.parseInstant("2019-04-30T16:00:00 NY");
        final Instant endTime = DateTimeUtils.parseInstant("2019-04-30T16:01:00 NY");

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Timestamp", "Sentinel"},
                        new UnsortedInstantGenerator(startTime, endTime, 0.1),
                        new IntGenerator(0, 1000)));

        final Instant lower = DateTimeUtils.plus(startTime, DateTimeUtils.SECOND);
        final Instant upper = DateTimeUtils.plus(startTime, DateTimeUtils.SECOND * 2);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp >= '" + lower.toString() + "' && Timestamp <= '" + upper.toString() + "'")),
                        "Condition", table.where(new InstantRangeFilter("Timestamp", lower, upper, true, true)),
                        "Range"),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp >= '" + lower.toString() + "' && Timestamp < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, true, false))),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp > '" + lower.toString() + "' && Timestamp <= '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, true, true))),
                new TableComparator(
                        table.where(filter.apply(
                                "Timestamp > '" + lower.toString() + "' && Timestamp < '" + upper.toString() + "'")),
                        table.where(new InstantRangeFilter("Timestamp", lower, upper, false, false))),
        };

        for (int i = 0; i < 500; i++) {
            simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    public void testReverse() {
        final QueryTable table = testRefreshingTable(i(1, 2, 3).toTracking(),
                col("Ticker", "AAPL", "IBM", "TSLA"),
                col("Timestamp", 1L, 10L, 50L));

        final Table reversed = table.reverse();
        show(reversed);

        checkReverse(table, reversed, "Ticker");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final ColumnHolder<?>[] columnAdditions =
                    new ColumnHolder<?>[] {col("Ticker", "SPY", "VXX"), col("Timestamp", 60L, 70L)};
            addToTable(table, i(2048, 2049), columnAdditions);
            table.notifyListeners(i(2048, 2049), i(), i());
        });

        show(reversed);

        checkReverse(table, reversed, "Ticker");

        assertEquals("TSLA", reversed.getColumnSource("Ticker").getPrev(reversed.getRowSet().copyPrev().get(0)));


        updateGraph.runWithinUnitTestCycle(() -> {
        });

        assertEquals("VXX", reversed.getColumnSource("Ticker").getPrev(reversed.getRowSet().copyPrev().get(0)));


        final ColumnSource<Long> longIdentityColumnSource =
                new AbstractColumnSource.DefaultedImmutable<>(long.class) {
                    @Override
                    public void startTrackingPrevValues() { /* nothing to do */ }

                    @Override
                    public Long get(long rowKey) {
                        return getLong(rowKey);
                    }

                    @Override
                    public long getLong(long rowKey) {
                        return rowKey;
                    }
                };

        final QueryTable bigTable = new QueryTable(
                i(0, Integer.MAX_VALUE, (long) Integer.MAX_VALUE * 2L).toTracking(),
                Collections.singletonMap("LICS", longIdentityColumnSource));
        bigTable.setRefreshing(true);
        final Table bigReversed = bigTable.reverse();
        // noinspection unchecked
        final DataColumn<Long> licsr = DataAccessHelpers.getColumn(bigReversed, "LICS");
        assertEquals((long) Integer.MAX_VALUE * 2L, (long) licsr.get(0));
        assertEquals(Integer.MAX_VALUE, (long) licsr.get(1));
        assertEquals(0, (long) licsr.get(2));

        updateGraph.runWithinUnitTestCycle(() -> {
            bigTable.getRowSet().writableCast().insert(Long.MAX_VALUE);
            bigTable.notifyListeners(i(Long.MAX_VALUE), i(), i());
        });

        assertEquals(4, bigReversed.size());

        assertEquals(Long.MAX_VALUE, (long) licsr.get(0));
        assertEquals((long) Integer.MAX_VALUE * 2L, (long) licsr.get(1));
        assertEquals(Integer.MAX_VALUE, (long) licsr.get(2));
        assertEquals(0, (long) licsr.get(3));

    }


    public void testReverse2() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), col("Timestamp", 1L));

        final Table reversed = table.reverse();
        show(reversed);

        checkReverse(table, reversed, "Timestamp");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final ColumnHolder<?>[] columnAdditions1 = new ColumnHolder<?>[] {col("Timestamp", 2048L, 2049L)};
            addToTable(table, i(2048, 2049), columnAdditions1);
            table.notifyListeners(i(2048, 2049), i(), i());
        });

        show(reversed);

        checkReverse(table, reversed, "Timestamp");

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(1, 2048, 2049));
            table.notifyListeners(i(), i(1, 2048, 2049), i());
        });

        assertEquals(0, reversed.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            final ColumnHolder<?>[] columnAdditions = new ColumnHolder<?>[] {col("Timestamp", 8192L)};
            addToTable(table, i(8192L), columnAdditions);
            table.notifyListeners(i(8192L), i(), i());
        });

        checkReverse(table, reversed, "Timestamp");


    }

    private void checkReverse(QueryTable table, Table reversed, String timestamp) {
        assertEquals(table.size(), reversed.size());
        for (long ii = 0; ii < table.size(); ++ii) {
            final long jj = table.size() - ii - 1;
            assertEquals(DataAccessHelpers.getColumn(table, timestamp).get(ii),
                    DataAccessHelpers.getColumn(reversed, timestamp).get(jj));
        }
    }

    public void testReverseClipping() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), col("Sentinel", 1));

        final QueryTable reverseTable = (QueryTable) table.reverse();
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(reverseTable);
        reverseTable.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.added = i();
            downstream.removed = i();
            downstream.modified = i(1);
            downstream.modifiedColumnSet = ModifiedColumnSet.ALL;

            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(1 << 29, 1 << 30, 1024);
            downstream.shifted = builder.build();
            table.notifyListeners(downstream);
        });

        assertNotNull(listener.update);
        assertNotNull(listener.update.shifted());
        assertEquals(0, listener.update.shifted().size());
    }

    public void testReverseClippingDuringShift() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), col("Sentinel", 1));
        final QueryTable reversedTable = (QueryTable) table.reverse();

        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(reversedTable);
        reversedTable.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.added = i();
            downstream.removed = i();
            downstream.modified = i();
            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 1024, 1 << 20);
            builder.shiftRange(1 << 29, 1 << 30, 1024);
            downstream.shifted = builder.build();

            try (final WritableRowSet rowSetCopy = table.getRowSet().copy()) {
                downstream.shifted().apply(rowSetCopy);
                removeRows(table, table.getRowSet());
                addToTable(table, rowSetCopy, col("Sentinel", 1));
            }

            table.notifyListeners(downstream);
        });

        assertNotNull(listener.update);
        assertNotNull(listener.update.shifted());
        assertEquals(1, listener.update.shifted().size());
        assertEquals(table.reverse().getRowSet(), reversedTable.getRowSet());
    }

    public void testReverseIncremental() throws ParseException {
        final Random random = new Random(0);

        final int size = 1000;
        // Increase this number if your BigIntegers collide
        final int bitSize = 1000;
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Date", "C1", "C2", "KEY"},
                        new DateGenerator(format.parse("2011-02-02"), format.parse("2011-02-03")),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30),
                        new SortedBigIntegerGenerator(bitSize)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> table.update("K=KEY").reverse()),
                new TableComparator(table.update("K=KEY").reverse(), table.update("K=KEY").sortDescending("K"))
        };

        final int updateSize = (int) Math.ceil(Math.sqrt(size));
        for (int step = 0; step < 500; step++) {
            simulateShiftAwareStep(updateSize, random, table, columnInfo, en);
        }
    }

    public void testReverseBlink() {
        final Table table = testRefreshingTable(RowSetFactory.flat(1).toTracking(), intCol("Sentinel", 100))
                .withAttributes(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true));
        final Table reverseTable = table.reverse();
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(reverseTable);
        reverseTable.addUpdateListener(listener);

        final long nextSize = ReverseOperation.MINIMUM_PIVOT + 2;
        final int[] data = new int[Math.toIntExact(nextSize - 1)];
        Arrays.fill(data, 200);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(table, RowSetFactory.fromRange(1, nextSize - 1), intCol("Sentinel", data));
            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.added = RowSetFactory.flat(nextSize);
            downstream.removed = RowSetFactory.flat(1);
            downstream.modified = i();
            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            downstream.shifted = RowSetShiftData.EMPTY;
            ((QueryTable) table).notifyListeners(downstream);
        });

        assertNotNull(listener.update);
        assertNotNull(listener.update.shifted());
        assertEquals(0, listener.update.shifted().size());
    }

    public void testSnapshot() {
        final QueryTable base = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));
        final QueryTable trigger1 = testRefreshingTable(col("T", 1));
        final Table expected = base.naturalJoin(trigger1, "", "T");
        TableTools.showWithRowSet(expected);
        final Table actual = base.snapshotWhen(trigger1, Flag.INITIAL);
        validateUpdates(actual);
        assertTableEquals(expected, actual);

        assertTableEquals(base.head(0).updateView("T=1"), base.snapshotWhen(trigger1));

        final QueryTable trigger2 = testRefreshingTable(col("T", 1, 2));
        final Table snapshot = base.snapshotWhen(trigger2, Flag.INITIAL);
        validateUpdates(snapshot);

        final Table expect1 = newTable(col("A", 3, 1, 2), col("B", "c", "a", "b"), col("T", 2, 2, 2));
        assertTableEquals(expect1, snapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            base.notifyListeners(i(20, 40), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(expect1, snapshot);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(3), col("T", 5));
            trigger2.notifyListeners(i(3), i(), i());
        });
        show(snapshot, 50);
        final Table expect2 =
                newTable(col("A", 3, 30, 1, 2, 50), col("B", "c", "aa", "a", "b", "bc"), col("T", 5, 5, 5, 5, 5));
        assertTableEquals(expect2, snapshot);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(10, 20, 30));
            addToTable(base, i(25), col("A", 11), col("B", "A"));
            base.notifyListeners(i(), i(10, 20, 30), i(25));
        });
        show(snapshot, 50);
        assertTableEquals(expect2, snapshot);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(4, 5), col("T", 7, 8));
            trigger2.notifyListeners(i(4, 5), i(), i());
        });
        show(base, 50);
        show(snapshot, 50);
        final Table expect3 = newTable(col("A", 11, 50), col("B", "A", "bc"), col("T", 8, 8));
        assertTableEquals(expect3, snapshot);
    }

    public void testSnapshotArrayTrigger() {
        final QueryTable base = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));

        final QueryTable left1 = testRefreshingTable(col("T", 1));
        final Table leftBy = left1.aggBy(AggGroup("T"));

        final Table expected = base.naturalJoin(leftBy, "", "T");
        TableTools.showWithRowSet(expected);
        final Table actual = base.snapshotWhen(leftBy, Flag.INITIAL);
        validateUpdates(actual);
        assertTableEquals(expected, actual);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            base.notifyListeners(i(20, 40), i(), i());
        });
        assertTableEquals(expected.where("A in 1, 2, 3"), actual);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left1, i(3), col("T", 5));
            left1.notifyListeners(i(3), i(), i());
        });
        assertTableEquals(expected, actual);
    }

    public void testSnapshotArrayValues() {
        final QueryTable right = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));
        final Table rightBy = right.aggAllBy(AggSpec.group());

        final QueryTable trigger1 = testRefreshingTable(col("T", 1));
        // noinspection RedundantArrayCreation
        final Table ex1 = newTable(col("A", new IntVector[] {new IntVectorDirect(3, 1, 2)}),
                col("B", new ObjectVector[] {new ObjectVectorDirect<>("c", "a", "b")}), intCol("T", 1));
        TableTools.showWithRowSet(ex1);

        final Table actual = rightBy.snapshotWhen(trigger1, Flag.INITIAL);
        validateUpdates(actual);
        assertTableEquals(ex1, actual);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            right.notifyListeners(i(20, 40), i(), i());
        });
        assertTableEquals(ex1, actual);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger1, i(3), col("T", 5));
            trigger1.notifyListeners(i(3), i(), i());
        });
        final Table ex2 = newTable(col("A", new IntVector[] {new IntVectorDirect(3, 30, 1, 2, 50)}),
                col("B", new ObjectVector[] {new ObjectVectorDirect<>("c", "aa", "a", "b", "bc")}), intCol("T", 5));
        assertTableEquals(ex2, actual);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(20), intCol("A", 31), stringCol("B", "aaa"));
            right.notifyListeners(i(), i(), i(20));
        });
        assertTableEquals(ex2, actual);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger1, i(4), col("T", 6));
            trigger1.notifyListeners(i(4), i(), i());
        });

        final Table ex3 = newTable(col("A", new IntVector[] {new IntVectorDirect(3, 31, 1, 2, 50)}),
                col("B", new ObjectVector[] {new ObjectVectorDirect<>("c", "aaa", "a", "b", "bc")}), intCol("T", 6));
        assertTableEquals(ex3, actual);
    }

    public void testSnapshotHistorical() {
        final QueryTable base = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));
        final QueryTable trigger1 = testRefreshingTable(col("T", 1));
        show(base.snapshotWhen(trigger1, Flag.HISTORY));
        assertEquals("", diff(base.snapshotWhen(trigger1, Flag.HISTORY),
                testRefreshingTable(col("T", 1, 1, 1), col("A", 3, 1, 2), col("B", "c", "a", "b")), 10));

        final QueryTable trigger2 = testRefreshingTable(col("T", 1, 2));
        final Table snapshot = base.snapshotWhen(trigger2, Flag.HISTORY);
        show(snapshot);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2),
                col("A", 3, 1, 2, 3, 1, 2),
                col("B", "c", "a", "b", "c", "a", "b")));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            base.notifyListeners(i(20, 40), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2),
                col("A", 3, 1, 2, 3, 1, 2),
                col("B", "c", "a", "b", "c", "a", "b")));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(3), col("T", 5));
            trigger2.notifyListeners(i(3), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2, 5, 5, 5, 5, 5),
                col("A", 3, 1, 2, 3, 1, 2, 3, 30, 1, 2, 50),
                col("B", "c", "a", "b", "c", "a", "b", "c", "aa", "a", "b", "bc")));

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(10, 20, 30));
            addToTable(base, i(25), col("A", 11), col("B", "A"));
            base.notifyListeners(i(), i(10, 20, 30), i(25));
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2, 5, 5, 5, 5, 5),
                col("A", 3, 1, 2, 3, 1, 2, 3, 30, 1, 2, 50),
                col("B", "c", "a", "b", "c", "a", "b", "c", "aa", "a", "b", "bc")));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(4, 5), col("T", 7, 8));
            trigger2.notifyListeners(i(4, 5), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2, 5, 5, 5, 5, 5, 7, 7, 8, 8),
                col("A", 3, 1, 2, 3, 1, 2, 3, 30, 1, 2, 50, 11, 50, 11, 50),
                col("B", "c", "a", "b", "c", "a", "b", "c", "aa", "a", "b", "bc", "A", "bc", "A", "bc")));

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet rowsToRemove = base.getRowSet().copy();
            removeRows(base, rowsToRemove);
            base.notifyListeners(i(), rowsToRemove, i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2, 5, 5, 5, 5, 5, 7, 7, 8, 8),
                col("A", 3, 1, 2, 3, 1, 2, 3, 30, 1, 2, 50, 11, 50, 11, 50),
                col("B", "c", "a", "b", "c", "a", "b", "c", "aa", "a", "b", "bc", "A", "bc", "A", "bc")));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(6), col("T", 9));
            trigger2.notifyListeners(i(6), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("T", 1, 1, 1, 2, 2, 2, 5, 5, 5, 5, 5, 7, 7, 8, 8),
                col("A", 3, 1, 2, 3, 1, 2, 3, 30, 1, 2, 50, 11, 50, 11, 50),
                col("B", "c", "a", "b", "c", "a", "b", "c", "aa", "a", "b", "bc", "A", "bc", "A", "bc")));
    }

    public void testSnapshotDependencies() {
        final QueryTable base = testRefreshingTable(i(10).toTracking(), col("A", 1));
        final QueryTable trigger = testRefreshingTable(col("T", 1));

        QueryScope.addParam("testSnapshotDependenciesCounter", new AtomicInteger());

        final Table snappedFirst = base.snapshotWhen(trigger, Flag.INITIAL);
        validateUpdates(snappedFirst);
        final Table snappedDep = snappedFirst.select("B=testSnapshotDependenciesCounter.incrementAndGet()");
        final Table snappedOfSnap = snappedDep.snapshotWhen(trigger, Flag.INITIAL);
        validateUpdates(snappedOfSnap);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
        });

        // This will do the notification for left; at which point we can do the first snapshot
        // This should flush the TUV and the select
        // Now we should flush the second snapshot
        // This should flush the second TUV
        // And now we should be done
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger, i(2), col("T", 2));
            trigger.notifyListeners(i(2), i(), i());
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // This will do the notification for left; at which point we can do the first snapshot
            boolean flushed = updateGraph.flushOneNotificationForUnitTests();

            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // This should flush the TUV and the select
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // Now we should flush the second snapshot
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            // Which also generates a result notification as a pass-through
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // This should flush the second TUV
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            // Which also generates a result notification as a pass-through
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);

            // And now we should be done
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertFalse(flushed);
        });
        TableTools.show(snappedOfSnap);

        TestCase.assertEquals(1, snappedOfSnap.size());
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(snappedOfSnap, "B").get(0));
    }

    public void testSnapshotAdditions() {
        final QueryTable base = testRefreshingTable(i(10).toTracking(), col("A", 1));
        final QueryTable trigger = testRefreshingTable(col("T", 1));

        final Table snapshot = base.snapshotWhen(trigger, Flag.INITIAL);
        validateUpdates(snapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(20), col("A", 2));
            trigger.notifyListeners(i(), i(), i(0));
        });

        TestCase.assertEquals(2, snapshot.size());
        assertTableEquals(testTable(col("A", 1, 2), col("T", 1, 1)), snapshot);
    }

    public void testSnapshotRemovals() {
        final QueryTable base = testRefreshingTable(i(10, 20).toTracking(), col("A", 1, 2));
        final QueryTable trigger = testRefreshingTable(col("T", 1));

        final Table snapshot = base.snapshotWhen(trigger, Flag.INITIAL);
        validateUpdates(snapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(20));
            trigger.notifyListeners(i(), i(), i(0));
        });

        TestCase.assertEquals(1, snapshot.size());
        assertTableEquals(testTable(col("A", 1), col("T", 1)), snapshot);
    }

    public void testSnapshotModifies() {
        final QueryTable base = testRefreshingTable(i(10).toTracking(), col("A", 1));
        final QueryTable trigger = testRefreshingTable(col("T", 1));

        final Table snapshot = base.snapshotWhen(trigger, Flag.INITIAL);
        validateUpdates(snapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            base.notifyListeners(i(), i(), i(20));
            trigger.notifyListeners(i(), i(), i(0));
        });

        TestCase.assertEquals(1, snapshot.size());
        assertTableEquals(testTable(col("A", 1), col("T", 1)), snapshot);
    }

    public void testSnapshotIncrementalDependencies() {
        final QueryTable base = testRefreshingTable(i(10).toTracking(), col("A", 1));
        final QueryTable trigger = testRefreshingTable(col("T", 1));

        QueryScope.addParam("testSnapshotDependenciesCounter", new AtomicInteger());

        final Table snappedFirst = base.snapshotWhen(trigger, Flag.INCREMENTAL);
        final Table snappedDep = snappedFirst.select("B=testSnapshotDependenciesCounter.incrementAndGet()");
        final Table snappedOfSnap = snappedDep.snapshotWhen(trigger, Flag.INCREMENTAL);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            System.out.println("Checking everything is satisfied with no updates.");
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            System.out.println("Simple Update Cycle Complete.");
        });

        updateGraph.runWithinUnitTestCycle(() -> {
            System.out.println("Adding Table.");
            addToTable(trigger, i(2), col("T", 2));
            trigger.notifyListeners(i(2), i(), i());

            System.out.println("Checking initial satisfaction.");
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing first notification.");
            // this will do the notification for left
            boolean flushed2 = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #1.");
            TestCase.assertTrue(flushed2);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing second notification, which should be our listener recorder");
            flushed2 = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #2.");
            TestCase.assertTrue(flushed2);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing third notification, which should be our merged listener");

            System.out.println("Checking satisfaction after #3.");
            flushed2 = updateGraph.flushOneNotificationForUnitTests();
            // this will do the merged notification; which means the snaphsot is satisfied
            TestCase.assertTrue(flushed2);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the select
            flushed2 = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed2);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the second snapshot recorder
            flushed2 = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed2);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the second snapshot merged listener
            flushed2 = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed2);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // nothing left
            flushed2 = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertFalse(flushed2);
        });
        TableTools.show(snappedOfSnap);

        TestCase.assertEquals(snappedOfSnap.size(), 1);
        TestCase.assertEquals(DataAccessHelpers.getColumn(snappedOfSnap, "B").get(0), 1);

        // this will do the notification for right; at which point we can should get the update going through
        // nothing left
        updateGraph.runWithinUnitTestCycle(() -> {
            System.out.println("Adding Right Table.");
            addToTable(base, i(2), col("A", 3));
            base.notifyListeners(i(2), i(), i());

            System.out.println("Checking initial satisfaction.");
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing first notification.");
            // this will do the notification for right; at which point we can should get the update going through
            boolean flushed1 = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #1.");
            TestCase.assertTrue(flushed1);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing second notification, which should be our merged listener");
            flushed1 = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #2.");
            TestCase.assertTrue(flushed1);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // nothing left
            flushed1 = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertFalse(flushed1);
        });
        TableTools.show(snappedOfSnap);

        TestCase.assertEquals(snappedOfSnap.size(), 1);
        TestCase.assertEquals(DataAccessHelpers.getColumn(snappedOfSnap, "B").get(0), 1);

        // now we should flush the select
        // now we should flush the second snapshot recorder
        // now we should flush the second snapshot merged listener
        // nothing left
        updateGraph.runWithinUnitTestCycle(() -> {
            System.out.println("Adding Right Table.");
            addToTable(base, i(2), col("A", 3));
            base.notifyListeners(i(2), i(), i());

            System.out.println("Adding Left Table.");
            addToTable(trigger, i(3), col("T", 3));
            trigger.notifyListeners(i(3), i(), i());

            System.out.println("Checking initial satisfaction.");
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing first notification.");
            boolean flushed = updateGraph.flushOneNotificationForUnitTests();
            System.out.println("Checking satisfaction after #1.");
            TestCase.assertTrue(flushed);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing second notification, which should be the recorder for our second snapshot");
            flushed = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #2.");
            TestCase.assertTrue(flushed);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing third notification, which should be our right recorder");
            flushed = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #3.");
            TestCase.assertTrue(flushed);
            TestCase.assertFalse(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            System.out.println("Flushing fourth notification, which should be our MergedListener");
            flushed = updateGraph.flushOneNotificationForUnitTests();

            System.out.println("Checking satisfaction after #4.");
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the select
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the second snapshot recorder
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertFalse(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // now we should flush the second snapshot merged listener
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertTrue(flushed);
            TestCase.assertTrue(
                    snappedFirst.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedDep.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));
            TestCase.assertTrue(
                    snappedOfSnap.satisfied(ExecutionContext.getContext().getUpdateGraph().clock().currentStep()));

            // nothing left
            flushed = updateGraph.flushOneNotificationForUnitTests();
            TestCase.assertFalse(flushed);
        });
        TableTools.show(snappedOfSnap);

        TestCase.assertEquals(snappedOfSnap.size(), 2);
        TestCase.assertEquals(DataAccessHelpers.getColumn(snappedOfSnap, "B").get(0), 2);
    }

    public void testWhereInScope() {
        final Table toBeFiltered = testRefreshingTable(
                TableTools.col("Key", "A", "B", "C", "D", "E"),
                TableTools.intCol("Value", 1, 2, 3, 4, 5));

        // The setScope will own the set table. rc == 1
        final SafeCloseable setScope = LivenessScopeStack.open();
        final QueryTable setTable = testRefreshingTable(TableTools.stringCol("Key"));

        // Owned by setScope, rc == 1
        // It will also manage setTable whose rc == 3 after (1 SwapListener, 1 SwapListener from groupBy)
        final Table whereIn = toBeFiltered.whereIn(setTable, "Key");

        // Manage it rcs == (2,3)
        final SingletonLivenessManager singletonManager = new SingletonLivenessManager(whereIn);

        // This will release setTable once and whereIn once, Rcs are now (1,2)
        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(setScope::close);

        assertEquals(0, whereIn.size());

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        addToTable(setTable, i(0), col("Key", "B"));
        setTable.notifyListeners(i(0), i(), i());
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();

        assertEquals(1, whereIn.size());
        assertEquals(new Object[] {"B", 2}, DataAccessHelpers.getRecord(whereIn, 0));

        assertTrue(whereIn.tryRetainReference());
        whereIn.dropReference();

        assertTrue(setTable.tryRetainReference());
        setTable.dropReference();

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        addToTable(setTable, i(1), col("Key", "D"));
        setTable.notifyListeners(i(1), i(), i());
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();

        assertEquals(2, whereIn.size());
        assertEquals(new Object[] {"B", 2}, DataAccessHelpers.getRecord(whereIn, 0));
        assertEquals(new Object[] {"D", 4}, DataAccessHelpers.getRecord(whereIn, 1));

        // Everything is dropped after this, the singletonManager was holding everything.
        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(singletonManager::release);

        assertFalse(whereIn.tryRetainReference());
        assertFalse(setTable.tryRetainReference());
    }

    public void testSnapshotIncremental() {
        QueryTable base = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));
        QueryTable trigger = testRefreshingTable(col("T", 1));
        Table empty = base.snapshotWhen(trigger, Flag.INCREMENTAL);
        assertEquals("", diff(empty, testRefreshingTable(intCol("A"), stringCol("B"), intCol("T")), 10));

        final QueryTable trigger2 = testRefreshingTable(col("T", 1, 2));

        final Table snapshot = base.snapshotWhen(trigger2, Flag.INCREMENTAL);
        System.out.println("Initial table:");
        show(snapshot);
        System.out.println("Initial prev:");
        show(prevTable(snapshot));
        assertTableEquals(snapshot, testRefreshingTable(intCol("A"), stringCol("B"), intCol("T")));

        final ListenerWithGlobals listener;
        snapshot.addUpdateListener(listener = newListenerWithGlobals(snapshot));
        listener.reset();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            base.notifyListeners(i(20, 40), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(intCol("A"), stringCol("B"), intCol("T")));
        assertEquals(listener.getCount(), 0);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(3), col("T", 5));
            trigger2.notifyListeners(i(3), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("A", 3, 30, 1, 2, 50),
                col("B", "c", "aa", "a", "b", "bc"),
                col("T", 5, 5, 5, 5, 5)));
        assertEquals(listener.getCount(), 1);
        assertEquals(base.getRowSet(), added);
        assertEquals(i(), modified);
        assertEquals(i(), removed);
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(10, 20, 30));
            addToTable(base, i(25, 75), col("A", 11, 34), col("B", "A", "Q"));
            base.notifyListeners(i(75), i(10, 20, 30), i(25));
        });
        TableTools.showWithRowSet(snapshot, 50);
        assertTableEquals(snapshot, testRefreshingTable(
                col("A", 3, 30, 1, 2, 50),
                col("B", "c", "aa", "a", "b", "bc"),
                col("T", 5, 5, 5, 5, 5)));
        assertEquals(listener.getCount(), 0);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger2, i(4, 5), col("T", 7, 8));
            trigger2.notifyListeners(i(4, 5), i(), i());
        });
        System.out.println("Right Table:");
        TableTools.showWithRowSet(base, 50);
        System.out.println("Snapshot Table:");
        TableTools.showWithRowSet(snapshot, 50);

        assertTableEquals(snapshot, testRefreshingTable(
                col("A", 11, 50, 34),
                col("B", "A", "bc", "Q"),
                col("T", 8, 5, 8)));
        assertEquals(listener.getCount(), 1);
        assertEquals(i(75), added);
        assertEquals(i(25), modified);
        assertEquals(i(10, 20, 30), removed);
        listener.reset();
    }

    public void testSnapshotIncrementalBigInitial() {
        final int size = 1000000;
        final Table base = emptyTable(size).update("X=Long.toString(ii)", "I=ii");
        final QueryTable trigger = testRefreshingTable(col("T", 1));
        final Table result = base.snapshotWhen(trigger, Flag.INCREMENTAL, Flag.INITIAL);
        final Table expected = emptyTable(size).updateView("X=Long.toString(ii)", "I=ii", "T=1");
        assertTableEquals(expected, result);

        final Table result2 = base.snapshotWhen(trigger, Flag.INCREMENTAL);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger, i(1), col("T", 2));
            trigger.notifyListeners(i(1), i(), i());
        });
        final Table expected2 = emptyTable(size).updateView("X=Long.toString(ii)", "I=ii", "T=2");
        assertTableEquals(expected2, result2);
    }

    public void testSnapshotIncrementalPrev() {
        final QueryTable base = testRefreshingTable(i(10, 25, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));
        final QueryTable trigger = testRefreshingTable(col("T", 1, 2));

        final Table snapshot = base.snapshotWhen(trigger, Flag.INCREMENTAL, Flag.INITIAL);
        validateUpdates(snapshot);

        System.out.println("Initial table:");
        show(snapshot);
        System.out.println("Initial prev:");
        show(prevTable(snapshot));
        final QueryTable firstResult =
                testRefreshingTable(col("A", 3, 1, 2), col("B", "c", "a", "b"), col("T", 2, 2, 2));
        assertTableEquals(snapshot, firstResult);
        assertTableEquals(prevTable(snapshot), firstResult);

        final io.deephaven.engine.table.impl.SimpleListener listener;
        snapshot.addUpdateListener(listener = new io.deephaven.engine.table.impl.SimpleListener(snapshot));
        listener.reset();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            assertTableEquals(prevTable(snapshot), firstResult);
            assertTableEquals(snapshot, firstResult);

            addToTable(base, i(20, 40), col("A", 30, 50), col("B", "aa", "bc"));
            base.notifyListeners(i(20, 40), i(), i());
        });
        show(snapshot, 50);
        assertTableEquals(snapshot, firstResult);
        assertTableEquals(prevTable(snapshot), firstResult);
        assertEquals(listener.getCount(), 0);


        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger, i(3), col("T", 5));
            trigger.notifyListeners(i(3), i(), i());
            assertEquals("", diff(prevTable(snapshot), firstResult, 10));
        });

        show(snapshot, 50);
        final QueryTable secondResult =
                testRefreshingTable(col("A", 3, 30, 1, 2, 50), col("B", "c", "aa", "a", "b", "bc"),
                        col("T", 2, 5, 2, 2, 5));
        assertTableEquals(snapshot, secondResult);
        assertEquals(listener.getCount(), 1);
        assertEquals(i(20, 40), listener.update.added());
        assertEquals(i(), listener.update.modified());
        assertEquals(i(), listener.update.removed());
        assertTrue(listener.update.shifted().empty());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(base, i(10, 20, 30));
            addToTable(base, i(25, 75), col("A", 11, 34), col("B", "A", "Q"));
            base.notifyListeners(i(75), i(10, 20, 30), i(25));
        });
        TableTools.showWithRowSet(snapshot, 50);
        assertTableEquals(snapshot, secondResult);
        assertEquals(listener.getCount(), 0);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger, i(4, 5), col("T", 7, 8));
            trigger.notifyListeners(i(4, 5), i(), i());
        });

        final QueryTable thirdResult =
                testRefreshingTable(col("A", 11, 50, 34), col("B", "A", "bc", "Q"), col("T", 8, 5, 8));
        assertTableEquals(snapshot, thirdResult);
        assertEquals(listener.getCount(), 1);
        assertEquals(i(75), listener.update.added());
        assertEquals(i(25), listener.update.modified());
        assertEquals(i(10, 20, 30), listener.update.removed());
        assertTrue(listener.update.shifted().empty());
        listener.reset();

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(base, i(25), col("A", 12), col("B", "R"));
            base.notifyListeners(i(), i(), i(25));
        });

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(trigger, i(6), col("T", 9));
            trigger.notifyListeners(i(6), i(), i());
        });

        final QueryTable fourthResult =
                testRefreshingTable(col("A", 12, 50, 34), col("B", "R", "bc", "Q"), col("T", 9, 5, 8));
        assertTableEquals(snapshot, fourthResult);
        assertEquals(listener.getCount(), 1);
        assertEquals(i(), listener.update.added());
        assertEquals(i(25), listener.update.modified());
        assertEquals(i(), listener.update.removed());
        assertTrue(listener.update.shifted().empty());
        listener.reset();

        TableTools.showWithRowSet(snapshot);
        listener.close();
    }

    public void testSnapshotIncrementalRandom() {
        final ColumnInfo<?, ?>[] stampInfo;
        final ColumnInfo<?, ?>[] rightInfo;

        final int stampSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable stampTable = getTable(stampSize, random, stampInfo = initColumnInfos(new String[] {"Stamp"},
                new IntGenerator(0, 100)));
        final QueryTable base = getTable(stampSize, random,
                rightInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));

        final QueryTable snapshot = (QueryTable) base.snapshotWhen(stampTable, Flag.INCREMENTAL);

        final SimpleShiftObliviousListener simpleListener = new SimpleShiftObliviousListener(snapshot);
        snapshot.addUpdateListener(simpleListener);

        final CoalescingListener coalescingListener = new CoalescingListener(base);
        base.addUpdateListener(coalescingListener, true);

        Table lastSnapshot = snapshot.silent().select();
        RowSet lastRowSet = RowSetFactory.empty();

        try {
            for (int step = 0; step < 200; step++) {
                final int fstep = step;
                final boolean modStamp = random.nextInt(3) < 1;
                final boolean modRight = random.nextBoolean();
                final boolean modifyRightFirst = modRight && modStamp && random.nextBoolean();

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> {
                    if (printTableUpdates) {
                        System.out.println("Step = " + fstep + ", modStamp=" + modStamp + ", modRight=" + modRight
                                + ", modifyRightFirst=" + modifyRightFirst);
                    }

                    if (modifyRightFirst) {
                        GenerateTableUpdates.generateTableUpdates(filteredSize, random, base, rightInfo);
                    }
                    if (modStamp) {
                        final long lastStamp1 = stampTable.getRowSet().lastRowKey();
                        final int numAdditions = 1 + random.nextInt(stampSize);
                        final RowSet stampsToAdd =
                                RowSetFactory.fromRange(lastStamp1 + 1, lastStamp1 + numAdditions);

                        final ColumnHolder<?>[] columnAdditions = new ColumnHolder<?>[stampInfo.length];
                        for (int ii = 0; ii < columnAdditions.length; ii++) {
                            columnAdditions[ii] = stampInfo[ii].generateUpdateColumnHolder(stampsToAdd, random);
                        }
                        addToTable(stampTable, stampsToAdd, columnAdditions);
                        stampTable.notifyListeners(stampsToAdd, RowSetFactory.empty(),
                                RowSetFactory.empty());
                    }
                    if (!modifyRightFirst && modRight) {
                        GenerateTableUpdates.generateTableUpdates(filteredSize, random, base, rightInfo);
                    }
                });
                if (modStamp) {
                    System.out.println("Snapshot Size: " + snapshot.size());
                    TableTools.showWithRowSet(snapshot);

                    assertTableEquals(base, snapshot.dropColumns("Stamp"));

                    if (coalescingListener.getCount() > 0) {
                        System.out.println("Snapshot Added: " + simpleListener.added);
                        System.out.println("Snapshot Removed: " + simpleListener.removed);
                        System.out.println("Snapshot Modified: " + simpleListener.modified);
                        final RowSet coalAdded = coalescingListener.indexUpdateCoalescer.takeAdded();
                        System.out.println("Right Coalesced Added: " + coalAdded);
                        final RowSet coalRemoved = coalescingListener.indexUpdateCoalescer.takeRemoved();
                        System.out.println("Right Coalesced Removed: " + coalRemoved);
                        final RowSet coalModified = coalescingListener.indexUpdateCoalescer.takeModified();
                        System.out.println("Right Coalesced Modified: " + coalModified);

                        final RowSet modified = simpleListener.added.union(simpleListener.modified);
                        final RowSet unmodified = snapshot.getRowSet().minus(modified);
                        System.out.println("Modified: " + modified);
                        System.out.println("Unmodified: " + unmodified);

                        // verify the modified stamps
                        final int lastStamp =
                                stampTable.getColumnSource("Stamp").getInt(stampTable.getRowSet().lastRowKey());
                        final ColumnSource<Integer> stamps = snapshot.getColumnSource("Stamp");
                        for (final RowSet.Iterator it = modified.iterator(); it.hasNext();) {
                            final long next = it.nextLong();
                            final int stamp = stamps.getInt(next);
                            assertEquals(lastStamp, stamp);
                        }

                        // and anything unmodified should be the same as last time
                        @SuppressWarnings("unchecked")
                        final DataColumn<Integer> lastStamps = DataAccessHelpers.getColumn(lastSnapshot, "Stamp");
                        for (final RowSet.Iterator it = unmodified.iterator(); it.hasNext();) {
                            final long next = it.nextLong();
                            final long lastPos = lastRowSet.find(next);
                            assertTrue(lastPos >= 0);
                            final int priorStamp = lastStamps.getInt((int) lastPos);
                            final int stamp = stamps.getInt(next);
                            assertEquals(priorStamp, stamp);
                        }

                        assertEquals(1, simpleListener.getCount());

                        assertEquals(coalAdded, simpleListener.added);
                        assertEquals(coalRemoved, simpleListener.removed);
                        assertEquals(coalModified, simpleListener.modified);
                    } else {
                        assertEquals(0, simpleListener.getCount());
                    }

                    // make sure everything from the right table matches the snapshot
                    lastSnapshot = new QueryTable(snapshot.getRowSet().copy().toTracking(),
                            snapshot.getColumnSourceMap());
                    lastRowSet = base.getRowSet().copy();
                    // the coalescing listener can be reset
                    coalescingListener.reset();
                    simpleListener.reset();
                } else {
                    assertEquals(0, simpleListener.getCount());
                    // make sure there were no changes
                    assertTableEquals(lastSnapshot, snapshot);
                }
            }
        } finally {
            simpleListener.freeResources();
        }
    }

    public void testSelectModifications() {
        testShiftingModifications(arg -> (QueryTable) arg.select());
    }

    static void testLegacyFlattenModifications(UnaryOperator<QueryTable> function) {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("intCol", 10, 20, 40, 60));

        final QueryTable selected = function.apply(queryTable);
        final io.deephaven.engine.table.impl.SimpleListener simpleListener =
                new io.deephaven.engine.table.impl.SimpleListener(selected);
        selected.addUpdateListener(simpleListener);

        final Supplier<TableUpdateImpl> newUpdate =
                () -> new TableUpdateImpl(i(), i(), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3), col("intCol", 30));
            removeRows(queryTable, i(2));
            final TableUpdateImpl update3 = newUpdate.get();
            update3.added = i(3);
            update3.removed = i(2);
            queryTable.notifyListeners(update3);
        });

        Assert.assertEquals("simpleListener.getCount() == 1", 1, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());
        Assert.assertEquals("simpleListener.update.added.size() = 1", 1, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 1", 1, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.modified.size() = 0", 0, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.added = {1}", i(1), simpleListener.update.added());
        Assert.assertEquals("simpleListener.update.removed = {1}", i(1), simpleListener.update.removed());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3), col("intCol", 30));
            final TableUpdateImpl update2 = newUpdate.get();
            update2.modified = i(3);
            update2.modifiedColumnSet = queryTable.newModifiedColumnSet("intCol");
            queryTable.notifyListeners(update2);
        });

        Assert.assertEquals("simpleListener.getCount() == 2", 2, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.added.size() = 0", 0, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 0", 0, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.modified.size() = 1", 1, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.modified = {1}", i(1), simpleListener.update.modified());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 5), col("intCol", 30, 50));
            final TableUpdateImpl update1 = newUpdate.get();
            update1.added = i(5);
            update1.modified = i(3);
            update1.modifiedColumnSet = queryTable.newModifiedColumnSet("intCol");
            queryTable.notifyListeners(update1);
        });

        Assert.assertEquals("simpleListener.getCount() == 3", 3, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.removed.size() = 0", 0, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.added.size() = 1", 1, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.added = {3}", i(3), simpleListener.update.added());
        Assert.assertEquals("simpleListener.update.modified.size() = 1", 1, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.modified = {1}", i(1), simpleListener.update.modified());
        Assert.assertEquals("simpleListener.update.shifted.size() = 1", 1, simpleListener.update.shifted().size());
        Assert.assertEquals("simpleListener.update.shifted.getBeginRange(0) = 3", 3,
                simpleListener.update.shifted().getBeginRange(0));
        Assert.assertEquals("simpleListener.update.shifted.getEndRange(0) = 3", 3,
                simpleListener.update.shifted().getEndRange(0));
        Assert.assertEquals("simpleListener.update.shifted.getShiftDelta(0) = 1", 1,
                simpleListener.update.shifted().getShiftDelta(0));

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(queryTable, i(4));
            final TableUpdateImpl update = newUpdate.get();
            update.removed = i(4);
            queryTable.notifyListeners(update);
        });

        Assert.assertEquals("simpleListener.getCount() == 4", 4, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.added.size() = 0", 0, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 1", 1, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.removed = {2}", i(2), simpleListener.update.removed());
        Assert.assertEquals("simpleListener.update.modified.size() = 0", 0, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.shifted.size() = 1", 1, simpleListener.update.shifted().size());
        Assert.assertEquals("simpleListener.update.shifted.getBeginRange(0) = 3", 3,
                simpleListener.update.shifted().getBeginRange(0));
        Assert.assertEquals("simpleListener.update.shifted.getEndRange(0) = 4", 4,
                simpleListener.update.shifted().getEndRange(0));
        Assert.assertEquals("simpleListener.update.shifted.getShiftDelta(0) = -1", -1,
                simpleListener.update.shifted().getShiftDelta(0));

        simpleListener.close();
    }

    static void testShiftingModifications(UnaryOperator<QueryTable> function) {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("intCol", 10, 20, 40, 60));

        final QueryTable selected = function.apply(queryTable);
        final io.deephaven.engine.table.impl.SimpleListener simpleListener =
                new io.deephaven.engine.table.impl.SimpleListener(selected);
        selected.addUpdateListener(simpleListener);

        final Supplier<TableUpdateImpl> newUpdate =
                () -> new TableUpdateImpl(i(), i(), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3), col("intCol", 30));
            removeRows(queryTable, i(2));
            final TableUpdateImpl update3 = newUpdate.get();
            update3.added = i(3);
            update3.removed = i(2);
            queryTable.notifyListeners(update3);
        });

        Assert.assertEquals("simpleListener.getCount() == 1", 1, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());
        Assert.assertEquals("simpleListener.update.added.size() = 1", 1, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 1", 1, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.modified.size() = 0", 0, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.added = {3}", i(3), simpleListener.update.added());
        Assert.assertEquals("simpleListener.update.removed = {2}", i(2), simpleListener.update.removed());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3), col("intCol", 30));
            final TableUpdateImpl update2 = newUpdate.get();
            update2.modified = i(3);
            update2.modifiedColumnSet = queryTable.newModifiedColumnSet("intCol");
            queryTable.notifyListeners(update2);
        });

        Assert.assertEquals("simpleListener.getCount() == 2", 2, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.added.size() = 0", 0, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 0", 0, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.modified.size() = 1", 1, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.modified = {3}", i(3), simpleListener.update.modified());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 5), col("intCol", 30, 50));
            final TableUpdateImpl update1 = newUpdate.get();
            update1.added = i(5);
            update1.modified = i(3);
            update1.modifiedColumnSet = queryTable.newModifiedColumnSet("intCol");
            queryTable.notifyListeners(update1);
        });

        Assert.assertEquals("simpleListener.getCount() == 3", 3, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.removed.size() = 0", 0, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.added.size() = 1", 1, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.added = {5}", i(5), simpleListener.update.added());
        Assert.assertEquals("simpleListener.update.modified.size() = 1", 1, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.modified = {1}", i(3), simpleListener.update.modified());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());
        // Assert.assertEquals("simpleListener.update.shifted.getBeginRange(0) = 3", 3,
        // simpleListener.update.shifted.getBeginRange(0));
        // Assert.assertEquals("simpleListener.update.shifted.getEndRange(0) = 3", 3,
        // simpleListener.update.shifted.getEndRange(0));
        // Assert.assertEquals("simpleListener.update.shifted.getShiftDelta(0) = 1", 1,
        // simpleListener.update.shifted.getShiftDelta(0));

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(queryTable, i(4));
            final TableUpdateImpl update = newUpdate.get();
            update.removed = i(4);
            queryTable.notifyListeners(update);
        });

        Assert.assertEquals("simpleListener.getCount() == 4", 4, simpleListener.getCount());
        Assert.assertEquals("simpleListener.update.added.size() = 0", 0, simpleListener.update.added().size());
        Assert.assertEquals("simpleListener.update.removed.size() = 1", 1, simpleListener.update.removed().size());
        Assert.assertEquals("simpleListener.update.removed = {4}", i(4), simpleListener.update.removed());
        Assert.assertEquals("simpleListener.update.modified.size() = 0", 0, simpleListener.update.modified().size());
        Assert.assertEquals("simpleListener.update.shifted.size() = 0", 0, simpleListener.update.shifted().size());
        // Assert.assertEquals("simpleListener.update.shifted.getBeginRange(0) = 3", 3,
        // simpleListener.update.shifted.getBeginRange(0));
        // Assert.assertEquals("simpleListener.update.shifted.getEndRange(0) = 4", 4,
        // simpleListener.update.shifted.getEndRange(0));
        // Assert.assertEquals("simpleListener.update.shifted.getShiftDelta(0) = -1", -1,
        // simpleListener.update.shifted.getShiftDelta(0));

        simpleListener.close();
    }

    public void testInstantColumns() {
        final QueryTable queryTable = testRefreshingTable(
                col("Sym", "aa", "bc", "aa", "aa"),
                col("Timestamp", DateTimeUtils.now(),
                        DateTimeUtils.now(),
                        DateTimeUtils.now(),
                        DateTimeUtils.now()));
        assertEquals(queryTable.groupBy("Sym").getDefinition().getColumn("Timestamp").getComponentType(),
                Instant.class);
        show(queryTable.update("x = Timestamp_[0]"));
        show(queryTable.update("TimeinSeconds=round((maxObj(Timestamp_)-minObj(Timestamp_))/1000000000)"));
        show(queryTable.groupBy("Sym").view("Sym", "x = Timestamp[0]"));
        show(queryTable.groupBy("Sym").view("Sym",
                "TimeinSeconds=round((maxObj(Timestamp)-minObj(Timestamp))/1000000000)"));
    }

    public void testUngroupingAgnostic() {
        int[][] data1 = new int[][] {new int[] {4, 5, 6}, new int[0], new int[] {7, 8}};
        Table table = testRefreshingTable(col("X", 1, 2, 3),
                col("Y", new String[] {"a", "b", "c"}, CollectionUtil.ZERO_LENGTH_STRING_ARRAY,
                        new String[] {"d", "e"}),
                col("Z", data1));

        Table t1 = table.ungroup("Y", "Z");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 3, 3), Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 5)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 5)));
        assertEquals(Arrays.asList(4, 5, 6, 7, 8), Arrays.asList(DataAccessHelpers.getColumn(t1, "Z").get(0, 5)));

        t1 = table.ungroup();
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 3, 3), Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 5)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 5)));
        assertEquals(Arrays.asList(4, 5, 6, 7, 8), Arrays.asList(DataAccessHelpers.getColumn(t1, "Z").get(0, 5)));

        int[][] data = new int[][] {new int[] {4, 5, 6}, new int[0], new int[] {7, 8}};
        table = testRefreshingTable(col("X", 1, 2, 3),
                col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"},
                        CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                col("Z", data));
        try {
            table.ungroup();
        } catch (Exception e) {
            assertEquals(
                    "Assertion failed: asserted sizes[i] == Array.getLength(arrayColumn.get(i)), instead referenceColumn == \"Y\", name == \"Z\", row == 1.",
                    e.getMessage());
        }

        try {
            table.ungroup("Y", "Z");
        } catch (Exception e) {
            assertEquals(
                    "Assertion failed: asserted sizes[i] == Array.getLength(arrayColumn.get(i)), instead referenceColumn == \"Y\", name == \"Z\", row == 1.",
                    e.getMessage());
        }

        t1 = table.ungroup("Y");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 2, 2), Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 5)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e"),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 5)));
        show(t1);
        show(t1.ungroup("Z"));
        t1 = t1.ungroup("Z");
        assertEquals(9, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 9)));
        assertEquals(Arrays.asList("a", "a", "a", "b", "b", "b", "c", "c", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 9)));
        assertEquals(Arrays.asList(4, 5, 6, 4, 5, 6, 4, 5, 6),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Z").get(0, 9)));


        t1 = table.ungroup("Z");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 3, 3), Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 5)));
        assertEquals(Arrays.asList(4, 5, 6, 7, 8), Arrays.asList(DataAccessHelpers.getColumn(t1, "Z").get(0, 5)));
        t1 = t1.ungroup("Y");
        assertEquals(9, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 9)));
        assertEquals(Arrays.asList(4, 4, 4, 5, 5, 5, 6, 6, 6),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Z").get(0, 9)));
        assertEquals(Arrays.asList("a", "b", "c", "a", "b", "c", "a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 9)));
    }

    public void testUngroupConstructSnapshotOfBoxedNull() {
        final Table t =
                testRefreshingTable(i(0).toTracking())
                        .update("X = new Integer[]{null, 2, 3}", "Z = new Integer[]{4, 5, null}");
        final Table ungrouped = t.ungroup();

        try (final BarrageMessage snap = ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) ungrouped)) {
            assertEquals(snap.rowsAdded, i(0, 1, 2));
            assertEquals(snap.addColumnData[0].data.get(0).asIntChunk().get(0),
                    io.deephaven.util.QueryConstants.NULL_INT);
            assertEquals(snap.addColumnData[1].data.get(0).asIntChunk().get(2),
                    io.deephaven.util.QueryConstants.NULL_INT);
        }
    }

    public void testUngroupableColumnSources() {
        final Table table = testRefreshingTable(col("X", 1, 1, 2, 2, 3, 3, 4, 4), col("Int", 1, 2, 3, 4, 5, 6, 7, null),
                col("Double", 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, null, 0.45),
                col("String", "a", "b", "c", "d", "e", "f", "g", null));
        final Table t1 = table.groupBy("X");
        final Table t2 = t1.ungroup();

        assertEquals(table.size(), t2.size());

        assertTableEquals(t2, table);

        final Table t3 = t1.ungroup().sortDescending("X");
        assertTableEquals(t3, table.sortDescending("X"));

        final Table t4 = t1.update("Array=new int[]{19, 40}");
        TableTools.showWithRowSet(t4);
        final Table t5 = t4.ungroup();
        TableTools.showWithRowSet(t5);

        final Table t6 = table.update("Array=i%2==0?19:40");
        assertTableEquals(t5, t6);

        final Table t7 = t6.sortDescending("X");
        final Table t8 = t4.sortDescending("X").ungroup();
        assertTableEquals(t8, t7);

        final Table t9 = t1.update("Array=new io.deephaven.vector.IntVectorDirect(19, 40)");
        final Table t10 = t9.ungroup();
        assertTableEquals(t10, t6);

        final Table t11 = t9.sortDescending("X").ungroup();
        assertTableEquals(t11, t7);

        final int[] intDirect = (int[]) DataAccessHelpers.getColumn(t2, "Int").getDirect();
        System.out.println(Arrays.toString(intDirect));

        final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, io.deephaven.util.QueryConstants.NULL_INT};

        if (!Arrays.equals(expected, intDirect)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Direct: " + Arrays.toString(intDirect));
            fail("Expected does not match direct value!");
        }

        int[] intPrevDirect =
                (int[]) IndexedDataColumn.makePreviousColumn(t2.getRowSet(), t2.getColumnSource("Int")).getDirect();
        if (!Arrays.equals(expected, intPrevDirect)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Prev: " + Arrays.toString(intPrevDirect));
            fail("Expected does not match previous value!");
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
        });

        intPrevDirect =
                (int[]) IndexedDataColumn.makePreviousColumn(t2.getRowSet(), t2.getColumnSource("Int")).getDirect();
        if (!Arrays.equals(expected, intPrevDirect)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Prev: " + Arrays.toString(intPrevDirect));
            fail("Expected does not match previous value!");
        }
    }

    public void testUngroupOverflow() {
        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            final QueryTable table = testRefreshingTable(i(5, 7).toTracking(), col("X", 1, 2),
                    col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            assertEquals(5, t1.size());
            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertEquals(Arrays.asList(1, 1, 1, 2, 2), Arrays.asList(DataAccessHelpers.getColumn(t1, "X").get(0, 5)));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e"),
                    Arrays.asList(DataAccessHelpers.getColumn(t1, "Y").get(0, 5)));

            final ErrorListener errorListener = new ErrorListener(t1);
            t1.addUpdateListener(errorListener);

            // This is too big, we should fail
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                final long bigIndex = 1L << 55;
                addToTable(table, i(bigIndex), intCol("X", 3),
                        new ColumnHolder<>("Y", String[].class, String.class, false, new String[] {"f"}));
                table.notifyListeners(i(bigIndex), i(), i());
            });
            TableTools.showWithRowSet(t1);

            if (errorListener.originalException() == null) {
                fail("errorListener.originalException == null");
            }
            if (!(errorListener.originalException() instanceof IllegalStateException)) {
                fail("!(errorListener.originalException instanceof IllegalStateException)");
            }
            if (!(errorListener.originalException().getMessage().startsWith("Key overflow detected"))) {
                fail("!errorListener.originalException.getMessage().startsWith(\"Key overflow detected\")");
            }
        }
    }


    public void testUngroupWithRebase() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            final QueryTable table = testRefreshingTable(i(5, 7).toTracking(),
                    col("X", 1, 2), col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            assertEquals(5, t1.size());
            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertEquals(Arrays.asList(1, 1, 1, 2, 2),
                    Ints.asList((int[]) DataAccessHelpers.getColumn(t1, "X").getDirect()));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e"),
                    Arrays.asList((String[]) DataAccessHelpers.getColumn(t1, "Y").getDirect()));
            validateUpdates(t1);

            // This is too big, we should fail
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table, i(9), col("X", 3), new ColumnHolder<>("Y", String[].class, String.class, false,
                        new String[] {"f", "g", "h", "i", "j", "k"}));
                table.notifyListeners(i(9), i(), i());
            });
            TableTools.showWithRowSet(t1);

            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertEquals(Arrays.asList(1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3),
                    Ints.asList((int[]) (DataAccessHelpers.getColumn(t1, "X").getDirect())));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"),
                    Arrays.asList((String[]) DataAccessHelpers.getColumn(t1, "Y").getDirect()));

            assertEquals(Arrays.asList(1, 1, 1, 2, 2), Ints.asList(
                    (int[]) IndexedDataColumn.makePreviousColumn(t1.getRowSet(), t1.getColumnSource("X")).getDirect()));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e"), Arrays.asList((String[]) IndexedDataColumn
                    .makePreviousColumn(t1.getRowSet(), t1.getColumnSource("Y")).getDirect()));

            updateGraph.runWithinUnitTestCycle(() -> {
            });

            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertEquals(Arrays.asList(1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3),
                    Ints.asList((int[]) DataAccessHelpers.getColumn(t1, "X").getDirect()));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"),
                    Arrays.asList((String[]) DataAccessHelpers.getColumn(t1, "Y").getDirect()));
            assertEquals(Arrays.asList(1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3), Ints.asList(
                    (int[]) IndexedDataColumn.makePreviousColumn(t1.getRowSet(), t1.getColumnSource("X")).getDirect()));
            assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"),
                    Arrays.asList((String[]) IndexedDataColumn
                            .makePreviousColumn(t1.getRowSet(), t1.getColumnSource("Y")).getDirect()));
        } finally {
            QueryTable.setMinimumUngroupBase(minimumUngroupBase);
        }
    }

    public void testUngroupIncremental() throws ParseException {
        testUngroupIncremental(100, false);
        testUngroupIncremental(100, true);
    }

    private void testUngroupIncremental(int tableSize, boolean nullFill) throws ParseException {
        final Random random = new Random(0);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"Date", "C1", "C2", "C3"},
                        new DateGenerator(format.parse("2011-02-02"), format.parse("2011-02-03")),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30),
                        new SetGenerator<>(CollectionUtil.ZERO_LENGTH_STRING_ARRAY, new String[] {"a", "b"},
                                new String[] {"a", "b", "c"})));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> table.groupBy().ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill)),
                new UpdateValidatorNugget(table.groupBy("C1").ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy().ungroup(nullFill, "C1")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill, "Date")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1", "C2").sort("C1", "C2").ungroup(nullFill)),
                EvalNugget
                        .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                                .ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill)),
                EvalNugget
                        .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                                .ungroup(nullFill, "C1")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2").ungroup(nullFill, "Date")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2").ungroup(nullFill)),
                EvalNugget.from(
                        () -> table.groupBy("C1", "C2").update("Date=Date.toArray()").sort("C1", "C2")
                                .ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill)),
                EvalNugget.from(() -> table.view("C3").ungroup(nullFill))
        };

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int i = 0; i < 100; i++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step == " + i);
            }
            simulateShiftAwareStep(stepSize, random, table, columnInfo, en);
        }
    }

    public void testUngroupMismatch() {
        testUngroupMismatch(100, true);
        try {
            testUngroupMismatch(100, false);
            fail("Expected AssertionFailure");
        } catch (AssertionFailure ignored) {
        }
    }

    private void testUngroupMismatch(int size, boolean nullFill) {
        final Random random = new Random(0);
        final Boolean[] boolArray = {true, false};
        final Double[] doubleArray = {1.0, 2.0, 3.0};
        QueryScope.addParam("boolArray", boolArray);
        QueryScope.addParam("doubleArray", doubleArray);
        for (int q = 0; q < 10; q++) {
            final ColumnInfo<?, ?>[] columnInfo;
            final QueryTable table = getTable(size, random,
                    columnInfo = initColumnInfos(new String[] {"Sym", "intCol",},
                            new SetGenerator<>("a", "b", "c", "d"),
                            new IntGenerator(10, 100)));

            final Table mismatch =
                    table.groupBy("Sym").sort("Sym").update("MyBoolean=boolArray", "MyDouble=doubleArray");

            final EvalNugget[] en = new EvalNugget[] {
                    EvalNugget.from(() -> mismatch.ungroup(nullFill)),
            };

            for (int i = 0; i < 10; i++) {
                // show(mismatch, 100);
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            }
        }
    }

    @SuppressWarnings({"RedundantCast", "unchecked"})
    public void testUngroupJoined_IDS6311() {
        final QueryTable left =
                testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'), intCol("Value", 0, 1, 2, 3));

        final QueryTable right = testRefreshingTable(col("Letter", '0', 'b'),
                byteCol("BValue", (byte) 0, (byte) 1),
                shortCol("SValue", (short) 0, (short) 1),
                intCol("EulavI", 0, 1),
                longCol("LValue", 0, 1),
                floatCol("FValue", 0.0f, 1.1f),
                doubleCol("DValue", 0.0d, 1.1d),
                charCol("CCol", 'a', 'b'),
                col("BoCol", true, false),
                col("OCol", (Pair<Integer, Integer>[]) new Pair[] {new Pair<>(0, 1), new Pair<>(2, 3)}));

        final Table leftBy = left.groupBy("Letter");
        final Table rightBy = right.groupBy("Letter");

        final Table joined = leftBy.naturalJoin(rightBy, "Letter")
                .withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true))
                .updateView("BValue = ((i%2) == 0) ? null : BValue");

        QueryTable expected = testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'),
                col("Value", (IntVector) new IntVectorDirect(0), (IntVector) new IntVectorDirect(1),
                        (IntVector) new IntVectorDirect(2), (IntVector) new IntVectorDirect(3)),
                col("BValue", null, (ByteVector) new ByteVectorDirect((byte) 1), null, null),
                col("SValue", null, (ShortVector) new ShortVectorDirect((short) 1), null, null),
                col("EulavI", null, (IntVector) new IntVectorDirect(1), null, null),
                col("LValue", null, (LongVector) new LongVectorDirect(1), null, null),
                col("FValue", null, (FloatVector) new FloatVectorDirect(1.1f), null, null),
                col("DValue", null, (DoubleVector) new DoubleVectorDirect(1.1d), null, null),
                col("CCol", null, (CharVector) new CharVectorDirect('b'), null, null),
                col("BoCol", null, (ObjectVector<Boolean>) new ObjectVectorDirect<>(false), null, null),
                col("OCol", null, (ObjectVector<Pair<Integer, Integer>>) new ObjectVectorDirect<>(new Pair<>(2, 3)),
                        null,
                        null));

        assertTableEquals(expected, joined);

        final Table ungrouped = joined.ungroup(true);

        expected = testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'),
                col("Value", 0, 1, 2, 3),
                byteCol("BValue", io.deephaven.util.QueryConstants.NULL_BYTE, (byte) 1,
                        io.deephaven.util.QueryConstants.NULL_BYTE, io.deephaven.util.QueryConstants.NULL_BYTE),
                shortCol("SValue", io.deephaven.util.QueryConstants.NULL_SHORT, (short) 1,
                        io.deephaven.util.QueryConstants.NULL_SHORT, io.deephaven.util.QueryConstants.NULL_SHORT),
                intCol("EulavI", io.deephaven.util.QueryConstants.NULL_INT, 1,
                        io.deephaven.util.QueryConstants.NULL_INT, io.deephaven.util.QueryConstants.NULL_INT),
                longCol("LValue", io.deephaven.util.QueryConstants.NULL_LONG, (long) 1,
                        io.deephaven.util.QueryConstants.NULL_LONG, io.deephaven.util.QueryConstants.NULL_LONG),
                floatCol("FValue", io.deephaven.util.QueryConstants.NULL_FLOAT, 1.1f,
                        io.deephaven.util.QueryConstants.NULL_FLOAT, io.deephaven.util.QueryConstants.NULL_FLOAT),
                doubleCol("DValue", io.deephaven.util.QueryConstants.NULL_DOUBLE, 1.1d,
                        io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_DOUBLE),
                charCol("CCol", io.deephaven.util.QueryConstants.NULL_CHAR, 'b',
                        io.deephaven.util.QueryConstants.NULL_CHAR, io.deephaven.util.QueryConstants.NULL_CHAR),
                col("BoCol", null, false, null, null),
                col("OCol", (Pair<Integer, Integer>[]) new Pair[] {null, new Pair<>(2, 3), null, null}));

        assertTableEquals(expected, ungrouped);

        // assertTableEquals only calls get(), we need to make sure the specialized get()s also work too.
        final long firstKey = ungrouped.getRowSet().firstRowKey();
        final long secondKey = ungrouped.getRowSet().get(1);

        assertEquals(io.deephaven.util.QueryConstants.NULL_BYTE, ungrouped.getColumnSource("BValue").getByte(firstKey));
        assertEquals((byte) 1, ungrouped.getColumnSource("BValue").getByte(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_SHORT,
                ungrouped.getColumnSource("SValue").getShort(firstKey));
        assertEquals((short) 1, ungrouped.getColumnSource("SValue").getShort(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_INT, ungrouped.getColumnSource("EulavI").getInt(firstKey));
        assertEquals(1, ungrouped.getColumnSource("EulavI").getInt(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, ungrouped.getColumnSource("LValue").getLong(firstKey));
        assertEquals(1, ungrouped.getColumnSource("LValue").getLong(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_FLOAT,
                ungrouped.getColumnSource("FValue").getFloat(firstKey));
        assertEquals(1.1f, ungrouped.getColumnSource("FValue").getFloat(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                ungrouped.getColumnSource("DValue").getDouble(firstKey));
        assertEquals(1.1d, ungrouped.getColumnSource("DValue").getDouble(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_CHAR, ungrouped.getColumnSource("CCol").getChar(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getChar(secondKey));

        // repeat with prev
        assertEquals(io.deephaven.util.QueryConstants.NULL_BYTE,
                ungrouped.getColumnSource("BValue").getPrevByte(firstKey));
        assertEquals((byte) 1, ungrouped.getColumnSource("BValue").getPrevByte(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_SHORT,
                ungrouped.getColumnSource("SValue").getPrevShort(firstKey));
        assertEquals((short) 1, ungrouped.getColumnSource("SValue").getPrevShort(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_INT,
                ungrouped.getColumnSource("EulavI").getPrevInt(firstKey));
        assertEquals(1, ungrouped.getColumnSource("EulavI").getPrevInt(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_LONG,
                ungrouped.getColumnSource("LValue").getPrevLong(firstKey));
        assertEquals(1, ungrouped.getColumnSource("LValue").getPrevLong(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_FLOAT,
                ungrouped.getColumnSource("FValue").getPrevFloat(firstKey));
        assertEquals(1.1f, ungrouped.getColumnSource("FValue").getPrevFloat(secondKey));

        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                ungrouped.getColumnSource("DValue").getPrevDouble(firstKey));
        assertEquals(1.1d, ungrouped.getColumnSource("DValue").getPrevDouble(secondKey));

        assertEquals(QueryConstants.NULL_CHAR, ungrouped.getColumnSource("CCol").getPrevChar(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getPrevChar(secondKey));

        assertNull(ungrouped.getColumnSource("CCol").getPrev(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getPrev(secondKey));

        // This tests the NPE condition in the ungrouped column sources
        final Table snappy = InitialSnapshotTable.setupInitialSnapshotTable(ungrouped,
                ConstructSnapshot.constructInitialSnapshot(this, (QueryTable) ungrouped));
        assertTableEquals(expected, snappy);
    }

    private void testMemoize(QueryTable source, UnaryOperator<Table> op) {
        testMemoize(source, true, op);
    }

    private void testMemoize(QueryTable source, boolean withCopy, UnaryOperator<Table> op) {
        final Table result = op.apply(source);
        final Table result2 = op.apply(source);
        Assert.assertSame(result, result2);

        // copy may or may not preserve the operation
        final Table result3 = op.apply(source.copy());
        if (withCopy) {
            assertTrue(result3 instanceof QueryTable.CopiedTable);
            assertTrue(((QueryTable.CopiedTable) result3).checkParent(result));
        } else {
            Assert.assertNotSame(result, result3);
        }
    }

    private void testNoMemoize(Table source, UnaryOperator<Table> op1, UnaryOperator<Table> op2) {
        final Table result = op1.apply(source);
        final Table result2 = op2.apply(source);
        Assert.assertNotSame(result, result2);
    }

    private void testMemoize(Table source, UnaryOperator<Table> op1, UnaryOperator<Table> op2) {
        final Table result = op1.apply(source);
        final Table result2 = op2.apply(source);
        Assert.assertSame(result, result2);
    }

    private void testNoMemoize(QueryTable source, UnaryOperator<Table> op) {
        final Table result = op.apply(source);
        final Table result2 = op.apply(source);
        Assert.assertNotSame(result, result2);
        // copying shouldn't make anything memoize magically
        final Table result3 = op.apply(source.copy());
        Assert.assertNotSame(result, result3);
    }

    private void testNoMemoize(QueryTable source, QueryTable copy, UnaryOperator<Table> op) {
        final Table result = op.apply(source);
        final Table result2 = op.apply(copy);
        Assert.assertNotSame(result, result2);
    }

    public void testMemoize() {
        final Random random = new Random(0);
        final QueryTable source = getTable(1000, random, initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                new IntGenerator(0, 100),
                new DoubleGenerator(0, 100)));

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            testMemoize(source, Table::flatten);

            testMemoize(source, t -> t.view("Sym"));
            testMemoize(source, t -> t.view("intCol"));
            testMemoize(source, t -> t.view("intCol", "doubleCol"));
            testMemoize(source, t -> t.view("doubleCol", "intCol"));
            testNoMemoize(source, t -> t.view("doubleCol", "intCol"), t -> t.view("doubleCol"));
            testNoMemoize(source, t -> t.view("doubleCol", "intCol"), t -> t.view("intCol", "doubleCol"));
            // we are not smart enough to handle formulas, because they might have different query scopes
            testNoMemoize(source, t -> t.view("doubleCol=doubleCol/2.0"));
            // we don't handle renames, because the SwitchColumn is hard to get right
            testNoMemoize(source, t -> t.view("intCol2=intCol", "doubleCol"));

            testMemoize(source, t -> t.sort("intCol", "doubleCol"));
            testMemoize(source, t -> t.sort("intCol"));
            testMemoize(source, t -> t.sortDescending("intCol"));
            testNoMemoize(source, t -> t.sort("intCol"), t -> t.sort("doubleCol"));
            testNoMemoize(source, t -> t.sort("intCol"), t -> t.sortDescending("intCol"));

            testMemoize(source, Table::reverse);

            testNoMemoize(source, t -> t.where("Sym.startsWith(`a`)"));
            testMemoize(source, t -> t.where("Sym=`aa`"));
            testMemoize(source, t -> t.where("Sym in `aa`, `bb`"));
            testMemoize(source,
                    t -> t.where(Filter.or(Filter.from("Sym in `aa`, `bb`", "intCol=7"))));
            testMemoize(source, t -> t.where(DisjunctiveFilter
                    .makeDisjunctiveFilter(WhereFilterFactory.getExpressions("Sym in `aa`, `bb`", "intCol=7"))));
            testMemoize(source, t -> t.where(ConjunctiveFilter
                    .makeConjunctiveFilter(WhereFilterFactory.getExpressions("Sym in `aa`, `bb`", "intCol=7"))));
            testNoMemoize(source,
                    t -> t.where(ConjunctiveFilter.makeConjunctiveFilter(
                            WhereFilterFactory.getExpressions("Sym in `aa`, `bb`", "intCol=7"))),
                    t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(
                            WhereFilterFactory.getExpressions("Sym in `aa`, `bb`", "intCol=7"))));
            testNoMemoize(source, t -> t.where("Sym in `aa`, `bb`"), t -> t.where("Sym not in `aa`, `bb`"));
            testNoMemoize(source, t -> t.where("Sym in `aa`, `bb`"), t -> t.where("Sym in `aa`, `cc`"));
            testNoMemoize(source, t -> t.where("Sym.startsWith(`a`)"));

            testMemoize(source, t -> t.countBy("Count", "Sym"));
            testMemoize(source, t -> t.countBy("Sym"));
            testMemoize(source, t -> t.sumBy("Sym"));
            testMemoize(source, t -> t.avgBy("Sym"));
            testMemoize(source, t -> t.minBy("Sym"));
            testMemoize(source, t -> t.dropColumns("Sym"));
            testMemoize(source, t -> t.dropColumns("Sym", "intCol"));
            testMemoize(source, t -> t.dropColumns("Sym", "intCol"), t -> t.dropColumns("intCol", "Sym"));
            testMemoize(source, t -> t.dropColumns("intCol", "Sym"));
            testNoMemoize(source, t -> t.dropColumns("Sym"), t -> t.dropColumns("intCol"));
            testMemoize(source, t -> t.maxBy("Sym"));
            testMemoize(source, t -> t.aggBy(List.of(AggSum("intCol"), AggAbsSum("absInt=intCol"), AggMax("doubleCol"),
                    AggFirst("Sym"), AggCountDistinct("UniqueCountSym=Sym"), AggDistinct("UniqueSym=Sym"))));
            testMemoize(source, t -> t.aggBy(List.of(AggCountDistinct("UniqueCountSym=Sym"))));
            testMemoize(source, t -> t.aggBy(List.of(AggCountDistinct(true, "UniqueCountSym=Sym"))));
            testNoMemoize(source, t -> t.aggBy(List.of(AggCountDistinct("UniqueCountSym=Sym"))),
                    t -> t.aggBy(List.of(AggCountDistinct(true, "UniqueCountSym=Sym"))));
            testMemoize(source, t -> t.aggBy(List.of(AggDistinct("UniqueSym=Sym"))));
            testMemoize(source, t -> t.aggBy(List.of(AggDistinct(true, "UniqueSym=Sym"))));
            testNoMemoize(source, t -> t.aggBy(List.of(AggCountDistinct("UniqueCountSym=Sym"))),
                    t -> t.aggBy(List.of(AggDistinct("UniqueCountSym=Sym"))));
            testNoMemoize(source, t -> t.aggBy(List.of(AggDistinct("UniqueSym=Sym"))),
                    t -> t.aggBy(List.of(AggDistinct(true, "UniqueSym=Sym"))));
            testNoMemoize(source, t -> t.countBy("Sym"), t -> t.countBy("Count", "Sym"));
            testNoMemoize(source, t -> t.sumBy("Sym"), t -> t.countBy("Count", "Sym"));
            testNoMemoize(source, t -> t.sumBy("Sym"), t -> t.avgBy("Sym"));
            testNoMemoize(source, t -> t.minBy("Sym"), t -> t.maxBy("Sym"));

            final List<String> value = new ArrayList<>();
            value.add("aa");
            value.add("bb");
            QueryScope.addParam("memoizeTestVariable", value);
            testMemoize(source, t -> t.where("Sym in memoizeTestVariable"));
            testNoMemoize(source, t -> t.where("Sym in memoizeTestVariable"), t -> {
                value.add("cc");
                return t.where("Sym in memoizeTestVariable");
            });

            final Table withRestrictions = source.copy().restrictSortTo("intCol", "doubleCol");
            testNoMemoize(source, (QueryTable) withRestrictions, t -> t.sort("intCol", "doubleCol"));
        } finally {
            QueryTable.setMemoizeResults(old);
        }
    }

    public void testMemoizeConcurrent() {
        final ExecutorService dualPool = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                ExecutionContext captured = ExecutionContext.getContext();
                return new Thread(() -> captured.apply(runnable));
            }
        });

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            final AtomicInteger counterValue = new AtomicInteger();
            final IntSupplier counter = () -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                return counterValue.incrementAndGet();
            };
            QueryScope.addParam("supplier", counter);

            final Table source = emptyTable(1);
            final QueryTable updated = (QueryTable) source.updateView("Int=supplier.getAsInt()");

            final Future<Table> resultFuture = dualPool.submit((Callable<Table>) updated::select);
            Thread.sleep(1);
            final Future<Table> resultFuture2 = dualPool.submit((Callable<Table>) updated::select);

            final Table result = resultFuture.get();
            final Table result2 = resultFuture2.get();

            Assert.assertSame(result, result2);

            assertEquals(1, counterValue.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            QueryTable.setMemoizeResults(old);
        }

        dualPool.shutdownNow();
    }

    public void testWhereInGrouped() throws IOException {
        diskBackedTestHarness(t -> {
            final ColumnSource<String> symbol = t.getColumnSource("Symbol");
            // noinspection unchecked,rawtypes
            final Map<String, RowSet> gtr = (Map) RowSetIndexer.of(t.getRowSet()).getGrouping(symbol);
            ((AbstractColumnSource<String>) symbol).setGroupToRange(gtr);
            final Table result =
                    t.whereIn(t.where("Truthiness=true"), "Symbol", "Timestamp");
            TableTools.showWithRowSet(result);
        });
    }

    private void diskBackedTestHarness(Consumer<Table> testFunction) throws IOException {
        final File testDirectory = Files.createTempDirectory("SymbolTableTest").toFile();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("Sentinel"),
                ColumnDefinition.ofString("Symbol").withGrouping(),
                ColumnDefinition.ofTime("Timestamp"),
                ColumnDefinition.ofBoolean("Truthiness"));

        final String[] syms = new String[] {"Apple", "Banana", "Cantaloupe"};
        final Instant baseTime = DateTimeUtils.parseInstant("2019-04-11T09:30 NY");
        final long[] dateOffset = new long[] {0, 5, 10, 15, 1, 6, 11, 16, 2, 7};
        final Boolean[] booleans = new Boolean[] {true, false, null, true, false, null, true, false, null, true, false};
        QueryScope.addParam("syms", syms);
        QueryScope.addParam("baseTime", baseTime);
        QueryScope.addParam("dateOffset", dateOffset);
        QueryScope.addParam("booleans", booleans);

        final Table source = emptyTable(10)
                .updateView("Sentinel=i", "Symbol=syms[i % syms.length]",
                        "Timestamp=baseTime+dateOffset[i]*3600L*1000000000L", "Truthiness=booleans[i]")
                .groupBy("Symbol").ungroup();
        testDirectory.mkdirs();
        final File dest = new File(testDirectory, "Table.parquet");
        try {
            ParquetTools.writeTable(source, dest, definition);
            final Table table = ParquetTools.readTable(dest);
            testFunction.accept(table);
            table.close();
        } finally {
            FileUtils.deleteRecursively(testDirectory);
        }
    }

    public void testIds7153() {
        final QueryTable lTable =
                testRefreshingTable(RowSetFactory.fromKeys(10, 12, 14, 16).toTracking(),
                        col("X", "a", "b", "c", "d"));
        final QueryTable rTable = testRefreshingTable(col("X", "a", "b", "c", "d"), col("R", 0, 1, 2, 3));

        final MutableObject<QueryTable> nj = new MutableObject<>();
        final MutableObject<QueryTable> ft = new MutableObject<>();

        // now is safe to create the nj
        // The real test happens here. Off of the UGP thread we do an operation, one that supports concurrent
        // instantiation, such that we use prev values when applicable. Assume the parent table has not ticked
        // this cycle: 1) if the parent table pre-existed then we want to use prev values (to handle when parent
        // is mid-tick but unpublished) 2) if the parent table was created this cycle, then A) prev values are
        // undefined, B) it must have been created AFTER any of its dependencies may have ticked this cycle and
        // C) the table is not allowed to tick this cycle.
        // The specific scenario we are trying to catch is when the parent re-uses data structures (i.e. RowSet)
        // from its parent, which have valid prev values, but the prev values must not be used during the first
        // cycle.
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newRows = i(2, 4, 18, 20);
            addToTable(lTable, newRows, col("X", "e", "f", "g", "h"));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = newRows;
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;
            lTable.notifyListeners(update);

            // now is safe to create the nj
            nj.setValue((QueryTable) lTable.naturalJoin(rTable, "X"));

            try {
                // The real test happens here. Off of the UGP thread we do an operation, one that supports concurrent
                // instantiation, such that we use prev values when applicable. Assume the parent table has not ticked
                // this cycle: 1) if the parent table pre-existed then we want to use prev values (to handle when parent
                // is mid-tick but unpublished) 2) if the parent table was created this cycle, then A) prev values are
                // undefined, B) it must have been created AFTER any of its dependencies may have ticked this cycle and
                // C) the table is not allowed to tick this cycle.

                // The specific scenario we are trying to catch is when the parent re-uses data structures (i.e. RowSet)
                // from its parent, which have valid prev values, but the prev values must not be used during the first
                // cycle.
                final Thread offugp = new Thread(() -> ft.setValue((QueryTable) nj.getValue().flatten()));
                offugp.start();
                offugp.join();
            } catch (final Exception e) {
                throw new UncheckedDeephavenException("test failed", e);
            }
        });

        Assert.assertEquals(lTable.getRowSet().size(), ft.getValue().getRowSet().sizePrev());
        Assert.assertTrue(ft.getValue().isFlat());
        Assert.assertEquals(ft.getValue().getRowSet().size(), 8);
    }

    public void testNoCoalesceOnNotification() {
        // SourceTable is an uncoalesced table that also has an idempotent "start" despite whether or not the coalesced
        // table continues to be live and managed. When the source table ticks and it is currently uncoalesced, there
        // was a regression inside of BaseTable#notifyListeners that would invoke build() and cause this table to be
        // coalesced and for the new result table to receive and propagate that update. IDS-7153 made it explicitly
        // illegal
        // to publish an update on a table's very first cycle if it was initiated under the UGP lock. It's also not
        // great
        // to coalesce a table and immediately make it garbage.

        // This regression check verifies that we do not see a lastNotificationStep !=
        // ExecutionContext.getContext().getUpdateGraph().logicalClock().currentStep()
        // assertion error when notifying from an uncoalesced table.

        final TrackingWritableRowSet parentRowSet = RowSetFactory.empty().toTracking();
        final Supplier<QueryTable> supplier = () -> testRefreshingTable(parentRowSet);

        final UncoalescedTable<?> table = new MockUncoalescedTable(supplier);
        table.setRefreshing(true);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();

            update.added = RowSetFactory.fromKeys(parentRowSet.size());
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.empty();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;

            parentRowSet.insert(update.added());
            table.notifyListeners(update);
            Assert.assertEquals(ExecutionContext.getContext().getUpdateGraph().clock().currentStep(),
                    table.getLastNotificationStep());
        });
    }

    public void testNotifyListenersReleasesUpdateEmptyUpdate() {
        final QueryTable src = testRefreshingTable(RowSetFactory.flat(100).toTracking());
        final TableUpdateImpl update = new TableUpdateImpl();
        update.added = i();
        update.removed = i();
        update.modified = i();
        update.shifted = RowSetShiftData.EMPTY;
        update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

        // any listener will do for this empty update test
        final TableUpdateListener listener = new io.deephaven.engine.table.impl.SimpleListener(src);
        src.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(src, update.added());
            src.notifyListeners(update);
        });
        Assert.assertNull(update.added());
    }

    public void testNotifyListenersReleasesUpdateNoListeners() {
        final QueryTable src = testRefreshingTable(RowSetFactory.flat(100).toTracking());
        final TableUpdateImpl update = new TableUpdateImpl();
        update.added = RowSetFactory.fromRange(200, 220); // must be a non-empty update
        update.removed = i();
        update.modified = i();
        update.shifted = RowSetShiftData.EMPTY;
        update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(src, update.added());
            src.notifyListeners(update);
        });
        Assert.assertNull(update.added());
    }

    public void testNotifyListenersReleasesUpdateChildListener() {
        final QueryTable src = testRefreshingTable(RowSetFactory.flat(100).toTracking());
        final TableUpdateImpl update = new TableUpdateImpl();
        update.added = RowSetFactory.fromRange(200, 220); // must be a non-empty update
        update.removed = i();
        update.modified = i();
        update.shifted = RowSetShiftData.EMPTY;
        update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

        // we want to specifically test non-shift-aware-listener path
        final ShiftObliviousListener listener = new SimpleShiftObliviousListener(src);
        src.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(src, update.added());
            src.notifyListeners(update);
        });
        Assert.assertNull(update.added());
    }

    public void testNotifyListenersReleasesUpdateShiftAwareChildListener() {
        final QueryTable src = testRefreshingTable(RowSetFactory.flat(100).toTracking());
        final TableUpdateImpl update = new TableUpdateImpl();
        update.added = RowSetFactory.fromRange(200, 220); // must be a non-empty update
        update.removed = i();
        update.modified = i();
        update.shifted = RowSetShiftData.EMPTY;
        update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

        // we want to specifically test shift-aware-listener path
        final io.deephaven.engine.table.impl.SimpleListener listener =
                new io.deephaven.engine.table.impl.SimpleListener(src);
        src.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(src, update.added());
            src.notifyListeners(update);
        });
        listener.close(); // we typically grab and ref-count this for testing

        Assert.assertNull(update.added());
    }

    public void testRegressionIssue544() {
        // The expression that fails in the console is:
        // x = merge(newTable(byteCol("Q", (byte)0)), timeTable("PT00:00:01").view("Q=(byte)(i%2)"))
        // .tail(1)
        // .view("Q=Q*i")
        // .sumBy()
        //
        // The exception we were getting was: java.lang.IllegalArgumentException: keys argument has elements not in the
        // row set
        //
        final Table t0 = newTable(byteCol("Q", (byte) 0));
        final QueryTable t1 = testRefreshingTable(i().toTracking(), intCol("T"));
        final Table t2 = t1.view("Q=(byte)(i%2)");
        final Table result = merge(t0, t2).tail(1)
                .withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true))
                .view("Q=Q*i")
                .sumBy();
        int i = 1;
        for (int step = 0; step < 2; ++step) {
            final int key = i++;
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet addRowSet = i(key);
                addToTable(t1, addRowSet, intCol("T", key));
                t1.notifyListeners(addRowSet, i(), i());
                TableTools.show(result);
            });
        }
        assertEquals(0, getUpdateErrors().size());
    }

    private static class TestInstantGroupingSource
            extends LongAsInstantColumnSource
            implements DeferredGroupingColumnSource<Instant> {

        final GroupingProvider<Object> groupingProvider = new GroupingProvider<>() {
            @Override
            public Map<Object, RowSet> getGroupToRange() {
                return null;
            }

            @Override
            public Pair<Map<Object, RowSet>, Boolean> getGroupToRange(RowSet hint) {
                return null;
            }
        };

        TestInstantGroupingSource(ColumnSource<Long> realSource) {
            super(realSource);
            // noinspection unchecked,rawtypes
            ((DeferredGroupingColumnSource) realSource).setGroupingProvider(groupingProvider);
        }

        @Override
        public GroupingProvider<Instant> getGroupingProvider() {
            return null;
        }

        @Override
        public void setGroupingProvider(@Nullable GroupingProvider<Instant> groupingProvider) {
            throw new UnsupportedOperationException();
        }
    }

    public void testDeferredGroupingPropagationInstantCol() {
        // This is a regression test on a class cast exception in QueryTable#propagateGrouping.
        // RegionedColumnSourceInstant is a grouping source, but is not an InMemoryColumnSource. The select column
        // destination is not a grouping source as it is reinterpreted.

        TrackingRowSet rowSet = RowSetFactory.flat(4).toTracking();

        // This dance with a custom column is because an InMemoryColumnSource will be reused at the select layer
        // which skips propagation of grouping, and avoids the class cast exception.
        final TestInstantGroupingSource cs = new TestInstantGroupingSource(colSource(0L, 1, 2, 3));
        final Map<String, ? extends ColumnSource<?>> columns = Maps.of("T", cs);
        final QueryTable t1 = new QueryTable(rowSet, columns);
        final Table t2 = t1.select("T");

        // noinspection rawtypes
        final DeferredGroupingColumnSource result =
                (DeferredGroupingColumnSource) t2.getColumnSource("T").reinterpret(long.class);
        assertSame(cs.groupingProvider, result.getGroupingProvider());
    }

    private static void validateUpdates(final Table table) {
        final TableUpdateValidator validator = TableUpdateValidator.make((QueryTable) table);
        final QueryTable validatorTable = validator.getResultTable();
        final TableUpdateListener validatorTableListener =
                // NB: We specify retain=true to ensure the listener is not GC'd. It will be dropped when
                // the enclosing LivenessScope is released.
                new InstrumentedTableUpdateListenerAdapter(validatorTable, true) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {}

                    @Override
                    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                        TestCase.fail(originalException.getMessage());
                    }
                };
        validatorTable.addUpdateListener(validatorTableListener);
    }

    private static class MockUncoalescedTable extends UncoalescedTable<MockUncoalescedTable> {

        private final Supplier<QueryTable> supplier;

        public MockUncoalescedTable(Supplier<QueryTable> supplier) {
            super(supplier.get().getDefinition(), "mock un-coalesced table");
            this.supplier = supplier;
        }

        @Override
        protected Table doCoalesce() {
            final QueryTable table = supplier.get();
            copyAttributes(table, CopyAttributeOperation.Coalesce);
            return table;
        }

        @Override
        protected MockUncoalescedTable copy() {
            return new MockUncoalescedTable(supplier);
        }
    }

    public void testMergedListenerWithFailure() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Note: parent1 and parent2 have no parents; they will be satisfied as long as the update graph is satisfied.
        final QueryTable parent1 = (QueryTable) emptyTable(100).updateView("AnInt=1").coalesce();
        parent1.setRefreshing(true);
        final ListenerRecorder listenerRecorder1 = new ListenerRecorder("ONE", parent1, null);
        parent1.addUpdateListener(listenerRecorder1);
        final QueryTable parent2 = (QueryTable) emptyTable(100).updateView("AnInt=2").coalesce();
        parent2.setRefreshing(true);
        final ListenerRecorder listenerRecorder2 = new ListenerRecorder("TWO", parent2, null);
        parent2.addUpdateListener(listenerRecorder2);

        // Result is just a place to record last notification step and satisfy MergedListener's constructor
        final QueryTable result = new QueryTable(
                RowSetFactory.flat(100).toTracking(),
                Map.of("NullString", NullValueColumnSource.getInstance(String.class, null)));
        result.setRefreshing(true);
        assertEquals(updateGraph.clock().currentStep(), result.getLastNotificationStep());
        assertFalse(result.isFailed());

        // Re-usable "fake update" that we'll propagate
        final TableUpdate fakeUpdate = new TableUpdateImpl(RowSetFactory.flat(100), RowSetFactory.flat(100),
                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY);

        // Merged listener should properly combine the two upstream notifications
        final AtomicLong processedStep = new AtomicLong();
        final MergedListener mergedListener = new MergedListener(
                List.of(listenerRecorder1, listenerRecorder2), List.of(), "MERGED", result) {
            @Override
            protected void process() {
                processedStep.set(getUpdateGraph().clock().currentStep());
                result.notifyListeners(fakeUpdate.acquire());
            }
        };
        result.addParentReference(mergedListener);
        listenerRecorder1.setMergedListener(mergedListener);
        listenerRecorder2.setMergedListener(mergedListener);

        updateGraph.runWithinUnitTestCycle(() -> parent1.notifyListeners(fakeUpdate.acquire()));
        assertEquals(updateGraph.clock().currentStep(), processedStep.get());
        assertEquals(updateGraph.clock().currentStep(), result.getLastNotificationStep());
        assertFalse(result.isFailed());

        try (final SafeCloseable ignoredCompleter = updateGraph::completeCycleForUnitTests) {
            final long previousStep = updateGraph.clock().currentStep();
            updateGraph.startCycleForUnitTests(false);
            final long currentStep = updateGraph.clock().currentStep();

            // We start out unsatisfied
            assertFalse(listenerRecorder1.satisfied(currentStep));
            assertFalse(listenerRecorder2.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Adding an update notification changes nothing
            parent1.notifyListeners(fakeUpdate.acquire());
            assertFalse(listenerRecorder1.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Adding an error notification changes nothing
            parent2.notifyListenersOnError(new RuntimeException("FAKE ERROR"), null);
            assertFalse(listenerRecorder2.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Since we have notifications enqueued, marking the update graph satisfied changes nothing
            updateGraph.markSourcesRefreshedForUnitTests();
            assertFalse(listenerRecorder1.satisfied(currentStep));
            assertFalse(listenerRecorder2.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Flushing the first parent's table update notification satisfies just its listener recorder
            updateGraph.flushOneNotificationForUnitTests();
            assertTrue(listenerRecorder1.satisfied(currentStep));
            assertFalse(listenerRecorder2.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Flushing the second parent's table update notification satisfies just its listener recorder
            try (final SafeCloseable ignoredErrorExpectation = new ErrorExpectation()) {
                updateGraph.flushOneNotificationForUnitTests();
            }
            assertTrue(listenerRecorder1.satisfied(currentStep));
            assertTrue(listenerRecorder2.satisfied(currentStep));
            assertFalse(mergedListener.satisfied(currentStep));
            assertFalse(result.satisfied(currentStep));

            // Make sure our recorded steps are the expected values: nothing has been delivered to the result yet
            assertEquals(previousStep, processedStep.get());
            assertEquals(previousStep, result.getLastNotificationStep());
            assertFalse(result.isFailed());

            // Flushing the merged listener's notification satisfies everything
            try (final SafeCloseable ignoredErrorExpectation = new ErrorExpectation()) {
                updateGraph.flushOneNotificationForUnitTests();
            }
            assertTrue(listenerRecorder1.satisfied(currentStep));
            assertTrue(listenerRecorder2.satisfied(currentStep));
            assertTrue(mergedListener.satisfied(currentStep));
            assertTrue(result.satisfied(currentStep));

            // Make sure our recorded steps are the expected values: we delivered a failure notification to the result,
            // but the merged listener never processed a table update
            assertEquals(previousStep, processedStep.get());
            assertEquals(currentStep, result.getLastNotificationStep());
            assertTrue(result.isFailed());
        }
    }

    public void testMultipleUpdateGraphs() {
        final QueryTable r1, s1, r2, s2;
        final UpdateGraph g1 = new DummyUpdateGraph("one");
        final UpdateGraph g2 = new DummyUpdateGraph("two");

        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(g1).open()) {
            r1 = testRefreshingTable(i().toTracking(), intCol("T"));
            s1 = testTable(i().toTracking(), intCol("T"));
        }
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(g2).open()) {
            r2 = testRefreshingTable(i().toTracking(), intCol("T"));
            s2 = testTable(i().toTracking(), intCol("T"));
        }

        try {
            g1.sharedLock().computeLocked(() -> g2.sharedLock().computeLocked(() -> merge(r1, r2)));
            fail("Expected conflict");
        } catch (UpdateGraphConflictException expected) {
        }

        try {
            g1.sharedLock().computeLocked(() -> g2.sharedLock().computeLocked(() -> merge(s1, r1, s2, r2)));
            fail("Expected conflict");
        } catch (UpdateGraphConflictException expected) {
        }

        assertEquals(g1, g1.sharedLock().computeLocked(() -> merge(r1, s2).getUpdateGraph()));
        assertEquals(g1, g1.sharedLock().computeLocked(() -> merge(s2, r1).getUpdateGraph()));
        assertEquals(g1, g1.sharedLock().computeLocked(() -> merge(r1, s1, s2).getUpdateGraph()));
        assertEquals(g1, g1.sharedLock().computeLocked(() -> merge(s2, s1, r1).getUpdateGraph()));

        assertEquals(g2, g2.sharedLock().computeLocked(() -> merge(r2, s1).getUpdateGraph()));
        assertEquals(g2, g2.sharedLock().computeLocked(() -> merge(s1, r2).getUpdateGraph()));
        assertEquals(g2, g2.sharedLock().computeLocked(() -> merge(r2, s2, s1).getUpdateGraph()));
        assertEquals(g2, g2.sharedLock().computeLocked(() -> merge(s1, s2, r2).getUpdateGraph()));
    }

    private static final class DummyUpdateGraph implements UpdateGraph {

        private final String name;
        private final UpdateGraphLock lock;

        private final ThreadLocal<Boolean> serialTableOperationsSafe = ThreadLocal.withInitial(() -> false);

        private DummyUpdateGraph(@NotNull final String name) {
            this.name = name;
            lock = UpdateGraphLock.create(this, true);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append(getClass().getName());
        }

        @Override
        public boolean satisfied(long step) {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return this;
        }

        @Override
        public void addNotification(@NotNull Notification notification) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addNotifications(@NotNull Collection<? extends Notification> notifications) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean maybeAddNotification(@NotNull Notification notification, long deliveryStep) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AwareFunctionalLock sharedLock() {
            return lock.sharedLock();
        }

        @Override
        public AwareFunctionalLock exclusiveLock() {
            return lock.exclusiveLock();
        }

        @Override
        public LogicalClock clock() {
            return () -> 1;
        }

        @Override
        public int parallelismFactor() {
            return 1;
        }

        @Override
        public LogEntry logDependencies() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean currentThreadProcessesUpdates() {
            return false;
        }

        @Override
        public boolean serialTableOperationsSafe() {
            return serialTableOperationsSafe.get();
        }

        @Override
        public boolean setSerialTableOperationsSafe(final boolean newValue) {
            final boolean oldValue = serialTableOperationsSafe.get();
            serialTableOperationsSafe.set(newValue);
            return oldValue;
        }

        @Override
        public boolean supportsRefreshing() {
            return true;
        }

        @Override
        public void addSource(@NotNull Runnable updateSource) {}

        @Override
        public void removeSource(@NotNull Runnable updateSource) {}

        @Override
        public void requestRefresh() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stop() {}
    }
}
