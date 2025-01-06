//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedIntGenerator;
import io.deephaven.engine.testutil.generator.UnsortedInstantGenerator;
import io.deephaven.time.DateTimeFormatter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.printTableUpdates;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class QueryTableJoinTest {

    private static final double DELTA = 0.000001;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testAjIncremental() {
        final int maxSteps = 10;
        final int[] leftSizes = new int[] {10, 20};
        final int[] rightSizes = new int[] {10, 20};
        for (long seed = 0; seed < 1; seed++) {
            for (int leftSize : leftSizes) {
                for (int rightSize : rightSizes) {
                    for (QueryTableTestBase.JoinIncrement joinIncrement : base.joinIncrementors) {
                        testAjIncremental(leftSize, rightSize, joinIncrement, seed, maxSteps);
                        testAjIncrementalSimple(leftSize, rightSize, joinIncrement, seed, maxSteps);
                        testAjIncrementalSimple2(leftSize, rightSize, joinIncrement, seed, maxSteps);
                    }
                }
            }
        }
    }

    private void testAjIncrementalSimple(int leftSize, int rightSize, QueryTableTestBase.JoinIncrement joinIncrement,
            long seed,
            @SuppressWarnings("SameParameterValue") long maxSteps) {
        final Random random = new Random(seed);

        final ColumnInfo<?, ?>[] leftColumnInfo;
        final QueryTable leftTable = getTable(leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                        new SortedIntGenerator(1, 10000),
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 30)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                        new SortedIntGenerator(1, 10000),
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(20, 40)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
                                "LI1 = (isNull(LI1) ? null : LI1+0.5)");
                    }
                },
                new QueryTableTestBase.TableComparator(
                        leftTable.aj(rightTable, "I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
                                "LI1 = (isNull(LI1) ? null : LI1+0.5)"),
                        leftTable.aj(rightTable, "I1>=I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
                                "LI1 = (isNull(LI1) ? null : LI1+0.5)")),
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,C2,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,C2,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },

                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "I1>I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "I1>I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
                                "LI1 = (isNull(LI1) ? null : LI1+0.5)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,I1>I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,C2,I1>I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,C2,I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
        };
        for (int step = 0; step < maxSteps; step++) {
            if (printTableUpdates) {
                System.out.println("Simple Step i = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                        + ", seed = " + seed);
                System.out.println("Left Table:" + leftTable.size());
                showWithRowSet(leftTable, 100);
                System.out.println("Right Table:" + rightTable.size());
                showWithRowSet(rightTable, 100);
            }
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    private void testAjIncrementalSimple2(int leftSize, int rightSize, QueryTableTestBase.JoinIncrement joinIncrement,
            long seed,
            @SuppressWarnings("SameParameterValue") long maxSteps) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] leftColumnInfo;
        final QueryTable leftTable = getTable(leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                        new SortedIntGenerator(1, 1000),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"I1", "C1", "C2"},
                        new IntGenerator(1, 1000),
                        new SetGenerator<>("a", "b", "c"),
                        new SetGenerator<>(20, 30, 40)));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.sort("I1"), "I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.sort("I1"), "C1,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.sort("I1"), "C1,C2,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("I1"), "I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("I1"), "C1,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("I1"), "C1,C2,I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
        };
        for (int step = 0; step < maxSteps; step++) {
            if (printTableUpdates) {
                System.out.println("Simple2 Step i = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize);
                System.out.println("Left Table:");
                showWithRowSet(leftTable);
                System.out.println("Right Table:");
                showWithRowSet(rightTable);
            }
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    private void testAjIncremental(int leftSize, int rightSize, QueryTableTestBase.JoinIncrement joinIncrement,
            long seed,
            @SuppressWarnings("SameParameterValue") long maxSteps) {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new DateTimeFormatter("dd HH:mm:ss"));

        final Instant start = DateTimeUtils.parseLocalDate("2011-02-02").atStartOfDay()
                .atZone(ZoneId.of("America/New_York")).toInstant();
        final Instant end = DateTimeUtils.parseLocalDate("2011-02-03").atStartOfDay()
                .atZone(ZoneId.of("America/New_York")).toInstant();
        final ColumnInfo<?, ?>[] leftColumnInfo;
        final QueryTable leftTable = getTable(leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"Date", "C1", "C2"},
                        new UnsortedInstantGenerator(start, end),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"Date", "C1", "C2"},
                        new UnsortedInstantGenerator(start, end),
                        new SetGenerator<>("a", "b", "c"),
                        new SetGenerator<>(20, 30, 40)));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "Date", "LDate=Date, LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.updateView("RIdx=k"), "C1,Date", "LDate=Date,LC1=C1,LC2=C2,RIdx")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.updateView("RIdx=k"), "C1,Date",
                                "LDate=Date,LC1=C1,LC2=C2,RIdx");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable
                                .aj(rightTable.updateView("RIdx=k").sort("Date"), "C1,Date",
                                        "LDate=Date,LC1=C1,LC2=C2,RIdx")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.sort("Date"), "C2,Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable.sort("Date"), "C1,C2,Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "Date", "LDate=Date,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "C1,Date", "LDate=Date,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "C1,Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "C2,Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable.sort("Date"), "C1,C2,Date", "LDate=Date,LC1=C1,LC2=C2")
                                .update("Date=f.format(Date)", "LDate=isNull(LDate)?null:f.format(LDate)");
                    }
                },
        };

        for (int step = 0; step < maxSteps; step++) {
            // System.out.println("Date Step = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize
            // + ", seed = " + seed + ", step=" + joinIncrement);
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    @Test
    public void testAj() {
        Table table = testRefreshingTable(
                col("Ticker", "AAPL", "IBM", "AAPL"),
                col("Timestamp", 1L, 10L, 50L));
        Table lookUpValue1 = testRefreshingTable(
                col("Timestamp", 1L, 5L, 10L, 25L, 50L),
                col("Ticker", "AAPL", "IBM", "AAPL", "IBM", "AAPL"),
                col("OptionBid", .1, .2, .3, .4, .5));

        Table result = table.aj(lookUpValue1, "Ticker,Timestamp", "OptionBid");
        assertEquals(asList("Ticker", "Timestamp", "OptionBid"), result.getDefinition().getColumnNames());

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.aj(lookUpValue1, "Timestamp>=OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "b", "c"));

        result = lookUpValue1.aj(table, "indx>=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {null, null, "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT, 2}, ColumnVectors.ofInt(result, "Int").toArray());

        result = lookUpValue1.aj(table, "indx>=String", "Int,String");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));
        result = lookUpValue1.aj(table, "indx>=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"c", "c", "e"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"c", "d", "e"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {2, 2, 4}, ColumnVectors.ofInt(result, "Int").toArray());

        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx>=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"g", "e", null},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {6, 4, NULL_INT}, ColumnVectors.ofInt(result, "Int").toArray());


        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx>=String", "String");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals(2, result.numColumns());
        assertArrayEquals(new String[] {"g", "e", null},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());

        lookUpValue1 = testRefreshingTable(col("String", "h", "e", "a"));
        result = lookUpValue1.aj(table, "String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"g", "e", null},
                ColumnVectors.ofObject(result, "xString", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new int[] {6, 4, NULL_INT}, ColumnVectors.ofInt(result, "Int").toArray());
    }


    @Test
    public void testAjLt() {
        Table table = testRefreshingTable(
                col("Ticker", "AAPL", "IBM", "AAPL"),
                col("Timestamp", 1L, 10L, 50L));
        Table lookUpValue1 = testRefreshingTable(
                col("Timestamp", 1L, 5L, 10L, 25L, 50L),
                col("Ticker", "AAPL", "IBM", "AAPL", "IBM", "AAPL"),
                col("OptionBid", .1, .2, .3, .4, .5));

        Table result = table.aj(lookUpValue1.renameColumns("TS2=Timestamp"), "Ticker,Timestamp>TS2", "OptionBid");
        assertEquals(asList("Ticker", "Timestamp", "TS2", "OptionBid"), result.getDefinition().getColumnNames());
        final long[] timestamps = ColumnVectors.ofLong(result, "TS2").toArray();
        TableTools.show(result);
        assertArrayEquals(new long[] {QueryConstants.NULL_LONG, 5L, 10L}, timestamps);
        assertArrayEquals(new double[] {QueryConstants.NULL_DOUBLE, .2, .3},
                ColumnVectors.ofDouble(result, "OptionBid").toArray(), 0.0);

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.aj(lookUpValue1, "Timestamp>OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());
        TableTools.show(result);
        assertArrayEquals(new long[] {QueryConstants.NULL_LONG, 5L, 25L},
                ColumnVectors.ofLong(result, "OptionTimestamp").toArray());
        assertArrayEquals(new double[] {QueryConstants.NULL_DOUBLE, .2, .4},
                ColumnVectors.ofDouble(result, "OptionBid").toArray(), 0.0);

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "c", "d"));

        result = lookUpValue1.aj(table, "indx>String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {null, null, "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"a", "c", "d"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, NULL_INT, 2}, ColumnVectors.ofInt(result, "Int").toArray());

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));

        result = lookUpValue1.aj(table, "indx>String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {null, "c", "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"c", "d", "e"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, 2, 2}, ColumnVectors.ofInt(result, "Int").toArray());

        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx>String", "String,Int");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"g", "c", null},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {6, 2, NULL_INT}, ColumnVectors.ofInt(result, "Int").toArray());


        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx>String", "String");
        System.out.println("LV1");
        TableTools.show(lookUpValue1);
        System.out.println("Table");
        TableTools.show(table);
        System.out.println("Result");
        TableTools.show(result);

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals(2, result.numColumns());
        assertArrayEquals(new String[] {"g", "c", null},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());

        lookUpValue1 = testRefreshingTable(col("String", "h", "e", "a"));
        result = lookUpValue1.aj(table, "String>String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"g", "c", null},
                ColumnVectors.ofObject(result, "xString", String.class).toArray());
        assertArrayEquals(new String[] {"h", "e", "a"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new int[] {6, 2, NULL_INT}, ColumnVectors.ofInt(result, "Int").toArray());
    }


    @Test
    public void testSelfAj() {
        final QueryTable table = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5, 6, 7, 8).toTracking(),
                col("Primary", "A", "A", "A", "A", "A", "A", "A", "A"),
                col("Secondary", "A", "C", "D", "D", "F", "G", "H", "H"));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.updateView("kp=k").where("kp % 2 == 1")
                                .aj(table.updateView("ks=k").where("ks % 2 == 0"), "Primary,Secondary", "ks");
                    }
                }
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i()));
        TstUtils.validate(en);

        System.out.println("Notifying listeners of modification.");
        updateGraph.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i(4, 5)));
        System.out.println("Finished notifying listeners of modification.");
        TstUtils.validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(table, i(4));
            table.notifyListeners(i(), i(4), i());
        });
        TstUtils.validate(en);

    }

    @Test
    public void testAjNull() {
        final QueryTable left = TstUtils.testRefreshingTable(i(1, 2, 3, 4).toTracking(),
                col("LInt", 2, 4, 6, 8),
                col("LSentinel", "a", "b", "c", "d"));

        final QueryTable right = TstUtils.testRefreshingTable(i(1, 2, 3, 4, 5, 6, 7, 8).toTracking(),
                col("RInt", null, null, 3, 4, 5, 6, 7, 8),
                col("RSentinel", "C1", "E2", "A3", "D4", "F5", "G6", "I7", "H8"));

        System.out.println("Left:");
        TableTools.show(left);
        System.out.println("Right:");
        TableTools.show(right);

        final Table aj = left.aj(right, "LInt>=RInt", "RInt,RSentinel");
        System.out.println("AJ:");
        TableTools.show(aj);

        assertArrayEquals(new String[] {"E2", "D4", "G6", "H8"},
                ColumnVectors.ofObject(aj, "RSentinel", String.class).toArray());

        System.out.println("AJ2:");
        // let's swap the left and right
        final Table aj2 = right.sort("RSentinel").aj(left, "RInt>=LInt", "LInt,LSentinel");
        TableTools.show(aj2);
        assertArrayEquals(new String[] {"a", null, "b", null, "b", "c", "d", "c"},
                ColumnVectors.ofObject(aj2, "LSentinel", String.class).toArray());

    }

    @Test
    public void testAjEmptyRight() {
        final QueryTable left = TstUtils.testRefreshingTable(i(1, 2, 3, 4).toTracking(),
                col("Group", "g", "g", "g", "g"),
                col("LInt", 2, 4, 6, 8),
                col("LSentinel", "a", "b", "c", "d"));

        final QueryTable right = TstUtils.testRefreshingTable(i().toTracking(),
                col("Group", ArrayTypeUtils.EMPTY_STRING_ARRAY),
                intCol("RInt"),
                col("RSentinel", ArrayTypeUtils.EMPTY_STRING_ARRAY));

        System.out.println("Left:");
        TableTools.show(left);
        System.out.println("Right:");
        TableTools.show(right);

        final Table aj = left.aj(right, "Group,LInt>=RInt", "RInt,RSentinel");
        System.out.println("AJ:");
        TableTools.show(aj);

        assertArrayEquals(new String[] {null, null, null, null},
                ColumnVectors.ofObject(aj, "RSentinel", String.class).toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(left, i(2), col("Group", "h"), col("LInt", 4), col("LSentinel", "b"));
            left.notifyListeners(i(), i(), i(2));
        });

        TableTools.show(aj);
        assertArrayEquals(new String[] {null, null, null, null},
                ColumnVectors.ofObject(aj, "RSentinel", String.class).toArray());

    }

    @Test
    public void testRaj() {
        Table table = testRefreshingTable(
                col("Ticker", "AAPL", "IBM", "AAPL"),
                col("Timestamp", 1L, 10L, 50L));
        Table lookUpValue1 = testRefreshingTable(
                col("Timestamp", 1L, 5L, 10L, 25L, 50L),
                col("Ticker", "AAPL", "IBM", "AAPL", "IBM", "AAPL"),
                col("OptionBid", .1, .2, .3, .4, .5));

        Table result = table.raj(lookUpValue1, "Ticker,Timestamp", "OptionBid");
        show(result, 10);
        assertEquals(asList("Ticker", "Timestamp", "OptionBid"), result.getDefinition().getColumnNames());
        assertEquals(3, result.size());
        assertEquals("Ticker", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Timestamp", result.getDefinition().getColumns().get(1).getName());
        assertEquals("OptionBid", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {"AAPL", "IBM", "AAPL"},
                ColumnVectors.ofObject(result, "Ticker", String.class).toArray());
        assertArrayEquals(new long[] {1L, 10L, 50L}, ColumnVectors.ofLong(result, "Timestamp").toArray());
        assertArrayEquals(new double[] {.1, .4, .5}, ColumnVectors.ofDouble(result, "OptionBid").toArray(), DELTA);

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.raj(lookUpValue1, "Timestamp<=OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "b", "c"));

        result = lookUpValue1.raj(table, "indx<=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {"c", "c", "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "c"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {2, 2, 2}, ColumnVectors.ofInt(result, "Int").toArray());

        lookUpValue1 = testRefreshingTable(col("indx", "f", "g", "h"));

        result = lookUpValue1.raj(table, "indx<=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {"g", "g", null},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"f", "g", "h"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {6, 6, NULL_INT}, ColumnVectors.ofInt(result, "Int").toArray());

        result = lookUpValue1.raj(table, "indx<=String", "Int,String");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));

        show(lookUpValue1);
        show(table);

        result = lookUpValue1.raj(table, "indx<=String", "String,Int");
        show(result);

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {"c", "e", "e"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"c", "d", "e"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {2, 4, 4}, ColumnVectors.ofInt(result, "Int").toArray());

        lookUpValue1 = testRefreshingTable(col("indx", "j", "e", "a"));
        result = lookUpValue1.raj(table, "indx<=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {null, "e", "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"j", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, 4, 2}, ColumnVectors.ofInt(result, "Int").toArray());


        lookUpValue1 = testRefreshingTable(col("indx", "j", "e", "a"));
        result = lookUpValue1.raj(table, "indx<=String", "String");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals(2, result.numColumns());
        assertArrayEquals(new String[] {null, "e", "c"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new String[] {"j", "e", "a"}, ColumnVectors.ofObject(result, "indx", String.class).toArray());

        lookUpValue1 = testRefreshingTable(col("String", "j", "e", "a"));
        result = lookUpValue1.raj(table, "String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertArrayEquals(new String[] {null, "e", "c"},
                ColumnVectors.ofObject(result, "xString", String.class).toArray());
        assertArrayEquals(new String[] {"j", "e", "a"},
                ColumnVectors.ofObject(result, "String", String.class).toArray());
        assertArrayEquals(new int[] {NULL_INT, 4, 2}, ColumnVectors.ofInt(result, "Int").toArray());
    }

    static final JoinControl SMALL_LEFT_CONTROL = new JoinControl() {
        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.LeftInput, 1 << 8);
        }
    };

    static final JoinControl SMALL_RIGHT_CONTROL = new JoinControl() {
        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.RightInput, 1 << 8);
        }
    };

    @Test
    public void testAjRegression0() {
        final QueryTable rightQueryTable = TstUtils.testRefreshingTable(
                i(28, 36, 39, 42, 46, 49, 50, 51, 55, 56, 58, 64, 65, 66, 92, 96).toTracking(),
                col("C1", "a", "a", "c", "a", "b", "a", "c", "b", "c", "a", "a", "c", "c", "a", "c", "c"),
                col("I1", 168, 851, 255, 142, 884, 841, 877, 248, 191, 207, 163, 250, 982, 432, 466, 139),
                col("Sentinel", 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160));


        final QueryTable leftQueryTable = TstUtils.testRefreshingTable(i(10, 11, 12, 14, 16, 22).toTracking(),
                col("C1", "b", "a", "a", "a", "a", "b"),
                col("I1", 78, 85, 96, 263, 474, 876));

        showWithRowSet(leftQueryTable);
        showWithRowSet(rightQueryTable);
        final Table sortedRightQueryTable = rightQueryTable.sort("I1");
        showWithRowSet(sortedRightQueryTable);
        final Table result = leftQueryTable.aj(sortedRightQueryTable, "C1,I1", "RI1=I1,RC1=C1,Sentinel");
        showWithRowSet(result);

        assertEquals(100, result.getColumnSource("Sentinel", int.class).getInt(result.getRowSet().get(3)));
        assertEquals(207, result.getColumnSource("RI1", int.class).getInt(result.getRowSet().get(3)));
        assertEquals(140, result.getColumnSource("Sentinel", int.class).getInt(result.getRowSet().get(4)));
        assertEquals(432, result.getColumnSource("RI1", int.class).getInt(result.getRowSet().get(4)));
    }

    @Test
    public void testAjRegression1() {
        final QueryTable rightQueryTable =
                TstUtils.testRefreshingTable(i(1, 27, 28, 35, 41, 46, 49, 50, 51, 55, 56, 65).toTracking(),
                        col("C1", "b", "c", "b", "b", "c", "b", "a", "b", "c", "c", "c", "b"),
                        col("I1", 591, 5, 952, 43, 102, 18, 475, 821, 676, 191, 657, 982),
                        col("Sentinel", 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120));


        final QueryTable leftQueryTable = TstUtils.testRefreshingTable(i(10, 11, 12, 14, 16, 22).toTracking(),
                col("C1", "b", "a", "a", "a", "a", "b"),
                col("I1", 78, 85, 96, 263, 474, 876));

        showWithRowSet(leftQueryTable);
        showWithRowSet(rightQueryTable);
        final Table sortedRightQueryTable = rightQueryTable.sort("I1");
        showWithRowSet(sortedRightQueryTable);
        final Table result = leftQueryTable.aj(sortedRightQueryTable, "C1,I1", "LI1=I1,LC1=C1,Sentinel");
        showWithRowSet(result);

        assertEquals(80, result.getColumnSource("Sentinel", int.class).getInt(result.getRowSet().get(5)));
        assertEquals(821, result.getColumnSource("LI1", int.class).getInt(result.getRowSet().get(5)));
    }


    @Test
    public void testJoin() {
        Table lTable = testRefreshingTable(col("X", "a", "b", "c"));
        Table rTable = testRefreshingTable(col("Y", "x", "y"));
        Table result = lTable.join(rTable, "");
        showWithRowSet(result);
        assertEquals(6, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Y", result.getDefinition().getColumns().get(1).getName());
        assertArrayEquals(new String[] {"a", "a", "b", "b", "c", "c"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"x", "y", "x", "y", "x", "y"},
                ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b"), col("Z", 1, 2, 3));
        result = lTable.join(rTable, "X=Y");
        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Y", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Z", result.getDefinition().getColumns().get(2).getName());
        assertArrayEquals(new String[] {"a", "b", "b"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b", "b"}, ColumnVectors.ofObject(result, "Y", String.class).toArray());
        assertArrayEquals(new int[] {1, 2, 3}, ColumnVectors.ofInt(result, "Z").toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = lTable.join(rTable, "X=Y");
        show(result);
        assertEquals(2, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Y", result.getDefinition().getColumns().get(1).getName());
        assertArrayEquals(new String[] {"a", "b"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertArrayEquals(new String[] {"a", "b"}, ColumnVectors.ofObject(result, "Y", String.class).toArray());

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = lTable.join(rTable, "X");
        show(result);
        assertEquals(2, result.size());
        assertEquals(1, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertArrayEquals(new String[] {"a", "b"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
    }

    @Test
    public void testNaturalJoinWithGroupBy() {
        Table table1 = newTable(
                col("String", "c", "e", "g"));
        Table table2 = newTable(col("String", "c", "e"), col("v", 1, 2), col("u", 3.0d, 4.0d));

        showWithRowSet(table1);
        showWithRowSet(table2);

        Table pairMatch = table1.naturalJoin(table2.groupBy("String"), "String");

        showWithRowSet(pairMatch);

        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.numColumns());
        assertEquals("String", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals("u", pairMatch.getDefinition().getColumns().get(2).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertEquals(DoubleVector.class, pairMatch.getDefinition().getColumns().get(2).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"}, ColumnVectors
                .ofObject(pairMatch, pairMatch.getDefinition().getColumns().get(0).getName(), String.class).toArray());
        IntVector[] vValues = ColumnVectors.ofObject(pairMatch, "v", IntVector.class).toArray();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
        DoubleVector[] uValues = ColumnVectors.ofObject(pairMatch, "u", DoubleVector.class).toArray();
        assertEquals(3.0, uValues[0].get(0), 0.000001);
        assertEquals(4.0, uValues[1].get(0), 0.000001);
        assertEquals(1, uValues[0].size());
        assertEquals(1, uValues[1].size());
        assertNull(vValues[2]);

        pairMatch = table1.naturalJoin(table2.groupBy("String"), "String", "v");
        assertEquals(3, pairMatch.size());
        assertEquals(2, pairMatch.numColumns());
        assertEquals("String", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"}, ColumnVectors
                .ofObject(pairMatch, pairMatch.getDefinition().getColumns().get(0).getName(), String.class).toArray());
        vValues = ColumnVectors.ofObject(pairMatch, "v", IntVector.class).toArray();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);

        pairMatch = table1.naturalJoin(table2.groupBy("String"), "String", "u,v");
        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.numColumns());
        assertEquals("String", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("u", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(2).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(DoubleVector.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(2).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"}, ColumnVectors
                .ofObject(pairMatch, pairMatch.getDefinition().getColumns().get(0).getName(), String.class).toArray());
        vValues = ColumnVectors.ofObject(pairMatch, "v", IntVector.class).toArray();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
        uValues = ColumnVectors.ofObject(pairMatch, "u", DoubleVector.class).toArray();
        assertEquals(3.0, uValues[0].get(0), 0.000001);
        assertEquals(4.0, uValues[1].get(0), 0.000001);
        assertEquals(1, uValues[0].size());
        assertEquals(1, uValues[1].size());
        assertNull(vValues[2]);

        pairMatch = table1.naturalJoin(table2.groupBy("String"), "String=String");
        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.numColumns());
        assertEquals("String", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"}, ColumnVectors
                .ofObject(pairMatch, pairMatch.getDefinition().getColumns().get(0).getName(), String.class).toArray());
        vValues = ColumnVectors.ofObject(pairMatch, "v", IntVector.class).toArray();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);

        table1 = TableTools.newTable(
                col("String1", "c", "e", "g"));

        table2 = TableTools.newTable(
                col("String2", "c", "e"), col("v", 1, 2));

        final Table noPairMatch = table1.naturalJoin(table2.groupBy(), "");
        assertEquals(3, noPairMatch.size());
        assertEquals(3, noPairMatch.numColumns());
        assertEquals("String1", noPairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("String2", noPairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals("v", noPairMatch.getDefinition().getColumns().get(2).getName());
        assertEquals(String.class, noPairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(ObjectVector.class, noPairMatch.getDefinition().getColumns().get(1).getDataType());
        assertEquals(IntVector.class, noPairMatch.getDefinition().getColumns().get(2).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"},
                ColumnVectors
                        .ofObject(noPairMatch, noPairMatch.getDefinition().getColumns().get(0).getName(), String.class)
                        .toArray());
        // noinspection unchecked
        final ObjectVector<String>[] aggregateString =
                ColumnVectors.ofObject(noPairMatch, "String2", ObjectVector.class).toArray();
        assertArrayEquals(new String[] {"c", "e"}, aggregateString[0].toArray());
        assertArrayEquals(new String[] {"c", "e"}, aggregateString[1].toArray());
        assertArrayEquals(new String[] {"c", "e"}, aggregateString[2].toArray());
        vValues = ColumnVectors.ofObject(noPairMatch, "v", IntVector.class).toArray();
        assertArrayEquals(new int[] {1, 2}, vValues[0].toArray());
        assertArrayEquals(new int[] {1, 2}, vValues[1].toArray());
        assertArrayEquals(new int[] {1, 2}, vValues[2].toArray());

        pairMatch = table1.naturalJoin(table2.groupBy("String2"), "String1=String2");
        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.numColumns());
        assertEquals("String1", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("String2", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(2).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(2).getDataType());
        assertArrayEquals(new String[] {"c", "e", "g"}, ColumnVectors
                .ofObject(pairMatch, pairMatch.getDefinition().getColumns().get(0).getName(), String.class).toArray());

        final String[] stringColumn = ColumnVectors.ofObject(pairMatch, "String2", String.class).toArray();
        assertEquals("c", stringColumn[0]);
        assertEquals("e", stringColumn[1]);
        assertNull(stringColumn[2]);

        vValues = ColumnVectors.ofObject(pairMatch, "v", IntVector.class).toArray();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
    }
}
