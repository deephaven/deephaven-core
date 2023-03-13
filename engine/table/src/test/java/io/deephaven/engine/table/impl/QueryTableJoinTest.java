/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedIntGenerator;
import io.deephaven.engine.testutil.generator.UnsortedDateTimeGenerator;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeFormatter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.printTableUpdates;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class QueryTableJoinTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testAjIncremental() throws ParseException {
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
                        leftTable.aj(rightTable, "I1<=I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
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
                        return leftTable.aj(rightTable, "I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "I1<I1", "LI1=I1,LC1=C1,LC2=C2").update("I1=I1+0.25",
                                "LI1 = (isNull(LI1) ? null : LI1+0.5)");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.aj(rightTable, "C1,C2,I1<I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "I1>I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,I1>I1", "LI1=I1,LC1=C1,LC2=C2");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return leftTable.raj(rightTable, "C1,C2,I1>I1", "LI1=I1,LC1=C1,LC2=C2");
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

        final DateTime start = DateTimeUtils.toDateTime(
                DateTimeUtils.convertDate("2011-02-02").atStartOfDay().atZone(ZoneId.of("America/New_York")));
        final DateTime end = DateTimeUtils.toDateTime(
                DateTimeUtils.convertDate("2011-02-03").atStartOfDay().atZone(ZoneId.of("America/New_York")));
        final ColumnInfo<?, ?>[] leftColumnInfo;
        final QueryTable leftTable = getTable(leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"Date", "C1", "C2"},
                        new UnsortedDateTimeGenerator(start, end),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"Date", "C1", "C2"},
                        new UnsortedDateTimeGenerator(start, end),
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
        assertEquals(Arrays.asList("Ticker", "Timestamp", "OptionBid"), result.getDefinition().getColumnNames());

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.aj(lookUpValue1, "Timestamp=OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "b", "c"));

        result = lookUpValue1.aj(table, "indx=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(asList(null, null, "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("a", "b", "c"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(null, null, 2), asList(result.getColumn("Int").get(0, 3)));

        result = lookUpValue1.aj(table, "indx=String", "Int,String");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));
        result = lookUpValue1.aj(table, "indx=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("c", "c", "e"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("c", "d", "e"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(2, 2, 4), asList(result.getColumn("Int").get(0, 3)));

        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("g", "e", null), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(6, 4, null), asList(result.getColumn("Int").get(0, 3)));


        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx=String", "String");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals(2, result.numColumns());
        assertEquals(asList("g", "e", null), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));

        lookUpValue1 = testRefreshingTable(col("String", "h", "e", "a"));
        result = lookUpValue1.aj(table, "String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("g", "e", null), asList((Object[]) result.getColumn("xString").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList(6, 4, null), asList(result.getColumn("Int").get(0, 3)));
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

        Table result = table.aj(lookUpValue1.renameColumns("TS2=Timestamp"), "Ticker,Timestamp<TS2", "OptionBid");
        assertEquals(Arrays.asList("Ticker", "Timestamp", "TS2", "OptionBid"), result.getDefinition().getColumnNames());
        final long[] timestamps = result.getColumn("TS2").getLongs(0, result.size());
        TableTools.show(result);
        assertArrayEquals(new long[] {QueryConstants.NULL_LONG, 5L, 10L}, timestamps);
        assertArrayEquals(new double[] {QueryConstants.NULL_DOUBLE, .2, .3},
                result.getColumn("OptionBid").getDoubles(0, result.size()), 0.0);

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.aj(lookUpValue1, "Timestamp<OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());
        TableTools.show(result);
        assertArrayEquals(new long[] {QueryConstants.NULL_LONG, 5L, 25L},
                result.getColumn("OptionTimestamp").getLongs(0, result.size()));
        assertArrayEquals(new double[] {QueryConstants.NULL_DOUBLE, .2, .4},
                result.getColumn("OptionBid").getDoubles(0, result.size()), 0.0);

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "c", "d"));

        result = lookUpValue1.aj(table, "indx<String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(asList(null, null, "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("a", "c", "d"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(null, null, 2), asList(result.getColumn("Int").get(0, 3)));

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));

        result = lookUpValue1.aj(table, "indx<String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList(null, "c", "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("c", "d", "e"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(null, 2, 2), asList(result.getColumn("Int").get(0, 3)));

        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx<String", "String,Int");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("g", "c", null), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(6, 2, null), asList(result.getColumn("Int").get(0, 3)));


        lookUpValue1 = testRefreshingTable(col("indx", "h", "e", "a"));
        result = lookUpValue1.aj(table, "indx<String", "String");
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
        assertEquals(asList("g", "c", null), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));

        lookUpValue1 = testRefreshingTable(col("String", "h", "e", "a"));
        result = lookUpValue1.aj(table, "String<String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("g", "c", null), asList((Object[]) result.getColumn("xString").getDirect()));
        assertEquals(asList("h", "e", "a"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList(6, 2, null), asList(result.getColumn("Int").get(0, 3)));
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

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i()));
        TstUtils.validate(en);

        System.out.println("Notifying listeners of modification.");
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> table.notifyListeners(i(), i(), i(4, 5)));
        System.out.println("Finished notifying listeners of modification.");
        TstUtils.validate(en);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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

        final Table aj = left.aj(right, "LInt=RInt", "RInt,RSentinel");
        System.out.println("AJ:");
        TableTools.show(aj);

        assertEquals(asList("E2", "D4", "G6", "H8"), asList((Object[]) aj.getColumn("RSentinel").getDirect()));

        System.out.println("AJ2:");
        // let's swap the left and right
        final Table aj2 = right.sort("RSentinel").aj(left, "RInt=LInt", "LInt,LSentinel");
        TableTools.show(aj2);
        assertEquals(asList("a", null, "b", null, "b", "c", "d", "c"),
                asList((Object[]) aj2.getColumn("LSentinel").getDirect()));

    }

    @Test
    public void testAjEmptyRight() {
        final QueryTable left = TstUtils.testRefreshingTable(i(1, 2, 3, 4).toTracking(),
                col("Group", "g", "g", "g", "g"),
                col("LInt", 2, 4, 6, 8),
                col("LSentinel", "a", "b", "c", "d"));

        final QueryTable right = TstUtils.testRefreshingTable(i().toTracking(),
                col("Group", CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                intCol("RInt"),
                col("RSentinel"));

        System.out.println("Left:");
        TableTools.show(left);
        System.out.println("Right:");
        TableTools.show(right);

        final Table aj = left.aj(right, "Group,LInt=RInt", "RInt,RSentinel");
        System.out.println("AJ:");
        TableTools.show(aj);

        assertEquals(asList(null, null, null, null), asList((Object[]) aj.getColumn("RSentinel").getDirect()));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(left, i(2), col("Group", "h"), col("LInt", 4), col("LSentinel", "b"));
            left.notifyListeners(i(), i(), i(2));
        });

        TableTools.show(aj);
        assertEquals(asList(null, null, null, null), asList((Object[]) aj.getColumn("RSentinel").getDirect()));

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
        assertEquals(Arrays.asList("Ticker", "Timestamp", "OptionBid"), result.getDefinition().getColumnNames());
        assertEquals(3, result.size());
        assertEquals("Ticker", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Timestamp", result.getDefinition().getColumns().get(1).getName());
        assertEquals("OptionBid", result.getDefinition().getColumns().get(2).getName());
        assertEquals(asList("AAPL", "IBM", "AAPL"), asList((Object[]) result.getColumn("Ticker").getDirect()));
        assertEquals(asList(1L, 10L, 50L), asList(result.getColumn("Timestamp").get(0, 3)));
        assertEquals(asList(.1, .4, .5), asList(result.getColumn("OptionBid").get(0, 3)));

        table = testRefreshingTable(
                col("Timestamp", 1L, 10L, 50L));
        lookUpValue1 = testRefreshingTable(
                col("OptionTimestamp", 1L, 5L, 10L, 25L, 50L),
                col("OptionBid", .1, .2, .3, .4, .5));
        result = table.raj(lookUpValue1, "Timestamp=OptionTimestamp", "OptionBid");
        assertEquals(long.class, result.getDefinition().getColumn("OptionTimestamp").getDataType());

        table = testRefreshingTable(
                col("String", "c", "e", "g"),
                col("Int", 2, 4, 6));
        lookUpValue1 = testRefreshingTable(col("indx", "a", "b", "c"));

        result = lookUpValue1.raj(table, "indx=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(asList("c", "c", "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("a", "b", "c"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(2, 2, 2), asList(result.getColumn("Int").get(0, 3)));

        lookUpValue1 = testRefreshingTable(col("indx", "f", "g", "h"));

        result = lookUpValue1.raj(table, "indx=String", "String,Int");

        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(asList("g", "g", null), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("f", "g", "h"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(6, 6, null), asList(result.getColumn("Int").get(0, 3)));

        result = lookUpValue1.raj(table, "indx=String", "Int,String");

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());

        lookUpValue1 = testRefreshingTable(col("indx", "c", "d", "e"));

        show(lookUpValue1);
        show(table);

        result = lookUpValue1.raj(table, "indx=String", "String,Int");
        show(result);

        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList("c", "e", "e"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("c", "d", "e"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(2, 4, 4), asList(result.getColumn("Int").get(0, 3)));

        lookUpValue1 = testRefreshingTable(col("indx", "j", "e", "a"));
        result = lookUpValue1.raj(table, "indx=String", "String,Int");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList(null, "e", "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("j", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));
        assertEquals(asList(null, 4, 2), asList(result.getColumn("Int").get(0, 3)));


        lookUpValue1 = testRefreshingTable(col("indx", "j", "e", "a"));
        result = lookUpValue1.raj(table, "indx=String", "String");
        assertEquals(3, result.size());
        assertEquals("indx", result.getDefinition().getColumns().get(0).getName());
        assertEquals("String", result.getDefinition().getColumns().get(1).getName());
        assertEquals(2, result.numColumns());
        assertEquals(asList(null, "e", "c"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList("j", "e", "a"), asList((Object[]) result.getColumn("indx").getDirect()));

        lookUpValue1 = testRefreshingTable(col("String", "j", "e", "a"));
        result = lookUpValue1.raj(table, "String", "xString=String,Int");
        assertEquals(3, result.size());
        assertEquals("String", result.getDefinition().getColumns().get(0).getName());
        assertEquals("xString", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Int", result.getDefinition().getColumns().get(2).getName());
        assertEquals(3, result.numColumns());
        assertEquals(asList(null, "e", "c"), asList((Object[]) result.getColumn("xString").getDirect()));
        assertEquals(asList("j", "e", "a"), asList((Object[]) result.getColumn("String").getDirect()));
        assertEquals(asList(null, 4, 2), asList(result.getColumn("Int").get(0, 3)));
    }

    static final JoinControl SMALL_LEFT_CONTROL = new JoinControl() {
        @Override
        boolean buildLeft(QueryTable leftTable, Table rightTable) {
            return true;
        }

        @Override
        int tableSizeForRightBuild(Table rightTable) {
            return 1 << 8;
        }

        @Override
        int tableSizeForLeftBuild(Table rightTable) {
            return 1 << 8;
        }
    };

    static final JoinControl SMALL_RIGHT_CONTROL = new JoinControl() {
        @Override
        boolean buildLeft(QueryTable leftTable, Table rightTable) {
            return false;
        }


        @Override
        int tableSizeForRightBuild(Table rightTable) {
            return 1 << 8;
        }

        @Override
        int tableSizeForLeftBuild(Table rightTable) {
            return 1 << 8;
        }
    };

    static final JoinControl HIGH_LOAD_FACTOR_CONTROL = new JoinControl() {
        @Override
        int tableSizeForRightBuild(Table rightTable) {
            return 1 << 8;
        }

        @Override
        int tableSizeForLeftBuild(Table leftTable) {
            return 1 << 8;
        }

        @Override
        double getMaximumLoadFactor() {
            return 20.0;
        }

        @Override
        double getTargetLoadFactor() {
            return 19.0;
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
        final Table result = leftQueryTable.aj(sortedRightQueryTable, "C1,I1", "LI1=I1,LC1=C1,Sentinel");
        showWithRowSet(result);

        assertEquals(100, result.getColumn("Sentinel").get(3));
        assertEquals(207, result.getColumn("LI1").get(3));
        assertEquals(140, result.getColumn("Sentinel").get(4));
        assertEquals(432, result.getColumn("LI1").get(4));
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

        assertEquals(80, result.getColumn("Sentinel").get(5));
        assertEquals(821, result.getColumn("LI1").get(5));
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
        assertEquals(Arrays.asList("a", "a", "b", "b", "c", "c"), Arrays.asList(result.getColumn("X").get(0, 6)));
        assertEquals(Arrays.asList("x", "y", "x", "y", "x", "y"), Arrays.asList(result.getColumn("Y").get(0, 6)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b", "b"), col("Z", 1, 2, 3));
        result = lTable.join(rTable, "X=Y");
        assertEquals(3, result.size());
        assertEquals(3, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Y", result.getDefinition().getColumns().get(1).getName());
        assertEquals("Z", result.getDefinition().getColumns().get(2).getName());
        assertEquals(Arrays.asList("a", "b", "b"), Arrays.asList(result.getColumn("X").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "b"), Arrays.asList(result.getColumn("Y").get(0, 3)));
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(result.getColumn("Z").get(0, 3)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("Y", "a", "b"));
        result = lTable.join(rTable, "X=Y");
        show(result);
        assertEquals(2, result.size());
        assertEquals(2, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals("Y", result.getDefinition().getColumns().get(1).getName());
        assertEquals(Arrays.asList("a", "b"), Arrays.asList(result.getColumn("X").get(0, 2)));
        assertEquals(Arrays.asList("a", "b"), Arrays.asList(result.getColumn("Y").get(0, 2)));

        lTable = testRefreshingTable(col("X", "a", "b", "c"));
        rTable = testRefreshingTable(col("X", "a", "b", "d"));
        result = lTable.join(rTable, "X");
        show(result);
        assertEquals(2, result.size());
        assertEquals(1, result.numColumns());
        assertEquals("X", result.getDefinition().getColumns().get(0).getName());
        assertEquals(Arrays.asList("a", "b"), Arrays.asList(result.getColumn("X").get(0, 2)));
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
        assertEquals(asList("c", "e", "g"), asList((Object[]) pairMatch.getColumn(0).getDirect()));
        IntVector[] vValues = (IntVector[]) pairMatch.getColumn("v").getDirect();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
        DoubleVector[] uValues = (DoubleVector[]) pairMatch.getColumn("u").getDirect();
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
        assertEquals(asList("c", "e", "g"), asList((Object[]) pairMatch.getColumn(0).getDirect()));
        vValues = (IntVector[]) pairMatch.getColumn("v").getDirect();
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
        assertEquals(asList("c", "e", "g"), asList((Object[]) pairMatch.getColumn(0).getDirect()));
        vValues = (IntVector[]) pairMatch.getColumn("v").getDirect();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
        uValues = (DoubleVector[]) pairMatch.getColumn("u").getDirect();
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
        assertEquals(asList("c", "e", "g"), asList((Object[]) pairMatch.getColumn(0).getDirect()));
        vValues = (IntVector[]) pairMatch.getColumn("v").getDirect();
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
        assertEquals(asList("c", "e", "g"), asList((Object[]) noPairMatch.getColumn(0).getDirect()));
        // noinspection unchecked
        final ObjectVector<String>[] aggregateString =
                (ObjectVector<String>[]) noPairMatch.getColumn("String2").getDirect();
        assertEquals(asList("c", "e"), asList(aggregateString[0].toArray()));
        assertEquals(asList("c", "e"), asList(aggregateString[1].toArray()));
        assertEquals(asList("c", "e"), asList(aggregateString[2].toArray()));
        vValues = (IntVector[]) noPairMatch.getColumn("v").getDirect();
        assertEquals(asList(1, 2), asList(ArrayTypeUtils.getBoxedArray(vValues[0].toArray())));
        assertEquals(asList(1, 2), asList(ArrayTypeUtils.getBoxedArray(vValues[1].toArray())));
        assertEquals(asList(1, 2), asList(ArrayTypeUtils.getBoxedArray(vValues[2].toArray())));

        pairMatch = table1.naturalJoin(table2.groupBy("String2"), "String1=String2");
        assertEquals(3, pairMatch.size());
        assertEquals(3, pairMatch.numColumns());
        assertEquals("String1", pairMatch.getDefinition().getColumns().get(0).getName());
        assertEquals("String2", pairMatch.getDefinition().getColumns().get(1).getName());
        assertEquals("v", pairMatch.getDefinition().getColumns().get(2).getName());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(0).getDataType());
        assertEquals(String.class, pairMatch.getDefinition().getColumns().get(1).getDataType());
        assertEquals(IntVector.class, pairMatch.getDefinition().getColumns().get(2).getDataType());
        assertEquals(asList("c", "e", "g"), asList((Object[]) pairMatch.getColumn(0).getDirect()));

        final String[] stringColumn = (String[]) pairMatch.getColumn("String2").getDirect();
        assertEquals("c", stringColumn[0]);
        assertEquals("e", stringColumn[1]);
        assertNull(stringColumn[2]);

        vValues = (IntVector[]) pairMatch.getColumn("v").getDirect();
        assertEquals(1, vValues[0].get(0));
        assertEquals(2, vValues[1].get(0));
        assertEquals(1, vValues[0].size());
        assertEquals(1, vValues[1].size());
        assertNull(vValues[2]);
    }
}
