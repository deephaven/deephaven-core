//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.WouldMatchPair;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.show;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class QueryTableWouldMatchTest extends QueryTableTestBase {

    public void testMatch() {
        final QueryTable t1 = testRefreshingTable(
                col("Text", "Hey", "Yo", "Lets go", "Dog", "Cat", "Cheese"),
                col("Number", 0, 1, 2, 3, 4, 5),
                col("Bool", true, false, true, true, false, false));

        final QueryTable t1Matched = (QueryTable) t1.wouldMatch("HasAnE=Text.contains(`e`)", "isGt3=Number > 3",
                "Compound=Bool || Text.length() < 5");
        final ShiftObliviousListener t1MatchedListener = newListenerWithGlobals(t1Matched);
        t1Matched.addUpdateListener(t1MatchedListener);

        show(t1Matched);
        assertArrayEquals(new Boolean[] {true, false, true, false, false, true},
                ColumnVectors.ofObject(t1Matched, "HasAnE", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {false, false, false, false, true, true},
                ColumnVectors.ofObject(t1Matched, "isGt3", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, true, true, true, true, false},
                ColumnVectors.ofObject(t1Matched, "Compound", Boolean.class).toArray());

        // Add
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(7, 9), col("Text", "Cake", "Zips For Fun"),
                    col("Number", 6, 1),
                    col("Bool", false, false));
            t1.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals(added, i(7, 9));
        assertEquals(modified, i());
        assertEquals(removed, i());
        assertArrayEquals(new Boolean[] {true, false, true, false, false, true, true, false},
                ColumnVectors.ofObject(t1Matched, "HasAnE", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {false, false, false, false, true, true, true, false},
                ColumnVectors.ofObject(t1Matched, "isGt3", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, true, true, true, true, false, true, false},
                ColumnVectors.ofObject(t1Matched, "Compound", Boolean.class).toArray());

        // Remove
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(t1, i(1, 3));
            t1.notifyListeners(i(), i(1, 3), i());
        });

        assertEquals(added, i());
        assertEquals(modified, i());
        assertEquals(removed, i(1, 3));
        assertArrayEquals(new Boolean[] {true, true, false, true, true, false},
                ColumnVectors.ofObject(t1Matched, "HasAnE", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {false, false, true, true, true, false},
                ColumnVectors.ofObject(t1Matched, "isGt3", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, true, true, false, true, false},
                ColumnVectors.ofObject(t1Matched, "Compound", Boolean.class).toArray());

        // Modify
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(4, 5),
                    col("Text", "Kittie", "Bacon"),
                    col("Number", 2, 1),
                    col("Bool", true, true));
            t1.notifyListeners(i(), i(), i(4, 5));
        });

        assertEquals(added, i());
        assertEquals(modified, i(4, 5));
        assertEquals(removed, i());
        assertArrayEquals(new Boolean[] {true, true, true, false, true, false},
                ColumnVectors.ofObject(t1Matched, "HasAnE", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {false, false, false, false, true, false},
                ColumnVectors.ofObject(t1Matched, "isGt3", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, true, true, true, true, false},
                ColumnVectors.ofObject(t1Matched, "Compound", Boolean.class).toArray());

        // All 3
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(0, 1, 4, 11),
                    col("Text", "Apple", "Bagel", "Boat", "YAY"),
                    col("Number", 100, -200, 300, 400),
                    col("Bool", true, false, false, true));
            removeRows(t1, i(9, 5));
            t1.notifyListeners(i(1, 11), i(9, 5), i(0, 4));
        });

        assertEquals(added, i(1, 11));
        assertEquals(modified, i(0, 4));
        assertEquals(removed, i(9, 5));
        assertArrayEquals(new Boolean[] {true, true, true, false, true, false},
                ColumnVectors.ofObject(t1Matched, "HasAnE", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, false, false, true, true, true},
                ColumnVectors.ofObject(t1Matched, "isGt3", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, false, true, true, true, true},
                ColumnVectors.ofObject(t1Matched, "Compound", Boolean.class).toArray());
    }

    public void testMatchRefilter() {
        doTestMatchRefilter(false);
        doTestMatchRefilter(true);
    }

    private void doTestMatchRefilter(boolean isRefreshing) {
        final QueryTable t1 = testRefreshingTable(
                col("Text", "Hey", "Yo", "Lets go", "Dog", "Cat", "Cheese"),
                col("Number", 0, 1, 2, 3, 4, 5),
                col("Bool", true, false, true, true, false, false));
        t1.setRefreshing(isRefreshing);

        final QueryTable textTable = testRefreshingTable(col("Text", "Dog", "Cat"));
        final QueryTable numberTable = testRefreshingTable(col("Number", 0, 5));

        final WouldMatchPair sp1 =
                new WouldMatchPair("InText", new DynamicWhereFilter(textTable, true, new MatchPair("Text", "Text")));
        final WouldMatchPair sp2 = new WouldMatchPair("InNum",
                new DynamicWhereFilter(numberTable, true, new MatchPair("Number", "Number")));

        final QueryTable t1Matched = (QueryTable) t1.wouldMatch(sp1, sp2);
        show(t1Matched);

        assertArrayEquals(new Boolean[] {false, false, false, true, true, false},
                ColumnVectors.ofObject(t1Matched, "InText", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, false, false, false, false, true},
                ColumnVectors.ofObject(t1Matched, "InNum", Boolean.class).toArray());

        // Tick one filter table
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(textTable, i(0, 2), col("Text", "Cheese", "Yo"));
            textTable.notifyListeners(i(2), i(), i(0));
        });

        assertArrayEquals(new Boolean[] {false, true, false, false, true, true},
                ColumnVectors.ofObject(t1Matched, "InText", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {true, false, false, false, false, true},
                ColumnVectors.ofObject(t1Matched, "InNum", Boolean.class).toArray());

        // Tick both of them
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(textTable, i(0, 2), col("Text", "Lets go", "Hey"));
            textTable.notifyListeners(i(), i(), i(0, 2));

            addToTable(numberTable, i(2), col("Number", 2));
            removeRows(numberTable, i(0));
            numberTable.notifyListeners(i(2), i(0), i());
        });

        assertArrayEquals(new Boolean[] {true, false, true, false, true, false},
                ColumnVectors.ofObject(t1Matched, "InText", Boolean.class).toArray());
        assertArrayEquals(new Boolean[] {false, false, true, false, false, true},
                ColumnVectors.ofObject(t1Matched, "InNum", Boolean.class).toArray());

        if (isRefreshing) {
            // Tick both of them, and the table itself
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(textTable, i(0, 2), col("Text", "Dog", "Yo"));
                textTable.notifyListeners(i(), i(), i(0, 2));

                addToTable(t1, i(0, 1, 4, 11),
                        col("Text", "Yo", "Hey", "Boat", "Yo"),
                        col("Number", 100, 1, 300, 0),
                        col("Bool", true, false, false, true));
                removeRows(t1, i(3));
                t1.notifyListeners(i(11), i(3), i(0, 1, 4));

                addToTable(numberTable, i(3, 5), col("Number", 0, 1));
                numberTable.notifyListeners(i(3, 5), i(), i());
            });

            show(t1);
            show(textTable);
            show(numberTable);

            assertArrayEquals(new Boolean[] {true, false, false, false, false, true},
                    ColumnVectors.ofObject(t1Matched, "InText", Boolean.class).toArray());
            assertArrayEquals(new Boolean[] {false, true, true, false, true, true},
                    ColumnVectors.ofObject(t1Matched, "InNum", Boolean.class).toArray());
        }
    }

    public void testMatchIterative() {
        final Random random = new Random(0xDEADDEAD);
        final ColumnInfo<?, ?>[] columnInfo =
                initColumnInfos(new String[] {"Sym", "Stringy", "Inty", "Floaty", "Charry", "Booly"},
                        new SetGenerator<>("AAPL", "GOOG", "GLD", "VXX"),
                        new StringGenerator(0xFEEDFEED),
                        new IntGenerator(10, 100),
                        new FloatGenerator(10.0f, 200.f),
                        new CharGenerator('A', 'Z'),
                        new BooleanGenerator());

        final QueryTable queryTable = getTable(500, random, columnInfo);

        QueryScope.addParam("bogus", Arrays.asList(new Object[] {null}));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.wouldMatch("hasAG=Sym.contains(`G`)",
                        "BigHero6=Stringy.length()>=6 && Booly", "Mathy=(Inty+Floaty)/2 > 40")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep("step == " + i, 1000, random, queryTable, columnInfo, en);
        }
    }

    public void testColumnSourceMatch() {
        final Random random = new Random(0xDEADDEAD);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "Sentinel"},
                new SetGenerator<>("AAPL", "GOOG", "GLD", "VXX"),
                new IntGenerator(10, 100));

        final QueryTable queryTable = getTable(500, random, columnInfo);

        QueryScope.addParam("bogus", Arrays.asList(new Object[] {null}));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG == true")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("!hasAG"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG == false")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("!hasAG"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG not in true")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG not in false")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("true"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG in true, false")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").head(0),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG in bogus")),
                new TableComparator(queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("true"),
                        queryTable.wouldMatch("hasAG=Sym.contains(`G`)").where("hasAG not in bogus")),
        };

        for (int i = 0; i < 10; i++) {
            simulateShiftAwareStep("step == " + i, 1000, random, queryTable, columnInfo, en);
        }
    }

    public void testMatchDynamicIterative() {
        final ColumnInfo<?, ?>[] symSetInfo;
        final ColumnInfo<?, ?>[] numSetInfo;
        final ColumnInfo<?, ?>[] filteredInfo;

        final int setSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable symSetTableBase = getTable(setSize, random, symSetInfo = initColumnInfos(new String[] {"Sym"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd")));

        final QueryTable numSetTableBase =
                getTable(setSize, random, numSetInfo = initColumnInfos(new String[] {"intCol"},
                        new IntGenerator(0, 100)));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final QueryTable symSetTable = (QueryTable) updateGraph.exclusiveLock().computeLocked(
                () -> symSetTableBase.selectDistinct("Sym"));
        final QueryTable numSetTable = (QueryTable) updateGraph.exclusiveLock().computeLocked(
                () -> numSetTableBase.selectDistinct("intCol"));

        final QueryTable matchTable = getTable(filteredSize, random,
                filteredInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> {
                    final WouldMatchPair sp1 = new WouldMatchPair("InSym",
                            new DynamicWhereFilter(symSetTable, true, new MatchPair("Sym", "Sym")));
                    final WouldMatchPair sp2 = new WouldMatchPair("InInt",
                            new DynamicWhereFilter(numSetTable, true, new MatchPair("intCol", "intCol")));
                    return matchTable.wouldMatch(sp1, sp2);
                })
        };

        try {
            for (int i = 0; i < 100; i++) {
                final boolean modSet = random.nextInt(10) < 3;
                final boolean modFiltered = random.nextBoolean();

                final int doit = i & 0x3;
                updateGraph.runWithinUnitTestCycle(() -> {
                    if (modSet) {
                        if (doit == 0 || doit == 2) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                    setSize, random, symSetTableBase, symSetInfo);
                        }

                        if (doit == 1 || doit == 2) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                    setSize, random, numSetTableBase, numSetInfo);
                        }
                    }
                });
                validate(en);

                updateGraph.runWithinUnitTestCycle(() -> {
                    if (modFiltered) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                filteredSize, random, matchTable, filteredInfo);
                    }
                });
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getMessage());
        }
    }

    /**
     * A match column that collides with an existing column would silently shadow it, so the operation refuses it.
     */
    public void testMatchRejectsACollidingColumnName() {
        final QueryTable source = testRefreshingTable(col("Text", "Hey", "Yo"), col("Number", 0, 1));
        try {
            source.wouldMatch("Text=Number > 0");
            TestCase.fail("Expected a colliding match column to be rejected");
        } catch (final UncheckedTableException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("already contains"));
        }
    }

    /**
     * The match column is evaluated against a row set that is not the result's own, so the virtual row variables would
     * not mean what they appear to mean.
     */
    public void testMatchRejectsVirtualRowVariables() {
        final QueryTable source = testRefreshingTable(col("Number", 0, 1, 2));
        try {
            source.wouldMatch("M=i > 1");
            TestCase.fail("Expected virtual row variables to be rejected");
        } catch (final UncheckedTableException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("virtual row variables"));
        }
    }

    /**
     * Column vectors have the same problem as the virtual row variables, and are refused for the same reason.
     */
    public void testMatchRejectsColumnVectors() {
        final QueryTable source = testRefreshingTable(col("Number", 0, 1, 2));
        try {
            source.wouldMatch("M=Number_.size() > 1");
            TestCase.fail("Expected column vectors to be rejected");
        } catch (final UncheckedTableException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("column Vectors"));
        }
    }

    /**
     * A static table with a static filter needs no snapshot control, no listener and no merged listener at all. This is
     * the only shape of {@code wouldMatch} that needs none of them, so it is the only one that exercises skipping them.
     */
    public void testStaticMatch() {
        final QueryTable source = testTable(col("Text", "Hey", "Yo", "Lets go"), col("Number", 0, 1, 2));
        final Table result = source.wouldMatch("M=Number > 0");

        assertFalse(result.isRefreshing());
        assertArrayEquals(new Boolean[] {false, true, true},
                ColumnVectors.ofObject(result, "M", Boolean.class).toArray());
    }

    /**
     * The match column answers for previous values as well as current ones, both a row at a time and through the match
     * that a {@code where} on the match column uses.
     */
    public void testMatchColumnPreviousValues() {
        final QueryTable source = testRefreshingTable(i(2, 4, 6).toTracking(), col("Number", 0, 1, 2));
        final Table result = source.wouldMatch("M=Number > 1");
        final ColumnSource<Boolean> matchColumn = result.getColumnSource("M");
        assertEquals(Boolean.FALSE, matchColumn.get(2));
        assertEquals(Boolean.TRUE, matchColumn.get(6));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        try {
            addToTable(source, i(2), col("Number", 7));
            source.notifyListeners(i(), i(), i(2));
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!result.satisfied(updateGraph.clock().currentStep())) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // The row now matches, but did not on the previous step.
            assertEquals(Boolean.TRUE, matchColumn.get(2));
            assertEquals(Boolean.FALSE, matchColumn.getPrev(2));
            // A row that matched on the previous step still reports that it did.
            assertEquals(Boolean.TRUE, matchColumn.getPrev(6));

            // The same distinction through the column source's own match, which is what a where on the match column
            // uses when it reads previous values.
            try (final WritableRowSet currentMatches =
                    matchColumn.match(false, MatchOptions.REGULAR, result.getRowSet(), true);
                    final WritableRowSet previousMatches =
                            matchColumn.match(true, MatchOptions.REGULAR, result.getRowSet(), true)) {
                assertEquals(i(2, 6), currentMatches);
                assertEquals(i(6), previousMatches);
            }
        } finally {
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * Matching a boolean column against both {@code true} and {@code false} answers for every row, or, inverted, for
     * none of them, without consulting the match column's row set at all.
     */
    public void testMatchColumnAgainstBothBooleans() {
        final QueryTable source = testRefreshingTable(i(2, 4, 6).toTracking(), col("Number", 0, 1, 2));
        final Table result = source.wouldMatch("M=Number > 1");
        final ColumnSource<Boolean> matchColumn = result.getColumnSource("M");

        try (final WritableRowSet all =
                matchColumn.match(false, MatchOptions.REGULAR, result.getRowSet(), true, false);
                final WritableRowSet none =
                        matchColumn.match(false, MatchOptions.INVERTED, result.getRowSet(), true, false)) {
            assertEquals(result.getRowSet(), all);
            assertTrue(none.isEmpty());
        }
    }

    /**
     * A filter that asks for a full recompute, rather than for matched or unmatched rows, re-evaluates the match column
     * on the next cycle. An incremental release filter is the simplest such filter.
     */
    public void testMatchWithFullRecomputeRequests() {
        final QueryTable source = testRefreshingTable(i(2, 4, 6).toTracking(), col("Number", 0, 1, 2));
        final IncrementalReleaseFilter releaseFilter = new IncrementalReleaseFilter(1, 1);
        final Table result = source.wouldMatch(new WouldMatchPair("M", releaseFilter));
        releaseFilter.start();

        assertArrayEquals(new Boolean[] {true, false, false},
                ColumnVectors.ofObject(result, "M", Boolean.class).toArray());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(releaseFilter::run);
        assertArrayEquals(new Boolean[] {true, true, false},
                ColumnVectors.ofObject(result, "M", Boolean.class).toArray());

        updateGraph.runWithinUnitTestCycle(releaseFilter::run);
        assertArrayEquals(new Boolean[] {true, true, true},
                ColumnVectors.ofObject(result, "M", Boolean.class).toArray());
    }

}
