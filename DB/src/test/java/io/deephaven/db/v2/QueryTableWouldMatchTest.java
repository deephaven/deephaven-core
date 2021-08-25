package io.deephaven.db.v2;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.WouldMatchPair;
import io.deephaven.db.v2.select.DynamicWhereFilter;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.show;
import static io.deephaven.db.v2.TstUtils.*;

@Category(OutOfBandTest.class)
public class QueryTableWouldMatchTest extends QueryTableTestBase {

    public void testMatch() {
        final QueryTable t1 = testRefreshingTable(
            c("Text", "Hey", "Yo", "Lets go", "Dog", "Cat", "Cheese"),
            c("Number", 0, 1, 2, 3, 4, 5),
            c("Bool", true, false, true, true, false, false));

        final QueryTable t1Matched = (QueryTable) t1.wouldMatch("HasAnE=Text.contains(`e`)",
            "isGt3=Number > 3", "Compound=Bool || Text.length() < 5");
        final Listener t1MatchedListener = new ListenerWithGlobals(t1Matched);
        t1Matched.listenForUpdates(t1MatchedListener);

        show(t1Matched);
        assertEquals(Arrays.asList(true, false, true, false, false, true),
            Arrays.asList(t1Matched.getColumn("HasAnE").get(0, 6)));
        assertEquals(Arrays.asList(false, false, false, false, true, true),
            Arrays.asList(t1Matched.getColumn("isGt3").get(0, 6)));
        assertEquals(Arrays.asList(true, true, true, true, true, false),
            Arrays.asList(t1Matched.getColumn("Compound").get(0, 6)));

        // Add
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(7, 9), c("Text", "Cake", "Zips For Fun"),
                c("Number", 6, 1),
                c("Bool", false, false));
            t1.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals(added, i(7, 9));
        assertEquals(modified, i());
        assertEquals(removed, i());
        assertEquals(Arrays.asList(true, false, true, false, false, true, true, false),
            Arrays.asList(t1Matched.getColumn("HasAnE").get(0, 8)));
        assertEquals(Arrays.asList(false, false, false, false, true, true, true, false),
            Arrays.asList(t1Matched.getColumn("isGt3").get(0, 8)));
        assertEquals(Arrays.asList(true, true, true, true, true, false, true, false),
            Arrays.asList(t1Matched.getColumn("Compound").get(0, 8)));

        // Remove
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            removeRows(t1, i(1, 3));
            t1.notifyListeners(i(), i(1, 3), i());
        });

        assertEquals(added, i());
        assertEquals(modified, i());
        assertEquals(removed, i(1, 3));
        assertEquals(Arrays.asList(true, true, false, true, true, false),
            Arrays.asList(t1Matched.getColumn("HasAnE").get(0, 8)));
        assertEquals(Arrays.asList(false, false, true, true, true, false),
            Arrays.asList(t1Matched.getColumn("isGt3").get(0, 8)));
        assertEquals(Arrays.asList(true, true, true, false, true, false),
            Arrays.asList(t1Matched.getColumn("Compound").get(0, 8)));

        // Modify
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(4, 5),
                c("Text", "Kittie", "Bacon"),
                c("Number", 2, 1),
                c("Bool", true, true));
            t1.notifyListeners(i(), i(), i(4, 5));
        });

        assertEquals(added, i());
        assertEquals(modified, i(4, 5));
        assertEquals(removed, i());
        assertEquals(Arrays.asList(true, true, true, false, true, false),
            Arrays.asList(t1Matched.getColumn("HasAnE").get(0, 8)));
        assertEquals(Arrays.asList(false, false, false, false, true, false),
            Arrays.asList(t1Matched.getColumn("isGt3").get(0, 8)));
        assertEquals(Arrays.asList(true, true, true, true, true, false),
            Arrays.asList(t1Matched.getColumn("Compound").get(0, 8)));

        // All 3
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(t1, i(0, 1, 4, 11),
                c("Text", "Apple", "Bagel", "Boat", "YAY"),
                c("Number", 100, -200, 300, 400),
                c("Bool", true, false, false, true));
            removeRows(t1, i(9, 5));
            t1.notifyListeners(i(1, 11), i(9, 5), i(0, 4));
        });

        assertEquals(added, i(1, 11));
        assertEquals(modified, i(0, 4));
        assertEquals(removed, i(9, 5));
        assertEquals(Arrays.asList(true, true, true, false, true, false),
            Arrays.asList(t1Matched.getColumn("HasAnE").get(0, 11)));
        assertEquals(Arrays.asList(true, false, false, true, true, true),
            Arrays.asList(t1Matched.getColumn("isGt3").get(0, 11)));
        assertEquals(Arrays.asList(true, false, true, true, true, true),
            Arrays.asList(t1Matched.getColumn("Compound").get(0, 11)));
    }

    public void testMatchRefilter() {
        doTestMatchRefilter(false);
        doTestMatchRefilter(true);
    }

    private void doTestMatchRefilter(boolean isLive) {
        final QueryTable t1 = testRefreshingTable(
            c("Text", "Hey", "Yo", "Lets go", "Dog", "Cat", "Cheese"),
            c("Number", 0, 1, 2, 3, 4, 5),
            c("Bool", true, false, true, true, false, false));
        t1.setRefreshing(isLive);

        final QueryTable textTable = testRefreshingTable(c("Text", "Dog", "Cat"));
        final QueryTable numberTable = testRefreshingTable(c("Number", 0, 5));

        final WouldMatchPair sp1 = new WouldMatchPair("InText",
            new DynamicWhereFilter(textTable, true, new MatchPair("Text", "Text")));
        final WouldMatchPair sp2 = new WouldMatchPair("InNum",
            new DynamicWhereFilter(numberTable, true, new MatchPair("Number", "Number")));

        final QueryTable t1Matched = (QueryTable) t1.wouldMatch(sp1, sp2);
        show(t1Matched);

        assertEquals(Arrays.asList(false, false, false, true, true, false),
            Arrays.asList(t1Matched.getColumn("InText").get(0, 6)));
        assertEquals(Arrays.asList(true, false, false, false, false, true),
            Arrays.asList(t1Matched.getColumn("InNum").get(0, 6)));

        // Tick one filter table
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(textTable, i(0, 2), c("Text", "Cheese", "Yo"));
            textTable.notifyListeners(i(2), i(), i(0));
        });

        assertEquals(Arrays.asList(false, true, false, false, true, true),
            Arrays.asList(t1Matched.getColumn("InText").get(0, 6)));
        assertEquals(Arrays.asList(true, false, false, false, false, true),
            Arrays.asList(t1Matched.getColumn("InNum").get(0, 6)));

        // Tick both of them
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(textTable, i(0, 2), c("Text", "Lets go", "Hey"));
            textTable.notifyListeners(i(), i(), i(0, 2));

            addToTable(numberTable, i(2), c("Number", 2));
            removeRows(numberTable, i(0));
            numberTable.notifyListeners(i(2), i(0), i());
        });

        assertEquals(Arrays.asList(true, false, true, false, true, false),
            Arrays.asList(t1Matched.getColumn("InText").get(0, 6)));
        assertEquals(Arrays.asList(false, false, true, false, false, true),
            Arrays.asList(t1Matched.getColumn("InNum").get(0, 6)));

        if (isLive) {
            // Tick both of them, and the table itself
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(textTable, i(0, 2), c("Text", "Dog", "Yo"));
                textTable.notifyListeners(i(), i(), i(0, 2));

                addToTable(t1, i(0, 1, 4, 11),
                    c("Text", "Yo", "Hey", "Boat", "Yo"),
                    c("Number", 100, 1, 300, 0),
                    c("Bool", true, false, false, true));
                removeRows(t1, i(3));
                t1.notifyListeners(i(11), i(3), i(0, 1, 4));

                addToTable(numberTable, i(3, 5), c("Number", 0, 1));
                numberTable.notifyListeners(i(3, 5), i(), i());
            });

            show(t1);
            show(textTable);
            show(numberTable);

            assertEquals(Arrays.asList(true, false, false, false, false, true),
                Arrays.asList(t1Matched.getColumn("InText").get(0, 11)));
            assertEquals(Arrays.asList(false, true, true, false, true, true),
                Arrays.asList(t1Matched.getColumn("InNum").get(0, 11)));
        }
    }

    public void testMatchIterative() {
        final Random random = new Random(0xDEADDEAD);
        final ColumnInfo[] columnInfo =
            initColumnInfos(new String[] {"Sym", "Stringy", "Inty", "Floaty", "Charry", "Booly"},
                new SetGenerator<>("AAPL", "GOOG", "GLD", "VXX"),
                new StringGenerator(0xFEEDFEED),
                new IntGenerator(10, 100),
                new FloatGenerator(10.0f, 200.f),
                new CharGenerator('A', 'Z'),
                new BooleanGenerator());

        final QueryTable queryTable = getTable(500, random, columnInfo);

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.wouldMatch("hasAG=Sym.contains(`G`)",
                    "BigHero6=Stringy.length()>=6 && Booly", "Mathy=(Inty+Floaty)/2 > 40")),
        };

        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep("step == " + i, 1000, random, queryTable, columnInfo, en);
        }
    }

    public void testMatchDynamicIterative() {
        final ColumnInfo[] symSetInfo;
        final ColumnInfo[] numSetInfo;
        final ColumnInfo[] filteredInfo;

        final int setSize = 10;
        final int filteredSize = 500;
        final Random random = new Random(0);

        final QueryTable symSetTableBase =
            getTable(setSize, random, symSetInfo = initColumnInfos(new String[] {"Sym"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd")));

        final QueryTable numSetTableBase =
            getTable(setSize, random, numSetInfo = initColumnInfos(new String[] {"intCol"},
                new IntGenerator(0, 100)));

        final QueryTable symSetTable = (QueryTable) LiveTableMonitor.DEFAULT.exclusiveLock()
            .computeLocked(() -> symSetTableBase.selectDistinct("Sym"));
        final QueryTable numSetTable = (QueryTable) LiveTableMonitor.DEFAULT.exclusiveLock()
            .computeLocked(() -> numSetTableBase.selectDistinct("intCol"));

        final QueryTable matchTable = getTable(filteredSize, random,
            filteredInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                new IntGenerator(0, 100),
                new DoubleGenerator(0, 100)));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> {
                    final WouldMatchPair sp1 = new WouldMatchPair("InSym",
                        new DynamicWhereFilter(symSetTable, true, new MatchPair("Sym", "Sym")));
                    final WouldMatchPair sp2 =
                        new WouldMatchPair("InInt", new DynamicWhereFilter(numSetTable, true,
                            new MatchPair("intCol", "intCol")));
                    return matchTable.wouldMatch(sp1, sp2);
                })
        };

        try {
            for (int i = 0; i < 1000; i++) {
                final boolean modSet = random.nextInt(10) < 3;
                final boolean modFiltered = random.nextBoolean();

                final int doit = i & 0x3;
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    if (modSet) {
                        if (doit == 0 || doit == 2) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, setSize, random,
                                symSetTableBase, symSetInfo);
                        }

                        if (doit == 1 || doit == 2) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, setSize, random,
                                numSetTableBase, numSetInfo);
                        }
                    }
                });
                validate(en);

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    if (modFiltered) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(
                            GenerateTableUpdates.DEFAULT_PROFILE, filteredSize, random, matchTable,
                            filteredInfo);
                    }
                });
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getMessage());
        }
    }
}
