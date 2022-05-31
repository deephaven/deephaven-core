package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.iterators.IntegerColumnIterator;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import junit.framework.TestCase;

import java.util.*;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.table.impl.TstUtils.*;

public class TestColumnsToRowsTransform extends RefreshingTableTestCase {
    public void testStatic() {
        final Table in = TableTools.newTable(stringCol("Sym", "AAPL", "SPY"), intCol("Val1", 1, 2),
                intCol("Val2", 3, 4), intCol("Val3", 5, 6));
        final Table out = ColumnsToRowsTransform.columnsToRows(in, "Name", "Value", "Val1", "Val2");
        showWithRowSet(out);

        final Table ex1 =
                TableTools.newTable(stringCol("Sym", "AAPL", "AAPL", "SPY", "SPY"), intCol("Val3", 5, 5, 6, 6),
                        stringCol("Name", "Val1", "Val2", "Val1", "Val2"), intCol("Value", 1, 3, 2, 4));
        assertTableEquals(ex1, out);

        final Table out2 = ColumnsToRowsTransform.columnsToRows(in, "Fribble", "Value", "Val1", "Val2", "Val3");
        showWithRowSet(out2);
        final Table ex2 = TableTools.newTable(stringCol("Sym", "AAPL", "AAPL", "AAPL", "SPY", "SPY", "SPY"),
                stringCol("Fribble", "Val1", "Val2", "Val3", "Val1", "Val2", "Val3"),
                intCol("Value", 1, 3, 5, 2, 4, 6));
        assertTableEquals(ex2, out2);

        final Table out3 = ColumnsToRowsTransform.columnsToRows(in, "Label", "Value",
                new String[] {"First", "Second", "Third"}, new String[] {"Val1", "Val2", "Val3"});
        showWithRowSet(out3);

        final int[] expected = {1, 3, 5, 2, 4, 6};
        final Table ex3 = TableTools.newTable(stringCol("Sym", "AAPL", "AAPL", "AAPL", "SPY", "SPY", "SPY"),
                stringCol("Label", "First", "Second", "Third", "First", "Second", "Third"), intCol("Value", expected));
        assertTableEquals(ex3, out3);
        final Iterator<Integer> it = out3.columnIterator("Value");
        final IntegerColumnIterator it2 = out3.integerColumnIterator("Value");
        int position = 0;
        while (it.hasNext()) {
            assertEquals(expected[position++], (int) it.next());
        }
        assertEquals(expected.length, position);
        position = 0;
        while (it2.hasNext()) {
            assertEquals(expected[position++], it2.nextInt());
        }
        assertEquals(expected.length, position);

        final Table inMulti = TableTools.newTable(stringCol("Sym", "AAPL", "SPY"), intCol("Val1", 1, 2),
                doubleCol("D1", 7.7, 8.8), doubleCol("D2", 9.9, 10.1), intCol("Val2", 3, 4), intCol("Val3", 5, 6),
                doubleCol("D3", 11.11, 12.12));
        TableTools.show(inMulti);
        final Table outMulti = ColumnsToRowsTransform.columnsToRows(inMulti, "Name", new String[] {"IV", "DV"},
                new String[] {"Apple", "Banana", "Canteloupe"},
                new String[][] {new String[] {"Val1", "Val2", "Val3"}, new String[] {"D1", "D2", "D3"}});
        TableTools.show(outMulti);
        final Table expectMulti = TableTools.newTable(col("Sym", "AAPL", "AAPL", "AAPL", "SPY", "SPY", "SPY"),
                col("Name", "Apple", "Banana", "Canteloupe", "Apple", "Banana", "Canteloupe"),
                intCol("IV", 1, 3, 5, 2, 4, 6), doubleCol("DV", 7.7, 9.9, 11.11, 8.8, 10.10, 12.12));
        assertTableEquals(expectMulti, outMulti);
    }

    public void testBadSharedContext() {
        final Table in = TableTools.newTable(stringCol("Sym", "AAPL", "SPY", "TSLA", "VXX"),
                intCol("Sentinel", 100, 101, 102, 103));
        final Table in2 = TableTools.newTable(stringCol("Sym", "VXX", "TSLA", "AAPL", "SPY"), intCol("V1", 1, 2, 3, 4),
                intCol("V2", 5, 6, 7, 8));

        final Table joined = in.naturalJoin(in2, "Sym");

        TableTools.show(joined);

        final Table out = ColumnsToRowsTransform.columnsToRows(joined, "Name", "Value", "V1", "V2");
        showWithRowSet(out);

        final Table filtered = out.where("Sentinel=101 && Name=`V1` || Sentinel=102 && Name=`V2`");
        showWithRowSet(filtered);

        // noinspection unchecked
        final ColumnSource<Integer> valueSource = filtered.getColumnSource("Value");
        try (final WritableIntChunk<Values> destination = WritableIntChunk.makeWritableChunk(2);
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final ChunkSource.FillContext f1 = valueSource.makeFillContext(2, sharedContext)) {
            valueSource.fillChunk(f1, destination, filtered.getRowSet());
            System.out.println(destination.get(0));
            System.out.println(destination.get(1));
            assertEquals(4, destination.get(0));
            assertEquals(6, destination.get(1));
        }
    }


    public void testTypeMismatch() {
        final Table in = TableTools.newTable(stringCol("Sym", "AAPL", "SPY"), intCol("Val1", 1, 2),
                intCol("Val2", 3, 4), intCol("Val3", 5, 6), doubleCol("Val4", 7.0, 8.0));
        try {
            ColumnsToRowsTransform.columnsToRows(in, "Name", "Value", "Val1", "Val2", "Val4");
            TestCase.fail("Expected an exception for mismatched types.");
        } catch (IllegalArgumentException iae) {
            TestCase.assertEquals("Incompatible transpose types Val1 is int, Val4 is double", iae.getMessage());
        }
    }

    public void testMisalignment() {
        final Table in = TableTools.newTable(stringCol("Sym", "AAPL", "SPY"), intCol("Val1", 1, 2),
                intCol("Val2", 3, 4), intCol("Val3", 5, 6), doubleCol("Val4", 7.0, 8.0));
        try {
            ColumnsToRowsTransform.columnsToRows(in, "Name", new String[] {"Foo", "Bar"}, new String[] {"A", "B"},
                    new String[][] {new String[] {"Val1", "Val2"}, new String[] {"Val4"}});
            TestCase.fail("Expected an exception for mismatched types.");
        } catch (IllegalArgumentException iae) {
            TestCase.assertEquals("2 labels defined, but 1 transpose columns defined for Bar.", iae.getMessage());
        }
    }

    public void testIncremental() {
        for (int seed = 0; seed < 1; ++seed) {
            testIncremental(seed);
        }
    }

    private void testIncremental(int seed) {
        final Random random = new Random(0);
        final TstUtils.ColumnInfo[] columnInfo;
        final int size = 30;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "D1", "D2", "D3", "I1", "I2", "I3", "I4", "I5"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"),
                        new TstUtils.DoubleGenerator(0, 10),
                        new TstUtils.DoubleGenerator(10, 100),
                        new TstUtils.DoubleGenerator(100, 1000),
                        new TstUtils.IntGenerator(1000, 10000),
                        new TstUtils.IntGenerator(10000, 100000),
                        new TstUtils.IntGenerator(100000, 1000000),
                        new TstUtils.IntGenerator(1000000, 10000000),
                        new TstUtils.IntGenerator(10000000, 100000000)));

        final Map<String, String> nameMap = new HashMap<>();
        nameMap.put("I1", "EyeOne");
        nameMap.put("I2", "AiTwo");
        nameMap.put("I3", "IThree");
        nameMap.put("First", "EyeOne");
        nameMap.put("Second", "AiTwo");
        nameMap.put("Third", "IThree");
        QueryScope.addParam("nameMap", Collections.unmodifiableMap(nameMap));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(
                        () -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "D1", "D2", "D3")),
                EvalNugget.from(() -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2",
                        "I3", "I4", "I5")),
                EvalNugget.from(() -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2",
                        "I3", "I4")),
                EvalNugget.from(
                        () -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3")),
                EvalNugget.from(() -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2")),
                EvalNugget.from(() -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1")),
                EvalNugget.from(() -> ColumnsToRowsTransform
                        .columnsToRows(queryTable.sort("I5"), "Name", "Value", "I1", "I2", "I3")
                        .where("Name in `I1`, `I3`")),
                EvalNugget.from(() -> ColumnsToRowsTransform
                        .columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3").where("Name in `I1`, `I3`")),
                EvalNugget.from(() -> ColumnsToRowsTransform
                        .columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3").where("Name in `I1`")),
                EvalNugget.from(() -> ColumnsToRowsTransform
                        .columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3")
                        .updateView("MappedVal=nameMap.get(Name)").where("MappedVal in `EyeOne` || Value > 50000")),
                new QueryTableTestBase.TableComparator(
                        ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3"),
                        UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> queryTable
                                        .update("Name=new String[]{`I1`, `I2`, `I3`}", "Value=new int[]{I1, I2, I3}")
                                        .dropColumns("I1", "I2", "I3").ungroup())),
                new QueryTableTestBase.TableComparator(
                        ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3")
                                .updateView("MappedVal=nameMap.get(Name)")
                                .where("MappedVal in `EyeOne` || Value > 50000"),
                        UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> queryTable
                                        .update("Name=new String[]{`I1`, `I2`, `I3`}", "Value=new int[]{I1, I2, I3}")
                                        .dropColumns("I1", "I2", "I3").ungroup())
                                .updateView("MappedVal=nameMap.get(Name)")
                                .where("MappedVal in `EyeOne` || Value > 50000")),
                new QueryTableTestBase.TableComparator(
                        ColumnsToRowsTransform.columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3")
                                .updateView("MappedVal=nameMap.get(Name)").where("MappedVal in `EyeOne`"),
                        UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> queryTable
                                        .update("Name=new String[]{`I1`, `I2`, `I3`}", "Value=new int[]{I1, I2, I3}")
                                        .dropColumns("I1", "I2", "I3").ungroup())
                                .updateView("MappedVal=nameMap.get(Name)").where("MappedVal in `EyeOne`")),
                EvalNugget.from(() -> ColumnsToRowsTransform
                        .columnsToRows(queryTable, "Name", "Value", "I1", "I2", "I3").where("Value > 50000")),
                EvalNugget.from(() -> ColumnsToRowsTransform.columnsToRows(queryTable, "Name",
                        new String[] {"IV", "DV"}, new String[] {"First", "Second", "Third"},
                        new String[][] {new String[] {"I1", "I2", "I3"}, new String[] {"D1", "D2", "D3"}})),
                new QueryTableTestBase.TableComparator(
                        ColumnsToRowsTransform.columnsToRows(queryTable, "Name", new String[] {"IV", "DV"},
                                new String[] {"First", "Second", "Third"},
                                new String[][] {new String[] {"I1", "I2", "I3"}, new String[] {"D1", "D2", "D3"}}),
                        UpdateGraphProcessor.DEFAULT.sharedLock()
                                .computeLocked(() -> queryTable
                                        .update("Name=new String[]{`First`, `Second`, `Third`}",
                                                "IV=new int[]{I1, I2, I3}", "DV=new double[]{D1, D2, D3}")
                                        .dropColumns("I1", "I2", "I3", "D1", "D2", "D3").ungroup())),
                new QueryTableTestBase.TableComparator(
                        ColumnsToRowsTransform
                                .columnsToRows(queryTable, "Name", new String[] {"IV", "DV"},
                                        new String[] {"First", "Second", "Third"},
                                        new String[][] {new String[] {"I1", "I2", "I3"},
                                                new String[] {"D1", "D2", "D3"}})
                                .updateView("MappedVal=nameMap.get(Name)").where("MappedVal in `AiTwo`"),
                        UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> queryTable
                                .update("Name=new String[]{`First`, `Second`, `Third`}", "IV=new int[]{I1, I2, I3}",
                                        "DV=new double[]{D1, D2, D3}")
                                .dropColumns("I1", "I2", "I3", "D1", "D2", "D3").ungroup()
                                .updateView("MappedVal=nameMap.get(Name)").where("MappedVal in `AiTwo`"))),
        };

        for (int step = 0; step < 100; step++) {
            if (printTableUpdates) {
                System.out.println("Step = " + step + ", seed = " + seed);
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}
