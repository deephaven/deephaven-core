//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.LongChunkEquals;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class ShiftedColumnOperationTest {
    @Rule
    public EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testSimpleMatchPair() {
        for (int shift = 1; shift < 5; shift++) {
            testSimpleMatchPair(shift);
        }
    }

    private void testSimpleMatchPair(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30),
                intCol("Value2", 202, 222, 242, 262, 282, 302));

        final Table shiftMinusConst =
                ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value", "CV32=Value2");
        final Table shiftPlusConst =
                ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value", "CV22=Value2");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6, 8, 22, 24), intCol("Sentinel", 4, 5, 12, 13), intCol("Value", 16, 18, 32, 34),
                    intCol("Value2", 162, 182, 322, 342));
            table.notifyListeners(i(6, 8, 22, 24), i(), i());

        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        String[] minusConstCols =
                new String[] {"CV3=Value_[i-" + shiftConst + "]", "CV32=Value2_[i-" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);

        String[] plusConstCols =
                new String[] {"CV2=Value_[i+" + shiftConst + "]", "CV22=Value2_[i+" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 26, 28), intCol("Sentinel", 2, 3, 14, 15), intCol("Value", 12, 14, 36, 38),
                    intCol("Value2", 122, 142, 362, 382));
            removeRows(table, i(6, 24));
            table.notifyListeners(i(2, 4, 26, 28), i(6, 24), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testSimpleFillChunkSingleColumnChanges() {
        for (int shift = 1; shift < 5; shift++) {
            testSimpleFillChunkSingleColumnChanges(shift);
        }
    }

    private void testSimpleFillChunkSingleColumnChanges(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0, 1, 12, 14), intCol("Sentinel", -1, 0, 6, 7), intCol("Value", 6, 8, 20, 22));
            addToTable(table, i(8), intCol("Sentinel", 18), intCol("Value", 25));
            table.notifyListeners(i(0, 1, 12, 14), i(), i(8));
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testModifiedColSet() {
        for (int shift = 1; shift < 5; shift++) {
            testModifiedColSet(shift);
        }
    }

    private void testModifiedColSet(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30),
                intCol("Value2", 202, 222, 242, 262, 282, 302),
                intCol("Value3", 2020, 2220, 2420, 2620, 2820, 3020));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value",
                "CV32=Value2", "CV33=Value3");
        final Table shiftPlusConst =
                ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value", "CV22=Value2", "CV23=Value3");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table,
                    i(10, 12, 18),
                    intCol("Sentinel", 6, 7, 10),
                    intCol("Value", 420, 422, 428),
                    intCol("Value2", 202, 222, 282),
                    intCol("Value3", 2020, 2220, 2820));

            final RowSetShiftData.Builder shiftDataBuilder1 = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData1 = shiftDataBuilder1.build();
            final TableUpdateImpl update1 = new TableUpdateImpl(i(), i(), i(10, 12, 18), shiftData1,
                    table.newModifiedColumnSet("Value"));
            table.notifyListeners(update1);
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        String[] minusConstCols = new String[] {"CV3=Value_[i-" + shiftConst + "]",
                "CV32=Value2_[i-" + shiftConst + "]", "CV33=Value3_[i-" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);

        String[] plusConstCols = new String[] {"CV2=Value_[i+" + shiftConst + "]", "CV22=Value2_[i+" + shiftConst + "]",
                "CV23=Value3_[i+" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(14, 16, 20),
                    intCol("Sentinel", 8, 9, 11),
                    intCol("Value", 24, 26, 30),
                    intCol("Value2", 3242, 3262, 3302),
                    intCol("Value3", 2420, 2620, 3020));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(14, 16, 20), shiftData,
                    table.newModifiedColumnSet("Value2"));
            table.notifyListeners(update);
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testModifiedColSetMultiColMapping() {
        for (int shift = 1; shift < 5; shift++) {
            testModifiedColSetMultiColMapping(shift);
        }
    }

    private void testModifiedColSetMultiColMapping(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30),
                intCol("Value2", 202, 222, 242, 262, 282, 302),
                intCol("Value3", 2020, 2220, 2420, 2620, 2820, 3020));


        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst),
                "CV3=Value", "C2V3=Value", "CV32=Value2", "C2V32=Value2", "CV33=Value3", "C2V33=Value3");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst,
                "CV2=Value", "C2V2=Value", "CV22=Value2", "C2V22=Value2", "CV23=Value3", "C2V23=Value3");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table,
                    i(10, 12, 18),
                    intCol("Sentinel", 6, 7, 10),
                    intCol("Value", 420, 422, 428),
                    intCol("Value2", 202, 222, 282),
                    intCol("Value3", 2020, 2220, 2820));

            final RowSetShiftData.Builder shiftDataBuilder1 = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData1 = shiftDataBuilder1.build();
            final TableUpdateImpl update1 = new TableUpdateImpl(i(), i(), i(10, 12, 18), shiftData1,
                    table.newModifiedColumnSet("Value"));
            table.notifyListeners(update1);
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        String[] minusConstCols = new String[] {
                "CV3=Value_[i-" + shiftConst + "]", "C2V3=Value_[i-" + shiftConst + "]",
                "CV32=Value2_[i-" + shiftConst + "]", "C2V32=Value2_[i-" + shiftConst + "]",
                "CV33=Value3_[i-" + shiftConst + "]", "C2V33=Value3_[i-" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);

        String[] plusConstCols = new String[] {
                "CV2=Value_[i+" + shiftConst + "]", "C2V2=Value_[i+" + shiftConst + "]",
                "CV22=Value2_[i+" + shiftConst + "]", "C2V22=Value2_[i+" + shiftConst + "]",
                "CV23=Value3_[i+" + shiftConst + "]", "C2V23=Value3_[i+" + shiftConst + "]"};
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(14, 16, 20),
                    intCol("Sentinel", 8, 9, 11),
                    intCol("Value", 24, 26, 30),
                    intCol("Value2", 3242, 3262, 3302),
                    intCol("Value3", 2420, 2620, 3020));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(14, 16, 20), shiftData,
                    table.newModifiedColumnSet("Value2"));
            table.notifyListeners(update);
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(minusConstCols)),
                shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(() -> table.update(plusConstCols)),
                shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testSimple5() {
        for (int shift = 1; shift < 5; shift++) {
            testSimple5(shift);
        }
    }

    private void testSimple5(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6, 8, 22, 24), intCol("Sentinel", 4, 5, 12, 13), intCol("Value", 16, 18, 32, 34));
            table.notifyListeners(i(6, 8, 22, 24), i(), i());

        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 26, 28), intCol("Sentinel", 2, 3, 14, 15), intCol("Value", 12, 14, 36, 38));
            removeRows(table, i(6, 24));
            table.notifyListeners(i(2, 4, 26, 28), i(6, 24), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddRemoveAtTheEnd() {
        for (int shift = 1; shift < 5; shift++) {
            testAddRemoveAtTheEnd(shift);
        }
    }

    private void testAddRemoveAtTheEnd(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6, 8, 22, 24), intCol("Sentinel", 4, 5, 12, 13), intCol("Value", 16, 18, 32, 34));
            table.notifyListeners(i(6, 8, 22, 24), i(), i());

        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);


        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 26, 28), intCol("Sentinel", 2, 3, 14, 15), intCol("Value", 12, 14, 36, 38));
            removeRows(table, i(6, 24));
            table.notifyListeners(i(2, 4, 26, 28), i(6, 24), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddRemoveOverlap() {
        for (int shift = 1; shift < 5; shift++) {
            testAddRemoveOverlap(shift);
        }
    }

    private void testAddRemoveOverlap(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6, 14, 18, 24), intCol("Sentinel", 4, 5, 12, 13), intCol("Value", 16, 18, 32, 34));
            removeRows(table, i(10, 14, 18, 20));
            table.notifyListeners(i(6, 24), i(10, 14, 18, 20), i());

        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddRemove() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 14, 16, 18, 20).toTracking(),
                intCol("Sentinel", 6, 7, 8, 9, 10, 11),
                intCol("Value", 20, 22, 24, 26, 28, 30));
        System.out.println("---table---");
        TableTools.showWithRowSet(table);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);

        final TableUpdateValidator tuvMinusOne = TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusOneFailureListener = new FailureListener();
        tuvMinusOne.getResultTable().addUpdateListener(tuvMinusOneFailureListener);

        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);

        final TableUpdateValidator tuvPlusOne = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusOneFailureListener = new FailureListener();
        tuvPlusOne.getResultTable().addUpdateListener(tuvPlusOneFailureListener);

        final int shiftConst = 2;
        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6, 8, 22, 24), intCol("Sentinel", 4, 5, 12, 13), intCol("Value", 16, 18, 32, 34));
            table.notifyListeners(i(6, 8, 22, 24), i(), i());

        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 26, 28), intCol("Sentinel", 2, 3, 14, 15), intCol("Value", 12, 14, 36, 38));
            removeRows(table, i(6, 24));
            table.notifyListeners(i(2, 4, 26, 28), i(6, 24), i());
        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);
    }

    @Test
    public void testContiguousAddUpdateRemove() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(10, 12, 20, 28, 30).toTracking(),
                intCol("Sentinel", 4, 5, 9, 13, 14),
                intCol("Value", 18, 20, 28, 36, 38));
        System.out.println("---table---");
        TableTools.showWithRowSet(table);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);

        final TableUpdateValidator tuvMinusOne = TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusOneFailureListener = new FailureListener();
        tuvMinusOne.getResultTable().addUpdateListener(tuvMinusOneFailureListener);

        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);

        final TableUpdateValidator tuvPlusOne = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusOneFailureListener = new FailureListener();
        tuvPlusOne.getResultTable().addUpdateListener(tuvPlusOneFailureListener);

        final int shiftConst = 2;
        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36),
                    intCol("Sentinel", 1, 2, 3, 6, 7, 8, 10, 11, 12, 15, 16, 17),
                    intCol("Value", 12, 14, 16, 22, 24, 26, 30, 32, 34, 40, 42, 44));
            table.notifyListeners(i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36), i(), i());

        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36),
                    intCol("Sentinel", 101, 102, 103, 106, 107, 108, 110, 111, 112, 115, 116, 117),
                    intCol("Value", 112, 114, 116, 122, 124, 126, 130, 132, 134, 140, 142, 144));
            table.notifyListeners(i(), i(), i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36));

        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36));
            table.notifyListeners(i(), i(4, 6, 8, 14, 16, 18, 22, 24, 26, 32, 34, 36), i());
        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);
    }

    @Test
    public void testSimpleRemoveRangeApproach() {
        for (int shift = 1; shift < 5; shift++) {
            testSimpleRemoveRangeApproach(shift);
        }
    }

    private void testSimpleRemoveRangeApproach(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(0, 2, 4, 6, 8, 10, 12, 14, 16, 18).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                intCol("Value", 8, 10, 12, 14, 16, 18, 20, 22, 24, 26));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);


        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(6, 10));
            table.notifyListeners(i(), i(6, 10), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testSimpleRemoveChanges() {
        for (int shift = 1; shift < 5; shift++) {
            testSimpleRemoveChanges(shift);
        }
    }

    private void testSimpleRemoveChanges(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));
        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(6));
            table.notifyListeners(i(), i(6), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testSimpleModifyChanges() {
        for (int shift = 1; shift < 5; shift++) {
            testSimpleModifyChanges(shift);
        }
    }

    private void testSimpleModifyChanges(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(0, 2, 4, 6, 8, 10, 12, 14, 16, 18).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                intCol("Value", 8, 10, 12, 14, 16, 18, 20, 22, 24, 26));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0, 2, 4, 8, 12, 14, 18), intCol("Sentinel", 11, 12, 13, 15, 17, 18, 20),
                    intCol("Value", 9, 11, 13, 17, 21, 23, 27));
            table.notifyListeners(i(), i(), i(0, 2, 4, 8, 12, 14, 18));
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0, 2, 4, 8, 12, 14, 18), intCol("Sentinel", 21, 22, 23, 25, 27, 28, 30),
                    intCol("Value", 109, 111, 113, 117, 121, 123, 127));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update = new TableUpdateImpl(i(), i(), i(0, 2, 4, 8, 12, 14, 18),
                    shiftData, table.newModifiedColumnSet(table.getDefinition().getColumnNamesArray()));
            table.notifyListeners(update);
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testSimpleAddSingleColumnChanges() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 100, 101, 102, 103, 104));
        System.out.println("---table---");
        TableTools.showWithRowSet(table);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);

        final TableUpdateValidator tuvMinusOne = TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusOneFailureListener = new FailureListener();
        tuvMinusOne.getResultTable().addUpdateListener(tuvMinusOneFailureListener);

        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);

        final TableUpdateValidator tuvPlusOne = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusOneFailureListener = new FailureListener();
        tuvPlusOne.getResultTable().addUpdateListener(tuvPlusOneFailureListener);

        final int shiftConst = 4;
        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);

        ColumnSource<?> shiftedSource = shiftMinusConst.getColumnSource("CV3");
        try (final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(shiftMinusConst.intSize());
                final ChunkSource.FillContext fc = shiftedSource.makeFillContext(chunk.capacity())) {
            shiftedSource.fillChunk(fc, chunk, shiftMinusConst.getRowSet());
            System.out.println("Chunked Read:");
            System.out.println(ChunkUtils.dumpChunk(chunk));
            Assert.assertEquals(shiftMinusConst.intSize(), chunk.size());
        }

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        ColumnSource<?> shiftedSourcePlus = shiftPlusConst.getColumnSource("CV2");
        try (final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(shiftPlusConst.intSize());
                final ChunkSource.FillContext fc = shiftedSourcePlus.makeFillContext(chunk.capacity())) {
            shiftedSourcePlus.fillChunk(fc, chunk, shiftPlusConst.getRowSet());
            System.out.println("Chunked Read:");
            System.out.println(ChunkUtils.dumpChunk(chunk));
            Assert.assertEquals(shiftPlusConst.intSize(), chunk.size());
        }

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener printListenerOrig = new PrintListener("original table", table, 10);
        final PrintListener printListener = new PrintListener("shiftMinus4", shiftMinusConst, 10);
        final PrintListener printListenerPlusConst = new PrintListener("shiftPlus4", shiftPlusConst, 10);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0, 1, 12, 14), intCol("Sentinel", -1, 0, 6, 7), intCol("Value", 98, 99, 105, 106));
            table.notifyListeners(i(0, 1, 12, 14), i(), i());
        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        printListenerOrig.stop();
        printListener.stop();
        printListenerPlusConst.stop();
    }

    @Test
    public void testSimpleAddMultipleColumnChanges() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 100, 101, 102, 103, 104),
                intCol("Value2", 1000, 1010, 1020, 1030, 1040),
                intCol("Value3", 2000, 2010, 2020, 2030, 2040));
        System.out.println("---table---");
        TableTools.showWithRowSet(table);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final Table shiftMinusOne =
                ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value", "V32=Value2", "V33=Value3");
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);

        final TableUpdateValidator tuvMinusOne = TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusOneFailureListener = new FailureListener();
        tuvMinusOne.getResultTable().addUpdateListener(tuvMinusOneFailureListener);

        final Table shiftPlusOne =
                ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value", "V22=Value2", "V23=Value3");
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);

        final TableUpdateValidator tuvPlusOne = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusOneFailureListener = new FailureListener();
        tuvPlusOne.getResultTable().addUpdateListener(tuvPlusOneFailureListener);

        final int shiftConst = 4;
        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value",
                "CV32=Value2", "CV33=Value3");
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);

        ColumnSource<?> shiftedSource = shiftMinusConst.getColumnSource("CV3");
        try (final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(shiftMinusConst.intSize());
                final ChunkSource.FillContext fc = shiftedSource.makeFillContext(chunk.capacity())) {
            shiftedSource.fillChunk(fc, chunk, shiftMinusConst.getRowSet());
            System.out.println("Chunked Read:");
            System.out.println(ChunkUtils.dumpChunk(chunk));
            Assert.assertEquals(shiftMinusConst.intSize(), chunk.size());
        }

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final Table shiftPlusConst =
                ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value", "CV22=Value2", "CV23=Value3");
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        ColumnSource<?> shiftedSourcePlus = shiftPlusConst.getColumnSource("CV2");
        try (final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(shiftPlusConst.intSize());
                final ChunkSource.FillContext fc = shiftedSourcePlus.makeFillContext(chunk.capacity())) {
            shiftedSourcePlus.fillChunk(fc, chunk, shiftPlusConst.getRowSet());
            System.out.println("Chunked Read:");
            System.out.println(ChunkUtils.dumpChunk(chunk));
            Assert.assertEquals(shiftPlusConst.intSize(), chunk.size());
        }

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener printListenerOrig = new PrintListener("original table", table, 10);
        final PrintListener printListenerMinusOne =
                new PrintListener("original table", shiftMinusOne, 10);
        final PrintListener printListener = new PrintListener("shiftMinus4", shiftMinusConst, 10);
        final PrintListener printListenerPlusConst = new PrintListener("shiftPlus4", shiftPlusConst, 10);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0, 1, 12, 14), intCol("Sentinel", -1, 0, 6, 7), intCol("Value", 98, 99, 105, 106),
                    intCol("Value2", 980, 990, 1050, 1060), intCol("Value3", 1980, 1990, 2050, 2060));
            table.notifyListeners(i(0, 1, 12, 14), i(), i());
        });
        System.out.println("---table---");
        TableTools.showWithRowSet(table);
        System.out.println("---shiftMinusOne---");
        TableTools.showWithRowSet(shiftMinusOne);
        System.out.println("---shiftPlusOne---");
        TableTools.showWithRowSet(shiftPlusOne);
        System.out.println("---shiftMinusConst---");
        TableTools.showWithRowSet(shiftMinusConst);
        System.out.println("---shiftPlusConst---");
        TableTools.showWithRowSet(shiftPlusConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]", "V32 = Value2_[i - 1]", "V33 = Value3_[i - 1]")),
                shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]", "V22 = Value2_[i + 1]", "V23 = Value3_[i + 1]")),
                shiftPlusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]",
                        "CV32 = Value2_[i - " + shiftConst + "]", "CV33 = Value3_[i - " + shiftConst + "]")),
                shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]",
                        "CV22 = Value2_[i + " + shiftConst + "]", "CV23 = Value3_[i + " + shiftConst + "]")),
                shiftPlusConst);

        printListenerOrig.stop();
        printListenerMinusOne.stop();
        printListener.stop();
        printListenerPlusConst.stop();
    }


    @Test
    public void testRemoveWithTUV() {
        for (int i = 1; i < 5; i++) {
            testRemoveWithTUV(i);
        }
    }

    private void testRemoveWithTUV(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14, 16, 18).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22, 24, 26));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(8));
            table.notifyListeners(i(), i(8), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddSingleNewRowToBeginning() {
        for (int shift = 1; shift < 5; shift++) {
            testAddSingleNewRowToBeginning(shift);
        }
    }

    private void testAddSingleNewRowToBeginning(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 2, 3, 4, 5),
                intCol("Value", 12, 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), intCol("Sentinel", 1), intCol("Value", 10));
            table.notifyListeners(i(2), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddMultipleNewRowToBeginning() {
        for (int shift = 1; shift < 5; shift++) {
            testAddMultipleNewRowToBeginning(shift);
        }
    }

    private void testAddMultipleNewRowToBeginning(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(6, 8, 10).toTracking(),
                intCol("Sentinel", 3, 4, 5),
                intCol("Value", 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4), intCol("Sentinel", 1, 2), intCol("Value", 10, 12));
            table.notifyListeners(i(2, 4), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddSingleNewRowToEnd() {
        for (int shift = 1; shift < 5; shift++) {
            testAddSingleNewRowToEnd(shift);
        }
    }

    private void testAddSingleNewRowToEnd(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(12), intCol("Sentinel", 6), intCol("Value", 20));
            table.notifyListeners(i(12), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddMultipleNewRowsToEnd() {
        for (int shift = 1; shift < 5; shift++) {
            testAddMultipleNewRowsToEnd(shift);
        }
    }

    private void testAddMultipleNewRowsToEnd(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(12, 14, 16), intCol("Sentinel", 6, 7, 8), intCol("Value", 20, 22, 24));
            table.notifyListeners(i(12, 14, 16), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddMultipleNonContiguousRows() {
        for (int shift = 1; shift < 5; shift++) {
            testAddMultipleNonContiguousRows(shift);
        }
    }

    private void testAddMultipleNonContiguousRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 3, 5, 7, 9),
                intCol("Value", 10, 12, 14, 16, 18));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(3, 5, 7), intCol("Sentinel", 2, 4, 6), intCol("Value", 11, 13, 15));
            table.notifyListeners(i(7, 5, 3), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddMultipleContiguousNewRows() {
        for (int shift = 1; shift < 5; shift++) {
            testAddMultipleContiguousNewRows(shift);
        }
    }

    private void testAddMultipleContiguousNewRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 8, 14, 20).toTracking(),
                intCol("Sentinel", 1, 7, 13, 19),
                intCol("Value", 10, 16, 22, 28));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(3, 5, 7, 9, 11, 13), intCol("Sentinel", 2, 4, 6, 8, 10, 12),
                    intCol("Value", 11, 13, 15, 17, 19, 21));
            table.notifyListeners(i(7, 5, 3, 9, 11, 13), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddRowsToBeginningMiddle() {
        for (int shift = 1; shift < 3; shift++) {
            testAddRowsToBeginningMiddle(shift);
        }
    }

    private void testAddRowsToBeginningMiddle(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(6, 12, 14).toTracking(),
                intCol("Sentinel", 3, 6, 7),
                intCol("Value", 14, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 8, 10), intCol("Sentinel", 1, 2, 4, 5), intCol("Value", 10, 12, 16, 18));
            table.notifyListeners(i(2, 4, 8, 10), i(), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddRowsToBeginningMiddleAndEnd() {
        for (int shift = 1; shift < 3; shift++) {
            testAddRowsToBeginningMiddleAndEnd(shift);
        }
    }

    private void testAddRowsToBeginningMiddleAndEnd(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(6, 12, 14).toTracking(),
                intCol("Sentinel", 3, 6, 7),
                intCol("Value", 14, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);
        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2, 4, 8, 10, 16, 18), intCol("Sentinel", 1, 2, 4, 5, 8, 9),
                    intCol("Value", 10, 12, 16, 18, 24, 26));
            table.notifyListeners(i(2, 4, 8, 10, 16, 18), i(), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testModificationNoShift() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));
        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");

        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "pre-update", 1);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusOne = new PrintListener("Minus One", shiftMinusOne);
        final PrintListener plPlusOne = new PrintListener("Plus One", shiftPlusOne);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(6), intCol("Sentinel", 6), intCol("Value", 20));
            table.notifyListeners(i(), i(), i(6));
        });
        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "post-update", 1);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);

        plTable.stop();
        plMinusOne.stop();
        plPlusOne.stop();
    }

    @Test
    public void testShiftOnly() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5),
                intCol("Value", 10, 12, 14, 16, 18));

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");

        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "pre-update", 1);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusOne = new PrintListener("Minus One", shiftMinusOne);
        final PrintListener plPlusOne = new PrintListener("Plus One", shiftPlusOne);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(6));
            addToTable(table, i(7), intCol("Sentinel", 3), intCol("Value", 14));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            shiftDataBuilder.shiftRange(6, 6, 1);
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update =
                    new TableUpdateImpl(i(), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            table.notifyListeners(update);
        });
        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "post-update", 1);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);

        plTable.stop();
        plMinusOne.stop();
        plPlusOne.stop();
    }

    @Test
    public void testShiftAndModify() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6),
                intCol("Value", 10, 12, 14, 16, 18, 20));

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");

        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "pre-update", 1);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusOne = new PrintListener("Minus One", shiftMinusOne);
        final PrintListener plPlusOne = new PrintListener("Plus One", shiftPlusOne);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(6, 10));
            addToTable(table, i(7), intCol("Sentinel", 3), intCol("Value", 16));
            addToTable(table, i(9), intCol("Sentinel", 5), intCol("Value", 19));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            shiftDataBuilder.shiftRange(6, 6, 1);
            shiftDataBuilder.shiftRange(10, 10, -1);
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update =
                    new TableUpdateImpl(i(), i(), i(7, 9), shiftData, table.newModifiedColumnSet("Value"));
            table.notifyListeners(update);
        });
        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "post-update", 1);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);

        plTable.stop();
        plMinusOne.stop();
        plPlusOne.stop();
    }

    @Test
    public void testAddAndShift() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(0, 4, 8).toTracking(),
                intCol("Sentinel", 1, 5, 9),
                intCol("Value", 10, 14, 18));

        final Table shiftMinusOne = ShiftedColumnOperation.addShiftedColumns(table, -1, "V3=Value");
        final Table shiftPlusOne = ShiftedColumnOperation.addShiftedColumns(table, 1, "V2=Value");

        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "pre-update", 1);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusOne", (QueryTable) shiftMinusOne);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst = TableUpdateValidator.make("shiftPlusOne", (QueryTable) shiftPlusOne);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusOne = new PrintListener("Minus One", shiftMinusOne);
        final PrintListener plPlusOne = new PrintListener("Plus One", shiftPlusOne);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(4));
            addToTable(table, i(3), intCol("Sentinel", 3), intCol("Value", 11));
            addToTable(table, i(5), intCol("Sentinel", 5), intCol("Value", 14));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            shiftDataBuilder.shiftRange(4, 4, 1);
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update =
                    new TableUpdateImpl(i(3), i(), i(), shiftData, ModifiedColumnSet.EMPTY);
            table.notifyListeners(update);
        });

        printTableUpdates(table, shiftMinusOne, shiftPlusOne, "post-update", 1);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V3 = Value_[i - 1]")), shiftMinusOne);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("V2 = Value_[i + 1]")), shiftPlusOne);

        plTable.stop();
        plMinusOne.stop();
        plPlusOne.stop();
    }

    @Test
    public void testAddAndUpdateWithShift() {
        for (int shift = 1; shift < 5; shift++) {
            testAddAndUpdateWithShift(shift);
        }
    }

    private void testAddAndUpdateWithShift(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(3, 6, 9, 12, 15, 18, 21, 24).toTracking(),
                intCol("Sentinel", 1, 3, 5, 7, 9, 11, 13, 15),
                intCol("Value", 10, 14, 18, 22, 26, 30, 34, 38));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9, 18));
            addToTable(table, i(8), intCol("Sentinel", 4), intCol("Value", 17));
            addToTable(table, i(10), intCol("Sentinel", 5), intCol("Value", 18));
            addToTable(table, i(17), intCol("Sentinel", 11), intCol("Value", 30));
            addToTable(table, i(20), intCol("Sentinel", 12), intCol("Value", 32));
            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();
            shiftDataBuilder.shiftRange(9, 9, 1);
            shiftDataBuilder.shiftRange(18, 18, -1);
            final RowSetShiftData shiftData = shiftDataBuilder.build();
            final TableUpdateImpl update =
                    new TableUpdateImpl(i(8, 20), i(), i(), shiftData, ModifiedColumnSet.ALL);
            table.notifyListeners(update);
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testAddAndUpdateWithOutShift() {
        for (int shift = 1; shift < 5; shift++) {
            testAddAndUpdateWithOutShift(shift);
        }
    }

    private void testAddAndUpdateWithOutShift(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(3, 6, 9, 12, 15, 18, 21, 24).toTracking(),
                intCol("Sentinel", 1, 3, 5, 7, 9, 11, 13, 15),
                intCol("Value", 10, 14, 18, 22, 26, 30, 34, 38));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), intCol("Sentinel", 4), intCol("Value", 17));
            addToTable(table, i(9), intCol("Sentinel", 6), intCol("Value", 19));
            addToTable(table, i(18), intCol("Sentinel", 10), intCol("Value", 29));
            addToTable(table, i(20), intCol("Sentinel", 12), intCol("Value", 32));
            table.notifyListeners(i(8, 20), i(), i(9, 18));
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveSingleMiddleRow() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveSingleMiddleRow(shift);
        }
    }

    private void testRemoveSingleMiddleRow(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(6));
            table.notifyListeners(i(), i(6), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveFirstRow() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveFirstRow(shift);
        }
    }

    private void testRemoveFirstRow(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2));
            table.notifyListeners(i(), i(2), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveFirstTwoRows() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveFirstTwoRows(shift);
        }
    }

    private void testRemoveFirstTwoRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 4));
            table.notifyListeners(i(), i(2, 4), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveLastRow() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveLastRow(shift);
        }
    }

    private void testRemoveLastRow(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(14));
            table.notifyListeners(i(), i(14), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveLastTwoRows() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveLastTwoRows(shift);
        }
    }

    private void testRemoveLastTwoRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(12, 14));
            table.notifyListeners(i(), i(12, 14), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveRandomNonFirstAndLastRows() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveRandomNonFirstAndLastRows(shift);
        }
    }

    private void testRemoveRandomNonFirstAndLastRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(4, 8, 12));
            table.notifyListeners(i(), i(4, 8, 12), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveFirstLastAndRandomRows() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveFirstLastAndRandomRows(shift);
        }
    }

    private void testRemoveFirstLastAndRandomRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 4, 8, 12, 14));
            table.notifyListeners(i(), i(2, 4, 8, 12, 14), i());
        });
        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveFirstLastAndRandomRowsWithMoreData() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveFirstLastAndRandomRowsWithMoreData(shift);
        }
    }

    private void testRemoveFirstLastAndRandomRowsWithMoreData(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14, 16, 18).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22, 24, 26));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 4, 8, 12, 14));
            table.notifyListeners(i(), i(2, 4, 8, 12, 14), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void testRemoveAllRows() {
        for (int shift = 1; shift < 5; shift++) {
            testRemoveAllRows(shift);
        }
    }

    private void testRemoveAllRows(final int shiftConst) {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 10, 12, 14).toTracking(),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Value", 10, 12, 14, 16, 18, 20, 22));

        final Table shiftMinusConst = ShiftedColumnOperation.addShiftedColumns(table, (-1 * shiftConst), "CV3=Value");
        final Table shiftPlusConst = ShiftedColumnOperation.addShiftedColumns(table, shiftConst, "CV2=Value");

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "pre-update", shiftConst);

        final TableUpdateValidator tuv = TableUpdateValidator.make("table", table);
        FailureListener tuvFailureListener = new FailureListener();
        tuv.getResultTable().addUpdateListener(tuvFailureListener);

        final TableUpdateValidator tuvMinusConst =
                TableUpdateValidator.make("shiftMinusConst", (QueryTable) shiftMinusConst);
        FailureListener tuvMinusConstFailureListener = new FailureListener();
        tuvMinusConst.getResultTable().addUpdateListener(tuvMinusConstFailureListener);

        final TableUpdateValidator tuvMPlusConst =
                TableUpdateValidator.make("shiftPlusConst", (QueryTable) shiftPlusConst);
        FailureListener tuvPlusConstFailureListener = new FailureListener();
        tuvMPlusConst.getResultTable().addUpdateListener(tuvPlusConstFailureListener);

        final PrintListener plTable = new PrintListener("Table", table);
        final PrintListener plMinusConst = new PrintListener("Minus Const", shiftMinusConst);
        final PrintListener plPlusConst = new PrintListener("Plus Const", shiftPlusConst);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 4, 6, 8, 10, 12, 14));
            table.notifyListeners(i(), i(2, 4, 6, 8, 10, 12, 14), i());
        });

        printTableUpdates(table, shiftMinusConst, shiftPlusConst, "post-update", shiftConst);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV3 = Value_[i - " + shiftConst + "]")), shiftMinusConst);
        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("CV2 = Value_[i + " + shiftConst + "]")), shiftPlusConst);

        plTable.stop();
        plMinusConst.stop();
        plPlusConst.stop();
    }

    @Test
    public void orderedKeysExample() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendRange(0, 100);
        builder.appendRange(400, 600);
        builder.appendRange(10000, 11000);
        builder.appendKey(20000);
        builder.appendKey(20001);
        final RowSet rowSet = builder.build();

        System.out.println("Whole RowSet: " + rowSet);

        try (final RowSequence.Iterator okIt = rowSet.getRowSequenceIterator()) {
            while (okIt.hasMore()) {
                final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(50);
                System.out.println("Chunk of 50:" + okString(chunkOk));
            }
        }

        try (final RowSequence.Iterator okIt = rowSet.getRowSequenceIterator()) {
            while (okIt.hasMore()) {
                final long nextKey = okIt.peekNextKey();
                final RowSequence chunkOk = okIt.getNextRowSequenceThrough(nextKey | 0xff);
                System.out.println("256 Masked Chunk: " + okString(chunkOk));
            }
        }
    }

    @Test
    public void testBigShift() {
        final QueryTable table = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).toTracking(),
                intCol("Sentinel", 100, 101, 102, 103),
                intCol("Value", 201, 202, 203, 204));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table shifted = updateGraph.sharedLock().computeLocked(
                () -> ShiftedColumnOperation.addShiftedColumns(table, -1, "VS=Value"));
        TableTools.showWithRowSet(shifted);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("VS=Value_[i-1]")), shifted);

        final PrintListener pl = new PrintListener("Shifted Result", shifted, 10);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2, 4, 6, 8));
            addToTable(table, i(1002, 1003, 1004, 1008), intCol("Sentinel", 100, 104, 101, 103),
                    intCol("Value", 201, 205, 202, 204));

            final TableUpdateImpl update = new TableUpdateImpl();
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 100, +1000);
            update.shifted = builder.build();
            update.added = i(1003);
            update.removed = i(6);
            update.modified = i(1002);
            update.modifiedColumnSet = table.newModifiedColumnSet("Value");
            table.notifyListeners(update);
        });
        TableTools.showWithRowSet(shifted);

        assertTableEquals(updateGraph.sharedLock().computeLocked(
                () -> table.update("VS=Value_[i-1]")), shifted);
        pl.stop();
    }

    @Test
    public void testFillChunk() {
        final Table dummy = TableTools.emptyTable(10_000_000L).update("X=ii");

        final MutableLong sum = new MutableLong();
        final ColumnSource<Long> xcs = dummy.getColumnSource("X", long.class);
        final long start = System.nanoTime();
        dummy.getRowSet().forAllRowKeys(idx -> sum.add(xcs.getLong(idx)));
        final long end = System.nanoTime();
        System.out.println("Sum: " + sum.longValue() + ", duration=" + (end - start));

        final long startCk = System.nanoTime();
        sum.setValue(0);
        try (final RowSequence.Iterator okIt = dummy.getRowSet().getRowSequenceIterator();
                final ChunkSource.GetContext gc = xcs.makeGetContext(2048)) {
            while (okIt.hasMore()) {
                final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(2048);
                final LongChunk<? extends Values> longChunk = xcs.getChunk(gc, chunkOk).asLongChunk();
                long smallSum = 0;
                for (int ii = 0; ii < longChunk.size(); ++ii) {
                    smallSum += longChunk.get(ii);
                }
                sum.add(smallSum);
            }
        }
        final long endCk = System.nanoTime();
        System.out.println("Sum: " + sum.longValue() + ", duration=" + (endCk - startCk));

    }

    @Test
    public void testSharedContext() {
        final RowSet rowSet = RowSetFactory.flat(10000);

        final MutableInt invertCount = new MutableInt();
        final TrackingRowSet proxyRowSet =
                new TrackingWritableRowSetImpl(((WritableRowSetImpl) rowSet).getInnerSet()) {
                    @Override
                    public WritableRowSet invert(RowSet keys) {
                        invertCount.increment();
                        return super.invert(keys);
                    }
                };

        final Table dummy = new QueryTable(proxyRowSet, Collections.emptyMap()).update("X=ii", "X2=ii*2");

        final Table shifted = ShiftedColumnOperation.addShiftedColumns(dummy, -1, "S=X");
        final Table shifted2 = ShiftedColumnOperation.addShiftedColumns(shifted, -1, "S2=X2");

        final ColumnSource<Long> scs = shifted2.getColumnSource("S", long.class);
        final ColumnSource<Long> s2cs = shifted2.getColumnSource("S2", long.class);

        final int chunkSize = 2048;
        try (final RowSequence.Iterator okIt = shifted2.getRowSet().getRowSequenceIterator();
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final ChunkSource.GetContext gc = scs.makeGetContext(chunkSize, sharedContext);
                final ChunkSource.GetContext gc2 = s2cs.makeGetContext(chunkSize, sharedContext);
                final WritableLongChunk<RowKeys> expect =
                        WritableLongChunk.makeWritableChunk(chunkSize)) {
            while (okIt.hasMore()) {
                sharedContext.reset();
                final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                final int invertStart = invertCount.intValue();
                final LongChunk<? extends Values> ck1 = scs.getChunk(gc, chunkOk).asLongChunk();
                final LongChunk<? extends Values> ck2 = s2cs.getChunk(gc2, chunkOk).asLongChunk();
                final int invertEnd = invertCount.intValue();
                Assert.assertEquals(1, invertEnd - invertStart);
                chunkOk.fillRowKeyChunk(expect);
                for (int ii = 0; ii < expect.size(); ++ii) {
                    final long vv = expect.get(ii);
                    if (vv == 0) {
                        expect.set(ii, QueryConstants.NULL_LONG);
                    } else {
                        expect.set(ii, vv - 1);
                    }
                }
                Assert.assertTrue(LongChunkEquals.equalReduce(expect, ck1));
                chunkOk.fillRowKeyChunk(expect);
                for (int ii = 0; ii < expect.size(); ++ii) {
                    final long vv = expect.get(ii);
                    if (vv == 0) {
                        expect.set(ii, QueryConstants.NULL_LONG);
                    } else {
                        expect.set(ii, (vv - 1) * 2);
                    }
                }
                Assert.assertTrue(LongChunkEquals.equalReduce(expect, ck2));
            }
        }
    }

    @Test
    public void testRandomizedUpdates() {
        for (int size = 10; size <= 1000; size *= 10) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Size = " + size + ", seed=" + seed);
                testRandomizedUpdates(seed, size, 4, 50);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void testRandomizedUpdates(final int seed, final int tableSize, final int maxShiftConst,
            final int simulationSize) {

        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1, 40.1, 50.1, 60.1, 70.1, 80.1, 90.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Original Table");
            TableTools.showWithRowSet(queryTable);
        }

        final EvalNugget[] en = new EvalNugget[maxShiftConst * 2];
        String[] matchColumns = new String[] {"Sym2=Sym", "intCol2=intCol", "doubleCol2=doubleCol"};

        for (int i = 0; i < maxShiftConst; i++) {
            long shift = i + 1;
            en[i * 2] =
                    EvalNugget.from(() -> ShiftedColumnOperation.addShiftedColumns(queryTable, -shift, matchColumns));
            en[i * 2 + 1] =
                    EvalNugget.from(() -> ShiftedColumnOperation.addShiftedColumns(queryTable, shift, matchColumns));
        }

        for (int step = 0; step < simulationSize; step++) {
            simulateShiftAwareStep(tableSize, random, queryTable, columnInfo, en);
        }
    }

    @NotNull
    private String okString(RowSequence chunkOk) {
        final StringBuilder strBuilder = new StringBuilder();
        final MutableBoolean first = new MutableBoolean(true);
        chunkOk.forAllRowKeyRanges((s, e) -> {
            if (!first.getValue()) {
                strBuilder.append(", ");
            } else {
                first.setFalse();
            }
            if (s == e) {
                strBuilder.append(s);
            } else {
                strBuilder.append(s).append("-").append(e);
            }
        });

        return strBuilder.toString();
    }

    private void printTableUpdates(@NotNull Table table, @NotNull Table shiftMinusConst, @NotNull Table shiftPlusConst,
            String location, final int shiftConst) {
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.printf("----------------------------%s  ShiftConst +/- %d----------------------------%n",
                    location, shiftConst);
            System.out.println("---table---");
            TableTools.showWithRowSet(table);
            System.out.println("---shiftMinusConst---");
            TableTools.showWithRowSet(shiftMinusConst);
            System.out.println("---shiftPlusConst---");
            TableTools.showWithRowSet(shiftPlusConst);
        }
    }
}
