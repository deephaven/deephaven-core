//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.QueryTableTest;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListenerAdapter;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.sources.ImmutableColumnHolder;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.qst.type.Type;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Optional;
import java.util.Random;

import static io.deephaven.engine.table.impl.util.TestKeyedArrayBackedInputTable.handleDelayedRefresh;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class TestFunctionGeneratedTableFactory extends RefreshingTableTestCase {
    public void testIterative() {
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(() -> queryTable, queryTable);

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
                // Note: disable update validation since the function backed table's prev values will always be
                // incorrect
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> functionBacked.update("Mult=intCol * doubleCol"));
                }),
        };

        for (int i = 0; i < 75; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testNoSources() {
        // If no sources are specified, function should still run once on initialization.
        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> newTable(
                        stringCol("StringCol", "MyString"),
                        intCol("IntCol", 12345)));

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }

    public void testMultipleSources() throws Exception {
        final AppendOnlyArrayBackedInputTable source1 = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("StringCol", Type.stringType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater inputTable1 = source1.makeUpdater();

        final AppendOnlyArrayBackedInputTable source2 = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater inputTable2 = source2.makeUpdater();

        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> source1.lastBy().naturalJoin(source2, ""), source1, source2);

        assertEquals(functionBacked.size(), 0);

        handleDelayedRefresh(() -> {
            inputTable1.addAsync(newTable(stringCol("StringCol", "MyString")), t -> {
            });
            inputTable2.addAsync(newTable(intCol("IntCol", 12345)), t -> {
            });
        }, source1, source2);

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }

    public void testSpecCopyDataIterative() {
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(() -> queryTable)
                .addDependencies(queryTable)
                .build());

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
        };

        for (int i = 0; i < 25; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testSpecSwitchColumnSources() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // copyData == false wires the result directly to the generated table's column sources via SwitchColumnSource.
        // A refreshing generated table is valid only when its column sources are immutable, so that previous values
        // remain available after the switch. Here the source has an immutable column source; we update it by adding and
        // removing rows (never changing any value) and verify both current and previous values through the cycle.
        final QueryTable source = testRefreshingTable(i(0, 1).toTracking(), immutableIntCol("Value", 10, 20));
        assertTrue(source.getColumnSource("Value").isImmutable());

        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(() -> source)
                .addDependencies(source)
                .copyData(false)
                .build());

        assertTableEquals(newTable(intCol("Value", 10, 20)), functionBacked);

        // Add row 2 (value 30) and remove row 0 (value 10); no existing value is modified.
        updateGraph.startCycleForUnitTests();
        try {
            addToTable(source, i(2), immutableIntCol("Value", 30));
            removeRows(source, i(0));
            source.notifyListeners(i(2), i(0), i());
            updateGraph.flushAllNormalNotificationsForUnitTests();

            // Within the cycle: current reflects rows {1,2} -> 20, 30; previous reflects rows {0,1} -> 10, 20.
            assertTableEquals(newTable(intCol("Value", 20, 30)), functionBacked);
            assertTableEquals(newTable(intCol("Value", 10, 20)), prevTable(functionBacked));
        } finally {
            updateGraph.completeCycleForUnitTests();
        }

        assertTableEquals(newTable(intCol("Value", 20, 30)), functionBacked);
    }

    private static ImmutableColumnHolder<Integer> immutableIntCol(final String name, final int... data) {
        final WritableIntChunk<Values> chunk = WritableIntChunk.writableChunkWrap(data);
        return new ImmutableColumnHolder<>(name, int.class, null, false, chunk);
    }

    public void testSwitchRejectsMutableSources() {
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(50, random,
                initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        // Without copying, a refreshing generated table whose column sources mutate in place would corrupt our
        // previous-value tracking. queryTable's array-backed sources are not immutable, so this must be rejected.
        try {
            FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> queryTable)
                    .addDependencies(queryTable)
                    .copyData(false)
                    .build());
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("immutable column sources"));
        }
    }

    public void testSpecDefinitionMismatchFails() {
        // A specified definition is authoritative; the generated table must be compatible with it.
        try {
            FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> newTable(intCol("IntCol", 1)))
                    .tableDefinition(TableDefinition.of(ColumnDefinition.of("IntCol", Type.stringType())))
                    .build());
            fail("Expected an IncompatibleTableDefinitionException");
        } catch (TableDefinition.IncompatibleTableDefinitionException expected) {
            assertTrue(expected.getMessage().contains("incompatibilities"));
        }
    }

    public void testDefinitionDefinesColumnOrder() {
        // The supplied definition orders the columns (B, A); the generator produces them in the opposite order (A, B).
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("B"),
                ColumnDefinition.ofString("A"));

        for (final boolean copyData : new boolean[] {true, false}) {
            final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> newTable(stringCol("A", "x"), intCol("B", 1)))
                    .tableDefinition(definition)
                    .copyData(copyData)
                    .build());

            // The result adopts the supplied definition's column order, not the generated table's.
            assertEquals(definition, functionBacked.getDefinition());
        }
    }

    public void testRetainingLast() throws Exception {
        final AppendOnlyArrayBackedInputTable source = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater updater = source.makeUpdater();

        // The supplier only produces a table when the source is non-empty; otherwise the previous result is retained.
        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .retainingLastTableSupplier(() -> source.size() == 0
                        ? Optional.empty()
                        : Optional.of(newTable(intCol("Sum", (int) source.size()))))
                .tableDefinition(TableDefinition.of(ColumnDefinition.of("Sum", Type.intType())))
                .addDependencies(source)
                .build());

        // At startup the source is empty, so the supplier produced nothing; the definition fallback yields an empty
        // result.
        assertEquals(0, functionBacked.size());

        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 1)), t -> {
        }), source);
        assertTableEquals(newTable(intCol("Sum", 1)), functionBacked);

        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 2)), t -> {
        }), source);
        assertTableEquals(newTable(intCol("Sum", 2)), functionBacked);
    }

    public void testBlinkTableCopy() throws Exception {
        checkBlinkTable(true);
    }

    public void testBlinkTableSwitch() throws Exception {
        checkBlinkTable(false);
    }

    private void checkBlinkTable(final boolean copyData) throws Exception {
        final AppendOnlyArrayBackedInputTable source = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater updater = source.makeUpdater();

        // The generator returns whatever we stage; the blink result should retain only the current cycle's rows.
        final MutableObject<Table> nextResult = new MutableObject<>(newTable(longCol("Size", 1L)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(nextResult::getValue)
                .addDependencies(source)
                .blinkTable(true)
                .copyData(copyData)
                .build());

        assertTrue(BlinkTableTools.isBlink(functionBacked));
        assertTableEquals(newTable(longCol("Size", 1L)), functionBacked);

        // A cycle that produces no new rows must clear the blink table.
        nextResult.setValue(newTable(longCol("Size")));
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 1)), t -> {
        }), source);
        assertEquals(0, functionBacked.size());

        // A subsequent non-empty cycle repopulates it.
        nextResult.setValue(newTable(longCol("Size", 2L, 3L)));
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 2)), t -> {
        }), source);
        assertTableEquals(newTable(longCol("Size", 2L, 3L)), functionBacked);
    }

    public void testBlinkRetainingLastClears() throws Exception {
        final AppendOnlyArrayBackedInputTable source = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater updater = source.makeUpdater();

        // A retaining-last supplier that declines to produce a table must still clear a blink result, which retains
        // only the current cycle's rows.
        final MutableObject<Optional<Table>> nextResult =
                new MutableObject<>(Optional.of(newTable(longCol("Size", 1L))));

        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .retainingLastTableSupplier(nextResult::getValue)
                .tableDefinition(TableDefinition.of(ColumnDefinition.of("Size", Type.longType())))
                .addDependencies(source)
                .blinkTable(true)
                .build());

        assertTableEquals(newTable(longCol("Size", 1L)), functionBacked);

        // The supplier declines this cycle; the blink result must be cleared rather than retaining the prior rows.
        nextResult.setValue(Optional.empty());
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 1)), t -> {
        }), source);
        assertEquals(0, functionBacked.size());

        // A subsequent producing cycle repopulates it.
        nextResult.setValue(Optional.of(newTable(longCol("Size", 2L, 3L))));
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 2)), t -> {
        }), source);
        assertTableEquals(newTable(longCol("Size", 2L, 3L)), functionBacked);
    }

    public void testCopyShrinkAndGrow() throws Exception {
        final AppendOnlyArrayBackedInputTable source = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater updater = source.makeUpdater();

        // An object (String) column exercises the shrink path that clears stale references from the writable sources.
        final MutableObject<Table> nextResult = new MutableObject<>(newTable(stringCol("Key", "a", "b", "c")));

        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(nextResult::getValue)
                .addDependencies(source)
                .build());

        assertTableEquals(newTable(stringCol("Key", "a", "b", "c")), functionBacked);

        // Shrink: the result should reflect only the smaller set of rows.
        nextResult.setValue(newTable(stringCol("Key", "x")));
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 1)), t -> {
        }), source);
        assertEquals(1, functionBacked.size());
        assertTableEquals(newTable(stringCol("Key", "x")), functionBacked);

        // Grow again: the result should reflect the larger set of rows.
        nextResult.setValue(newTable(stringCol("Key", "p", "q")));
        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 2)), t -> {
        }), source);
        assertTableEquals(newTable(stringCol("Key", "p", "q")), functionBacked);
    }

    public void testBlinkRequiresTrigger() {
        try {
            FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> emptyTable(0))
                    .blinkTable(true)
                    .build();
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("blinkTable requires"));
        }
    }

    public void testMissingDefinitionFails() {
        // A retaining-last supplier that produces nothing at startup, with no definition, must fail on construction.
        try {
            FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                    .retainingLastTableSupplier(Optional::empty)
                    .build());
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("no table definition"));
        }
    }

    public void testMutuallyExclusiveSuppliers() {
        try {
            FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> emptyTable(0))
                    .retainingLastTableSupplier(Optional::empty)
                    .build();
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("Exactly one"));
        }
    }

    public void testMutuallyExclusiveTriggers() {
        try {
            FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> emptyTable(0))
                    .refreshInterval(java.time.Duration.ofSeconds(1))
                    .addDependencies(timeTable("PT1S"))
                    .build();
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("mutually exclusive"));
        }
    }

    public void testSubMillisecondIntervalFails() {
        // A sub-millisecond interval would truncate to zero milliseconds later, so it must be rejected.
        try {
            FunctionGeneratedTableSpec.builder()
                    .tableSupplier(() -> emptyTable(0))
                    .refreshInterval(java.time.Duration.ofNanos(500_000))
                    .build();
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("at least one millisecond"));
        }
    }

    public void testIntervalRefresh() throws Exception {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // A refresh far in the future: driving the periodic source before its interval elapses does nothing.
        final Table slow = FunctionGeneratedTableFactory.create(() -> newTable(intCol("V", 9)), 3_600_000);
        assertTableEquals(newTable(intCol("V", 9)), slow);
        updateGraph.runWithinUnitTestCycle(
                () -> updateGraph.refreshUpdateSourceForUnitTests((Runnable) slow));
        assertTableEquals(newTable(intCol("V", 9)), slow);

        // A short interval: once it elapses, driving the periodic source regenerates the result.
        final MutableObject<Table> nextResult = new MutableObject<>(newTable(intCol("V", 1)));
        final Table functionBacked = FunctionGeneratedTableFactory.create(nextResult::getValue, 1);
        assertTableEquals(newTable(intCol("V", 1)), functionBacked);

        Thread.sleep(10);
        nextResult.setValue(newTable(intCol("V", 2, 3)));
        updateGraph.runWithinUnitTestCycle(
                () -> updateGraph.refreshUpdateSourceForUnitTests((Runnable) functionBacked));
        assertTableEquals(newTable(intCol("V", 2, 3)), functionBacked);
    }

    public void testIntervalSourceDestroyed() {
        // Releasing the only reference to an interval-driven result destroys it and removes it as a periodic source.
        final LivenessScope scope = new LivenessScope();
        final Table functionBacked;
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            functionBacked = FunctionGeneratedTableFactory.create(() -> newTable(intCol("V", 1)), 3_600_000);
        }
        assertTableEquals(newTable(intCol("V", 1)), functionBacked);
        scope.release();
        assertFalse(functionBacked.tryRetainReference());
    }

    public void testDependenciesMustBeRefreshing() {
        // A static dependency is rejected: the factory can only be driven by refreshing sources.
        try {
            FunctionGeneratedTableFactory.create(() -> emptyTable(1), emptyTable(1));
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must be refreshing"));
        }
    }

    public void testSwitchNoInitialTable() throws Exception {
        final AppendOnlyArrayBackedInputTable source = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater updater = source.makeUpdater();

        // copyData == false with a retaining-last supplier that produces nothing at construction: the switch sources
        // are backed by null sources and the supplied definition provides the columns.
        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .retainingLastTableSupplier(() -> source.size() == 0
                        ? Optional.empty()
                        : Optional.of(newTable(intCol("Sum", (int) source.size()))))
                .tableDefinition(TableDefinition.of(ColumnDefinition.of("Sum", Type.intType())))
                .addDependencies(source)
                .copyData(false)
                .build());

        assertEquals(0, functionBacked.size());
        assertEquals(TableDefinition.of(ColumnDefinition.of("Sum", Type.intType())), functionBacked.getDefinition());

        handleDelayedRefresh(() -> updater.addAsync(newTable(intCol("IntCol", 1)), t -> {
        }), source);
        assertTableEquals(newTable(intCol("Sum", 1)), functionBacked);
    }

    public void testGeneratedTableWrongUpdateGraph() {
        // A generated refreshing table belonging to a different UpdateGraph than the factory's must be rejected.
        final UpdateGraph otherUpdateGraph = new ControlledUpdateGraph(TestExecutionContext.OPERATION_INITIALIZATION);
        final QueryTable foreign;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(otherUpdateGraph).open()) {
            foreign = testRefreshingTable(i(0).toTracking(), intCol("V", 1));
        }
        final QueryTable localDependency = testRefreshingTable(i(0).toTracking(), intCol("D", 1));
        try {
            FunctionGeneratedTableFactory.create(() -> foreign, localDependency);
            fail("Expected an IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("same UpdateGraph"));
        }
    }

    public void testRefreshWrongUpdateGraphNotifies() {
        final UpdateGraph otherUpdateGraph = new ControlledUpdateGraph(TestExecutionContext.OPERATION_INITIALIZATION);
        final QueryTable foreign;
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(otherUpdateGraph).open()) {
            foreign = testRefreshingTable(i(0).toTracking(), intCol("V", 1));
        }

        // Construct successfully with a local static table; a later refresh returns a refreshing table belonging to a
        // different UpdateGraph, which must be detected during generation and drive the error path.
        final QueryTable source = testRefreshingTable(i(0).toTracking(), intCol("IntCol", 1));
        final MutableObject<Table> nextResult = new MutableObject<>(newTable(intCol("V", 1)));
        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(nextResult::getValue)
                .addDependencies(source)
                .build());
        final ErrorListener errorListener = new ErrorListener(functionBacked);

        nextResult.setValue(foreign);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests();
        allowingError(() -> {
            addToTable(source, i(1), intCol("IntCol", 2));
            source.notifyListeners(i(1), i(), i());
            updateGraph.completeCycleForUnitTests();
        }, throwables -> throwables.size() == 1 && throwables.get(0) instanceof IllegalStateException
                && throwables.get(0).getMessage().contains("same UpdateGraph"));

        assertNotNull(errorListener.originalException);
    }

    public void testRefreshErrorNotifies() {
        final QueryTable source = testRefreshingTable(i(0).toTracking(), intCol("IntCol", 1));
        final MutableObject<Table> nextResult = new MutableObject<>(newTable(intCol("V", 1)));
        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(nextResult::getValue)
                .addDependencies(source)
                .build());
        assertTableEquals(newTable(intCol("V", 1)), functionBacked);

        final ErrorListener errorListener = new ErrorListener(functionBacked);

        // The next generation is definition-incompatible, failing the per-cycle compatibility check during refresh and
        // driving the error-notification path.
        nextResult.setValue(newTable(stringCol("V", "boom")));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests();
        allowingError(() -> {
            addToTable(source, i(1), intCol("IntCol", 2));
            source.notifyListeners(i(1), i(), i());
            updateGraph.completeCycleForUnitTests();
        }, throwables -> throwables.size() == 1
                && throwables.get(0) instanceof TableDefinition.IncompatibleTableDefinitionException);

        assertNotNull(errorListener.originalException);
        assertTrue(errorListener.originalException instanceof TableDefinition.IncompatibleTableDefinitionException);
    }

    private static class ErrorListener extends ShiftObliviousInstrumentedListenerAdapter {
        Throwable originalException;

        ErrorListener(final Table table) {
            super("FunctionGeneratedTable error checker", table, false);
            table.addUpdateListener(this, false);
        }

        @Override
        public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
            fail("Should not have received a normal update!");
        }

        @Override
        public void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
            this.originalException = originalException;
        }
    }
}
