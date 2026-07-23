//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.QueryTableTest;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.qst.type.Type;
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
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        // copyData == false wires the result directly to the generated table's column sources via SwitchColumnSource.
        final Table functionBacked = FunctionGeneratedTableFactory.create(FunctionGeneratedTableSpec.builder()
                .tableSupplier(() -> queryTable)
                .addDependencies(queryTable)
                .copyData(false)
                .build());

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
        };

        for (int i = 0; i < 25; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
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
}
