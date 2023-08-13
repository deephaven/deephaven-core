package io.deephaven.stream;

import io.deephaven.base.verify.Assert;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.SimpleListener;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.deephaven.engine.util.TableTools.merge;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TablePublisherTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void add() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofTime("T"));

        final Table emptyTable = TableTools.newTable(definition);

        final Table fooRow = TableTools.newTable(definition,
                TableTools.stringCol("S", "Foo"),
                TableTools.booleanCol("B", true),
                TableTools.intCol("I", 42),
                TableTools.longCol("L", 43L),
                TableTools.doubleCol("D", 44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(55)));

        final Table barRow = TableTools.newTable(definition,
                TableTools.stringCol("S", "Bar"),
                TableTools.booleanCol("B", false),
                TableTools.intCol("I", -42),
                TableTools.longCol("L", -43L),
                TableTools.doubleCol("D", -44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(-55)));

        final Table nullRow = TableTools.newTable(definition,
                TableTools.stringCol("S", new String[] {null}),
                TableTools.booleanCol("B", new Boolean[] {null}),
                TableTools.intCol("I", QueryConstants.NULL_INT),
                TableTools.longCol("L", QueryConstants.NULL_LONG),
                TableTools.doubleCol("D", QueryConstants.NULL_DOUBLE),
                TableTools.instantCol("T", new Instant[] {null}));

        final Table fooBarNull = merge(fooRow, barRow, nullRow);

        // Ensure we exercise publisher#add where there are multiple large chunks.
        // +37 just to have the last chunk not be the full BLOCK_SIZE
        final Table bigFooBarNull = repeat(fooBarNull, ArrayBackedColumnSource.BLOCK_SIZE * 5 + 37);

        final AtomicInteger flushCount = new AtomicInteger();
        final MutableBoolean onShutdown = new MutableBoolean();
        final TablePublisher publisher = TablePublisher.of("add", definition, tp -> flushCount.getAndIncrement(),
                () -> onShutdown.setValue(true));
        final Table blinkTable = publisher.table();
        TstUtils.assertTableEquals(emptyTable, blinkTable);
        assertThat(flushCount.get()).isEqualTo(0);

        publisher.add(fooRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooRow, blinkTable);
        assertThat(flushCount.get()).isEqualTo(1);

        publisher.add(barRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(barRow, blinkTable);
        assertThat(flushCount.get()).isEqualTo(2);

        publisher.add(nullRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(nullRow, blinkTable);
        assertThat(flushCount.get()).isEqualTo(3);

        publisher.add(fooBarNull);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooBarNull, blinkTable);
        assertThat(flushCount.get()).isEqualTo(4);

        publisher.add(fooRow);
        publisher.add(barRow);
        publisher.add(nullRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooBarNull, blinkTable);
        assertThat(flushCount.get()).isEqualTo(5);

        publisher.add(bigFooBarNull);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(bigFooBarNull, blinkTable);
        assertThat(flushCount.get()).isEqualTo(6);

        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(emptyTable, blinkTable);
        assertThat(flushCount.get()).isEqualTo(7);

        assertThat(onShutdown.booleanValue()).isFalse();
        assertThat(publisher.isAlive()).isTrue();
    }

    @Test
    public void addOnFlush() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofTime("T"));

        final Table emptyTable = TableTools.newTable(definition);

        final Table fooRow = TableTools.newTable(definition,
                TableTools.stringCol("S", "Foo"),
                TableTools.booleanCol("B", true),
                TableTools.intCol("I", 42),
                TableTools.longCol("L", 43L),
                TableTools.doubleCol("D", 44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(55)));

        final Table barRow = TableTools.newTable(definition,
                TableTools.stringCol("S", "Bar"),
                TableTools.booleanCol("B", false),
                TableTools.intCol("I", -42),
                TableTools.longCol("L", -43L),
                TableTools.doubleCol("D", -44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(-55)));

        final Table nullRow = TableTools.newTable(definition,
                TableTools.stringCol("S", new String[] {null}),
                TableTools.booleanCol("B", new Boolean[] {null}),
                TableTools.intCol("I", QueryConstants.NULL_INT),
                TableTools.longCol("L", QueryConstants.NULL_LONG),
                TableTools.doubleCol("D", QueryConstants.NULL_DOUBLE),
                TableTools.instantCol("T", new Instant[] {null}));

        final Table fooBarNull = merge(fooRow, barRow, nullRow);

        // Ensure we exercise publisher#add where there are multiple large chunks.
        // +37 just to have the last chunk not be the full BLOCK_SIZE
        final Table bigFooBarNull = repeat(fooBarNull, ArrayBackedColumnSource.BLOCK_SIZE * 5 + 37);

        final List<Table> onFlushTables = new ArrayList<>();
        final Consumer<TablePublisher> onFlushCallback = tp -> {
            try {
                for (Table table : onFlushTables) {
                    tp.add(table);
                }
            } finally {
                onFlushTables.clear();
            }
        };

        final MutableBoolean onShutdown = new MutableBoolean();
        final TablePublisher publisher =
                TablePublisher.of("addOnFlush", definition, onFlushCallback, () -> onShutdown.setValue(true));
        final Table blinkTable = publisher.table();
        TstUtils.assertTableEquals(emptyTable, blinkTable);

        onFlushTables.add(fooRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooRow, blinkTable);

        onFlushTables.add(barRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(barRow, blinkTable);

        onFlushTables.add(nullRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(nullRow, blinkTable);

        onFlushTables.add(fooBarNull);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooBarNull, blinkTable);

        onFlushTables.add(fooRow);
        onFlushTables.add(barRow);
        onFlushTables.add(nullRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooBarNull, blinkTable);

        onFlushTables.add(bigFooBarNull);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(bigFooBarNull, blinkTable);

        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(emptyTable, blinkTable);

        assertThat(onShutdown.booleanValue()).isFalse();
        assertThat(publisher.isAlive()).isTrue();
    }

    @Test
    public void publishFailure() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(ColumnDefinition.ofInt("I"));

        final Table emptyTable = TableTools.newTable(definition);

        final MutableBoolean onShutdown = new MutableBoolean();
        final TablePublisher publisher =
                TablePublisher.of("publishFailure", definition, null, () -> onShutdown.setValue(true));
        final Table blinkTable = publisher.table();
        TstUtils.assertTableEquals(emptyTable, blinkTable);

        final Throwable[] exceptions = new Throwable[1];
        final SimpleListener listener = new SimpleListener(blinkTable) {
            @Override
            public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                exceptions[0] = originalException;
            }
        };
        blinkTable.addUpdateListener(listener);

        final RuntimeException e = new RuntimeException("Some sort of failure");
        publisher.publishFailure(e);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(emptyTable, blinkTable);
        Assert.eq(e, "e", exceptions[0]);
        assertThat(onShutdown.booleanValue()).isTrue();
        assertThat(publisher.isAlive()).isFalse();
    }

    @Test
    public void publishFailureOnFlush() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(ColumnDefinition.ofInt("I"));

        final Table emptyTable = TableTools.newTable(definition);

        final MutableBoolean onShutdown = new MutableBoolean();
        final RuntimeException e = new RuntimeException("Some sort of failure");
        final TablePublisher publisher = TablePublisher.of("publishFailureOnFlush", definition,
                tp -> tp.publishFailure(e), () -> onShutdown.setValue(true));
        final Table blinkTable = publisher.table();
        TstUtils.assertTableEquals(emptyTable, blinkTable);

        final Throwable[] exceptions = new Throwable[1];
        final SimpleListener listener = new SimpleListener(blinkTable) {
            @Override
            public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                exceptions[0] = originalException;
            }
        };
        blinkTable.addUpdateListener(listener);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(emptyTable, blinkTable);
        Assert.eq(e, "e", exceptions[0]);
        assertThat(onShutdown.booleanValue()).isTrue();
        assertThat(publisher.isAlive()).isFalse();
    }

    @Test
    public void addWithMissingColumn() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofTime("T"));

        final TableDefinition definitionMissingColumn = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"));

        // missing time column
        final Table fooRowMissingColumn = TableTools.newTable(definitionMissingColumn,
                TableTools.stringCol("S", "Foo"),
                TableTools.booleanCol("B", true),
                TableTools.intCol("I", 42),
                TableTools.longCol("L", 43L),
                TableTools.doubleCol("D", 44.0));

        final TablePublisher publisher =
                TablePublisher.of("TablePublisherTest#addWithMissingColumn", definition, null, null);

        try {
            publisher.add(fooRowMissingColumn);
            failBecauseExceptionWasNotThrown(NoSuchColumnException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [T]");
        }
    }

    @Test
    public void addWithExtraColumn() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"));

        final TableDefinition definitionExtraColumn = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofTime("T"));

        // extra time column
        final Table fooRowExtraColumn = TableTools.newTable(definitionExtraColumn,
                TableTools.stringCol("S", "Foo"),
                TableTools.booleanCol("B", true),
                TableTools.intCol("I", 42),
                TableTools.longCol("L", 43L),
                TableTools.doubleCol("D", 44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(55)));

        final TablePublisher publisher =
                TablePublisher.of("TablePublisherTest#addWithExtraColumn", definition, null, null);
        final Table blinkTable = publisher.table();

        publisher.add(fooRowExtraColumn);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooRowExtraColumn.dropColumns("T"), blinkTable);

        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
    }

    @Test
    public void addWithReordering() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofString("S"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofTime("T"));

        final TableDefinition definitionBackwards = TableDefinition.of(
                ColumnDefinition.ofTime("T"),
                ColumnDefinition.ofDouble("D"),
                ColumnDefinition.ofLong("L"),
                ColumnDefinition.ofInt("I"),
                ColumnDefinition.ofBoolean("B"),
                ColumnDefinition.ofString("S"));

        final Table foo = TableTools.newTable(definition,
                TableTools.stringCol("S", "Foo"),
                TableTools.booleanCol("B", true),
                TableTools.intCol("I", 42),
                TableTools.longCol("L", 43L),
                TableTools.doubleCol("D", 44.0),
                TableTools.instantCol("T", Instant.ofEpochMilli(55)));

        // reverse column order
        final Table fooReversed = TableTools.newTable(definitionBackwards,
                TableTools.instantCol("T", Instant.ofEpochMilli(55)),
                TableTools.doubleCol("D", 44.0),
                TableTools.longCol("L", 43L),
                TableTools.intCol("I", 42),
                TableTools.booleanCol("B", true),
                TableTools.stringCol("S", "Foo"));

        final TablePublisher publisher =
                TablePublisher.of("TablePublisherTest#addWithReordering", definition, null, null);
        final Table blinkTable = publisher.table();

        publisher.add(fooReversed);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);

        TstUtils.assertTableEquals(foo, blinkTable);

        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
    }

    private static Table repeat(Table x, int times) {
        Table[] repeated = new Table[times];
        Arrays.fill(repeated, x);
        return merge(repeated);
    }
}
