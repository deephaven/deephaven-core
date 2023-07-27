package io.deephaven.stream;

import io.deephaven.base.verify.Assert;
import io.deephaven.csv.util.MutableBoolean;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
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
import java.util.Arrays;

import static io.deephaven.engine.util.TableTools.merge;
import static org.assertj.core.api.Assertions.assertThat;

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

        final MutableBoolean onShutdown = new MutableBoolean();
        final TablePublisher publisher =
                TablePublisher.of("publisher.add", definition, () -> onShutdown.setValue(true));
        final Table blinkTable = publisher.table();
        TstUtils.assertTableEquals(emptyTable, blinkTable);

        publisher.add(fooRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooRow, blinkTable);

        publisher.add(barRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(barRow, blinkTable);

        publisher.add(nullRow);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(nullRow, blinkTable);

        publisher.add(fooBarNull);
        updateGraph.runWithinUnitTestCycle(publisher::runForUnitTests);
        TstUtils.assertTableEquals(fooBarNull, blinkTable);

        publisher.add(bigFooBarNull);
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
                TablePublisher.of("pubhlisher.publishFailure", definition, () -> onShutdown.setValue(true));
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

    private static Table repeat(Table x, int times) {
        Table[] repeated = new Table[times];
        Arrays.fill(repeated, x);
        return merge(repeated);
    }
}
