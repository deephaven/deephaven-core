package io.deephaven.stream;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;

public class TableToStreamTableTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testSimple() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("S", "I", "L", "D"),
                List.of(String.class, int.class, long.class, double.class));

        final Table empty = TableTools.newTable(tableDefinition);

        final Table bill = TableTools.newTable(
                col("S", "Bill"),
                intCol("I", 2),
                longCol("L", 4L),
                doubleCol("D", Math.PI));

        final Table ted = TableTools.newTable(
                col("S", "Ted"),
                intCol("I", 3),
                longCol("L", 5L),
                doubleCol("D", Math.E));

        final TableToStreamTable adapter = tableToStreamTable(tableDefinition);
        final TableStreamConsumer consumer = adapter.consumer(2048, true);
        final Table result = adapter.table();

        TstUtils.assertTableEquals(empty, result);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);

        consumer.add(bill);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(bill, result);

        consumer.add(ted);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(ted, result);

        consumer.add(bill);
        consumer.add(ted);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(TableTools.merge(bill, ted), result);

        consumer.add(ted);
        consumer.add(bill);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(TableTools.merge(ted, bill), result);

        consumer.add(bill);
        consumer.add(ted);
        consumer.add(bill);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(TableTools.merge(bill, ted, bill), result);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
    }

    @Test
    public void testBig() {
        final TableDefinition tableDefinition = TableDefinition.from(List.of("L"), List.of(long.class));

        final Table empty = TableTools.newTable(tableDefinition);

        final Table t_1024 = TableTools.emptyTable(1024).view("L=ii");

        final Table t_2048 = TableTools.emptyTable(2048).view("L=ii");

        final Table t_4096 = TableTools.emptyTable(4096).view("L=ii");

        final TableToStreamTable adapter = tableToStreamTable(tableDefinition);

        final TableStreamConsumer consumer = adapter.consumer(2048, true);

        final Table result = adapter.table();

        TstUtils.assertTableEquals(empty, result);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);

        consumer.add(t_1024);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(t_1024, result);

        consumer.add(t_2048);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(t_2048, result);

        consumer.add(t_4096);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(t_4096, result);

        consumer.add(t_1024);
        consumer.add(t_2048);
        consumer.add(t_4096);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(TableTools.merge(t_1024, t_2048, t_4096), result);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
    }

    private static TableToStreamTable tableToStreamTable(TableDefinition tableDefinition) {
        return TableToStreamTable.of("test", tableDefinition, UpdateGraphProcessor.DEFAULT, null);
    }
}
