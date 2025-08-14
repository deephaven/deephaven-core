//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.testutil.TstUtils.i;

public class TableToRecordAdapterTest extends RefreshingTableTestCase {

    private static final NullNode NULL_JSON_NODE = NullNode.getInstance();

    public void testJsonRecordAdapter() throws InterruptedException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false);

        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L,
                        40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d));
        TableTools.show(source);

        // For the sake of the test, we need to run some operation (e.g. a lastBy) on the
        // table before passing it to the TableToRecordAdapter in order to test using prev.
        // If we pass the raw 'source', the TableToRecordAdapter will never read from prev,
        // even in the middle of an update cycle.
        final Table recordAdapterSource = source.lastBy("KeyCol1", "KeyCol2");

        TableToRecordAdapter<ObjectNode> recordAdapter = new TableToRecordAdapter<>(
                recordAdapterSource,
                JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source.getDefinition(),
                        Arrays.asList("KeyCol1", "KeyCol2", "StringCol", "CharCol", "ByteCol", "ShortCol",
                                "IntCol", "FloatCol", "LongCol", "DoubleCol")));

        ObjectNode[] records = recordAdapter.getRecords();
        assertEquals("records.length", 4, records.length);

        ObjectNode record = records[0];
        assertNotNull(record);

        assertEquals("KeyA", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertEquals("Aa", record.get("StringCol").textValue());
        assertEquals('A', record.get("CharCol").textValue().charAt(0));
        assertEquals((byte) 0, (byte) record.get("ByteCol").shortValue());
        assertEquals((short) 1, record.get("ShortCol").shortValue());
        assertEquals(100, record.get("IntCol").intValue());
        assertEquals(0.1f, record.get("FloatCol").floatValue());
        assertEquals(10_000_000_000L, record.get("LongCol").longValue());
        assertEquals(1.1d, record.get("DoubleCol").doubleValue());

        record = records[1];
        assertNotNull(record);

        assertEquals("KeyB", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertEquals(NULL_JSON_NODE, record.get("StringCol"));
        assertEquals(NULL_JSON_NODE, record.get("CharCol"));
        assertEquals(NULL_JSON_NODE, record.get("ByteCol"));
        assertEquals(NULL_JSON_NODE, record.get("ShortCol"));
        assertEquals(NULL_JSON_NODE, record.get("IntCol"));
        assertEquals(NULL_JSON_NODE, record.get("FloatCol"));
        assertEquals(NULL_JSON_NODE, record.get("LongCol"));
        assertEquals(NULL_JSON_NODE, record.get("DoubleCol"));

        record = records[2];
        assertNotNull(record);

        assertEquals("KeyA", record.get("KeyCol1").textValue());
        assertEquals(1, record.get("KeyCol2").intValue());
        assertEquals("Cc", record.get("StringCol").textValue());
        assertEquals("C", record.get("CharCol").textValue());
        assertEquals((byte) 3, record.get("ByteCol").shortValue());
        assertEquals((short) 3, record.get("ShortCol").shortValue());
        assertEquals(300, record.get("IntCol").intValue());
        assertEquals(0.3f, record.get("FloatCol").floatValue());
        assertEquals(30_000_000_000L, record.get("LongCol").longValue());
        assertEquals(3.3d, record.get("DoubleCol").doubleValue());

        record = records[3];
        assertNotNull(record);

        assertEquals("KeyB", record.get("KeyCol1").textValue());
        assertEquals(1, record.get("KeyCol2").intValue());
        assertEquals("Dd", record.get("StringCol").textValue());
        assertEquals("D", record.get("CharCol").textValue());
        assertEquals((byte) 4, record.get("ByteCol").shortValue());
        assertEquals((short) 4, record.get("ShortCol").shortValue());
        assertEquals(400, record.get("IntCol").intValue());
        assertEquals(0.4f, record.get("FloatCol").floatValue());
        assertEquals(40_000_000_000L, record.get("LongCol").longValue());
        assertEquals(4.4d, record.get("DoubleCol").doubleValue());

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(6));
            source.notifyListeners(i(), i(6), i());
        });

        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);
        CountDownLatch l3 = new CountDownLatch(1);

        // modify a row
        new Thread(() -> {
            updateGraph.startCycleForUnitTests();
            TstUtils.addToTable(source, i(4).toTracking(),
                    TableTools.col("KeyCol1", "KeyB"),
                    TableTools.col("KeyCol2", 0),
                    TableTools.col("StringCol", "Zz"),
                    TableTools.charCol("CharCol", 'Z'),
                    TableTools.byteCol("ByteCol", (byte) 9),
                    TableTools.shortCol("ShortCol", (short) 9),
                    TableTools.intCol("IntCol", 900),
                    TableTools.floatCol("FloatCol", 0.9f),
                    TableTools.longCol("LongCol", 90_000_000_000L),
                    TableTools.doubleCol("DoubleCol", 9.9d));
            source.notifyListeners(i(), i(), i(4));
            TableTools.show(source);

            l1.countDown();
            try {
                l2.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            updateGraph.completeCycleForUnitTests();
            l3.countDown();
        }, "UpdaterThread").start();

        l1.await();

        // Check the second row (rowKey=4; KeyB/0) and make sure we got the old value
        record = recordAdapter.getRecordByPosition(1);

        assertNotNull(record);

        assertEquals("KeyB", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertEquals(NULL_JSON_NODE, record.get("StringCol"));
        assertEquals(NULL_JSON_NODE, record.get("CharCol"));
        assertEquals(NULL_JSON_NODE, record.get("ByteCol"));
        assertEquals(NULL_JSON_NODE, record.get("ShortCol"));
        assertEquals(NULL_JSON_NODE, record.get("IntCol"));
        assertEquals(NULL_JSON_NODE, record.get("FloatCol"));
        assertEquals(NULL_JSON_NODE, record.get("LongCol"));
        assertEquals(NULL_JSON_NODE, record.get("DoubleCol"));

        // Let the UGP finish processing the update, and wait for it to do so
        l2.countDown();
        l3.await();

        // Check the second row and make sure we got the new value
        // Check the second row (rowKey=4; KeyB/0) and make sure we got the new value
        record = recordAdapter.getRecordByPosition(1);

        assertNotNull(record);

        assertEquals("KeyB", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertEquals("Zz", record.get("StringCol").textValue());
        assertEquals("Z", record.get("CharCol").textValue());
        assertEquals((byte) 9, record.get("ByteCol").shortValue());
        assertEquals((short) 9, record.get("ShortCol").shortValue());
        assertEquals(900, record.get("IntCol").intValue());
        assertEquals(0.9f, record.get("FloatCol").floatValue());
        assertEquals(90_000_000_000L, record.get("LongCol").longValue());
        assertEquals(9.9d, record.get("DoubleCol").doubleValue());
    }

}
