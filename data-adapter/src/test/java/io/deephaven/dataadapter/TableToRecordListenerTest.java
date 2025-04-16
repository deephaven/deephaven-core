//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.verify.Assert;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

import static io.deephaven.engine.testutil.TstUtils.i;

public class TableToRecordListenerTest extends RefreshingTableTestCase {

    private static final NullNode NULL_JSON_NODE = NullNode.getInstance();

    @SuppressWarnings("FieldCanBeLocal") // can't be local; for retention
    private TableToRecordListener<ObjectNode> tableToRecordListener;

    /**
     * Test synchronous publication of records (i.e., table data is converted to records and published on the UGP
     * thread).
     */
    public void testJsonRecordListenerNoAsync() {
        runJsonRecordListenerTest(false);
    }

    /**
     * Test asynchronous publication of records (i.e., table data is retrieved in the listener on the UGP thread, but
     * converted to records and published on a separate thread).
     */
    public void testJsonRecordListenerAsync() {
        runJsonRecordListenerTest(true);
    }

    /**
     * @param async {@code true} for the {@link TableToRecordListener} to create/process records on a separate thread;
     *        {@code false} to process them on a UGP thread. (See {@link TableToRecordListener#TableToRecordListener}.)
     */
    public void runJsonRecordListenerTest(boolean async) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

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

        Queue<ObjectNode> addedModified = new ConcurrentLinkedDeque<>();
        Queue<ObjectNode> removedModifiedPrev = new ConcurrentLinkedDeque<>();

        ReentrantLock l = !async ? null : new ReentrantLock();
        Condition recordsProcessedCondition = !async ? null : l.newCondition();
        MutableInt nProcessedRecords = !async ? null : new MutableInt(0);

        final boolean processInitialData = true;

        tableToRecordListener = updateGraph.sharedLock()
                .computeLocked(() -> new TableToRecordListener<>(
                        "desc",
                        source,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source,
                                Arrays.asList("KeyCol1", "KeyCol2", "StringCol", "CharCol", "ByteCol", "ShortCol",
                                        "IntCol", "FloatCol", "LongCol", "DoubleCol")),
                        addedModified::add,
                        removedModifiedPrev::add,
                        processInitialData,
                        async,
                        !async ? null : nUpdatesProcessed -> {
                            // This is used with awaitRecordProcessing to verify that records are enqueued/processed
                            // correctly.
                            try {
                                l.lock();
                                nProcessedRecords.setValue(nUpdatesProcessed);
                                recordsProcessedCondition.signal();
                            } finally {
                                l.unlock();
                            }
                        }));

        IntConsumer awaitRecordProcessing;
        if (!async) {
            awaitRecordProcessing = (expectedRecords) -> {
            };
        } else {
            awaitRecordProcessing = (expectedRecords) -> {
                final int timeout = 10;
                final TimeUnit timeoutUnit = TimeUnit.SECONDS;

                l.lock();
                try {
                    if (nProcessedRecords.intValue() == 0 && !recordsProcessedCondition.await(timeout, timeoutUnit)) {
                        throw new RuntimeException("No records processed after " + timeout + " " + timeoutUnit);
                    }

                    Assert.equals(expectedRecords, "expectedRecords", nProcessedRecords.getValue(),
                            "nProcessedRecords.getValue()");

                    nProcessedRecords.setValue(0);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for records to process", e);
                } finally {
                    l.unlock();
                }
            };
        }

        ObjectNode record = addedModified.remove();
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

        record = addedModified.remove();
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

        record = addedModified.remove();
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

        record = addedModified.remove();
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

        assertTrue(addedModified.isEmpty());
        assertTrue(removedModifiedPrev.isEmpty());

        // modify a row
        updateGraph.runWithinUnitTestCycle(() -> {
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
            TableTools.show(source);
            source.notifyListeners(i(), i(), i(4));
        });

        // expect 2 changes (1 add_modify, 1 remove_modprev)
        awaitRecordProcessing.accept(2);

        // check the new row at the modified index
        record = addedModified.remove();
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

        // check the old row at the modified index
        record = removedModifiedPrev.remove();
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

        assertTrue(addedModified.isEmpty());
        assertTrue(removedModifiedPrev.isEmpty());

        // remove a row
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(6));
            TableTools.show(source);
            source.notifyListeners(i(), i(6), i());
        });

        awaitRecordProcessing.accept(1);

        record = removedModifiedPrev.remove();
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

        assertTrue(addedModified.isEmpty());
        assertTrue(removedModifiedPrev.isEmpty());

        // add a row (same one that was removed, but new index)
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(7).toTracking(),
                    TableTools.col("KeyCol1", "KeyA"),
                    TableTools.col("KeyCol2", 1),
                    TableTools.col("StringCol", "Cc"),
                    TableTools.charCol("CharCol", 'C'),
                    TableTools.byteCol("ByteCol", (byte) 3),
                    TableTools.shortCol("ShortCol", (short) 3),
                    TableTools.intCol("IntCol", 300),
                    TableTools.floatCol("FloatCol", 0.3f),
                    TableTools.longCol("LongCol", 30_000_000_000L),
                    TableTools.doubleCol("DoubleCol", 3.3d));
            TableTools.show(source);
            source.notifyListeners(i(7), i(), i());
        });

        awaitRecordProcessing.accept(1);

        record = addedModified.remove();
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

        assertTrue(addedModified.isEmpty());
        assertTrue(removedModifiedPrev.isEmpty());
    }

}
