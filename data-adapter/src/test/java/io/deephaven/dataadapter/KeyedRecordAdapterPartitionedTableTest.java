//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.base.Pair;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Basic;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.testutil.TstUtils.i;


public class KeyedRecordAdapterPartitionedTableTest extends KeyedRecordAdapterTestBase {

    public void testCustomKeyedRecordAdapterWithOneObjKeyCol() {
        final Pair<QueryTable, PartitionedTable> result = getSource("KeyCol1");
        final Table source = result.first;
        final PartitionedTable sourcePartitioned = result.second;

        final KeyedRecordAdapter<String, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        sourcePartitioned,
                        RecordAdapterDescriptorBuilder.create(MyRecord::new)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addCharColumnAdapter("CharCol", (myRecord, s) -> myRecord.myChar = s)
                                .addByteColumnAdapter("ByteCol", (myRecord, s) -> myRecord.myByte = s)
                                .addShortColumnAdapter("ShortCol", (myRecord, s) -> myRecord.myShort = s)
                                .addIntColumnAdapter("IntCol", (myRecord, s) -> myRecord.myInt = s)
                                .addFloatColumnAdapter("FloatCol", (myRecord, s) -> myRecord.myFloat = s)
                                .addLongColumnAdapter("LongCol", (myRecord, s) -> myRecord.myLong = s)
                                .addDoubleColumnAdapter("DoubleCol", (myRecord, s) -> myRecord.myDouble = s)
                                .addStringColumnAdapter("KeyCol1", (myRecord, s) -> myRecord.myKeyString = s)
                                .build(),
                        String.class);

        // getRecord tests
        final MyRecord recordA = keyedRecordAdapter.getRecord("KeyA");
        assertEquals("KeyA", recordA.myKeyString);
        assertEquals(0, recordA.myKeyInt);
        assertEquals("Xx", recordA.myString);
        assertEquals('X', recordA.myChar);
        assertEquals((byte) 99, recordA.myByte);
        assertEquals((short) 99, recordA.myShort);
        assertEquals(900, recordA.myInt);
        assertEquals(0.9f, recordA.myFloat);
        assertEquals(90_000_000_000L, recordA.myLong);
        assertEquals(9.9d, recordA.myDouble);

        final MyRecord recordB = keyedRecordAdapter.getRecord("KeyB");
        assertEquals("KeyB", recordB.myKeyString);
        assertEquals(0, recordA.myKeyInt);
        assertEquals("Yy", recordB.myString);
        assertEquals('Y', recordB.myChar);
        assertEquals((byte) 100, recordB.myByte);
        assertEquals((short) 100, recordB.myShort);
        assertEquals(1000, recordB.myInt);
        assertEquals(1.0f, recordB.myFloat);
        assertEquals(100_000_000_000L, recordB.myLong);
        assertEquals(10.0d, recordB.myDouble);

        // test null key
        final MyRecord recordNull = keyedRecordAdapter.getRecord(null);
        assertNull(recordNull.myKeyString);
        assertEquals(0, recordNull.myKeyInt);
        assertEquals("", recordNull.myString);
        assertEquals('0', recordNull.myChar);
        assertEquals((byte) -1, recordNull.myByte);
        assertEquals((short) -1, recordNull.myShort);
        assertEquals(-1, recordNull.myInt);
        assertEquals(-1.0f, recordNull.myFloat);
        assertEquals(-1L, recordNull.myLong);
        assertEquals(-1.0d, recordNull.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord("MissingKey"));

        // getRecordList tests
        final List<MyRecord> recordsA = keyedRecordAdapter.getRecordList("KeyA");
        assertEquals(3, recordsA.size());
        assertEquals("Aa", recordsA.get(0).myString);
        assertEquals("Cc", recordsA.get(1).myString);
        assertEquals("Xx", recordsA.get(2).myString);

        final List<MyRecord> recordsB = keyedRecordAdapter.getRecordList("KeyB");
        assertEquals(3, recordsB.size());
        assertNull(recordsB.get(0).myString);
        assertEquals("Dd", recordsB.get(1).myString);
        assertEquals("Yy", recordsB.get(2).myString);

        final List<MyRecord> recordsNull = keyedRecordAdapter.getRecordList(null);
        assertEquals(1, recordsNull.size());
        assertEquals(recordNull, recordsNull.get(0));

        // test single-argument composite key:
        assertEquals(recordA, keyedRecordAdapter.getRecordCompositeKey("KeyA"));

        // test missing composite key:
        assertNull(keyedRecordAdapter.getRecordCompositeKey("MissingKey"));

        // test invalid composite key:
        try {
            keyedRecordAdapter.getRecordCompositeKey("KeyA", 0);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 2 components; expected 1", ex.getMessage());
        }

        // getRecordListCompositeKey tests
        final List<MyRecord> recordsA_composite = keyedRecordAdapter.getRecordListCompositeKey("KeyA");
        assertEquals(recordsA, recordsA_composite);

        final List<MyRecord> recordsB_composite = keyedRecordAdapter.getRecordListCompositeKey("KeyB");
        assertEquals(recordsB, recordsB_composite);

        final List<MyRecord> recordsNull_composite = keyedRecordAdapter.getRecordListCompositeKey((Object) null);
        assertEquals(1, recordsNull_composite.size());
        assertEquals(recordNull, recordsNull_composite.get(0));

        assertNull(keyedRecordAdapter.getRecordListCompositeKey("MissingKey"));

        // Test retrieving multiple records (for different keys)
        final Map<String, MyRecord> records = keyedRecordAdapter.getRecords("KeyA", "KeyB", "MissingKey");
        assertEquals(recordA, records.get("KeyA"));
        assertEquals(recordB, records.get("KeyB"));
        assertFalse("records.containsKey(\"MissingKey\")", records.containsKey("MissingKey"));
    }

    public void testCustomKeyedRecordAdapterWithOnePrimitiveKeyCol() {
        final Pair<QueryTable, PartitionedTable> result = getSource("KeyCol2");
        final Table source = result.first;
        final PartitionedTable sourcePartitioned = result.second;

        final KeyedRecordAdapter<Integer, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        sourcePartitioned,
                        RecordAdapterDescriptorBuilder.create(MyRecord::new)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addCharColumnAdapter("CharCol", (myRecord, s) -> myRecord.myChar = s)
                                .addByteColumnAdapter("ByteCol", (myRecord, s) -> myRecord.myByte = s)
                                .addShortColumnAdapter("ShortCol", (myRecord, s) -> myRecord.myShort = s)
                                .addIntColumnAdapter("IntCol", (myRecord, s) -> myRecord.myInt = s)
                                .addFloatColumnAdapter("FloatCol", (myRecord, s) -> myRecord.myFloat = s)
                                .addLongColumnAdapter("LongCol", (myRecord, s) -> myRecord.myLong = s)
                                .addDoubleColumnAdapter("DoubleCol", (myRecord, s) -> myRecord.myDouble = s)
                                .addIntColumnAdapter("KeyCol2", (myRecord, s) -> myRecord.myKeyInt = s)
                                .build(),
                        int.class);

        // getRecord tests
        final MyRecord record0 = keyedRecordAdapter.getRecord(0);
        assertNull(record0.myKeyString);
        assertEquals(0, record0.myKeyInt);
        assertEquals("Xx", record0.myString);
        assertEquals('X', record0.myChar);
        assertEquals((byte) 99, record0.myByte);
        assertEquals((short) 99, record0.myShort);
        assertEquals(900, record0.myInt);
        assertEquals(0.9f, record0.myFloat);
        assertEquals(90_000_000_000L, record0.myLong);
        assertEquals(9.9d, record0.myDouble);

        final MyRecord record1 = keyedRecordAdapter.getRecord(1);
        assertNull(record1.myKeyString);
        assertEquals(1, record1.myKeyInt);
        assertEquals("Yy", record1.myString);
        assertEquals('Y', record1.myChar);
        assertEquals((byte) 100, record1.myByte);
        assertEquals((short) 100, record1.myShort);
        assertEquals(1000, record1.myInt);
        assertEquals(1.0f, record1.myFloat);
        assertEquals(100_000_000_000L, record1.myLong);
        assertEquals(10.0d, record1.myDouble);

        // test null key
        final MyRecord recordNull = keyedRecordAdapter.getRecord(null);
        assertNull(recordNull.myKeyString);
        assertEquals(QueryConstants.NULL_INT, recordNull.myKeyInt);
        assertEquals("", recordNull.myString);
        assertEquals('0', recordNull.myChar);
        assertEquals((byte) -1, recordNull.myByte);
        assertEquals((short) -1, recordNull.myShort);
        assertEquals(-1, recordNull.myInt);
        assertEquals(-1.0f, recordNull.myFloat);
        assertEquals(-1L, recordNull.myLong);
        assertEquals(-1.0d, recordNull.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord(-1));

        // getRecordList tests
        final List<MyRecord> records0 = keyedRecordAdapter.getRecordList(0);
        assertEquals(3, records0.size());
        assertEquals("Aa", records0.get(0).myString);
        assertNull(records0.get(1).myString);
        assertEquals("Xx", records0.get(2).myString);

        final List<MyRecord> records1 = keyedRecordAdapter.getRecordList(1);
        assertEquals(3, records1.size());
        assertEquals("Cc", records1.get(0).myString);
        assertEquals("Dd", records1.get(1).myString);
        assertEquals("Yy", records1.get(2).myString);

        final List<MyRecord> recordsNull = keyedRecordAdapter.getRecordList(null);
        assertEquals(1, recordsNull.size());
        assertEquals(recordNull, recordsNull.get(0));

        // test single-argument composite key:
        assertEquals(record0, keyedRecordAdapter.getRecordCompositeKey(0));

        // test missing composite key:
        assertNull(keyedRecordAdapter.getRecordCompositeKey(-1));

        // test invalid composite key:
        try {
            keyedRecordAdapter.getRecordCompositeKey("KeyA", 0);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 2 components; expected 1", ex.getMessage());
        }

        // getRecordListCompositeKey tests
        final List<MyRecord> records0_composite = keyedRecordAdapter.getRecordListCompositeKey(0);
        assertEquals(records0, records0_composite);

        final List<MyRecord> records1_composite = keyedRecordAdapter.getRecordListCompositeKey(1);
        assertEquals(records1, records1_composite);

        final List<MyRecord> recordsNull_composite = keyedRecordAdapter.getRecordListCompositeKey((Object) null);
        assertEquals(1, recordsNull_composite.size());
        assertEquals(recordNull, recordsNull_composite.get(0));

        assertNull(keyedRecordAdapter.getRecordListCompositeKey(-1));

        // Test retrieving multiple records
        final Map<Integer, MyRecord> records = keyedRecordAdapter.getRecords(0, 1, null);
        assertEquals(record0, records.get(0));
        assertEquals(record1, records.get(1));
        assertEquals(recordNull, records.get(null));
    }

    public void testCustomKeyedRecordAdapterWithTwoKeyCols() {
        final Pair<QueryTable, PartitionedTable> result = getSource("KeyCol1", "KeyCol2");
        final QueryTable source = result.first;
        final PartitionedTable sourcePartitioned = result.second;

        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        sourcePartitioned,
                        RecordAdapterDescriptorBuilder.create(MyRecord::new)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addCharColumnAdapter("CharCol", (myRecord, s) -> myRecord.myChar = s)
                                .addByteColumnAdapter("ByteCol", (myRecord, s) -> myRecord.myByte = s)
                                .addShortColumnAdapter("ShortCol", (myRecord, s) -> myRecord.myShort = s)
                                .addIntColumnAdapter("IntCol", (myRecord, s) -> myRecord.myInt = s)
                                .addFloatColumnAdapter("FloatCol", (myRecord, s) -> myRecord.myFloat = s)
                                .addLongColumnAdapter("LongCol", (myRecord, s) -> myRecord.myLong = s)
                                .addDoubleColumnAdapter("DoubleCol", (myRecord, s) -> myRecord.myDouble = s)
                                .addStringColumnAdapter("KeyCol1", (myRecord, s) -> myRecord.myKeyString = s)
                                .addIntColumnAdapter("KeyCol2", (myRecord, s) -> myRecord.myKeyInt = s)
                                .build());

        // getRecord tests
        final MyRecord recordA0 = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("KeyA", recordA0.myKeyString);
        assertEquals(0, recordA0.myKeyInt);
        assertEquals("Xx", recordA0.myString);
        assertEquals('X', recordA0.myChar);
        assertEquals((byte) 99, recordA0.myByte);
        assertEquals((short) 99, recordA0.myShort);
        assertEquals(900, recordA0.myInt);
        assertEquals(0.9f, recordA0.myFloat);
        assertEquals(90_000_000_000L, recordA0.myLong);
        assertEquals(9.9d, recordA0.myDouble);

        final MyRecord recordB0 = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
        assertEquals("KeyB", recordB0.myKeyString);
        assertEquals(0, recordB0.myKeyInt);
        assertTrue(Basic.isNull(recordB0.myString));
        assertTrue(Basic.isNull(recordB0.myChar));
        assertTrue(Basic.isNull(recordB0.myByte));
        assertTrue(Basic.isNull(recordB0.myShort));
        assertTrue(Basic.isNull(recordB0.myInt));
        assertTrue(Basic.isNull(recordB0.myFloat));
        assertTrue(Basic.isNull(recordB0.myLong));
        assertTrue(Basic.isNull(recordB0.myDouble));

        // test null key
        final MyRecord recordNull = keyedRecordAdapter.getRecord(Arrays.asList(null, null));
        assertNull(recordNull.myKeyString);
        assertEquals(QueryConstants.NULL_INT, recordNull.myKeyInt);
        assertEquals("", recordNull.myString);
        assertEquals('0', recordNull.myChar);
        assertEquals((byte) -1, recordNull.myByte);
        assertEquals((short) -1, recordNull.myShort);
        assertEquals(-1, recordNull.myInt);
        assertEquals(-1.0f, recordNull.myFloat);
        assertEquals(-1L, recordNull.myLong);
        assertEquals(-1.0d, recordNull.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord(Arrays.asList("MissingKey", 0)));

        // getRecordList tests
        final List<MyRecord> recordsA0 = keyedRecordAdapter.getRecordList(Arrays.asList("KeyA", 0));
        final MyRecord recordA0_0 = recordsA0.get(0);
        assertEquals("KeyA", recordA0_0.myKeyString);
        assertEquals(0, recordA0_0.myKeyInt);
        assertEquals("Aa", recordA0_0.myString);
        assertEquals('A', recordA0_0.myChar);
        assertEquals((byte) 0, recordA0_0.myByte);
        assertEquals((short) 1, recordA0_0.myShort);
        assertEquals(100, recordA0_0.myInt);
        assertEquals(0.1f, recordA0_0.myFloat);
        assertEquals(10_000_000_000L, recordA0_0.myLong);
        assertEquals(1.1d, recordA0_0.myDouble);

        final MyRecord recordA0_1 = recordsA0.get(1);
        assertEquals("KeyA", recordA0_1.myKeyString);
        assertEquals(0, recordA0_1.myKeyInt);
        assertEquals("Xx", recordA0_1.myString);
        assertEquals('X', recordA0_1.myChar);
        assertEquals((byte) 99, recordA0_1.myByte);
        assertEquals((short) 99, recordA0_1.myShort);
        assertEquals(900, recordA0_1.myInt);
        assertEquals(0.9f, recordA0_1.myFloat);
        assertEquals(90_000_000_000L, recordA0_1.myLong);
        assertEquals(9.9d, recordA0_1.myDouble);

        // test composite key:
        assertEquals(recordA0_1, keyedRecordAdapter.getRecordCompositeKey("KeyA", 0));

        // test missing composite key:
        assertNull(keyedRecordAdapter.getRecordCompositeKey("MissingKey", -1));

        // test invalid key:
        try {
            keyedRecordAdapter.getRecord(Collections.singletonList("KeyA"));
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 1 components; expected 2", ex.getMessage());
        }

        // getRecordListCompositeKey tests
        final List<MyRecord> recordsA0_composite = keyedRecordAdapter.getRecordListCompositeKey("KeyA", 0);
        assertEquals(recordsA0, recordsA0_composite);

        final List<MyRecord> recordsNull_composite = keyedRecordAdapter.getRecordListCompositeKey(null, null);
        assertEquals(1, recordsNull_composite.size());
        assertEquals(recordNull, recordsNull_composite.get(0));

        assertNull(keyedRecordAdapter.getRecordListCompositeKey("MissingKey", -1));

        // Test retrieving multiple records
        final Map<List<?>, MyRecord> records = keyedRecordAdapter.getRecords(Arrays.asList("KeyA", 0), Arrays.asList("KeyB", 0), Arrays.asList(null, null));
        assertEquals(recordA0_1, records.get(List.of("KeyA", 0)));
        assertEquals(recordB0, records.get(List.of("KeyB", 0)));
        assertEquals(recordNull, records.get(Arrays.asList(null, null)));
    }

    public void testGenericKeyedRecordAdapterUpdating() throws InterruptedException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false);

        final Pair<QueryTable, PartitionedTable> result = getSource("KeyCol1", "KeyCol2");
        final QueryTable source = result.first;
        final PartitionedTable sourcePartitioned = result.second;

        final KeyedRecordAdapter<List<?>, Map<String, Object>> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        sourcePartitioned,
                        Arrays.asList("StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol",
                                "DoubleCol"));

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(4).copy().toTracking(),
                    TableTools.col("KeyCol1", "KeyB"),
                    TableTools.col("KeyCol2", 0),
                    TableTools.col("StringCol", "bB"),
                    TableTools.charCol("CharCol", 'B'),
                    TableTools.byteCol("ByteCol", (byte) 2),
                    TableTools.shortCol("ShortCol", (short) 2),
                    TableTools.intCol("IntCol", 200),
                    TableTools.floatCol("FloatCol", 0.2f),
                    TableTools.longCol("LongCol", 20_000_000_000L),
                    TableTools.doubleCol("DoubleCol", 2.2d));
            TableTools.show(source);
            source.notifyListeners(i(), i(), i(4));
        });

        Map<String, Object> record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));

        assertEquals("bB", record.get("StringCol"));
        assertEquals('B', record.get("CharCol"));
        assertEquals((byte) 2, record.get("ByteCol"));
        assertEquals((short) 2, record.get("ShortCol"));
        assertEquals(200, record.get("IntCol"));
        assertEquals(0.2f, record.get("FloatCol"));
        assertEquals(20_000_000_000L, record.get("LongCol"));
        assertEquals(2.2d, record.get("DoubleCol"));

        Map<String, Object> recordFromVarrgs = keyedRecordAdapter.getRecordCompositeKey("KeyB", 0);
        assertEquals(record, recordFromVarrgs);


        Map<List<?>, Map<String, Object>> records = keyedRecordAdapter.getRecords(
                Arrays.asList("KeyB", 0),
                Arrays.asList("KeyZZZ", 99999),
                Arrays.asList("KeyA", 1));

        assertEquals(2, records.size());

        final Map<String, Object> recordB = records.get(Arrays.asList("KeyB", 0));
        assertEquals("bB", recordB.get("StringCol"));
        assertEquals('B', recordB.get("CharCol"));
        assertEquals((byte) 2, recordB.get("ByteCol"));
        assertEquals((short) 2, recordB.get("ShortCol"));
        assertEquals(200, recordB.get("IntCol"));
        assertEquals(0.2f, recordB.get("FloatCol"));
        assertEquals(20_000_000_000L, recordB.get("LongCol"));
        assertEquals(2.2d, recordB.get("DoubleCol"));

        final Map<String, Object> recordC = records.get(Arrays.asList("KeyA", 1));
        assertEquals("Cc", recordC.get("StringCol"));
        assertEquals('C', recordC.get("CharCol"));
        assertEquals((byte) 3, recordC.get("ByteCol"));
        assertEquals((short) 3, recordC.get("ShortCol"));
        assertEquals(300, recordC.get("IntCol"));
        assertEquals(0.3f, recordC.get("FloatCol"));
        assertEquals(30_000_000_000L, recordC.get("LongCol"));
        assertEquals(3.3d, recordC.get("DoubleCol"));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);
        new Thread(() -> {
            updateGraph.startCycleForUnitTests();
            TstUtils.removeRows(source, i(6));
            source.notifyListeners(i(), i(6), i());
            TableTools.show(source);
            l1.countDown();
            try {
                l2.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            updateGraph.completeCycleForUnitTests();
            l3.countDown();
        }).start();

        // Let the LTM cycle start
        l1.await();

        // LTM is being updated (On another thread) -- we should use prev, and still see KeyA/1
        Map<List<?>, Map<String, Object>> recordsDuringLTMpreRemove = keyedRecordAdapter.getRecords(
                Arrays.asList("KeyA", 0),
                Arrays.asList("KeyB", 0),
                Arrays.asList("KeyA", 1),
                Arrays.asList("KeyB", 1));

        assertEquals(4, recordsDuringLTMpreRemove.size());
        assertNotNull(recordsDuringLTMpreRemove.get(Arrays.asList("KeyA", 0)));
        assertEquals(recordB, recordsDuringLTMpreRemove.get(Arrays.asList("KeyB", 0)));
        assertEquals(recordC, recordsDuringLTMpreRemove.get(Arrays.asList("KeyA", 1)));
        assertNotNull(recordsDuringLTMpreRemove.get(Arrays.asList("KeyB", 1)));

        // Let the LTM cycle finish
        l2.countDown();
        l3.await();

        TableTools.show(source);

        // LTM cycle complete -- KeyA/1 should be gone
        Map<List<?>, Map<String, Object>> recordsAfterLTM = keyedRecordAdapter.getRecords(
                Arrays.asList("KeyA", 0),
                Arrays.asList("KeyB", 0),
                Arrays.asList("KeyA", 1),
                Arrays.asList("KeyB", 1));

        // TODO: how do I know that the row for KeyA/1 is gone, even though I can still find its data??
        //   The data is still in the column source, and the AggregationRowLookup never forgets the slot?
        assertEquals(3, recordsAfterLTM.size());
        assertEquals(recordB, recordsAfterLTM.get(Arrays.asList("KeyB", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyA", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyB", 1)));

    }

    private @NotNull Pair<QueryTable, PartitionedTable> getSource(final String... keyCols) {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final boolean dropKeys = false;
        final PartitionedTable partitionedTable = source.partitionBy(dropKeys, keyCols);
        TableTools.show(partitionedTable.table());

        return new Pair<>(source, partitionedTable);
    }


    /**
     * Test a KeyedRecordAdapter that converts rows into ObjectNodes, from a partitioned table
     */
    public void testJsonKeyedRecordAdapterPartitionedTable() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 9, 10).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1, 0, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd", "Xx", "Yy"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D', 'X', 'Y'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4, (byte) 99, (byte) 100),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4, (short) 99, (short) 100),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400, 900, 1000),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f, 0.9f, 1.0f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L,
                        40_000_000_000L, 90_000_000_000L, 100_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d, 9.9d, 10.0d));
        TableTools.show(source);

        final PartitionedTable sourcePartitioned = source.partitionBy(true, "KeyCol1", "KeyCol2");
        TableTools.show(sourcePartitioned.table());


        final KeyedRecordAdapter<List<?>, ObjectNode> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        sourcePartitioned,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source,
                                Arrays.asList("KeyCol1", "KeyCol2", "StringCol", "CharCol", "ByteCol", "ShortCol",
                                        "IntCol", "FloatCol", "LongCol", "DoubleCol")));

        List<ObjectNode> records = keyedRecordAdapter.getRecordList(Arrays.asList("KeyA", 0));
        final ObjectNode record0 = records.get(0);
        assertEquals("KeyA", record0.get("KeyCol1").textValue());
        assertEquals(0, record0.get("KeyCol2").intValue());
        assertEquals("Aa", record0.get("StringCol").textValue());
        assertEquals('A', record0.get("CharCol").textValue().charAt(0));
        assertEquals((byte) 0, (byte) record0.get("ByteCol").shortValue());
        assertEquals((short) 1, record0.get("ShortCol").shortValue());
        assertEquals(100, record0.get("IntCol").intValue());
        assertEquals(0.1f, record0.get("FloatCol").floatValue());
        assertEquals(10_000_000_000L, record0.get("LongCol").longValue());
        assertEquals(1.1d, record0.get("DoubleCol").doubleValue());

        final ObjectNode record1 = records.get(1);
        assertEquals("KeyA", record1.get("KeyCol1").textValue());
        assertEquals(0, record1.get("KeyCol2").intValue());
        assertEquals("Xx", record1.get("StringCol").textValue());
        assertEquals('X', record1.get("CharCol").textValue().charAt(0));
        assertEquals((byte) 99, (byte) record1.get("ByteCol").shortValue());
        assertEquals((short) 99, record1.get("ShortCol").shortValue());
        assertEquals(900, record1.get("IntCol").intValue());
        assertEquals(0.9f, record1.get("FloatCol").floatValue());
        assertEquals(90_000_000_000L, record1.get("LongCol").longValue());
        assertEquals(9.9d, record1.get("DoubleCol").doubleValue());

        ObjectNode record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
        assertEquals("KeyB", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertTrue(record.get("StringCol").isNull());
        assertTrue(record.get("CharCol").isNull());
        assertTrue(record.get("ByteCol").isNull());
        assertTrue(record.get("ShortCol").isNull());
        assertTrue(record.get("IntCol").isNull());
        assertTrue(record.get("FloatCol").isNull());
        assertTrue(record.get("LongCol").isNull());
        assertTrue(record.get("DoubleCol").isNull());

        // test missing key
        assertNull(keyedRecordAdapter.getRecord(Arrays.asList("MissingKey", 0)));

        // test invalid key
        try {
            keyedRecordAdapter.getRecord(Collections.singletonList("KeyA"));
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 1 components; expected 2", ex.getMessage());
        }
    }


}
