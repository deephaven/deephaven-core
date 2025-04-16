//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.TableBackedDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.testutil.TstUtils.i;


public class KeyedRecordAdapterTest extends KeyedRecordAdapterTestBase {

    /**
     * Test a KeyedRecordAdapter that converts rows into HashMaps
     */
    public void testGenericKeyedRecordAdapter() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, Map<String, Object>> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
                        Arrays.asList("StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol",
                                "DoubleCol"),
                        "KeyCol1", "KeyCol2");

        Map<String, Object> record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("Xx", record.get("StringCol"));
        assertEquals('X', record.get("CharCol"));
        assertEquals((byte) 99, record.get("ByteCol"));
        assertEquals((short) 99, record.get("ShortCol"));
        assertEquals(900, record.get("IntCol"));
        assertEquals(0.9f, record.get("FloatCol"));
        assertEquals(90_000_000_000L, record.get("LongCol"));
        assertEquals(9.9d, record.get("DoubleCol"));

        record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
        assertNull(record.get("StringCol"));
        assertNull(record.get("CharCol"));
        assertNull(record.get("ByteCol"));
        assertNull(record.get("ShortCol"));
        assertNull(record.get("IntCol"));
        assertNull(record.get("FloatCol"));
        assertNull(record.get("LongCol"));
        assertNull(record.get("DoubleCol"));

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

    /**
     * Test a KeyedRecordAdapter that converts rows into ObjectNodes
     */
    public void testJsonKeyedRecordAdapter() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, ObjectNode> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source,
                                Arrays.asList("KeyCol1", "KeyCol2", "StringCol", "CharCol", "ByteCol", "ShortCol",
                                        "IntCol", "FloatCol", "LongCol", "DoubleCol")),
                        "KeyCol1", "KeyCol2");

        ObjectNode record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("KeyA", record.get("KeyCol1").textValue());
        assertEquals(0, record.get("KeyCol2").intValue());
        assertEquals("Xx", record.get("StringCol").textValue());
        assertEquals('X', record.get("CharCol").textValue().charAt(0));
        assertEquals((byte) 99, (byte) record.get("ByteCol").shortValue());
        assertEquals((short) 99, record.get("ShortCol").shortValue());
        assertEquals(900, record.get("IntCol").intValue());
        assertEquals(0.9f, record.get("FloatCol").floatValue());
        assertEquals(90_000_000_000L, record.get("LongCol").longValue());
        assertEquals(9.9d, record.get("DoubleCol").doubleValue());

        record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
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

    public void testCustomKeyedRecordAdapterWithOneObjKeyCol() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<String, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
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
                        "KeyCol1", String.class);

        final MyRecord recordA = keyedRecordAdapter.getRecord("KeyA");
        assertEquals("KeyA", recordA.myKeyString);
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

        // getRecordList tests (only return last match due to implicit lastBy)
        final List<MyRecord> recordsA = keyedRecordAdapter.getRecordList("KeyA");
        assertEquals(1, recordsA.size());
        assertEquals("Xx", recordsA.get(0).myString);

        final List<MyRecord> recordsB = keyedRecordAdapter.getRecordList("KeyB");
        assertEquals(1, recordsB.size());
        assertEquals("Yy", recordsB.get(0).myString);

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
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<Integer, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
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
                        "KeyCol2", Integer.class);

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

        // getRecordList tests (only return last match due to implicit lastBy)
        final List<MyRecord> records0 = keyedRecordAdapter.getRecordList(0);
        assertEquals(1, records0.size());
        assertEquals("Xx", records0.get(0).myString);

        final List<MyRecord> records1 = keyedRecordAdapter.getRecordList(1);
        assertEquals(1, records1.size());
        assertEquals("Yy", records1.get(0).myString);

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

        // Test retrieving multiple records (for different keys)
        final Map<Integer, MyRecord> records = keyedRecordAdapter.getRecords(0, 1, null);
        assertEquals(record0, records.get(0));
        assertEquals(record1, records.get(1));
        assertEquals(recordNull, records.get(null));
    }

    public void testCustomKeyedRecordAdapterWithTwoKeyCols() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
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
                                .build(),
                        "KeyCol1", "KeyCol2");

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
        assertNull(recordB0.myString);
        assertEquals(QueryConstants.NULL_CHAR, recordB0.myChar);
        assertEquals(QueryConstants.NULL_BYTE, recordB0.myByte);
        assertEquals(QueryConstants.NULL_SHORT, recordB0.myShort);
        assertEquals(QueryConstants.NULL_INT, recordB0.myInt);
        assertEquals(QueryConstants.NULL_FLOAT, recordB0.myFloat);
        assertEquals(QueryConstants.NULL_LONG, recordB0.myLong);
        assertEquals(QueryConstants.NULL_DOUBLE, recordB0.myDouble);

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


        // test composite key:
        assertEquals(recordA0, keyedRecordAdapter.getRecordCompositeKey("KeyA", 0));

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord(Arrays.asList("MissingKey", 0)));

        // test invalid key:
        try {
            keyedRecordAdapter.getRecord(Collections.singletonList("KeyA"));
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 1 components; expected 2", ex.getMessage());
        }

        // Test retrieving multiple records
        final Map<List<?>, MyRecord> records = keyedRecordAdapter.getRecords(Arrays.asList("KeyA", 0), Arrays.asList("KeyB", 0), Arrays.asList(null, null));
        assertEquals(recordA0, records.get(List.of("KeyA", 0)));
        assertEquals(recordB0, records.get(List.of("KeyB", 0)));
        assertEquals(recordNull, records.get(Arrays.asList(null, null)));
    }

    /**
     * Test a KeyedRecordAdapter that converts rows into instances of a custom object {@link MyRecord}, where
     * the key columns are not part of the object.
     */
    public void testCustomKeyedRecordAdapterWithTwoKeyColsNoKeysInObj() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
                        RecordAdapterDescriptorBuilder.create(MyRecord::new)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addCharColumnAdapter("CharCol", (myRecord, s) -> myRecord.myChar = s)
                                .addByteColumnAdapter("ByteCol", (myRecord, s) -> myRecord.myByte = s)
                                .addShortColumnAdapter("ShortCol", (myRecord, s) -> myRecord.myShort = s)
                                .addIntColumnAdapter("IntCol", (myRecord, s) -> myRecord.myInt = s)
                                .addFloatColumnAdapter("FloatCol", (myRecord, s) -> myRecord.myFloat = s)
                                .addLongColumnAdapter("LongCol", (myRecord, s) -> myRecord.myLong = s)
                                .addDoubleColumnAdapter("DoubleCol", (myRecord, s) -> myRecord.myDouble = s)
                                .build(),
                        "KeyCol1", "KeyCol2");

        MyRecord record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertNull(record.myKeyString);     // key columns not used in populating record, so we have Java initial vals
        assertEquals(0, record.myKeyInt);   // key columns not used in populating record, so we have Java initial vals
        assertEquals("Xx", record.myString);
        assertEquals('X', record.myChar);
        assertEquals((byte) 99, record.myByte);
        assertEquals((short) 99, record.myShort);
        assertEquals(900, record.myInt);
        assertEquals(0.9f, record.myFloat);
        assertEquals(90_000_000_000L, record.myLong);
        assertEquals(9.9d, record.myDouble);

        record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
        assertNull(record.myKeyString);     // key columns not used in populating record, so we have Java initial vals
        assertEquals(0, record.myKeyInt);   // key columns not used in populating record, so we have Java initial vals
        assertNull(record.myString);
        assertEquals(QueryConstants.NULL_CHAR, record.myChar);
        assertEquals(QueryConstants.NULL_BYTE, record.myByte);
        assertEquals(QueryConstants.NULL_SHORT, record.myShort);
        assertEquals(QueryConstants.NULL_INT, record.myInt);
        assertEquals(QueryConstants.NULL_FLOAT, record.myFloat);
        assertEquals(QueryConstants.NULL_LONG, record.myLong);
        assertEquals(QueryConstants.NULL_DOUBLE, record.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord(Arrays.asList("MissingKey", 0)));

        // test invalid key:
        try {
            keyedRecordAdapter.getRecord(Collections.singletonList("KeyA"));
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 1 components; expected 2", ex.getMessage());
        }
    }

    public void testGenericKeyedRecordAdapterUpdating() throws InterruptedException {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false);

        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, Map<String, Object>> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
                        Arrays.asList("StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol",
                                "DoubleCol"),
                        "KeyCol1", "KeyCol2");

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(4).copy().toTracking(),
                    TableTools.col("KeyCol1", "KeyB"),
                    TableTools.col("KeyCol2", 0),
                    TableTools.col("KeyCol3", baseInstant.plusSeconds(1)),
                    TableTools.col("StringCol", "bB"),
                    TableTools.charCol("CharCol", 'B'),
                    TableTools.byteCol("ByteCol", (byte) 2),
                    TableTools.shortCol("ShortCol", (short) 2),
                    TableTools.intCol("IntCol", 200),
                    TableTools.floatCol("FloatCol", 0.2f),
                    TableTools.longCol("LongCol", 20_000_000_000L),
                    TableTools.doubleCol("DoubleCol", 2.2d),
                    TableTools.instantCol("InstantCol", baseInstant.plusSeconds(200)));
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

        TableTools.show(source); // this is proof that KeyA/1 is gone from source
        assertEquals(3, recordsAfterLTM.size());
        assertEquals(recordB, recordsAfterLTM.get(Arrays.asList("KeyB", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyA", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyB", 1)));
    }

    public void testInvalidRecordAdapterCreation() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd")
        );

        {
            // Creating composite-key KeyedRecordAdapter with only one key column
            final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, "KeyCol1");
            try {
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                                source,
                                (TableBackedDataIndex) dataIndex,
                                JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source, Arrays.asList("KeyCol1", "StringCol")));
                fail("should have thrown an exception");
            } catch (IllegalArgumentException ex) {
                assertEquals("Attempting to create composite-key KeyedRecordAdapter but dataIndex has only one key column. Use makeRecordAdapterSimpleKey instead.", ex.getMessage());
            }
        }

        {
            // Creating simple-key KeyedRecordAdapter with multiple key columns
            final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, "KeyCol1", "StringCol");
            try {
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
                        (TableBackedDataIndex) dataIndex,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source, Arrays.asList("KeyCol1", "StringCol")),
                        String.class
                        );
                fail("should have thrown an exception");
            } catch (IllegalArgumentException ex) {
                assertEquals("Attempting to create simple-key KeyedRecordAdapter but dataIndex has multiple key columns. Use makeRecordAdapterCompositeKey instead.", ex.getMessage());
            }
        }

        {
            // Creating simple-key KeyedRecordAdapter with invalid key type
            final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, "KeyCol1");
            try {
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
                        (TableBackedDataIndex) dataIndex,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source, List.of("KeyCol1")),
                        Integer.class
                );
                fail("should have thrown an exception");
            } catch (IllegalArgumentException ex) {
                assertEquals("Key column type mismatch: expected type java.lang.Integer, found java.lang.String in dataIndex for column KeyCol1", ex.getMessage());
            }
        }

        {
            // Creating KeyedRecordAdapter with column in descriptor that doesn't exist in source table
            try {
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
                        JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source, Arrays.asList("KeyCol1", "MissingCol123ABC")),
                        "KeyCol1",
                        Integer.class
                );
                fail("should have thrown an exception");
            } catch (NoSuchColumnException ex) {
                assertTrue(ex.getMessage().contains("MissingCol123ABC"));
            }
        }

    }

    /**
     * Test with a key column of a {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils reinterpreted} type.
     */
    public void testCustomKeyedRecordAdapterWithOneObjKeyColAndReinterperting() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<Instant, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterSimpleKey(
                        source,
                        RecordAdapterDescriptorBuilder.create(MyRecord::new)
                                .addStringColumnAdapter("StringCol", (myRecord, s) -> myRecord.myString = s)
                                .addCharColumnAdapter("CharCol", (myRecord, s) -> myRecord.myChar = s)
                                .addByteColumnAdapter("ByteCol", (myRecord, s) -> myRecord.myByte = s)
                                .addShortColumnAdapter("ShortCol", (myRecord, s) -> myRecord.myShort = s)
                                .addIntColumnAdapter("IntCol", (myRecord, s) -> myRecord.myInt = s)
                                .addFloatColumnAdapter("FloatCol", (myRecord, s) -> myRecord.myFloat = s)
                                .addLongColumnAdapter("LongCol", (myRecord, s) -> myRecord.myLong = s)
                                .addDoubleColumnAdapter("DoubleCol", (myRecord, s) -> myRecord.myDouble = s)
                                .addObjColumnAdapter("KeyCol3", Instant.class,(myRecord, s) -> myRecord.myKeyInstant = s)
                                .addObjColumnAdapter("InstantCol", Instant.class, (myRecord, s) -> myRecord.myInstant = s)
                                .build(),
                        "KeyCol3", Instant.class);

        final MyRecord recordA = keyedRecordAdapter.getRecord(baseInstant);
        assertEquals(baseInstant, recordA.myKeyInstant);
        assertEquals("Xx", recordA.myString);
        assertEquals('X', recordA.myChar);
        assertEquals((byte) 99, recordA.myByte);
        assertEquals((short) 99, recordA.myShort);
        assertEquals(900, recordA.myInt);
        assertEquals(0.9f, recordA.myFloat);
        assertEquals(90_000_000_000L, recordA.myLong);
        assertEquals(9.9d, recordA.myDouble);
        assertEquals(baseInstant.plusSeconds(900), recordA.myInstant);

        final MyRecord recordB = keyedRecordAdapter.getRecord(baseInstant.plusSeconds(1));
        assertEquals(baseInstant.plusSeconds(1), recordB.myKeyInstant);
        assertEquals("Yy", recordB.myString);
        assertEquals('Y', recordB.myChar);
        assertEquals((byte) 100, recordB.myByte);
        assertEquals((short) 100, recordB.myShort);
        assertEquals(1000, recordB.myInt);
        assertEquals(1.0f, recordB.myFloat);
        assertEquals(100_000_000_000L, recordB.myLong);
        assertEquals(10.0d, recordB.myDouble);
        assertEquals(baseInstant.plusSeconds(1000), recordB.myInstant);

        // test null key
        final MyRecord recordNull = keyedRecordAdapter.getRecord(null);
        assertNull(recordNull.myKeyInstant);
        assertEquals(0, recordNull.myKeyInt);
        assertEquals("", recordNull.myString);
        assertEquals('0', recordNull.myChar);
        assertEquals((byte) -1, recordNull.myByte);
        assertEquals((short) -1, recordNull.myShort);
        assertEquals(-1, recordNull.myInt);
        assertEquals(-1.0f, recordNull.myFloat);
        assertEquals(-1L, recordNull.myLong);
        assertEquals(-1.0d, recordNull.myDouble);
        assertEquals(baseInstant.plusSeconds(-1), recordNull.myInstant);

        // test missing key:
        final Instant missingInstantKey = Instant.parse("2000-01-01T00:00:00Z");
        assertNull(keyedRecordAdapter.getRecord(missingInstantKey));

        // getRecordList tests (only return last match due to implicit lastBy)
        final List<MyRecord> recordsA = keyedRecordAdapter.getRecordList(baseInstant);
        assertEquals(1, recordsA.size());
        assertEquals("Xx", recordsA.get(0).myString);

        final List<MyRecord> recordsB = keyedRecordAdapter.getRecordList(baseInstant.plusSeconds(1));
        assertEquals(1, recordsB.size());
        assertEquals("Yy", recordsB.get(0).myString);

        final List<MyRecord> recordsNull = keyedRecordAdapter.getRecordList(null);
        assertEquals(1, recordsNull.size());
        assertEquals(recordNull, recordsNull.get(0));

        // test single-argument composite key:
        assertEquals(recordA, keyedRecordAdapter.getRecordCompositeKey(baseInstant));

        // test missing composite key:
        assertNull(keyedRecordAdapter.getRecordCompositeKey(missingInstantKey));

        // test invalid composite key:
        try {
            keyedRecordAdapter.getRecordCompositeKey(baseInstant, 0);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 2 components; expected 1", ex.getMessage());
        }

        // getRecordListCompositeKey tests
        final List<MyRecord> recordsA_composite = keyedRecordAdapter.getRecordListCompositeKey(baseInstant);
        assertEquals(recordsA, recordsA_composite);

        final List<MyRecord> recordsB_composite = keyedRecordAdapter.getRecordListCompositeKey(baseInstant.plusSeconds(1));
        assertEquals(recordsB, recordsB_composite);

        final List<MyRecord> recordsNull_composite = keyedRecordAdapter.getRecordListCompositeKey((Object) null);
        assertEquals(1, recordsNull_composite.size());
        assertEquals(recordNull, recordsNull_composite.get(0));

        assertNull(keyedRecordAdapter.getRecordListCompositeKey(missingInstantKey));

        // Test retrieving multiple records (for different keys)
        final Map<Instant, MyRecord> records = keyedRecordAdapter.getRecords(baseInstant, baseInstant.plusSeconds(1), missingInstantKey);
        assertEquals(recordA, records.get(baseInstant));
        assertEquals(recordB, records.get(baseInstant.plusSeconds(1)));
        assertFalse("records.containsKey(missingInstantKey)", records.containsKey(missingInstantKey));
    }

    /**
     * Test with multiple key columns, with at least one of a
     * {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils reinterpreted} type.
     */
    public void testCustomKeyedRecordAdapterWithTwoKeyColsReinterpreting() {
        final QueryTable source = getSimpleTestTable();
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter =
                KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                        source,
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
                                .addObjColumnAdapter("KeyCol3", Instant.class,(myRecord, s) -> myRecord.myKeyInstant = s)
                                .addObjColumnAdapter("InstantCol", Instant.class, (myRecord, s) -> myRecord.myInstant = s)
                                .build(),
                        "KeyCol1", "KeyCol3");

        // getRecord tests
        final MyRecord recordA0 = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", baseInstant));
        assertEquals("KeyA", recordA0.myKeyString);
        assertEquals(baseInstant, recordA0.myKeyInstant);
        assertEquals(0, recordA0.myKeyInt);
        assertEquals("Xx", recordA0.myString);
        assertEquals('X', recordA0.myChar);
        assertEquals((byte) 99, recordA0.myByte);
        assertEquals((short) 99, recordA0.myShort);
        assertEquals(900, recordA0.myInt);
        assertEquals(0.9f, recordA0.myFloat);
        assertEquals(90_000_000_000L, recordA0.myLong);
        assertEquals(9.9d, recordA0.myDouble);
        assertEquals(baseInstant.plusSeconds(900), recordA0.myInstant);

        final MyRecord recordB0 = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", baseInstant));
        assertEquals("KeyB", recordB0.myKeyString);
        assertEquals(baseInstant, recordB0.myKeyInstant);
        assertEquals(0, recordB0.myKeyInt);
        assertNull(recordB0.myString);
        assertEquals(QueryConstants.NULL_CHAR, recordB0.myChar);
        assertEquals(QueryConstants.NULL_BYTE, recordB0.myByte);
        assertEquals(QueryConstants.NULL_SHORT, recordB0.myShort);
        assertEquals(QueryConstants.NULL_INT, recordB0.myInt);
        assertEquals(QueryConstants.NULL_FLOAT, recordB0.myFloat);
        assertEquals(QueryConstants.NULL_LONG, recordB0.myLong);
        assertEquals(QueryConstants.NULL_DOUBLE, recordB0.myDouble);
        assertNull(recordB0.myInstant);

        // test null key
        final MyRecord recordNull = keyedRecordAdapter.getRecord(Arrays.asList(null, null));
        assertNull(recordNull.myKeyString);
        assertNull(recordNull.myKeyInstant);
        assertEquals(QueryConstants.NULL_INT, recordNull.myKeyInt);
        assertEquals("", recordNull.myString);
        assertEquals('0', recordNull.myChar);
        assertEquals((byte) -1, recordNull.myByte);
        assertEquals((short) -1, recordNull.myShort);
        assertEquals(-1, recordNull.myInt);
        assertEquals(-1.0f, recordNull.myFloat);
        assertEquals(-1L, recordNull.myLong);
        assertEquals(-1.0d, recordNull.myDouble);


        // test composite key:
        assertEquals(recordA0, keyedRecordAdapter.getRecordCompositeKey("KeyA", baseInstant));

        // test missing key:
        final Instant missingInstantKey = Instant.parse("2000-01-01T00:00:00Z");
        assertNull(keyedRecordAdapter.getRecord(Arrays.asList("MissingKey", baseInstant)));

        // test invalid key:
        try {
            keyedRecordAdapter.getRecord(Collections.singletonList("KeyA"));
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 1 components; expected 2", ex.getMessage());
        }

        // Test retrieving multiple records (for different keys)
        final Map<List<?>, MyRecord> records = keyedRecordAdapter.getRecords(Arrays.asList("KeyA", baseInstant), Arrays.asList("KeyB", baseInstant), Arrays.asList(null, null));
        assertEquals(recordA0, records.get(List.of("KeyA", baseInstant)));
        assertEquals(recordB0, records.get(List.of("KeyB", baseInstant)));
        assertEquals(recordNull, records.get(Arrays.asList(null, null)));
    }


}
