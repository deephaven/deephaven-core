package io.deephaven.queryutil.dataadapter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.queryutil.dataadapter.rec.RecordUpdaters;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptorBuilder;
import io.deephaven.queryutil.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.table.impl.TstUtils.i;


public class KeyedRecordAdapterTest extends io.deephaven.engine.table.impl.RefreshingTableTestCase {

    /**
     * Test a KeyedRecordAdapter that just converts rows into HashMaps
     */
    public void testGenericKeyedRecordAdapter() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
        );
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, Map<String, Object>> keyedRecordAdapter = KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                source,
                Arrays.asList("StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol", "DoubleCol"),
                "KeyCol1", "KeyCol2"
        );

        Map<String, Object> record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("Aa", record.get("StringCol"));
        assertEquals('A', record.get("CharCol"));
        assertEquals((byte) 0, record.get("ByteCol"));
        assertEquals((short) 1, record.get("ShortCol"));
        assertEquals(100, record.get("IntCol"));
        assertEquals(0.1f, record.get("FloatCol"));
        assertEquals(10_000_000_000L, record.get("LongCol"));
        assertEquals(1.1d, record.get("DoubleCol"));

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
     * Test a KeyedRecordAdapter that just converts rows into ObjectNodes
     */
    public void testJsonKeyedRecordAdapter() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
        );
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, ObjectNode> keyedRecordAdapter = KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                source,
                JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(source, Arrays.asList("KeyCol1", "KeyCol2", "StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol", "DoubleCol")),
                "KeyCol1", "KeyCol2"
        );

        ObjectNode record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
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

    /**
     * Test a KeyedRecordAdapter that converts rows into instances of a custom object {@link MyRecord}.
     */
    public void testCustomKeyedRecordAdapterExcludingKeyCols() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
        );
        TableTools.show(source);


        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter = KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                source,
                RecordAdapterDescriptorBuilder.create(MyRecord::new)
                        .addColumnAdapter("StringCol", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myString = s))
                        .addColumnAdapter("CharCol", RecordUpdaters.getCharUpdater((myRecord, s) -> myRecord.myChar = s))
                        .addColumnAdapter("ByteCol", RecordUpdaters.getByteUpdater((myRecord, s) -> myRecord.myByte = s))
                        .addColumnAdapter("ShortCol", RecordUpdaters.getShortUpdater((myRecord, s) -> myRecord.myShort = s))
                        .addColumnAdapter("IntCol", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myInt = s))
                        .addColumnAdapter("FloatCol", RecordUpdaters.getFloatUpdater((myRecord, s) -> myRecord.myFloat = s))
                        .addColumnAdapter("LongCol", RecordUpdaters.getLongUpdater((myRecord, s) -> myRecord.myLong = s))
                        .addColumnAdapter("DoubleCol", RecordUpdaters.getDoubleUpdater((myRecord, s) -> myRecord.myDouble = s))
                        .build(),
                "KeyCol1", "KeyCol2"
        );

        MyRecord record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
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

    public void testCustomKeyedRecordAdapterWithOneObjKeyCol() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB"),
                TableTools.col("StringCol", "Aa", null),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE)
        );
        TableTools.show(source);


        final KeyedRecordAdapter<String, MyRecord> keyedRecordAdapter = KeyedRecordAdapter.makeKeyedRecordAdapterSimpleKey(
                source,
                RecordAdapterDescriptorBuilder.create(MyRecord::new)
                        .addColumnAdapter("StringCol", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myString = s))
                        .addColumnAdapter("CharCol", RecordUpdaters.getCharUpdater((myRecord, s) -> myRecord.myChar = s))
                        .addColumnAdapter("ByteCol", RecordUpdaters.getByteUpdater((myRecord, s) -> myRecord.myByte = s))
                        .addColumnAdapter("ShortCol", RecordUpdaters.getShortUpdater((myRecord, s) -> myRecord.myShort = s))
                        .addColumnAdapter("IntCol", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myInt = s))
                        .addColumnAdapter("FloatCol", RecordUpdaters.getFloatUpdater((myRecord, s) -> myRecord.myFloat = s))
                        .addColumnAdapter("LongCol", RecordUpdaters.getLongUpdater((myRecord, s) -> myRecord.myLong = s))
                        .addColumnAdapter("DoubleCol", RecordUpdaters.getDoubleUpdater((myRecord, s) -> myRecord.myDouble = s))
                        .addColumnAdapter("KeyCol1", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myKeyString = s))
                        .build(),
                "KeyCol1", String.class
        );

        MyRecord record = keyedRecordAdapter.getRecord("KeyA");
        assertEquals("KeyA", record.myKeyString);
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        record = keyedRecordAdapter.getRecord("KeyB");
        assertEquals("KeyB", record.myKeyString);
        assertNull(record.myString);
        assertEquals(QueryConstants.NULL_CHAR, record.myChar);
        assertEquals(QueryConstants.NULL_BYTE, record.myByte);
        assertEquals(QueryConstants.NULL_SHORT, record.myShort);
        assertEquals(QueryConstants.NULL_INT, record.myInt);
        assertEquals(QueryConstants.NULL_FLOAT, record.myFloat);
        assertEquals(QueryConstants.NULL_LONG, record.myLong);
        assertEquals(QueryConstants.NULL_DOUBLE, record.myDouble);

        // test single-argument composite key:
        record = keyedRecordAdapter.getRecordCompositeKey("KeyA");
        assertEquals("KeyA", record.myKeyString);
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord("MissingKey"));

        // test invalid key:
        try {
            keyedRecordAdapter.getRecordCompositeKey("KeyA", 0);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 2 components; expected 1", ex.getMessage());
        }
    }

    public void testCustomKeyedRecordAdapterWithOnePrimitiveKeyCol() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4).copy().toTracking(),
                TableTools.intCol("KeyCol1", 0, 1),
                TableTools.col("StringCol", "Aa", null),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE)
        );
        TableTools.show(source);


        final KeyedRecordAdapter<Integer, MyRecord> keyedRecordAdapter = KeyedRecordAdapter.makeKeyedRecordAdapterSimpleKey(
                source,
                RecordAdapterDescriptorBuilder.create(MyRecord::new)
                        .addColumnAdapter("StringCol", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myString = s))
                        .addColumnAdapter("CharCol", RecordUpdaters.getCharUpdater((myRecord, s) -> myRecord.myChar = s))
                        .addColumnAdapter("ByteCol", RecordUpdaters.getByteUpdater((myRecord, s) -> myRecord.myByte = s))
                        .addColumnAdapter("ShortCol", RecordUpdaters.getShortUpdater((myRecord, s) -> myRecord.myShort = s))
                        .addColumnAdapter("IntCol", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myInt = s))
                        .addColumnAdapter("FloatCol", RecordUpdaters.getFloatUpdater((myRecord, s) -> myRecord.myFloat = s))
                        .addColumnAdapter("LongCol", RecordUpdaters.getLongUpdater((myRecord, s) -> myRecord.myLong = s))
                        .addColumnAdapter("DoubleCol", RecordUpdaters.getDoubleUpdater((myRecord, s) -> myRecord.myDouble = s))
                        .addColumnAdapter("KeyCol1", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myKeyInt = s))
                        .build(),
                "KeyCol1", Integer.class
        );

        MyRecord record = keyedRecordAdapter.getRecord(0);
        assertEquals(0, record.myKeyInt);
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        record = keyedRecordAdapter.getRecord(1);
        assertEquals(1, record.myKeyInt);
        assertNull(record.myString);
        assertEquals(QueryConstants.NULL_CHAR, record.myChar);
        assertEquals(QueryConstants.NULL_BYTE, record.myByte);
        assertEquals(QueryConstants.NULL_SHORT, record.myShort);
        assertEquals(QueryConstants.NULL_INT, record.myInt);
        assertEquals(QueryConstants.NULL_FLOAT, record.myFloat);
        assertEquals(QueryConstants.NULL_LONG, record.myLong);
        assertEquals(QueryConstants.NULL_DOUBLE, record.myDouble);

        // test single-argument composite key:
        record = keyedRecordAdapter.getRecordCompositeKey(0);
        assertEquals(0, record.myKeyInt);
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        // test missing key:
        assertNull(keyedRecordAdapter.getRecord(-1));

        // test invalid composite key:
        try {
            keyedRecordAdapter.getRecordCompositeKey("KeyA", 0);
            fail("should have thrown an exception");
        } catch (IllegalArgumentException ex) {
            assertEquals("dataKey has 2 components; expected 1", ex.getMessage());
        }
    }

    public void testCustomKeyedRecordAdapterWithTwoKeyCols() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
        );
        TableTools.show(source);


        final KeyedRecordAdapter<List<?>, MyRecord> keyedRecordAdapter = KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                source,
                RecordAdapterDescriptorBuilder.create(MyRecord::new)
                        .addColumnAdapter("StringCol", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myString = s))
                        .addColumnAdapter("CharCol", RecordUpdaters.getCharUpdater((myRecord, s) -> myRecord.myChar = s))
                        .addColumnAdapter("ByteCol", RecordUpdaters.getByteUpdater((myRecord, s) -> myRecord.myByte = s))
                        .addColumnAdapter("ShortCol", RecordUpdaters.getShortUpdater((myRecord, s) -> myRecord.myShort = s))
                        .addColumnAdapter("IntCol", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myInt = s))
                        .addColumnAdapter("FloatCol", RecordUpdaters.getFloatUpdater((myRecord, s) -> myRecord.myFloat = s))
                        .addColumnAdapter("LongCol", RecordUpdaters.getLongUpdater((myRecord, s) -> myRecord.myLong = s))
                        .addColumnAdapter("DoubleCol", RecordUpdaters.getDoubleUpdater((myRecord, s) -> myRecord.myDouble = s))
                        .addColumnAdapter("KeyCol1", RecordUpdaters.getStringUpdater((myRecord, s) -> myRecord.myKeyString = s))
                        .addColumnAdapter("KeyCol2", RecordUpdaters.getIntUpdater((myRecord, s) -> myRecord.myKeyInt = s))
                        .build(),
                "KeyCol1", "KeyCol2"
        );

        MyRecord record = keyedRecordAdapter.getRecord(Arrays.asList("KeyA", 0));
        assertEquals("KeyA", record.myKeyString);
        assertEquals(0, record.myKeyInt);
        assertEquals("Aa", record.myString);
        assertEquals('A', record.myChar);
        assertEquals((byte) 0, record.myByte);
        assertEquals((short) 1, record.myShort);
        assertEquals(100, record.myInt);
        assertEquals(0.1f, record.myFloat);
        assertEquals(10_000_000_000L, record.myLong);
        assertEquals(1.1d, record.myDouble);

        record = keyedRecordAdapter.getRecord(Arrays.asList("KeyB", 0));
        assertEquals("KeyB", record.myKeyString);
        assertEquals(0, record.myKeyInt);
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
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
                TableTools.col("KeyCol2", 0, 0, 1, 1),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
        );
        TableTools.show(source);

        final KeyedRecordAdapter<List<?>, Map<String, Object>> keyedRecordAdapter = KeyedRecordAdapter.makeRecordAdapterCompositeKey(
                source,
                Arrays.asList("StringCol", "CharCol", "ByteCol", "ShortCol", "IntCol", "FloatCol", "LongCol", "DoubleCol"),
                "KeyCol1", "KeyCol2"
        );

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
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
                    TableTools.doubleCol("DoubleCol", 2.2d)
            );
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
                Arrays.asList("KeyA", 1)
        );

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
            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            TstUtils.removeRows(source, i(6));
            source.notifyListeners(i(), i(6), i());
            TableTools.show(source);
            l1.countDown();
            try {
                l2.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            l3.countDown();
        }).start();

        // Let the LTM cycle start
        l1.await();

        // LTM is being updated (On another thread) -- we should use prev, and still see KeyA/1
        Map<List<?>, Map<String, Object>> recordsDuringLTMpreRemove = keyedRecordAdapter.getRecords(
                Arrays.asList("KeyA", 0),
                Arrays.asList("KeyB", 0),
                Arrays.asList("KeyA", 1),
                Arrays.asList("KeyB", 1)
        );

        assertEquals(4, recordsDuringLTMpreRemove.size());
        assertNotNull(recordsDuringLTMpreRemove.get(Arrays.asList("KeyA", 0)));
        assertEquals(recordB, recordsDuringLTMpreRemove.get(Arrays.asList("KeyB", 0)));
        assertEquals(recordC, recordsDuringLTMpreRemove.get(Arrays.asList("KeyA", 1)));
        assertNotNull(recordsDuringLTMpreRemove.get(Arrays.asList("KeyB", 1)));

        // Let the LTM cycle finish
        l2.countDown();
        l3.await();

        // LTM cycle complete -- KeyA/1 should be gone
        Map<List<?>, Map<String, Object>> recordsAfterLTM = keyedRecordAdapter.getRecords(
                Arrays.asList("KeyA", 0),
                Arrays.asList("KeyB", 0),
                Arrays.asList("KeyA", 1),
                Arrays.asList("KeyB", 1)
        );

        assertEquals(3, recordsAfterLTM.size());
        assertEquals(recordB, recordsAfterLTM.get(Arrays.asList("KeyB", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyA", 0)));
        assertNotNull(recordsAfterLTM.get(Arrays.asList("KeyB", 1)));

    }

    static class MyRecord {
        String myKeyString;
        int myKeyInt;

        String myString;
        char myChar;
        byte myByte;
        short myShort;
        int myInt;
        float myFloat;
        long myLong;
        double myDouble;
    }

}