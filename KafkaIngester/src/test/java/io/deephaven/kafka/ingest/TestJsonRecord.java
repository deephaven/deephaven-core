package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static junit.framework.TestCase.*;
import static junit.framework.TestCase.assertEquals;

public class TestJsonRecord {
    private static JsonRecord record;

    private static JsonRecord getRecord() {
        String json ="{ \"Col1\":5,\"Col2\":3.141592,\"Col3\":\"Hello World\"," +
                "\"Col4\":\"2016-12-31T18:45:01.123 NY\",\"LongValue\":8000000000," +
                "\"FirstName\":\"John\",\"LastName\":\"Doe\",\"empty\":null,\"zerolength\":\"\", \"ISODate\":\"1986-06-02T14:05:45-04:00\"," +
                "\"BigIntValue\":12345678901234567890,\"BigDecimalValue\":1234567890.1234567890," +
                "\"NanoDate\":1577898000000000000, \"EpochSeconds\": 1600359104, \"BoolValue\":false }";

        return new JsonRecord(json);
    }

    @BeforeClass
    public static void setUp() {
        record = getRecord();
    }

    @AfterClass
    public static void afterClass() {
        record = null;
    }

    @Test
    public void testMissingKeysAllowed() {
        final JsonRecord testRecord = new JsonRecord("{ \"col1\":1 }",true,true);
        assertEquals(QueryConstants.NULL_INT, record.getValue("col2",int.class));
    }

    @Test
    public void testMissingKeysDenied() {
        final JsonRecord testRecord = new JsonRecord("{ \"col1\":1 }",false, true);
        Exception ex = null;
        try {
            assertNull(testRecord.getValue("col2", int.class));
        } catch (IllegalArgumentException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertEquals("Key col2 not found in the record, and allowMissingKeys is false.", ex.getMessage());
    }

    @Test
    public void testNullValuesDenied() {
        final JsonRecord testRecord = new JsonRecord("{ \"empty\":null }",true, false);
        Exception ex = null;
        try {
            assertNull(testRecord.getValue("empty", int.class));
        } catch (IllegalArgumentException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertEquals("Value for Key empty is null in the record, and allowNullValues is false.", ex.getMessage());
    }

    @Test
    public void testBigInt() {
        assertEquals(new BigInteger("12345678901234567890"), record.getValue("BigIntValue", BigInteger.class));
    }

    @Test
    public void testBigDecimal() {
        assertEquals(new BigDecimal("1234567890.1234567890"), record.getValue("BigDecimalValue", BigDecimal.class));
    }

    @Test
    public void testCharSequence() {
        assertEquals("John", record.getValue("FirstName", CharSequence.class));
    }

    @Test
    public void testIntRead() {
        assertEquals(5, record.getValue("Col1", int.class));
    }

    @Test
    public void testShortRead() {
        assertEquals((short)5, record.getValue("Col1", short.class));
    }

    @Test
    public void testShortNullRead() {
        assertNull(record.getValue("empty", Short.class));
    }

    @Test
    public void testLongRead() {
        assertEquals(8000000000L, record.getValue("LongValue", long.class));
    }

    @Test
    public void testDoubleRead() {
        assertEquals(3.141592, record.getValue("Col2", double.class));
        assertEquals(3.141592, record.getValue("Col2", Double.class));
        assertEquals(QueryConstants.NULL_DOUBLE, record.getValue("empty", double.class));
        assertNull(record.getValue("empty", Double.class));
    }

    @Test
    public void testFloatRead() {
        assertEquals(3.141592f, record.getValue("Col2", float.class));
        assertEquals(3.141592f, record.getValue("Col2", Float.class));
        assertEquals(QueryConstants.NULL_FLOAT, record.getValue("empty", float.class));
        assertNull(record.getValue("empty", Float.class));
    }

    @Test
    public void testStringRead() {
        assertEquals("Hello World", record.getValue("Col3", String.class));
    }

    @Test
    public void testCharRead() {
        assertEquals('H', record.getValue("Col3", char.class));
        assertEquals('H', record.getValue("Col3", Character.class));
        assertEquals(QueryConstants.NULL_CHAR, record.getValue("empty", char.class));
        assertNull(record.getValue("empty", Character.class));
        assertEquals(QueryConstants.NULL_CHAR, record.getValue("zerolength", char.class));
        assertNull(record.getValue("zerolength", Character.class));
    }

    @Test
    public void testByteRead() {
        assertEquals((byte)(72), record.getValue("Col3", byte.class));
        assertEquals(QueryConstants.NULL_BYTE, record.getValue("empty", byte.class));
        assertEquals(QueryConstants.NULL_BYTE, record.getValue("zerolength", byte.class));
        assertEquals((byte)(72), record.getValue("Col3", Byte.class));
        assertNull(record.getValue("empty", Byte.class));
        assertNull(record.getValue("zerolength", Byte.class));
    }

    @Test
    public void testLongMillisDateRead() {
        assertEquals(DBTimeUtils.convertDateTime("2223-07-06T10:13:20.000000000 NY"), record.getValue("LongValue",DBDateTime.class));
        assertNull(record.getValue("empty", DBDateTime.class));
    }

    @Test
    public void testLongNanosDateRead() {
        assertEquals("2020-01-01T12:00:00.000000000 NY", record.getValue("NanoDate",DBDateTime.class).toString());
        assertEquals(new DBDateTime(1600359104L * 1000_000_000L), record.getValue("EpochSeconds",DBDateTime.class));
    }

    @Test
    public void testBooleanNull() {
        assertNull(record.getValue("empty",Boolean.class));
    }

    @Test
    public void testLongNull() {
        assertNull(record.getValue("empty",Long.class));
    }

    @Test
    public void testIntNull() {
        assertNull(record.getValue("empty",Integer.class));
    }

    @Test
    public void testBigIntNull() {
        assertNull(record.getValue("empty",BigInteger.class));
    }

    @Test
    public void testBoolean() {
        assertFalse((Boolean)record.getValue("BoolValue",Boolean.class));
    }

    @Test
    public void testDateTimeParse() {
        assertEquals("2016-12-31T18:45:01.123000000 NY",record.getValue("Col4",DBDateTime.class).toString());
    }

    @Test
    public void testDateTimeParseFail() {
        Exception ex = null;
        try {
            assertEquals("1986-06-02T14:05:45.000000000 NY",record.getValue("ISODate",DBDateTime.class).toString());
        } catch (RuntimeException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertEquals("Cannot parse datetime : 1986-06-02T14:05:45-04:00", ex.getMessage());
    }

    @Test
    public void testSetterGetter() {
        final JsonRecordSetter setter = JsonRecordUtil.getSetter(int.class);
        assertEquals(JsonRecordUtil.JsonIntSetter.class, setter.getClass());
    }

    @Test
    public void testNullObject() {
        assertNull(JsonRecordUtil.getValue(record,"empty"));
    }

    @Test
    public void testNesting() {
        final String json = "{ \"a\": 1, \"b\": 2, \"c\": [4, null, 6], \"d\": {\"e\": 7, \"f\": 8}, \"g\": [{\"h\": 9, \"i\": 10}, {\"h\": 11, \"i\": 12}] }";
        final JsonRecord parent = new JsonRecord(json);
        System.out.println(parent.getRecord());
        assertEquals(1, JsonRecordUtil.getInt(parent, "a"));
        assertEquals(2, JsonRecordUtil.getInt(parent, "b"));
        final Object objectC = JsonRecordUtil.getValue(parent, "c");
        assertNotNull(objectC);
        assertEquals(ArrayNode.class, objectC.getClass());
        if (objectC instanceof ArrayNode) {
            final ArrayNode arrayC = (ArrayNode)objectC;
            final List<Integer> resultList = new ArrayList<>();
            final List<Integer> resultListBoxed = new ArrayList<>();
            for (int ii = 0; ii < arrayC.size(); ++ii) {
                final JsonNode x = arrayC.get(ii);
                resultList.add(JsonRecordUtil.getInt(x));
                resultListBoxed.add(JsonRecordUtil.getBoxedInt(x));
            }
            final List<Integer> expected = Arrays.asList(4, QueryConstants.NULL_INT, 6);
            assertEquals(expected, resultList);
            final List<Integer> expectedBoxed = Arrays.asList(4, null, 6);
            assertEquals(expectedBoxed, resultListBoxed);
        }
        assertEquals(1, JsonRecordUtil.getInt(parent, "a"));
        final Object objectD = JsonRecordUtil.getValue(parent, "d");
        assertNotNull(objectD);
        final JsonRecord recordD = new JsonRecord((JsonNode)objectD);
        assertEquals(7, JsonRecordUtil.getInt(recordD, "e"));
        assertEquals(8, JsonRecordUtil.getInt(recordD, "f"));

        final Object objectG = JsonRecordUtil.getValue(parent, "g");
        assertNotNull(objectG);
        assertEquals(ArrayNode.class, objectG.getClass());
        final Iterator<JsonNode> elements = ((ArrayNode) objectG).elements();
        int count = 0;
        while (elements.hasNext()) {
            final JsonNode node = elements.next();
            final JsonRecord child = new JsonRecord(node);
            switch(count++) {
                case 0:
                    assertEquals(9, JsonRecordUtil.getInt(child, "h"));
                    assertEquals(10, JsonRecordUtil.getInt(child, "i"));
                    break;
                case 1:
                    assertEquals(11, JsonRecordUtil.getInt(child, "h"));
                    assertEquals(12, JsonRecordUtil.getInt(child, "i"));
                    break;
                default:
                    TestCase.fail("Expected two nodes.");
            }
        }
        assertEquals(2, count);
    }
}