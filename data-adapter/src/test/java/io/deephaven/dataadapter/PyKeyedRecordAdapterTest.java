//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import gnu.trove.map.TLongIntMap;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.util.Collections;

import static io.deephaven.engine.testutil.TstUtils.i;


public class PyKeyedRecordAdapterTest extends RefreshingTableTestCase {

    /**
     * Test a KeyedRecordAdapter that converts rows into HashMaps
     */
    public void testPyKeyedRecordAdapter() {
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
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE));
        TableTools.show(source);

        final PyKeyedRecordAdapter<String> keyedRecordAdapter = new PyKeyedRecordAdapter<>(
                source,
                new String[] {"KeyCol1"},
                new String[] {
                        "StringCol",
                        "CharCol",
                        "ByteCol",
                        "ShortCol",
                        "IntCol",
                        "FloatCol",
                        "LongCol",
                        "DoubleCol"
                });

        {
            PyKeyedRecordAdapter.RecordRetrievalResult record =
                    keyedRecordAdapter.getRecordsForPython(new Object[] {"KeyA"});

            final long[] recordDataRowKeys = record.recordDataRowKeys;
            final TLongIntMap rowKeyToDataKeyPositionalIndex = record.rowKeyToDataKeyPositionalIndex;
            final Object[] recordDataArrs = record.recordDataArrs;

            assertEquals(0, recordDataRowKeys[0]);
            assertEquals(0, rowKeyToDataKeyPositionalIndex.get(0));

            assertEquals("Aa", ((String[]) recordDataArrs[0])[0]);
            assertEquals('A', ((char[]) recordDataArrs[1])[0]);
            assertEquals((byte) 0, ((byte[]) recordDataArrs[2])[0]);
            assertEquals((short) 1, ((short[]) recordDataArrs[3])[0]);
            assertEquals(100, ((int[]) recordDataArrs[4])[0]);
            assertEquals(0.1f, ((float[]) recordDataArrs[5])[0]);
            assertEquals(10_000_000_000L, ((long[]) recordDataArrs[6])[0]);
            assertEquals(1.1d, ((double[]) recordDataArrs[7])[0]);
        }


        {
            PyKeyedRecordAdapter.RecordRetrievalResult record =
                    keyedRecordAdapter.getRecordsForPython(new Object[] {"KeyB"});

            final long[] recordDataRowKeys = record.recordDataRowKeys;
            final TLongIntMap rowKeyToDataKeyPositionalIndex = record.rowKeyToDataKeyPositionalIndex;
            final Object[] recordDataArrs = record.recordDataArrs;

            assertEquals(1, recordDataRowKeys[0]);
            assertEquals(0, rowKeyToDataKeyPositionalIndex.get(1));

            assertNull(((String[]) recordDataArrs[0])[0]);
            assertEquals(QueryConstants.NULL_CHAR, ((char[]) recordDataArrs[1])[0]);
            assertEquals(QueryConstants.NULL_BYTE, ((byte[]) recordDataArrs[2])[0]);
            assertEquals(QueryConstants.NULL_SHORT, ((short[]) recordDataArrs[3])[0]);
            assertEquals(QueryConstants.NULL_INT, ((int[]) recordDataArrs[4])[0]);
            assertEquals(QueryConstants.NULL_FLOAT, ((float[]) recordDataArrs[5])[0]);
            assertEquals(QueryConstants.NULL_LONG, ((long[]) recordDataArrs[6])[0]);
            assertEquals(QueryConstants.NULL_DOUBLE, ((double[]) recordDataArrs[7])[0]);
        }

        // test missing key
        assertEquals(0, keyedRecordAdapter.getRecords(Collections.singletonList("MissingKey")).size());
    }

    // public void testPyKeyedRecordAdapterMultiKey() {
    // final QueryTable source = TstUtils.testRefreshingTable(
    // i(2, 4, 6, 8).copy().toTracking(),
    // TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB"),
    // TableTools.col("KeyCol2", 0, 1, 0, 1),
    // TableTools.col("StringCol", "Aa", null, "Cc", "Dd"),
    // TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D'),
    // TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4),
    // TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4),
    // TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400),
    // TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f),
    // TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L, 40_000_000_000L),
    // TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d)
    // );
    // TableTools.show(source);
    //
    // final PyKeyedRecordAdapter<String> keyedRecordAdapter = new PyKeyedRecordAdapter<>(
    // source,
    // new String[]{"KeyCol1", "KeyCol2"},
    // new String[]{
    // "StringCol",
    // "CharCol",
    // "ByteCol",
    // "ShortCol",
    // "IntCol",
    // "FloatCol",
    // "LongCol",
    // "DoubleCol"
    // }
    // );
    //
    // PyKeyedRecordAdapter.RecordRetrievalResult record = keyedRecordAdapter.getRecordsForPython();
    //
    // final long[] recordDataRowKeys = record.recordDataRowKeys;
    // final TLongIntMap rowKeyToDataKeyPositionalIndex = record.rowKeyToDataKeyPositionalIndex;
    // final Object[] recordDataArrs = record.recordDataArrs;
    //
    // assertEquals(2, recordDataRowKeys[0]);
    // assertEquals(rowKeyToDataKeyPositionalIndex.get(2), 0);
    //
    // assertEquals("Aa", ((String[]) recordDataArrs[0])[0]);
    // assertEquals('A', ((char[]) recordDataArrs[1])[0]);
    // assertEquals((byte) 0, ((byte[]) recordDataArrs[2])[0]);
    // assertEquals((short) 1, ((short[]) recordDataArrs[3])[0]);
    // assertEquals(100, ((int[]) recordDataArrs[4])[0]);
    // assertEquals(0.1f, ((float[]) recordDataArrs[5])[0]);
    // assertEquals(10_000_000_000L, ((long[]) recordDataArrs[6])[0]);
    // assertEquals(1.1d, ((double[]) recordDataArrs[7])[0]);
    //
    //// record = keyedRecordAdapter.getRecord("KeyB");
    //// assertNull(record.get("StringCol"));
    //// assertNull(record.get("CharCol"));
    //// assertNull(record.get("ByteCol"));
    //// assertNull(record.get("ShortCol"));
    //// assertNull(record.get("IntCol"));
    //// assertNull(record.get("FloatCol"));
    //// assertNull(record.get("LongCol"));
    //// assertNull(record.get("DoubleCol"));
    //
    // // test missing key
    // assertNull(keyedRecordAdapter.getRecords(Collections.singletonList("MissingKey")));
    // }

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
