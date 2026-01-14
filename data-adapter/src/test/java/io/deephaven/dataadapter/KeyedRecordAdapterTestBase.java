//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.time.Instant;
import java.util.Objects;

import static io.deephaven.engine.testutil.TstUtils.i;


public abstract class KeyedRecordAdapterTestBase extends RefreshingTableTestCase {

    protected final Instant baseInstant = Instant.parse("2025-01-01T12:00:00Z");

    protected KeyedRecordAdapterTestBase() {
        // Enable trace logging for all tests
        System.setProperty("KeyedRecordAdapter.trace", "true");
    }

    /**
     * Get a basic test table to use for testig (which can be modified, indexed, or partitioned as needed)
     * 
     * @return
     */
    public QueryTable getSimpleTestTable() {
        final QueryTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8, 9, 10, 12).copy().toTracking(),
                TableTools.col("KeyCol1", "KeyA", "KeyB", "KeyA", "KeyB", "KeyA", "KeyB", null),
                TableTools.col("KeyCol2", 0, 0, 1, 1, 0, 1, QueryConstants.NULL_INT),
                TableTools.instantCol("KeyCol3", baseInstant, baseInstant, baseInstant.plusSeconds(1),
                        baseInstant.plusSeconds(1), baseInstant, baseInstant.plusSeconds(1), null),
                TableTools.col("StringCol", "Aa", null, "Cc", "Dd", "Xx", "Yy", ""),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'C', 'D', 'X', 'Y', '0'),
                TableTools.byteCol("ByteCol", (byte) 0, QueryConstants.NULL_BYTE, (byte) 3, (byte) 4, (byte) 99,
                        (byte) 100, (byte) -1),
                TableTools.shortCol("ShortCol", (short) 1, QueryConstants.NULL_SHORT, (short) 3, (short) 4, (short) 99,
                        (short) 100, (short) -1),
                TableTools.intCol("IntCol", 100, QueryConstants.NULL_INT, 300, 400, 900, 1000, -1),
                TableTools.floatCol("FloatCol", 0.1f, QueryConstants.NULL_FLOAT, 0.3f, 0.4f, 0.9f, 1.0f, -1.0f),
                TableTools.longCol("LongCol", 10_000_000_000L, QueryConstants.NULL_LONG, 30_000_000_000L,
                        40_000_000_000L, 90_000_000_000L, 100_000_000_000L, -1L),
                TableTools.doubleCol("DoubleCol", 1.1d, QueryConstants.NULL_DOUBLE, 3.3d, 4.4d, 9.9d, 10.0d, -1.0d),
                TableTools.instantCol("InstantCol", baseInstant.plusSeconds(100), null, baseInstant.plusSeconds(300),
                        baseInstant.plusSeconds(400), baseInstant.plusSeconds(900), baseInstant.plusSeconds(1000),
                        baseInstant.plusSeconds(-1)));
        TableTools.show(source);

        return source;
    }

    static class MyRecord {
        int myKeyInt;
        String myKeyString;
        Instant myKeyInstant;

        String myString;
        char myChar;
        byte myByte;
        short myShort;
        int myInt;
        float myFloat;
        long myLong;
        double myDouble;
        Instant myInstant;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;

            MyRecord myRecord = (MyRecord) o;
            return myKeyInt == myRecord.myKeyInt && myChar == myRecord.myChar && myByte == myRecord.myByte
                    && myShort == myRecord.myShort && myInt == myRecord.myInt
                    && Float.compare(myFloat, myRecord.myFloat) == 0 && myLong == myRecord.myLong
                    && Double.compare(myDouble, myRecord.myDouble) == 0
                    && Objects.equals(myKeyString, myRecord.myKeyString)
                    && Objects.equals(myKeyInstant, myRecord.myKeyInstant)
                    && Objects.equals(myString, myRecord.myString) && Objects.equals(myInstant, myRecord.myInstant);
        }

        @Override
        public int hashCode() {
            int result = myKeyInt;
            result = 31 * result + Objects.hashCode(myKeyString);
            result = 31 * result + Objects.hashCode(myKeyInstant);
            result = 31 * result + Objects.hashCode(myString);
            result = 31 * result + myChar;
            result = 31 * result + myByte;
            result = 31 * result + myShort;
            result = 31 * result + myInt;
            result = 31 * result + Float.hashCode(myFloat);
            result = 31 * result + Long.hashCode(myLong);
            result = 31 * result + Double.hashCode(myDouble);
            result = 31 * result + Objects.hashCode(myInstant);
            return result;
        }
    }
}
