//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.util;

import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;

import java.time.Instant;

public class TestDataUtil {

    /**
     * Produces a QST {@code NewTable} of test data, with columns of the main supported types and their array
     * equivalents. Use {@link InMemoryTable#from} to create a regular Deephaven {@link QueryTable} from this.
     * 
     * @return A new table with test data that covers many datatypes.
     */
    public static NewTable getTestDataNewTable() {
        return ColumnHeader.of(
                ColumnHeader.ofBoolean("booleanCol"),
                ColumnHeader.ofByte("byteCol"),
                ColumnHeader.ofChar("charCol"),
                ColumnHeader.ofShort("shortCol"),
                ColumnHeader.ofInt("intCol"),
                ColumnHeader.ofLong("longCol"),
                ColumnHeader.ofFloat("floatCol"),
                ColumnHeader.ofDouble("doubleCol"),
                ColumnHeader.ofString("stringCol"),
                ColumnHeader.ofInstant("instantCol"),
                ColumnHeader.of("localTimeCol", java.time.LocalTime.class),
                ColumnHeader.of("localDateCol", java.time.LocalDate.class),
                ColumnHeader.of("durationCol", java.time.Duration.class),
                ColumnHeader.of("byteArrayCol", byte[].class),
                ColumnHeader.of("charArrayCol", char[].class),
                ColumnHeader.of("shortArrayCol", short[].class),
                ColumnHeader.of("intArrayCol", int[].class),
                ColumnHeader.of("longArrayCol", long[].class),
                ColumnHeader.of("floatArrayCol", float[].class),
                ColumnHeader.of("doubleArrayCol", double[].class),
                ColumnHeader.of("stringArrayCol", String[].class),
                ColumnHeader.of("instantArrayCol", java.time.Instant[].class),
                ColumnHeader.of("localTimeArrayCol", java.time.LocalTime[].class),
                ColumnHeader.of("localDateArrayCol", java.time.LocalDate[].class),
                ColumnHeader.of("durationArrayCol", java.time.Duration[].class))
                .start(3)
                .row(true, (byte) 1, 'a', (short) 10, 100, 1000L, 1.1f, 1.1d, "hello",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        java.time.LocalTime.of(10, 30, 45),
                        java.time.LocalDate.of(2025, 1, 1),
                        java.time.Duration.ofHours(1),
                        new byte[] {1, 2},
                        new char[] {'a', 'b'},
                        new short[] {10, 20},
                        new int[] {100, 200},
                        new long[] {1000L, 3000000000L},
                        new float[] {1.1f, 2.2f},
                        new double[] {1.1, 2.2},
                        new String[] {"a", "b"},
                        new Instant[] {Instant.parse("2025-01-01T10:00:00Z")},
                        new java.time.LocalTime[] {java.time.LocalTime.of(9, 0)},
                        new java.time.LocalDate[] {java.time.LocalDate.of(2025, 1, 15)},
                        new java.time.Duration[] {java.time.Duration.ofHours(2)})
                .row(false, (byte) 2, 'b', (short) 20, 200, 3000000000L, 2.2f, 2.2d, null,
                        (Instant) null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null)
                .row(true, (byte) 3, 'c', (short) 30, 300, 3000L, 3.3f, 3.3d, "world",
                        Instant.parse("2025-01-03T12:00:00Z"),
                        java.time.LocalTime.of(22, 45, 0),
                        java.time.LocalDate.of(2025, 12, 31),
                        java.time.Duration.ofMinutes(30),
                        new byte[] {3},
                        new char[] {'c'},
                        new short[] {30},
                        new int[] {300},
                        new long[] {3000L},
                        new float[] {3.3f},
                        new double[] {3.3},
                        new String[] {"c"},
                        new Instant[] {Instant.parse("2025-01-03T12:00:00Z")},
                        new java.time.LocalTime[] {java.time.LocalTime.of(18, 0)},
                        new java.time.LocalDate[] {java.time.LocalDate.of(2025, 12, 25)},
                        new java.time.Duration[] {java.time.Duration.ofSeconds(45)})
                .newTable();
    }

    private TestDataUtil() {}

}
