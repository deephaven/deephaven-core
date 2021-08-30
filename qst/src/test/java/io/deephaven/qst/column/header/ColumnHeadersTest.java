package io.deephaven.qst.column.header;

import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnHeadersTest {

    private static final ColumnHeader<Boolean> HEADER_BOOLEAN = ColumnHeader.ofBoolean("Boolean");
    private static final ColumnHeader<Byte> HEADER_BYTE = ColumnHeader.ofByte("Byte");
    private static final ColumnHeader<Character> HEADER_CHAR = ColumnHeader.ofChar("Char");
    private static final ColumnHeader<Short> HEADER_SHORT = ColumnHeader.ofShort("Short");
    private static final ColumnHeader<Integer> HEADER_INT = ColumnHeader.ofInt("Int");
    private static final ColumnHeader<Long> HEADER_LONG = ColumnHeader.ofLong("Long");
    private static final ColumnHeader<Float> HEADER_FLOAT = ColumnHeader.ofFloat("Float");
    private static final ColumnHeader<Double> HEADER_DOUBLE = ColumnHeader.ofDouble("Double");
    private static final ColumnHeader<String> HEADER_STRING = ColumnHeader.ofString("String");
    private static final ColumnHeader<Instant> HEADER_INSTANT = ColumnHeader.ofInstant("Instant");
    private static final ColumnHeader<Custom> HEADER_CUSTOM =
            ColumnHeader.of("Custom", CustomType.of(Custom.class));

    @Test
    void h11Header() {
        assertThat(h11().tableHeader()).isEqualTo(TableHeader.of(HEADER_BOOLEAN, HEADER_BYTE,
                HEADER_CHAR, HEADER_SHORT, HEADER_INT, HEADER_LONG, HEADER_FLOAT, HEADER_DOUBLE,
                HEADER_STRING, HEADER_INSTANT, HEADER_CUSTOM));
    }

    @Test
    void h11Table() {
        // @formatter:off
        NewTable rowOriented = h11()
            .row(true, (byte) 1, 'c', (short) 13, 42, 31337L, 3.14f, 99.99, "foobar", Instant.ofEpochMilli(0), Custom.A)
            .newTable();

        NewTable expected = NewTable.of(
            Column.of("Boolean", BooleanArray.ofUnsafe((byte) 1)),
            Column.of("Byte", ByteArray.ofUnsafe((byte) 1)),
            Column.of("Char", CharArray.ofUnsafe('c')),
            Column.of("Short", ShortArray.ofUnsafe((short) 13)),
            Column.of("Int", IntArray.ofUnsafe(42)),
            Column.of("Long", LongArray.ofUnsafe(31337L)),
            Column.of("Float", FloatArray.ofUnsafe(3.14f)),
            Column.of("Double", DoubleArray.ofUnsafe(99.99)),
            Column.of("String", GenericArray.of(StringType.instance(), "foobar")),
            Column.of("Instant", GenericArray.of(InstantType.instance(), Instant.ofEpochMilli(0))),
            Column.of("Custom", GenericArray.of(CustomType.of(Custom.class), Custom.A)));
        // @formatter:on

        assertThat(rowOriented).isEqualTo(expected);
    }

    private static ColumnHeaders2<Boolean, Byte> h2() {
        return HEADER_BOOLEAN.header(HEADER_BYTE);
    }

    private static ColumnHeaders3<Boolean, Byte, Character> h3() {
        return h2().header(HEADER_CHAR);
    }

    private static ColumnHeaders4<Boolean, Byte, Character, Short> h4() {
        return h3().header(HEADER_SHORT);
    }

    private static ColumnHeaders5<Boolean, Byte, Character, Short, Integer> h5() {
        return h4().header(HEADER_INT);
    }

    private static ColumnHeaders6<Boolean, Byte, Character, Short, Integer, Long> h6() {
        return h5().header(HEADER_LONG);
    }

    private static ColumnHeaders7<Boolean, Byte, Character, Short, Integer, Long, Float> h7() {
        return h6().header(HEADER_FLOAT);
    }

    private static ColumnHeaders8<Boolean, Byte, Character, Short, Integer, Long, Float, Double> h8() {
        return h7().header(HEADER_DOUBLE);
    }

    private static ColumnHeaders9<Boolean, Byte, Character, Short, Integer, Long, Float, Double, String> h9() {
        return h8().header(HEADER_STRING);
    }

    private static ColumnHeadersN<Boolean, Byte, Character, Short, Integer, Long, Float, Double, String> h10() {
        return h9().header(HEADER_INSTANT);
    }

    private static ColumnHeadersN<Boolean, Byte, Character, Short, Integer, Long, Float, Double, String> h11() {
        return h10().header(HEADER_CUSTOM);
    }

    enum Custom {
        A, B
    }
}
