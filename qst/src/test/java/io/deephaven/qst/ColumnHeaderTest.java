package io.deephaven.qst;

import static io.deephaven.qst.table.column.header.ColumnHeader.ofBoolean;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofByte;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofChar;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofDouble;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofFloat;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofInt;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofLong;
import static io.deephaven.qst.table.column.header.ColumnHeader.ofShort;
import static io.deephaven.qst.table.column.type.ColumnType.booleanType;
import static io.deephaven.qst.table.column.type.ColumnType.byteType;
import static io.deephaven.qst.table.column.type.ColumnType.charType;
import static io.deephaven.qst.table.column.type.ColumnType.doubleType;
import static io.deephaven.qst.table.column.type.ColumnType.floatType;
import static io.deephaven.qst.table.column.type.ColumnType.intType;
import static io.deephaven.qst.table.column.type.ColumnType.longType;
import static io.deephaven.qst.table.column.type.ColumnType.shortType;
import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.qst.table.column.header.ColumnHeader;
import org.junit.jupiter.api.Test;

public class ColumnHeaderTest {

    @Test
    void name() {
        assertThat(ColumnHeader.of("TheName", intType()).name()).isEqualTo("TheName");
    }

    @Test
    void ofBooleanType() {
        assertThat(ofBoolean("X").type()).isEqualTo(booleanType());
    }

    @Test
    void ofByteType() {
        assertThat(ofByte("X").type()).isEqualTo(byteType());
    }

    @Test
    void ofCharType() {
        assertThat(ofChar("X").type()).isEqualTo(charType());
    }

    @Test
    void ofShortType() {
        assertThat(ofShort("X").type()).isEqualTo(shortType());
    }

    @Test
    void ofIntType() {
        assertThat(ofInt("X").type()).isEqualTo(intType());
    }

    @Test
    void ofLongType() {
        assertThat(ofLong("X").type()).isEqualTo(longType());
    }

    @Test
    void ofFloatType() {
        assertThat(ofFloat("X").type()).isEqualTo(floatType());
    }

    @Test
    void ofDoubleType() {
        assertThat(ofDouble("X").type()).isEqualTo(doubleType());
    }
}
