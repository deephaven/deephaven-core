package io.deephaven.qst.column.header;

import org.junit.jupiter.api.Test;

import static io.deephaven.qst.column.header.ColumnHeader.ofBoolean;
import static io.deephaven.qst.column.header.ColumnHeader.ofByte;
import static io.deephaven.qst.column.header.ColumnHeader.ofChar;
import static io.deephaven.qst.column.header.ColumnHeader.ofDouble;
import static io.deephaven.qst.column.header.ColumnHeader.ofFloat;
import static io.deephaven.qst.column.header.ColumnHeader.ofInstant;
import static io.deephaven.qst.column.header.ColumnHeader.ofInt;
import static io.deephaven.qst.column.header.ColumnHeader.ofLong;
import static io.deephaven.qst.column.header.ColumnHeader.ofShort;
import static io.deephaven.qst.column.header.ColumnHeader.ofString;
import static io.deephaven.qst.type.Type.booleanType;
import static io.deephaven.qst.type.Type.byteType;
import static io.deephaven.qst.type.Type.charType;
import static io.deephaven.qst.type.Type.doubleType;
import static io.deephaven.qst.type.Type.floatType;
import static io.deephaven.qst.type.Type.instantType;
import static io.deephaven.qst.type.Type.intType;
import static io.deephaven.qst.type.Type.longType;
import static io.deephaven.qst.type.Type.shortType;
import static io.deephaven.qst.type.Type.stringType;
import static org.assertj.core.api.Assertions.assertThat;

public class ColumnHeaderTest {

    @Test
    void name() {
        assertThat(ColumnHeader.of("TheName", intType()).name()).isEqualTo("TheName");
    }

    @Test
    void ofBooleanType() {
        assertThat(ofBoolean("X").componentType()).isEqualTo(booleanType());
    }

    @Test
    void ofByteType() {
        assertThat(ofByte("X").componentType()).isEqualTo(byteType());
    }

    @Test
    void ofCharType() {
        assertThat(ofChar("X").componentType()).isEqualTo(charType());
    }

    @Test
    void ofShortType() {
        assertThat(ofShort("X").componentType()).isEqualTo(shortType());
    }

    @Test
    void ofIntType() {
        assertThat(ofInt("X").componentType()).isEqualTo(intType());
    }

    @Test
    void ofLongType() {
        assertThat(ofLong("X").componentType()).isEqualTo(longType());
    }

    @Test
    void ofFloatType() {
        assertThat(ofFloat("X").componentType()).isEqualTo(floatType());
    }

    @Test
    void ofDoubleType() {
        assertThat(ofDouble("X").componentType()).isEqualTo(doubleType());
    }

    @Test
    void ofStringType() {
        assertThat(ofString("X").componentType()).isEqualTo(stringType());
    }

    @Test
    void ofInstantType() {
        assertThat(ofInstant("X").componentType()).isEqualTo(instantType());
    }
}
