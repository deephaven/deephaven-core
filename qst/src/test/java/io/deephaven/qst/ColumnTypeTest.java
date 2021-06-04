package io.deephaven.qst;

import static io.deephaven.qst.table.column.type.ColumnType.booleanType;
import static io.deephaven.qst.table.column.type.ColumnType.byteType;
import static io.deephaven.qst.table.column.type.ColumnType.charType;
import static io.deephaven.qst.table.column.type.ColumnType.doubleType;
import static io.deephaven.qst.table.column.type.ColumnType.find;
import static io.deephaven.qst.table.column.type.ColumnType.floatType;
import static io.deephaven.qst.table.column.type.ColumnType.intType;
import static io.deephaven.qst.table.column.type.ColumnType.longType;
import static io.deephaven.qst.table.column.type.ColumnType.ofGeneric;
import static io.deephaven.qst.table.column.type.ColumnType.shortType;
import static io.deephaven.qst.table.column.type.ColumnType.staticTypes;
import static io.deephaven.qst.table.column.type.ColumnType.stringType;
import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.qst.table.column.type.ColumnType;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ColumnTypeTest {

    @Test
    void numberOfStaticTypes() {
        // A reminder that when the number of static types increases, we should
        // add tests in this class for it specifically
        assertThat(staticTypes()).hasSize(9);
    }

    @Test
    void findBooleans() {
        assertThat(find(boolean.class)).isEqualTo(booleanType());
        assertThat(find(Boolean.class)).isEqualTo(booleanType());
    }

    @Test
    void findBytes() {
        assertThat(find(byte.class)).isEqualTo(byteType());
        assertThat(find(Byte.class)).isEqualTo(byteType());
    }

    @Test
    void findChars() {
        assertThat(find(char.class)).isEqualTo(charType());
        assertThat(find(Character.class)).isEqualTo(charType());
    }

    @Test
    void findShorts() {
        assertThat(find(short.class)).isEqualTo(shortType());
        assertThat(find(Short.class)).isEqualTo(shortType());
    }

    @Test
    void findInts() {
        assertThat(find(int.class)).isEqualTo(intType());
        assertThat(find(Integer.class)).isEqualTo(intType());
    }

    @Test
    void findLongs() {
        assertThat(find(long.class)).isEqualTo(longType());
        assertThat(find(Long.class)).isEqualTo(longType());
    }

    @Test
    void findFloats() {
        assertThat(find(float.class)).isEqualTo(floatType());
        assertThat(find(Float.class)).isEqualTo(floatType());
    }

    @Test
    void findDoubles() {
        assertThat(find(double.class)).isEqualTo(doubleType());
        assertThat(find(Double.class)).isEqualTo(doubleType());
    }

    @Test
    void findString() {
        assertThat(find(String.class)).isEqualTo(stringType());
    }

    @Test
    void findGenericObject() {
        assertThat(find(Object.class)).isEqualTo(ofGeneric(Object.class));
    }

    @Test
    void nonEqualityCheck() {
        final List<ColumnType<?>> staticTypes = staticTypes().collect(Collectors.toList());
        final int L = staticTypes.size();
        for (int i = 0; i < L - 1; ++i) {
            for (int j = i + 1; j < L; ++j) {
                assertThat(staticTypes.get(i)).isNotEqualTo(staticTypes.get(j));
                assertThat(staticTypes.get(j)).isNotEqualTo(staticTypes.get(i));
            }
        }
    }
}
