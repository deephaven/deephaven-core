//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ColumnSourceValuesAssertTest {

    private static final String COL = "Foo";

    private static <T> ColumnSourceValuesAssert<T> assertValues(ColumnHolder<T> columnHolder) {
        assertThat(columnHolder.getName()).isEqualTo(COL);
        return TableAssert.assertThat(TableTools.newTable(columnHolder)).columnSourceValues(COL);
    }

    @Test
    void chars() {
        assertValues(TableTools.charCol(COL, 'a', 'b', 'c'))
                .chars()
                .containsExactly('a', 'b', 'c');
    }

    @Test
    void bytes() {
        assertValues(TableTools.byteCol(COL, (byte) 0, (byte) 1, (byte) 2))
                .bytes()
                .containsExactly((byte) 0, (byte) 1, (byte) 2);
    }

    @Test
    void shorts() {
        assertValues(TableTools.shortCol(COL, (short) 1, (short) 2, (short) 3))
                .shorts()
                .containsExactly((short) 1, (short) 2, (short) 3);
    }

    @Test
    void ints() {
        assertValues(TableTools.intCol(COL, 1, 2, 3))
                .ints()
                .containsExactly(1, 2, 3);
    }

    @Test
    void longs() {
        assertValues(TableTools.longCol(COL, 1, 2, 3))
                .longs()
                .containsExactly(1, 2, 3);
    }

    @Test
    void floats() {
        assertValues(TableTools.floatCol(COL, 1.0f, 2.0f, 3.0f))
                .floats()
                .containsExactly(1.0f, 2.0f, 3.0f);
    }

    @Test
    void doubles() {
        assertValues(TableTools.doubleCol(COL, 1.0, 2.0, 3.0))
                .doubles()
                .containsExactly(1.0, 2.0, 3.0);
    }
}
