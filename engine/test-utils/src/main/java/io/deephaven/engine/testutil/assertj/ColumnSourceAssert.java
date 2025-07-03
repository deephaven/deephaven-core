//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ClassAssert;

public class ColumnSourceAssert<T> extends AbstractAssert<ColumnSourceAssert<T>, ColumnSource<T>> {

    public static <T> ColumnSourceAssert<T> assertThat(ColumnSource<T> columnSource) {
        return new ColumnSourceAssert<>(columnSource);
    }

    public ColumnSourceAssert(ColumnSource<T> columnSource) {
        super(columnSource, ColumnSourceAssert.class);
    }

    public ClassAssert type() {
        return Assertions.assertThat(actual.getType());
    }

    public ClassAssert componentType() {
        return Assertions.assertThat(actual.getComponentType());
    }

    public void isImmutable() {
        Assertions.assertThat(actual.isImmutable()).isTrue();
    }

    public void isNotImmutable() {
        Assertions.assertThat(actual.isImmutable()).isFalse();
    }

    public ColumnSourceValuesAssert<T> values(RowSet rowSet) {
        return new ColumnSourceValuesAssert<>(actual, rowSet);
    }
}
