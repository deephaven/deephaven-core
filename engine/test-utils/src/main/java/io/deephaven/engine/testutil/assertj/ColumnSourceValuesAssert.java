//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSet.Iterator;
import io.deephaven.engine.table.ColumnSource;
import org.assertj.core.api.CharArrayAssert;
import org.assertj.core.api.DoubleArrayAssert;
import org.assertj.core.api.FloatArrayAssert;
import org.assertj.core.api.IteratorAssert;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnSourceValuesAssert<T> {

    private final ColumnSource<T> actual;
    private final RowSet rowSet;

    public ColumnSourceValuesAssert(ColumnSource<T> columnSource, RowSet rowSet) {
        this.actual = Objects.requireNonNull(columnSource);
        this.rowSet = Objects.requireNonNull(rowSet);
    }

    public CharArrayAssert chars() {
        return chars(false);
    }

    public FloatArrayAssert floats() {
        return floats(false);
    }

    public DoubleArrayAssert doubles() {
        return doubles(false);
    }

    public CharArrayAssert chars(boolean prev) {
        return assertThatChars(actual.cast(char.class), rowSet, prev);
    }

    public FloatArrayAssert floats(boolean prev) {
        return assertThatFloats(actual.cast(float.class), rowSet, prev);
    }

    public DoubleArrayAssert doubles(boolean prev) {
        return assertThatDoubles(actual.cast(double.class), rowSet, prev);
    }

    public static CharArrayAssert assertThatChars(ColumnSource<Character> source, RowSet rowSet, boolean prev) {
        final char[] out = new char[rowSet.intSize()];
        try (final Iterator it = rowSet.iterator()) {
            final IteratorAssert<Long> itAssert = assertThat(it);
            for (int i = 0; i < out.length; ++i) {
                itAssert.hasNext();
                out[i] = prev ? source.getPrevChar(it.nextLong()) : source.getChar(it.nextLong());
            }
            itAssert.isExhausted();
        }
        return new CharArrayAssert(out);
    }

    public static FloatArrayAssert assertThatFloats(ColumnSource<Float> source, RowSet rowSet, boolean prev) {
        final float[] out = new float[rowSet.intSize()];
        try (final Iterator it = rowSet.iterator()) {
            final IteratorAssert<Long> itAssert = assertThat(it);
            for (int i = 0; i < out.length; ++i) {
                itAssert.hasNext();
                out[i] = prev ? source.getPrevFloat(it.nextLong()) : source.getFloat(it.nextLong());
            }
            itAssert.isExhausted();
        }
        return new FloatArrayAssert(out);
    }

    public static DoubleArrayAssert assertThatDoubles(ColumnSource<Double> source, RowSet rowSet, boolean prev) {
        final double[] out = new double[rowSet.intSize()];
        try (final Iterator it = rowSet.iterator()) {
            final IteratorAssert<Long> itAssert = assertThat(it);
            for (int i = 0; i < out.length; ++i) {
                itAssert.hasNext();
                out[i] = prev ? source.getPrevDouble(it.nextLong()) : source.getDouble(it.nextLong());
            }
            itAssert.isExhausted();
        }
        return new DoubleArrayAssert(out);
    }
}
