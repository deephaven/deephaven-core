//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleLongArrayTest extends JpyModuleArrayTestBase<long[]> {

    @Override
    PrimitiveArrayType<long[]> getType() {
        return PrimitiveArrayType.longs();
    }

    @Override
    long[] emptyArrayFromJava(int len) {
        return new long[len];
    }

    @Override
    boolean arraysEqual(long[] expected, long[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(long[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = Long.MAX_VALUE - i;
        }
    }
}
