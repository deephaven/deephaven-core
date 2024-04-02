//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleIntArrayTest extends JpyModuleArrayTestBase<int[]> {

    @Override
    PrimitiveArrayType<int[]> getType() {
        return PrimitiveArrayType.ints();
    }

    @Override
    int[] emptyArrayFromJava(int len) {
        return new int[len];
    }

    @Override
    boolean arraysEqual(int[] expected, int[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(int[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = i;
        }
    }
}
