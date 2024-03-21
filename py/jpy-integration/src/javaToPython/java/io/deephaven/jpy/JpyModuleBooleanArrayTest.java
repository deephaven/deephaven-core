//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleBooleanArrayTest extends JpyModuleArrayTestBase<boolean[]> {

    @Override
    PrimitiveArrayType<boolean[]> getType() {
        return PrimitiveArrayType.booleans();
    }

    @Override
    boolean[] emptyArrayFromJava(int len) {
        return new boolean[len];
    }

    @Override
    boolean arraysEqual(boolean[] expected, boolean[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(boolean[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = i % 3 == 0;
        }
    }
}
