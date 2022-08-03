/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleShortArrayTest extends JpyModuleArrayTestBase<short[]> {

    @Override
    PrimitiveArrayType<short[]> getType() {
        return PrimitiveArrayType.shorts();
    }

    @Override
    short[] emptyArrayFromJava(int len) {
        return new short[len];
    }

    @Override
    boolean arraysEqual(short[] expected, short[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(short[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = (short) i;
        }
    }
}
