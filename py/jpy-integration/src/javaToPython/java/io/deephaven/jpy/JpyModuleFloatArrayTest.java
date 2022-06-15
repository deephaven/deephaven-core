/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleFloatArrayTest extends JpyModuleArrayTestBase<float[]> {

    @Override
    PrimitiveArrayType<float[]> getType() {
        return PrimitiveArrayType.floats();
    }

    @Override
    float[] emptyArrayFromJava(int len) {
        return new float[len];
    }

    @Override
    boolean arraysEqual(float[] expected, float[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(float[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = i / 42.0f;
        }
    }
}
