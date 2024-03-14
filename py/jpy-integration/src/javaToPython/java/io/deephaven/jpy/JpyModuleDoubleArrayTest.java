//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleDoubleArrayTest extends JpyModuleArrayTestBase<double[]> {

    @Override
    PrimitiveArrayType<double[]> getType() {
        return PrimitiveArrayType.doubles();
    }

    @Override
    double[] emptyArrayFromJava(int len) {
        return new double[len];
    }

    @Override
    boolean arraysEqual(double[] expected, double[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(double[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = i / 41.0;
        }
    }
}
