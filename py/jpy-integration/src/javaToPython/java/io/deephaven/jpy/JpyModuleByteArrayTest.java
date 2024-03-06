//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleByteArrayTest extends JpyModuleArrayTestBase<byte[]> {

    @Override
    PrimitiveArrayType<byte[]> getType() {
        return PrimitiveArrayType.bytes();
    }

    @Override
    byte[] emptyArrayFromJava(int len) {
        return new byte[len];
    }

    @Override
    boolean arraysEqual(byte[] expected, byte[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(byte[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = (byte) i;
        }
    }
}
