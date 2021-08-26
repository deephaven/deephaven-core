package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import java.util.Arrays;

public class JpyModuleCharArrayTest extends JpyModuleArrayTestBase<char[]> {

    @Override
    PrimitiveArrayType<char[]> getType() {
        return PrimitiveArrayType.chars();
    }

    @Override
    char[] emptyArrayFromJava(int len) {
        return new char[len];
    }

    @Override
    boolean arraysEqual(char[] expected, char[] actual) {
        return Arrays.equals(expected, actual);
    }

    @Override
    void fillAsDesired(char[] array) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = (char) i;
        }
    }
}
