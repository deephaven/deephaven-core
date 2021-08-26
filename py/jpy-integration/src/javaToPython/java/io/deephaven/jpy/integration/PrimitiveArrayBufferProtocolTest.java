package io.deephaven.jpy.integration;

import io.deephaven.jpy.BuiltinsModule;
import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import io.deephaven.util.PrimitiveArrayType.Booleans;
import io.deephaven.util.PrimitiveArrayType.Bytes;
import io.deephaven.util.PrimitiveArrayType.Chars;
import io.deephaven.util.PrimitiveArrayType.Doubles;
import io.deephaven.util.PrimitiveArrayType.Floats;
import io.deephaven.util.PrimitiveArrayType.Ints;
import io.deephaven.util.PrimitiveArrayType.Longs;
import io.deephaven.util.PrimitiveArrayType.Shorts;
import java.util.function.Supplier;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PrimitiveArrayBufferProtocolTest extends PythonTest {
    private BuiltinsModule builtins;
    private NoopModule noop;
    private JpyModule jpy;
    private ReferenceCounting ref;

    @Before
    public void setUp() {
        ref = ReferenceCounting.create();
        builtins = BuiltinsModule.create();
        noop = NoopModule.create(getCreateModule());
        jpy = JpyModule.create();
        // jpy.setFlags(EnumSet.of(Flag.ALL));

        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();
    }

    @After
    public void tearDown() {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        jpy.close();
        noop.close();
        builtins.close();
        ref.close();
    }

    @Test
    public void boolean_array() {
        check(() -> jpy.ops(Booleans.INSTANCE).newPyCopy(new boolean[1]));
    }

    @Test
    public void byte_array() {
        check(() -> jpy.ops(Bytes.INSTANCE).newPyCopy(new byte[1]));
    }

    @Test
    public void char_array() {
        check(() -> jpy.ops(Chars.INSTANCE).newPyCopy(new char[1]));
    }

    @Test
    public void short_array() {
        check(() -> jpy.ops(Shorts.INSTANCE).newPyCopy(new short[1]));
    }

    @Test
    public void int_array() {
        check(() -> jpy.ops(Ints.INSTANCE).newPyCopy(new int[1]));
    }

    @Test
    public void long_array() {
        check(() -> jpy.ops(Longs.INSTANCE).newPyCopy(new long[1]));
    }

    @Test
    public void float_array() {
        check(() -> jpy.ops(Floats.INSTANCE).newPyCopy(new float[1]));
    }

    @Test
    public void double_array() {
        check(() -> jpy.ops(Doubles.INSTANCE).newPyCopy(new double[1]));
    }

    private void check(Supplier<PyObject> supplier) {
        final PyObject mvOutlive;
        try (final PyObject java_array = supplier.get()) {
            ref.check(1, java_array);
            try (final PyObject mv1 = builtins.memoryview(java_array)) {
                ref.check(2, java_array);
                ref.check(1, mv1);
                try (final PyObject mv2 = builtins.memoryview(java_array)) {
                    ref.check(3, java_array);
                    ref.check(1, mv1);
                    ref.check(1, mv2);
                }
                ref.check(2, java_array);
                ref.check(1, mv1);
            }
            ref.check(1, java_array);

            mvOutlive = builtins.memoryview(java_array);
            ref.check(2, java_array);
            ref.check(1, mvOutlive);
        }
        ref.check(1, mvOutlive);
        Assert.assertEquals(1, builtins.len(mvOutlive));
        mvOutlive.close();
    }
}
