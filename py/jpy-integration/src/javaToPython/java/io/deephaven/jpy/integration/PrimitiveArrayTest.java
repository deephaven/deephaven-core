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
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that when passing array from java to python, it is correctly recognized by jpy.
 */
public class PrimitiveArrayTest extends PythonTest {
    // Note: this may be a good model if we wanted to extend the functionality of jpy without
    // actually writing jpy / C code.
    interface ArrayProxy {
        boolean is_jpy_boolean_array(Object o);

        boolean is_jpy_char_array(Object o);

        boolean is_jpy_byte_array(Object o);

        boolean is_jpy_short_array(Object o);

        boolean is_jpy_int_array(Object o);

        boolean is_jpy_long_array(Object o);

        boolean is_jpy_float_array(Object o);

        boolean is_jpy_double_array(Object o);

        boolean is_jpy_boolean_array(PyObject o);

        boolean is_jpy_char_array(PyObject o);

        boolean is_jpy_byte_array(PyObject o);

        boolean is_jpy_short_array(PyObject o);

        boolean is_jpy_int_array(PyObject o);

        boolean is_jpy_long_array(PyObject o);

        boolean is_jpy_float_array(PyObject o);

        boolean is_jpy_double_array(PyObject o);

        boolean is_jpy_boolean_array(boolean[] o);

        boolean is_jpy_char_array(char[] o);

        boolean is_jpy_byte_array(byte[] o);

        boolean is_jpy_short_array(short[] o);

        boolean is_jpy_int_array(int[] o);

        boolean is_jpy_long_array(long[] o);

        boolean is_jpy_float_array(float[] o);

        boolean is_jpy_double_array(double[] o);
    }

    enum PrimitiveArrayType {
        BOOLEAN(new boolean[] {true, false, true}, Booleans.INSTANCE), CHAR(new char[] {'a', 'b', 'c'},
                Chars.INSTANCE), BYTE(new byte[] {(byte) 'd', (byte) 'e', (byte) 'a', (byte) 'd'},
                        Bytes.INSTANCE), SHORT(new short[] {1, 42, 31, 15, -5}, Shorts.INSTANCE), INT(
                                new int[] {1, 42, 31, 15, -5}, Ints.INSTANCE), LONG(new long[] {1, 42, 31, 15, -5},
                                        Longs.INSTANCE), FLOAT(new float[] {42.0f},
                                                Floats.INSTANCE), DOUBLE(new double[] {42.0}, Doubles.INSTANCE);

        private final Object o;
        private final io.deephaven.util.PrimitiveArrayType type;

        PrimitiveArrayType(Object o, io.deephaven.util.PrimitiveArrayType<?> type) {
            this.o = o;
            this.type = type;
        }

        void checkDirect(ArrayProxy proxy) {
            switch (this) {
                case BOOLEAN:
                    Assert.assertTrue(proxy.is_jpy_boolean_array((boolean[]) o));
                    break;
                case CHAR:
                    Assert.assertTrue(proxy.is_jpy_char_array((char[]) o));
                    break;
                case BYTE:
                    Assert.assertTrue(proxy.is_jpy_byte_array((byte[]) o));
                    break;
                case SHORT:
                    Assert.assertTrue(proxy.is_jpy_short_array((short[]) o));
                    break;
                case INT:
                    Assert.assertTrue(proxy.is_jpy_int_array((int[]) o));
                    break;
                case LONG:
                    Assert.assertTrue(proxy.is_jpy_long_array((long[]) o));
                    break;
                case FLOAT:
                    Assert.assertTrue(proxy.is_jpy_float_array((float[]) o));
                    break;
                case DOUBLE:
                    Assert.assertTrue(proxy.is_jpy_double_array((double[]) o));
                    break;
                default:
                    throw new IllegalStateException("Unexpected type " + this);
            }
        }

        void check(ArrayProxy proxy) {
            Assert.assertEquals(this == BOOLEAN, proxy.is_jpy_boolean_array(o));
            Assert.assertEquals(this == CHAR, proxy.is_jpy_char_array(o));
            Assert.assertEquals(this == BYTE, proxy.is_jpy_byte_array(o));
            Assert.assertEquals(this == SHORT, proxy.is_jpy_short_array(o));
            Assert.assertEquals(this == INT, proxy.is_jpy_int_array(o));
            Assert.assertEquals(this == LONG, proxy.is_jpy_long_array(o));
            Assert.assertEquals(this == FLOAT, proxy.is_jpy_float_array(o));
            Assert.assertEquals(this == DOUBLE, proxy.is_jpy_double_array(o));
        }

        void check(ArrayProxy proxy, PyObject pyObject) {
            Assert.assertEquals(this == BOOLEAN, proxy.is_jpy_boolean_array(pyObject));
            Assert.assertEquals(this == CHAR, proxy.is_jpy_char_array(pyObject));
            Assert.assertEquals(this == BYTE, proxy.is_jpy_byte_array(pyObject));
            Assert.assertEquals(this == SHORT, proxy.is_jpy_short_array(pyObject));
            Assert.assertEquals(this == INT, proxy.is_jpy_int_array(pyObject));
            Assert.assertEquals(this == LONG, proxy.is_jpy_long_array(pyObject));
            Assert.assertEquals(this == FLOAT, proxy.is_jpy_float_array(pyObject));
            Assert.assertEquals(this == DOUBLE, proxy.is_jpy_double_array(pyObject));
        }

        void check(ArrayProxy proxy, Object object) {
            Assert.assertEquals(this == BOOLEAN, proxy.is_jpy_boolean_array(object));
            Assert.assertEquals(this == CHAR, proxy.is_jpy_char_array(object));
            Assert.assertEquals(this == BYTE, proxy.is_jpy_byte_array(object));
            Assert.assertEquals(this == SHORT, proxy.is_jpy_short_array(object));
            Assert.assertEquals(this == INT, proxy.is_jpy_int_array(object));
            Assert.assertEquals(this == LONG, proxy.is_jpy_long_array(object));
            Assert.assertEquals(this == FLOAT, proxy.is_jpy_float_array(object));
            Assert.assertEquals(this == DOUBLE, proxy.is_jpy_double_array(object));
        }

        void checkNewPyCopy(PrimitiveArrayTest pat) {
            final Object o = type.newInstance(0);
            pat.check(this, () -> pat.jpy.ops(type).newPyCopy(o));
        }
    }

    private ArrayProxy SUT;
    private BuiltinsModule builtins;
    private JpyModule jpy;

    @Before
    public void setUp() {
        PyObject.executeCode(readResource("primitive_array_test.py"), PyInputMode.SCRIPT);
        SUT = PyObject
                .executeCode("ArrayTest()", PyInputMode.EXPRESSION)
                .createProxy(ArrayProxy.class);
        builtins = BuiltinsModule.create();
        jpy = JpyModule.create();
    }

    @After
    public void tearDown() {
        jpy.close();
        builtins.close();
    }

    @Test
    public void booleanNewPyCopy() {
        PrimitiveArrayType.BOOLEAN.checkNewPyCopy(this);
    }

    @Test
    public void charNewPyCopy() {
        PrimitiveArrayType.CHAR.checkNewPyCopy(this);
    }

    @Test
    public void byteNewPyCopy() {
        PrimitiveArrayType.BYTE.checkNewPyCopy(this);
    }

    @Test
    public void shortNewPyCopy() {
        PrimitiveArrayType.SHORT.checkNewPyCopy(this);
    }

    @Test
    public void intNewPyCopy() {
        PrimitiveArrayType.INT.checkNewPyCopy(this);
    }

    @Test
    public void longNewPyCopy() {
        PrimitiveArrayType.LONG.checkNewPyCopy(this);
    }

    @Test
    public void floatNewPyCopy() {
        PrimitiveArrayType.FLOAT.checkNewPyCopy(this);
    }

    @Test
    public void doubleNewPyCopy() {
        PrimitiveArrayType.DOUBLE.checkNewPyCopy(this);
    }

    private void check(PrimitiveArrayType type, Supplier<PyObject> supplier) {
        type.checkDirect(SUT);
        type.check(SUT);
        try (final PyObject pyObject = supplier.get()) {
            type.check(SUT, pyObject);
            type.check(SUT, (Object) pyObject);
        }
    }

    private static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(PrimitiveArrayTest.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
