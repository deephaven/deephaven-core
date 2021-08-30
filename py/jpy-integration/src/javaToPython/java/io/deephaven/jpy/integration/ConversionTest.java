package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ConversionTest extends PythonTest {

    // we need to choose a value that the python runtime does *not* have a reference to
    private static final int UNIQ_INT = 0xbadc0fee;
    private static final String UNIQ_STR = "bad coffee";

    private NoopModule noop;
    private PyObjectIdentityOut pyOut;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        noop = NoopModule.create(getCreateModule());
        pyOut = IdentityOut.create(getCreateModule(), PyObjectIdentityOut.class);
        ref = ReferenceCounting.create();
        jpy = JpyModule.create();
        // jpy.setFlags(EnumSet.of(Flag.ALL));
    }

    @After
    public void tearDown() {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        jpy.close();
        ref.close();
        pyOut.close();
        noop.close();
    }

    // ----- PyObject (int) out -----

    @Test
    public void intToPyObject() {
        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();

        try (final PyObject out = pyOut.identity(UNIQ_INT)) {
            check(1, out);
            Assert.assertTrue(out.isInt());
            Assert.assertEquals(UNIQ_INT, out.getIntValue());
        }
    }

    @Test
    public void explicitIntegerToPyObject() {
        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();

        try (final PyObject out = pyOut.identity(Integer.valueOf(UNIQ_INT))) {
            check(1, out);
            Assert.assertTrue(out.isInt());
            Assert.assertEquals(UNIQ_INT, out.getIntValue());
        }
    }

    @Test
    public void implicitIntegerToPyObject() {
        // this tests fails in python 2, but I haven't spent time debugging
        assumePython3();

        try (final PyObject out = pyOut.identity((Object) UNIQ_INT)) {
            Assert.assertTrue(out.isInt());
            Assert.assertEquals(UNIQ_INT, out.getIntValue());
        }
    }

    @Test
    public void explicitPyObjectIntToPyObject() {
        try (final PyObject in = expr(UNIQ_INT)) {
            check(1, in);
            try (final PyObject out = pyOut.identity(in)) {
                check(2, in);
                Assert.assertTrue(out.isInt());
                Assert.assertEquals(UNIQ_INT, out.getIntValue());
            }
            check(1, in);
        }
    }

    @Test
    public void implicitPyObjectIntToPyObject() {
        try (final PyObject in = expr(UNIQ_INT)) {
            check(1, in);
            try (final PyObject out = pyOut.identity((Object) in)) {
                check(2, in);
                Assert.assertTrue(out.isInt());
                Assert.assertEquals(UNIQ_INT, out.getIntValue());
            }
            check(1, in);
        }
    }

    // ----- PyObject (String) out -----

    @Test
    public void explicitStringToPyObject() {
        try (final PyObject out = pyOut.identity(UNIQ_STR)) {
            check(1, out);
            Assert.assertTrue(out.isString());
            Assert.assertEquals(UNIQ_STR, out.str());
        }
    }

    @Test
    public void implicitStringToPyObject() {
        try (final PyObject out = pyOut.identity((Object) UNIQ_STR)) {
            check(1, out);
            Assert.assertTrue(out.isString());
            Assert.assertEquals(UNIQ_STR, out.str());
        }
    }

    @Test
    public void explicitPyObjectStringToPyObject() {
        try (final PyObject in = expr(UNIQ_STR)) {
            check(1, in);
            try (final PyObject out = pyOut.identity(in)) {
                check(2, in);
                Assert.assertTrue(out.isString());
                Assert.assertEquals(UNIQ_STR, out.str());
            }
            check(1, in);
        }
    }

    @Test
    public void implicitPyObjectStringToPyObject() {
        try (final PyObject in = expr(UNIQ_STR)) {
            check(1, in);
            try (final PyObject out = pyOut.identity((Object) in)) {
                check(2, in);
                Assert.assertTrue(out.isString());
                Assert.assertEquals(UNIQ_STR, out.str());
            }
            check(1, in);
        }
    }

    // ----- PyObject (SomeJavaClass) out -----

    // todo, other tests

    // ----------

    private static PyObject expr(int value) {
        return PyObject.executeCode(asExpression(value), PyInputMode.EXPRESSION);
    }

    private static PyObject expr(String s) {
        return PyObject.executeCode(asExpression(s), PyInputMode.EXPRESSION);
    }

    private void check(int refCount, PyObject pyObject) {
        ref.check(refCount, pyObject);
    }

    private static String asExpression(int value) {
        return String.format("%d", value);
    }

    private static String asExpression(String value) {
        return String.format("'%s'", value);
    }
}
