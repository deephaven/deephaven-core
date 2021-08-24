package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntegerOutTest extends PythonTest {

    // we need to choose a value that the python runtime does *not* have a reference to
    private static final int UNIQ_INT = 0xbadc0fee;

    interface IntegerOut extends IdentityOut {
        Integer identity(int object);

        Integer identity(Integer object);

        Integer identity(PyObject object);

        Integer identity(Object object);
    }

    private IntegerOut out;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        out = IdentityOut.create(getCreateModule(), IntegerOut.class);
        ref = ReferenceCounting.create();
        jpy = JpyModule.create();
        // jpy.setFlags(EnumSet.of(Flag.ALL));
    }

    @After
    public void tearDown() {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        jpy.close();
        ref.close();
        out.close();
    }

    // ----- Integer out -----

    @Test
    public void intToInteger() {
        Assert.assertEquals(Integer.valueOf(UNIQ_INT), out.identity(UNIQ_INT));
    }

    @Test
    public void explicitIntegerToInteger() {
        Assert.assertEquals(Integer.valueOf(UNIQ_INT), out
            .identity(Integer.valueOf(UNIQ_INT)));
    }

    @Test
    public void explicitPyObjectToInteger() {
        try (final PyObject in = expr(UNIQ_INT)) {
            check(1, in);
            Assert.assertEquals(Integer.valueOf(UNIQ_INT), out.identity(in));
            check(1, in);
        }
    }

    @Test
    public void implicitIntegerToInteger() {
        Assert.assertEquals(Integer.valueOf(UNIQ_INT), out.identity((Object) UNIQ_INT));
    }

    @Test
    public void implicitPyObjectToInteger() {
        try (final PyObject in = expr(UNIQ_INT)) {
            check(1, in);
            Assert.assertEquals(Integer.valueOf(UNIQ_INT), out.identity((Object) in));
            check(1, in);
        }
    }

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
