package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StringOutTest extends PythonTest {


    // we need to choose a value that the python runtime does *not* have a reference to
    private static final String UNIQ_STR = "bad coffee";

    interface StringOut extends IdentityOut {
        String identity(String object);

        String identity(PyObject object);

        String identity(Object object);
    }

    private StringOut out;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        out = IdentityOut.create(getCreateModule(), StringOut.class);
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

    // ----- String out -----

    @Test
    public void explicitStringToString() {
        Assert.assertEquals(UNIQ_STR, out.identity(UNIQ_STR));
    }

    @Test
    public void implicitStringToString() {
        Assert.assertEquals(UNIQ_STR, out.identity((Object) UNIQ_STR));
    }

    @Test
    public void explicitPyObjectToString() {
        try (final PyObject in = expr(UNIQ_STR)) {
            check(1, in);
            Assert.assertEquals(UNIQ_STR, out.identity(in));
            check(1, in);
        }
    }

    @Test
    public void implicitPyObjectToString() {
        try (final PyObject in = expr(UNIQ_STR)) {
            check(1, in);
            Assert.assertEquals(UNIQ_STR, out.identity((Object) in));
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
