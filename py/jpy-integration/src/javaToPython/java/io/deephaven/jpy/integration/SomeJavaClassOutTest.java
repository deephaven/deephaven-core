package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SomeJavaClassOutTest extends PythonTest {

    private static final SomeJavaClass SJC = new SomeJavaClass();

    static class SomeJavaClass {
    }

    interface SJCOut extends IdentityOut {
        SomeJavaClass identity(SomeJavaClass object);

        SomeJavaClass identity(PyObject object);

        SomeJavaClass identity(Object object);
    }

    private SJCOut out;
    private PyObjectIdentityOut pyOut;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        out = IdentityOut.create(getCreateModule(), SJCOut.class);
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
        out.close();
    }

    // ----- SomeJavaClass out -----

    @Ignore
    @Test
    public void explicitSJCToSJC() {
        Assert.assertEquals(SJC, out.identity(SJC));
    }

    @Ignore
    @Test
    public void implicitSJCToSJC() {
        Assert.assertEquals(SJC, out.identity((Object) SJC));
    }

    @Test
    public void explicitPyObjectToSJC() {
        try (final PyObject in = pyOut.identity(SJC)) {
            check(1, in);
            final SomeJavaClass out = this.out.identity(in);
            Assert.assertEquals(SJC, out);
            check(1, in);
        }
    }

    @Test
    public void implicitPyObjectToSJC() {
        try (final PyObject in = pyOut.identity(SJC)) {
            check(1, in);
            final SomeJavaClass out = this.out.identity((Object) in);
            Assert.assertEquals(SJC, out);
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
