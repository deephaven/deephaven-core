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

public class IntArrayOutTest extends PythonTest {

    // we need to choose a value that the python runtime does *not* have a reference to
    private static final int UNIQ_INT = 0xbadc0fee;
    private static final int[] INTS = new int[] {31337, 42, UNIQ_INT};

    interface IntArrayOut extends IdentityOut {
        int[] identity(int[] object);

        int[] identity(Integer[] object);

        int[] identity(PyObject object);

        int[] identity(Object object);
    }

    private IntArrayOut out;
    private ReferenceCounting ref;
    private JpyModule jpy;

    @Before
    public void setUp() {
        out = IdentityOut.create(getCreateModule(), IntArrayOut.class);
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

    // ----- int[] out -----

    @Ignore // currently fails
    @Test
    public void explicitIntsToInts() {
        Assert.assertArrayEquals(INTS, out.identity(INTS));
    }

    @Ignore // currently fails
    @Test
    public void implicitIntsToInts() {
        Assert.assertArrayEquals(INTS, out.identity((Object) INTS));
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
