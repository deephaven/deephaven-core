package io.deephaven.jpy;

import io.deephaven.util.PrimitiveArrayType;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class JpyModuleArrayTestBase<T> extends PythonTest {

    private JpyModule jpy;
    private BuiltinsModule builtins;
    private JpyModule.ArrayOps<T> ops;

    @Before
    public void setUp() throws Exception {
        jpy = JpyModule.create();
        builtins = BuiltinsModule.create();
        ops = jpy.ops(getType());
        // jpy.setFlags(EnumSet.of(Flag.ALL));
    }

    @After
    public void tearDown() throws Exception {
        // jpy.setFlags(EnumSet.of(Flag.OFF));
        builtins.close();
        jpy.close();
    }


    abstract PrimitiveArrayType<T> getType();

    abstract T emptyArrayFromJava(int len);

    abstract boolean arraysEqual(T expected, T actual);

    abstract void fillAsDesired(T array);

    @Test
    public void pyObjectFromArrayLen() {
        try (final PyObject pyObject = ops.newPyInstance(15)) {
            Assert.assertEquals(15, builtins.len(pyObject));
            // todo, other checks
        }
    }

    @Test
    public void pyObjectFromJavaArray() {
        final T array = emptyArrayFromJava(15);
        fillAsDesired(array);

        try (final PyObject pyObject = ops.newPyCopy(array)) {
            Assert.assertEquals(15, builtins.len(pyObject));
            // pyObject.getObjectArrayValue(ops.getPrimitiveType());
        }
    }

    @Test
    public void nativeArrayFromLen() {
        final T nativeArray = ops.newInstance(15);
        Assert.assertTrue(arraysEqual(emptyArrayFromJava(15), nativeArray));
    }

    @Test
    public void nativeArrayFromJavaArray() {
        final T array = emptyArrayFromJava(15);
        fillAsDesired(array);
        final T nativeArray = ops.newCopy(array);
        Assert.assertTrue(arraysEqual(array, nativeArray));
    }
}
