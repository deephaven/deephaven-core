package io.deephaven.jpy.integration;

import io.deephaven.jpy.PythonTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PyLibNullArgTest extends PythonTest {

    interface SomeModule extends AutoCloseable {
        String some_function();

        String some_function(String x);
    }

    private SomeModule someModule;

    @Before
    public void setUp() throws Exception {
        someModule = getCreateModule().callAsFunctionModule(
                "some_module",
                readResource(PyLibNullArgTest.class, "pylib_null_arg_test.py"),
                SomeModule.class);
    }

    @After
    public void tearDown() throws Exception {
        someModule.close();
    }

    @Test
    public void defaultPythonValue() {
        Assert.assertEquals("sentinel", someModule.some_function());
    }

    @Test
    public void nonePythonValue() {
        Assert.assertEquals("None", someModule.some_function(null));
    }

    @Test
    public void otherPythonValue() {
        Assert.assertEquals("some_value", someModule.some_function("some_value"));
    }
}
