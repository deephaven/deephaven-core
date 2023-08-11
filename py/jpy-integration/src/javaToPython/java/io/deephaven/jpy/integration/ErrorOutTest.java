/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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


public class ErrorOutTest extends PythonTest {
    // ----- error out -----
    @Test(expected = RuntimeException.class)
    @Ignore("Need to wait for the next JPY release to fix https://github.com/jpy-consortium/jpy/issues/98")
    public void errorIntOutFromNone() {
        String expr = "[None, []]";
        PyObject in = PyObject.executeCode(expr, PyInputMode.EXPRESSION);
        in.asList().get(0).getIntValue();
    }

    @Test
    public void intRangeTest() {
        String max_int_expr = "2**31 - 1";
        PyObject in = PyObject.executeCode(max_int_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(Integer.MAX_VALUE, in.getIntValue());

        String min_int_expr = "-2**31";
        in = PyObject.executeCode(min_int_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(Integer.MIN_VALUE, in.getIntValue());

        String max_long_expr = "2**63 - 1";
        in = PyObject.executeCode(max_long_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(-1, in.getIntValue());

        String min_long_expr = "-2**63";
        in = PyObject.executeCode(min_long_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(0, in.getIntValue());
    }

    @Test(expected = RuntimeException.class)
    @Ignore("Need to wait for the next JPY release to fix https://github.com/jpy-consortium/jpy/issues/98")
    public void overflowTest() {
        String beyond_long_expr = "2**64";
        PyObject in = PyObject.executeCode(beyond_long_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(-1, in.getIntValue());
    }

    @Test(expected = RuntimeException.class)
    @Ignore("Need to wait for the next JPY release to fix https://github.com/jpy-consortium/jpy/issues/98")
    public void errorLongOutFromNone() {
        String expr = "[None, []]";
        PyObject in = PyObject.executeCode(expr, PyInputMode.EXPRESSION);
        in.asList().get(0).getLongValue();
    }

    @Test
    public void longRangeTest() {
        String max_long_expr = "2**63 - 1";
        PyObject in = PyObject.executeCode(max_long_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(Long.MAX_VALUE, in.getLongValue());

        String min_long_expr = "-2**63";
        in = PyObject.executeCode(min_long_expr, PyInputMode.EXPRESSION);
        Assert.assertEquals(Long.MIN_VALUE, in.getLongValue());
    }

    @Test(expected = RuntimeException.class)
    public void errorDoubleOutFromNone() {
        String expr = "[None, []]";
        PyObject in = PyObject.executeCode(expr, PyInputMode.EXPRESSION);
        in.asList().get(0).getDoubleValue();
    }
}
