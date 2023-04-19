/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyModule;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyObject;
import org.junit.After;
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

    @Test(expected = RuntimeException.class)
    @Ignore("Need to wait for the next JPY release to fix https://github.com/jpy-consortium/jpy/issues/98")
    public void errorLongOutFromNone() {
        String expr = "[None, []]";
        PyObject in = PyObject.executeCode(expr, PyInputMode.EXPRESSION);
        in.asList().get(0).getLongValue();
    }

    @Test(expected = RuntimeException.class)
    @Ignore("Need to wait for the next JPY release to fix https://github.com/jpy-consortium/jpy/issues/98")
    public void errorDoubleOutFromNone() {
        String expr = "[None, []]";
        PyObject in = PyObject.executeCode(expr, PyInputMode.EXPRESSION);
        in.asList().get(0).getDoubleValue();
    }
}
