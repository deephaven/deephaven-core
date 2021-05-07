package org.jpy.fixtures;

import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class EvalTestFixture {

    public static PyObject expression(String expression) {
        return PyObject.executeCode(expression,PyInputMode.EXPRESSION, PyLib.getCurrentGlobals(),PyLib.getCurrentLocals());
    }

    public static void script(String expression) {
        PyObject.executeCode(expression,PyInputMode.SCRIPT, PyLib.getCurrentGlobals(),PyLib.getCurrentLocals());
    }

}
