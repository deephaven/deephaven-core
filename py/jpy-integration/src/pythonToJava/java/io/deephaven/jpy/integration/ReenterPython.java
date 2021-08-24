package io.deephaven.jpy.integration;

import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;

public class ReenterPython {
    public static int calc1Plus41InPython() {
        if (!PyLib.isPythonRunning()) {
            throw new IllegalStateException("Expected python to already be running");
        }
        PyObject result = PyObject.executeCode("1 + 41", PyInputMode.EXPRESSION);
        if (!result.isInt()) {
            throw new RuntimeException("Expected int");
        }
        int intResult = result.getIntValue();
        if (intResult != 42) {
            throw new RuntimeException("Expected 42");
        }
        return intResult;
    }
}
