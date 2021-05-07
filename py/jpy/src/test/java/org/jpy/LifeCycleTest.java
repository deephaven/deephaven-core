package org.jpy;

import org.junit.Assert;
import org.junit.Test;

public class LifeCycleTest {
    private static final boolean ON_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");

    @Test
    public void testCanStartAndStopWithoutException() {
        PyLib.startPython();
        Assert.assertTrue(PyLib.isPythonRunning());
        PyModule sys1 = PyModule.importModule("sys");
        Assert.assertNotNull(sys1);
        final long sys1Pointer = sys1.getPointer();

        PyLib.stopPython();
        if (!ON_WINDOWS) {
            Assert.assertFalse(PyLib.isPythonRunning());
        }

        PyLib.startPython();
        Assert.assertTrue(PyLib.isPythonRunning());

        PyModule sys2 = PyModule.importModule("sys");
        Assert.assertNotNull(sys2);
        final long sys2Pointer = sys2.getPointer();

        if (!ON_WINDOWS) {
            Assert.assertNotEquals(sys1Pointer, sys2Pointer);
        }

        PyLib.stopPython();
        if (!ON_WINDOWS) {
            Assert.assertFalse(PyLib.isPythonRunning());
        }
    }
}
