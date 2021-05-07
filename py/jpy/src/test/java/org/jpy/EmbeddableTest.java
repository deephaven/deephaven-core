package org.jpy;

import java.io.Closeable;
import java.util.Objects;

/**
 * This class is meant to be invoked both from junit via {@link EmbeddableTestJunit}, and embedded
 * in python (see jpy_java_embeddable_test.py).
 *
 * For simplicity purposes, we don't want to have any external dependencies (ie, junit), so we can
 * keep the embedded classpath simple.
 */
public class EmbeddableTest {
    private static final boolean ON_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");

    public static void testStartingAndStoppingIfAvailable() {
        try (PyLibControl controller = new PyLibControl()) {

        }
    }

    public static void testPassStatement() {
        try (PyLibControl controller = new PyLibControl()) {
            PyObject object = PyObject.executeCode("pass", PyInputMode.STATEMENT);
            assertNotNull(object);
            assertNull(object.getObjectValue());
        }
    }

    public static void testPrintStatement() {
        try (PyLibControl controller = new PyLibControl()) {
            PyObject.executeCode("print('hi')", PyInputMode.STATEMENT);
        }
    }

    public static void testIncrementByOne() {
        try (PyLibControl controller = new PyLibControl()) {
            PyObject.executeCode("def incByOne(x): return x + 1", PyInputMode.SCRIPT);
            PyObject results = PyModule.getMain().call("incByOne", 41);
            assertNotNull(results);
            assertTrue(results.isInt() || results.isLong());
            assertEquals(results.getIntValue(), 42);
        }
    }

    private static void assertTrue(boolean b) {
        if (!b) {
            throw new AssertionError();
        }
    }

    private static void assertFalse(boolean b) {
        if (!b) {
            throw new AssertionError();
        }
    }

    private static void assertNotNull(Object o) {
        if (o == null) {
            throw new AssertionError();
        }
    }

    private static void assertNull(Object o) {
        if (o != null) {
            throw new AssertionError();
        }
    }

    private static void assertNotEquals(Object a, Object b) {
        if (Objects.equals(a, b)) {
            throw new AssertionError();
        }
    }

    private static void assertEquals(int a, int b) {
        if (a != b) {
            throw new AssertionError();
        }
    }

    private static class PyLibControl implements Closeable {
        private final boolean weAreEmbeddingPython;

        PyLibControl() {
            weAreEmbeddingPython = !PyLib.isPythonRunning();
            if (weAreEmbeddingPython) {
                PyLib.startPython();
            }
            assertTrue(PyLib.isPythonRunning());
        }

        @Override
        public void close() {
            if (weAreEmbeddingPython) {
                PyLib.stopPython();
                if (!ON_WINDOWS) {
                    assertFalse(PyLib.isPythonRunning());
                }
            }
        }
    }
}
