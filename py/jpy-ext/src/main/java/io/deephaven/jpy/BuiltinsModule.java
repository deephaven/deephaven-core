package io.deephaven.jpy;

import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

/**
 * Java proxy module for <a href="https://docs.python.org/3/library/builtins.html">builtins</a>.
 */
public interface BuiltinsModule extends AutoCloseable {

    static BuiltinsModule create() {
        if (PyLib.getPythonVersion().startsWith("3")) {
            return PyModule.importModule("builtins").createProxy(BuiltinsModule.class);
        } else {
            return PyModule.importModule("__builtin__").createProxy(BuiltinsModule.class);
        }
    }

    /**
     * Get the list of names in the current local scope.
     *
     * @return the list of names in the current local scope
     * @see <a href="https://docs.python.org/3/library/functions.html#dir">dir</a>
     */
    // PyObject dir();

    /**
     * Attempt to return a list of valid attributes for that object.
     *
     * @param pyObject the python object
     * @return the list of valid attributes for the object
     * @see <a href="https://docs.python.org/3/library/functions.html#dir">dir</a>
     */
    PyObject dir(PyObject pyObject);

    /**
     * Create a new dictionary.
     *
     * @return the dictionary
     * @see <a href="https://docs.python.org/3/library/functions.html#func-dict>dict</a>
     */
    PyObject dict();

    /**
     * Get the length (the number of items) of an object. The argument may be a sequence (such as a
     * string, bytes, tuple, list, or range) or a collection (such as a dictionary, set, or frozen
     * set).
     *
     * @param pyObject the python object
     * @return the length (the number of items) of an object
     * @see <a href="https://docs.python.org/3/library/functions.html#len">len</a>
     */
    int len(PyObject pyObject);

    /**
     * Create a memoryview that references obj. obj must support the buffer protocol.
     *
     * @param pyObject the python object.
     * @return the memoryview
     * @see <a href="https://docs.python.org/3/library/stdtypes.html#typememoryview">memoryview</a>
     */
    PyObject memoryview(PyObject pyObject);

    PyObject compile(String expression, String filename, String mode);

    PyObject globals();

    PyObject locals();


    @Override
    void close();
}
