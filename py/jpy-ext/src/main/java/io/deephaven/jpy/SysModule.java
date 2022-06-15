/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import org.jpy.PyModule;
import org.jpy.PyObject;

/**
 * Java proxy module for <a href="https://docs.python.org/3/library/sys.html">sys</a>.
 */
public interface SysModule extends AutoCloseable {

    static SysModule create() {
        return PyModule.importModule("sys").createProxy(SysModule.class);
    }

    /**
     * Get the reference count for an object.
     *
     * @param o the object
     * @return the reference count of the object
     * @see <a href="https://docs.python.org/3/library/sys.html#sys.getrefcount">getrefcount</a>
     * @deprecated Users should strongly prefer to use {@link #getrefcount(PyObject)}.
     */
    @Deprecated
    int getrefcount(Object o);

    /**
     * Get the reference count for an object.
     *
     * @param pyObject the python object
     * @return the reference count of the object
     * @see <a href="https://docs.python.org/3/library/sys.html#sys.getrefcount">getrefcount</a>
     */
    int getrefcount(PyObject pyObject);

    @Override
    void close();
}
