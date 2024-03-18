//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import org.jpy.PyModule;
import org.jpy.PyObject;

/**
 * Java proxy module for <a href="https://docs.python.org/3/library/gc.html">gc</a>.
 */
public interface GcModule extends AutoCloseable {

    static GcModule create() {
        return PyModule.importModule("gc").createProxy(GcModule.class);
    }

    /**
     * Run a full collection.
     *
     * @return the number of unreachable objects found
     * @see <a href="https://docs.python.org/3/library/gc.html#gc.collect">collect</a>
     */
    int collect();

    /**
     *
     * Get the referrers for an object. This method should be used for debugging only.
     *
     * @param pyObject the python object
     * @return the list of objects that directly refer to any of objs
     * @see <a href="https://docs.python.org/3/library/gc.html#gc.get_referrers">get_referrers</a>
     */
    PyObject get_referrers(PyObject pyObject);

    @Override
    void close();
}
