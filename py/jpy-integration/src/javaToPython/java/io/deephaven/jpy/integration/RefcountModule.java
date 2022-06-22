/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import org.jpy.PyObject;

import static io.deephaven.jpy.PythonTest.readResource;

public interface RefcountModule extends AutoCloseable {

    static RefcountModule of(CreateModule createModule) {
        return createModule.callAsFunctionModule(
                "refcount_module",
                readResource(RefcountModule.class, "refcount.py"),
                RefcountModule.class);
    }

    static int refcount(RefcountModule module, PyObject pyObject) {
        return module.refcount(pyObject.getPointer());
    }

    int refcount(long address);

    @Override
    void close();
}
