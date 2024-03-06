//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.python;

import io.deephaven.engine.util.PyCallableWrapperJpyImpl;
import io.deephaven.integrations.python.PythonObjectWrapper;

public abstract class PythonImportInitializer {
    public static void init() {
        // ensured that these classes are initialized during Jpy initialization as they import python modules and we'd
        // like to avoid deadlocking the GIL during class initialization
        PythonObjectWrapper.init();
        PyCallableWrapperJpyImpl.init();
    }

    private PythonImportInitializer() {
        // no instances should be created
    }
}
