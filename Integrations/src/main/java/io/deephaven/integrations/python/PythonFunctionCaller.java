/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;
import java.util.function.Function;

import static io.deephaven.integrations.python.PythonUtilities.pyApplyFunc;

/**
 * A class which calls a Python callable.
 */
@ScriptApi
public class PythonFunctionCaller implements Function<Object[], Object> {
    private final PyObject pyCallable;

    /**
     * Creates a {@link Function} which calls a Python function.
     *
     * @param pyObjectIn the python object providing the function - must either be callable or have an `apply` attribute
     *        which is callable.
     */
    public PythonFunctionCaller(final PyObject pyObjectIn) {

        pyCallable = pyApplyFunc(pyObjectIn);
    }

    @Override
    public Object apply(Object... args) {
        return pyCallable.call("__call__", args);
    }
}
