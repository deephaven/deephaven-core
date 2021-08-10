/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;
import java.util.function.Function;

/**
 * A class which calls a Python callable.
 */
@ScriptApi
public class PythonFunctionCaller implements Function<Object[], Object>{
    private final PyObject pyCallable;

    /**
     * Creates a {@link Function} which calls a Python function.
     *
     * @param pyObjectIn the python object providing the function - must either be callable or
     *                   have an `apply` attribute which is callable.
     */
    public PythonFunctionCaller(final PyObject pyObjectIn){
        if(pyObjectIn.hasAttribute("apply")){
            pyCallable = pyObjectIn.getAttribute("apply");
            if (!pyCallable.hasAttribute("__call__")){
                throw new IllegalArgumentException("The Python object provided has an apply attribute " +
                        "which is not callable");
            }
        }else if (pyObjectIn.hasAttribute("__call__")){
            pyCallable = pyObjectIn;
        }else{
            throw new IllegalArgumentException("The Python object specified should either be callable, or a " +
                    "class instance with an apply method");
        }
    }

    /**
     * Calls the function defined in pyCallable with the given args
     *
     * @param args arguments to pass to the Python function
     * @return the result of the function call
     */
    @Override
    public Object apply(Object ... args) {
        PyObject out = pyCallable.call("__call__", args);
        return out;
    }
}
