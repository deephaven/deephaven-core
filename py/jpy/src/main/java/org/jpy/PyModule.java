/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jpy;

import java.util.Objects;
import static org.jpy.PyLib.assertPythonRuns;

/**
 * Represents a Python module.
 *
 * @author Norman Fomferra
 * @since 0.7
 */
public class PyModule extends PyObject {

    /**
     * The Python module's name.
     */
    private final String name;

    PyModule(String name, long pointer) {
        super(pointer);
        this.name = name;
    }

    /**
     * @return The Python module's name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the Python interpreter's main module and return its Java representation.
     * It can be used to access global scope variables and functions. For example:
     * <pre>
     *      PyLib.execScript("def incByOne(x): return x + 1");
     *      PyModule mainModule = PyModule.getMain();
     *      PyObject eleven = mainModule.call("incByOne", 10);
     * </pre>
     *
     * @return The Python main module's Java representation.
     * @since 0.8
     */
    public static PyModule getMain() {
        return importModule("__main__");
    }

    /**
     * Get the Python interpreter's buildins module and returns its Java representation.
     * It can be used to call functions such as {@code len()}, {@code type()}, {@code list()}, etc. For example:
     * <pre>
     *      builtins = PyModule.getBuiltins();
     *      PyObject size = builtins.call("len", pyList);
     * </pre>
     *
     * @return Java representation of Python's builtin module.
     * @see org.jpy.PyObject#call(String, Object...)
     * @since 0.8
     */
    public static PyModule getBuiltins() {
        try {
            //Python 3.3+
            return importModule("builtins");
        } catch (Exception e) {
            //Python 2.7
            return importModule("__builtin__");
        }
    }

    /**
     * Import a Python module into the Python interpreter and return its Java representation.
     *
     * @param name The Python module's name.
     * @return The Python module's Java representation.
     */
    public static PyModule importModule(String name) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        long pointer = PyLib.importModule(name);
        return pointer != 0 ? new PyModule(name, pointer) : null;
    }

    /**
     * Extends Python's 'sys.path' variable by the given module path.
     *
     * @param modulePath The new module path. Should be an absolute pathname.
     * @param prepend    If true, the new path will be the new first element of 'sys.path', otherwise it will be the last.
     * @return The altered 'sys.path' list.
     * @since 0.8
     */
    public static PyObject extendSysPath(String modulePath, boolean prepend) {
        Objects.requireNonNull(modulePath, "path must not be null");
        try (final PyModule sys = importModule("sys")) {
            final PyObject sysPath = sys.getAttribute("path");
            if (prepend) {
                //noinspection EmptyTryBlock
                try (final PyObject pyObj = sysPath.call("insert", 0, modulePath)) {

                }
            } else {
                //noinspection EmptyTryBlock
                try (final PyObject pyObj = sysPath.call("append", modulePath)) {

                }
            }
            return sysPath;
        }
    }


    /**
     * Create a Java proxy instance of this Python module which contains compatible functions to the ones provided in the
     * interface given by the {@code type} parameter.
     *
     * @param type The interface's type.
     * @param <T>  The interface name.
     * @return A (proxy) instance implementing the given interface.
     */
    @Override
    public <T> T createProxy(Class<T> type) {
        assertPythonRuns();
        Objects.requireNonNull(type, "type must not be null");
        return (T) createProxy(PyLib.CallableKind.FUNCTION, type);
    }
}
