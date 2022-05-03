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
 *
 * This file was modified by Deephaven Data Labs.
 *
 */

package org.jpy;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.jpy.PyLibConfig.*;

/**
 * Represents the library that provides the Python interpreter (CPython).
 * <p>
 * When the {@code PyLib} class is loaded, it reads its configuration from a Java properties file called {@code .jpy}
 * which must exist in the current user's home directory. The configuration file has been written to this location
 * by installing the Python jpy module using {@code python3 setup.py install --user} on Unix
 * and {@code python setup.py install}) on Windows.
 * <p>
 * Currently, the following properties are recognised in the {@code .jpy} file:
 * <ul>
 * <li>{@code python.lib} - the Python shared library (usually required on Unix only)</li>
 * <li>{@code jpy.lib} - the jpy shared library path for Python (Unix: {@code jpy*.so}, Windows: {@code jpy*.pyd})</li>
 * </ul>
 * <p>
 * jpy API clients should first call {@link #isPythonRunning()} in order to check if a Python interpreter is already available.
 * If not, {@link #startPython(String...)} must be called before any other jpy API is used.
 * <p>
 * <i>Important note for developers: If you change the signature of any of the native {@code PyLib} methods,
 * you must first run {@code javah} on the compiled class, and then adapt {@code src/main/c/jni/org_jpy_PyLib.c}.</i>
 *
 * @author Norman Fomferra
 * @since 0.7
 */
@SuppressWarnings("WeakerAccess")
public class PyLib {

    private static final boolean DEBUG = Boolean.getBoolean("jpy.debug");
    private static final boolean ON_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
    private static final boolean STOP_IS_NO_OP = Boolean.getBoolean("jpy.stopIsNoOp") || ON_WINDOWS;
    private static String dllFilePath;
    private static Throwable dllProblem;
    private static boolean dllLoaded;

    /**
     * The kind of callable Python objects.
     */
    public enum CallableKind {
        /**
         * Function call without the Python {@code self} as first argument.
         */
        FUNCTION,
        /**
         * An instance method call with the Python {@code self} (= the instance) as first argument.
         */
        METHOD,
    }

    /**
     * Controls output of diagnostic information for debugging.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class Diag {

        static {
            PyLib.loadLib();
        }

        /**
         * Print no diagnostic information at all.
         */
        public static final int F_OFF = 0x00;
        /**
         * Print diagnostic information while Java types are resolved.
         */
        public static final int F_TYPE = 0x01;
        /**
         * Print diagnostic information while Java methods overloads are selected.
         */
        public static final int F_METH = 0x02;
        /**
         * Print diagnostic information when code execution flow is passed from Java to Python or the other way round.
         */
        public static final int F_EXEC = 0x04;
        /**
         * Print diagnostic information about memory allocation/deallocation.
         */
        public static final int F_MEM = 0x08;
        /**
         * Print diagnostic information usage of the Java VM Invocation API.
         */
        public static final int F_JVM = 0x10;
        /**
         * Print diagnostic information if erroneous states are detected in the jpy Python module.
         */
        public static final int F_ERR = 0x20;
        /**
         * Print any diagnostic information.
         */
        public static final int F_ALL = 0xff;

        /**
         * @return the current diagnostic flags.
         */
        public static native int getFlags();

        /**
         * Sets the current diagnostic flags.
         *
         * @param flags the current diagnostic flags.
         */
        public static native void setFlags(int flags);

        private Diag() {
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static String getDllFilePath() {
        return dllFilePath;
    }


    /**
     * Throws a runtime exception if Python interpreter is not running. Possible reasons for this are
     * <ul>
     * <li>You have not called {@link #startPython(String...)} yet.</li>
     * <li>The Python shared library code for the Python interpreter could not be found or could not be be loaded.</li>
     * <li>The Python shared library code for the Python 'jpy' module could not be found or could not be be loaded.</li>
     * <li>The Python interpreter could not be initialised.</li>
     * </ul>
     */
    public static void assertPythonRuns() {
        if (dllProblem != null) {
            throw new RuntimeException("PyLib not initialized", dllProblem);
        }
        if (!isPythonRunning()) {
            throw new RuntimeException("PyLib not initialized");
        }
    }

    /**
     * @return {@code true} if the Python interpreter is running and the the 'jpy' module has been loaded.
     */
    public static native boolean isPythonRunning();

    /**
     * Delegates to {@link #startPython(int, String...)} with {@code flags = Diag.F_OFF}.
     */
    public static void startPython(String... extraPaths) {
        startPython(Diag.F_OFF, extraPaths);
    }

    /**
     * Starts the Python interpreter. It does the following:
     * <ol>
     * <li>Initializes the Python interpreter, if not already running.</li>
     * <li>Prepends any given extra paths to Python's 'sys.path' (e.g. so that 'jpy' can be loaded from isolated directories).</li>
     * <li>Imports the 'jpy' extension module, if not already done.</li>
     * </ol>
     *
     * @param flags      If non-zero, is passed to {@link Diag#setFlags(int)} before python is started
     * @param extraPaths List of paths that will be prepended to Python's 'sys.path'.
     * @throws RuntimeException if Python could not be started or if the 'jpy' extension module could not be loaded.
     */
    public static void startPython(int flags, String... extraPaths) {
        ArrayList<File> dirList = new ArrayList<>(1 + extraPaths.length);

        File moduleDir = new File(dllFilePath).getParentFile();
        if (moduleDir != null) {
            dirList.add(moduleDir.getAbsoluteFile());
        }

        for (String extraPath : extraPaths) {
            File extraDir = new File(extraPath).getAbsoluteFile();
            if (!dirList.contains(extraDir)) {
                dirList.add(extraDir);
            }
        }

        extraPaths = new String[dirList.size()];
        for (int i = 0; i < dirList.size(); i++) {
            extraPaths[i] = dirList.get(i).getPath();
        }

        if (DEBUG) {
            System.out.printf("org.jpy.PyLib: Starting Python with %d extra module path(s):%n", extraPaths.length);
            for (String path : extraPaths) {
                System.out.printf("org.jpy.PyLib:   %s%n", path);
            }
            Diag.setFlags(Diag.F_EXEC | flags);
        } else if (flags != 0) {
            Diag.setFlags(flags);
        }

        startPython0(extraPaths);
    }

    static native boolean startPython0(String... paths);

    /**
     * Does the equivalent of setting the PYTHONHOME environment variable.  If used,
     * this must be called prior to calling {@code startPython()}.
     * Supported for Python 2.7, and Python 3.5 or higher
     * @param pythonHome Path to Python Home (must be less than 256 characters!)
     * @return true if successful, false if it fails
     */
    public static native boolean setPythonHome(String pythonHome);

    /**
     * Useful for virtual environments, helps in setting sys.prefix/exec_prefix.
     * If used, this must be called prior to calling {@code startPython()}.
     * @param programName Path to Python executable (must be less than 256 characters!)
     * @return true if successful, false if it fails
     * @see <a href="https://docs.python.org/2/c-api/init.html#c.Py_SetProgramName">Py_SetProgramName (2)</a>
     * @see <a href="https://docs.python.org/3/c-api/init.html#c.Py_SetProgramName">Py_SetProgramName (3)</a>
     */
    public static native boolean setProgramName(String programName);

    /**
     * @return The Python interpreter version string.
     */
    public static native String getPythonVersion();

    /**
     * Stops the Python interpreter.
     *
     * <strong>Important note:</strong> Stopping the Python interpreter again after it has been restarted using
     * {@link #startPython} currently causes a fatal error in the the Java Runtime Environment originating from
     * the Python interpreter (function {@code Py_Finalize} in standard CPython implementation).
     * There is currently no workaround for that problem other than not restarting the Python interpreter from
     * your code.
     * For more information refer to https://github.com/bcdev/jpy/issues/70
     */
    public static void stopPython() {
        PyObject.cleanup();
        if (!STOP_IS_NO_OP) {
            stopPython0();
        }
    }

    static native void stopPython0();

    @Deprecated
    public static native int execScript(String script);

    /**
     * Callers must close the returned reference with {@link #decRef(long)}.
     */
    static native long executeCode(String code, int start, Object globals, Object locals);

    /**
     * Callers must close the returned reference with {@link #decRef(long)}.
     */
    static native long executeScript
            (String file, int start, Object globals, Object locals) throws FileNotFoundException;

    public static native PyObject getMainGlobals();

    /**
     * Return a dictionary of the global variables in the current execution frame, or NULL if no
     * frame is currently executing.
     *
     * @return the current globals, or null
     * @see <a href="https://docs.python.org/2/c-api/reflection.html#c.PyEval_GetGlobals">PyEval_GetGlobals (2)</a>
     * @see <a href="https://docs.python.org/3/c-api/reflection.html#c.PyEval_GetGlobals">PyEval_GetGlobals (3)</a>
     */
    public static native PyObject getCurrentGlobals();

    /**
     * Return a dictionary of the local variables in the current execution frame, or NULL if no
     * frame is currently executing.
     *
     * @return the current locals, or null
     * @see <a href="https://docs.python.org/2/c-api/reflection.html#c.PyEval_GetLocals">PyEval_GetLocals (2)</a>
     * @see <a href="https://docs.python.org/3/c-api/reflection.html#c.PyEval_GetLocals">PyEval_GetLocals (3)</a>
     */
    public static native PyObject getCurrentLocals();

    static native PyObject copyDict(long pyPointer);

    static native void incRef(long pointer);

    static native void decRef(long pointer);

    static native void decRefs(long[] pointers, int len);

    static native int getIntValue(long pointer);

    static native long getLongValue(long pointer);

    static native boolean getBooleanValue(long pointer);

    static native double getDoubleValue(long pointer);

    static native String getStringValue(long pointer);

    static native Object getObjectValue(long pointer);

    static native boolean isConvertible(long pointer);
    static native boolean pyNoneCheck(long pointer);
    static native boolean pyDictCheck(long pointer);
    static native boolean pyListCheck(long pointer);
    static native boolean pyBoolCheck(long pointer);
    static native boolean pyIntCheck(long pointer);
    static native boolean pyLongCheck(long pointer);
    static native boolean pyFloatCheck(long pointer);
    static native boolean pyStringCheck(long pointer);
    static native boolean pyCallableCheck(long pointer);
    static native boolean pyFunctionCheck(long pointer);
    static native boolean pyModuleCheck(long pointer);
    static native boolean pyTupleCheck(long pointer);

    static native long getType(long pointer);

    static native String str(long pointer);

    static native String repr(long pointer);

    static native long hash(long pointer);

    static native boolean eq(long pointer1, Object other);

    static native PyObject newDict();

    /**
     * https://docs.python.org/2/c-api/dict.html#c.PyDict_Keys
     * https://docs.python.org/3/c-api/dict.html#c.PyDict_Keys
     * @return Return a PyListObject containing all the keys from the dictionary.
     */
    static native PyObject pyDictKeys(long pointer);

    /**
     * https://docs.python.org/2/c-api/dict.html#c.PyDict_Values
     * https://docs.python.org/3/c-api/dict.html#c.PyDict_Values
     * @return Return a PyListObject containing all the values from the dictionary p.
     */
    static native PyObject pyDictValues(long pointer);

    /**
     * Determine if dictionary dict contains key.
     * This is equivalent to the Python expression `key in dict`
     *
     * https://docs.python.org/2/c-api/dict.html#c.PyDict_Contains
     * https://docs.python.org/3/c-api/dict.html#c.PyDict_Contains
     *
     * @param dict     the dictionary
     * @param key      the key
     * @param keyClass Optional type for converting the key to a Python object
     * @return True iff key is in dict.
     */
    static native <T> boolean pyDictContains(long dict, T key, Class<? extends T> keyClass);

    static native <T> T[] getObjectArrayValue(long pointer, Class<? extends T> itemType);

    /**
     * Callers must close the returned reference with {@link #decRef(long)}.
     */
    static native long importModule(String name);

    /**
     * Gets the value of a given Python attribute as Python object pointer.
     * <p>
     * Callers must close the returned reference with {@link #decRef(long)}.
     *
     * @param pointer Identifies the Python object which contains the attribute {@code name}.
     * @param name    The attribute name.
     * @return Pointer to a Python object that is the value of the attribute (always a new reference).
     */
    static native long getAttributeObject(long pointer, String name);

    /**
     * Gets the value of a given Python attribute as Java value.
     *
     * @param pointer   Identifies the Python object which contains the attribute {@code name}.
     * @param name      The attribute name.
     * @param valueType The expected return type.
     * @return A value that represents the converted Python attribute value.
     */
    static native <T> T getAttributeValue(long pointer, String name, Class<? extends T> valueType);

    /**
     * Sets the Python attribute given by {@code name} of the Python object pointed to by {@code pointer}.
     * <p>
     * Before the Python attribute is set, the Java {@code value} is converted into a corresponding
     * Python object using the optional {@code valueType}.
     * The {@code value} may also be of type {@code PyObject}.
     * In this case, it will be directly translated into the corresponding Python object without conversion.
     *
     * @param pointer   Identifies the Python object which contains the attribute {@code name}.
     * @param name      The attribute name.
     * @param value     The new attribute value.
     * @param valueType Optional type for converting the value to a Python object.
     */
    static native <T> void setAttributeValue(long pointer, String name, T value, Class<? extends T> valueType);

    /**
     * Deletes the Python attribute given by {@code name} of the Python object pointed to by {@code pointer}.
     * <p>
     *
     * @param pointer Identifies the Python object which contains the attribute {@code name}.
     * @param name    The attribute name.
     */
    static native void delAttribute(long pointer, String name);

    /**
     * Checks for the existence the Python attribute given by {@code name} of the Python object pointed to by {@code pointer}.
     * <p>
     *
     * @param pointer Identifies the Python object which contains the attribute {@code name}.
     * @param name    The attribute name.
     * @return true if the Python object has an attribute named {@code name}
     */
    static native boolean hasAttribute(long pointer, String name);

    public static native boolean hasGil();

    public static native <T> T ensureGil(Supplier<T> runnable);

    /**
     * Calls a Python callable and returns the resulting Python object.
     * <p>
     * Before the Python callable is called, the {@code args} array of Java objects is converted into corresponding
     * Python objects.
     * The {@code args} array may also contain objects of type {@code PyObject}.
     * These will be directly translated into the corresponding Python objects without conversion.
     * <p>
     * Callers must close the returned reference with {@link #decRef(long)}.
     *
     * @param pointer    Identifies the Python object which contains the callable {@code name}.
     * @param methodCall true, if this is a call of a method of the Python object pointed to by {@code pointer}.
     * @param name       The name of the callable.
     * @param argCount   The argument count (length of the following {@code args} array).
     * @param args       The arguments.
     * @param paramTypes Optional array of parameter types for the conversion of the {@code args} into a Python tuple.
     *                   If not null, it must be an array of the same length as {@code args}.
     * @return The resulting Python object (always a new reference).
     */
    static native long callAndReturnObject(long pointer,
                                           boolean methodCall,
                                           String name,
                                           int argCount,
                                           Object[] args,
                                           Class<?>[] paramTypes);

    /**
     * Calls a Python callable and returns the a Java Object.
     * <p>
     * Before the Python callable is called, the {@code args} array of Java objects is converted into corresponding
     * Python objects. The return value of the Python call is converted to a Java object according the the given
     * return type.
     * The {@code args} array may also contain objects of type {@code PyObject}.
     * These will be directly translated into the corresponding Python objects without conversion.
     *
     * @param pointer    Identifies the Python object which contains the callable {@code name}.
     * @param methodCall true, if this is a call of a method of the Python object pointed to by {@code pointer}.
     * @param name       The name of the callable.
     * @param argCount   The argument count (length of the following {@code args} array).
     * @param args       The arguments.
     * @param paramTypes Optional array of parameter types for the conversion of the {@code args} into a Python tuple.
     *                   If not null, it must be an array of the same length as {@code args}.
     * @param returnType Optional return type.
     * @return The resulting Python object (always a new reference).
     */
    static native <T> T callAndReturnValue(long pointer,
                                           boolean methodCall,
                                           String name,
                                           int argCount,
                                           Object[] args,
                                           Class<?>[] paramTypes,
                                           Class<T> returnType);

    private static void loadLib() {
        if (dllLoaded || dllProblem != null) {
            return;
        }
        try {
            String pythonLibPath = getProperty(PYTHON_LIB_KEY, false);
            if (pythonLibPath != null && new File(pythonLibPath).isFile()) {
                preloadPythonLib(pythonLibPath);
            }

            // E.g. dllFilePath = "/usr/local/lib/python3.3/dist-packages/jpy.cpython-33m.so";
            dllFilePath = getProperty(JPY_LIB_KEY, true);
            dllFilePath = new File(dllFilePath).getAbsolutePath();

            if (DEBUG) System.out.printf("org.jpy.PyLib: System.load(\"%s\")%n", dllFilePath);
            //if (DEBUG) System.out.println("org.jpy.PyLib: context class loader: " + Thread.currentThread().getContextClassLoader());
            //if (DEBUG) System.out.println("org.jpy.PyLib: class class loader:   " + PyLib.class.getClassLoader());

            System.load(dllFilePath);
            dllProblem = null;
            dllLoaded = true;
        } catch (Throwable t) {
            dllProblem = t;
            throw t;
        }
    }

    private static void preloadPythonLib(String pythonLibPath) {
        if (getOS() != OS.WINDOWS) {
            // For PyLib, we load the shared library that was generated for the Python extension module 'jpy'.
            // However, to use 'jpy' from Java we also need the Python shared library to be loaded as well.
            // On Windows, this is done auto-magically, on Linux and Darwin we have to either change 'setup.py'
            // to also include a dependency to the Python shared lib or, as done here, explicitly load it.
            //
            // If the Python shared lib is not found, we get error messages similar to the following:
            // java.lang.UnsatisfiedLinkError: /usr/local/lib/python3.3/dist-packages/jpy.cpython-33m.so:
            //      /usr/local/lib/python3.3/dist-packages/jpy.cpython-33m.so: undefined symbol: PyFloat_Type
            if (DEBUG)
                System.out.printf("org.jpy.PyLib: DL.dlopen(\"%s\", DL.RTLD_GLOBAL + DL.RTLD_LAZY%n", pythonLibPath);

            long handle = DL.dlopen(pythonLibPath, DL.RTLD_GLOBAL + DL.RTLD_LAZY);
            if (handle == 0) {
                String message = "Failed to load Python shared library '" + pythonLibPath + "'";
                String dlError = DL.dlerror();
                if (dlError != null) {
                    message += ": " + dlError;
                }
                throw new RuntimeException(message);
            }
        } else {
            // Fixes https://github.com/bcdev/jpy/issues/58
            // Loading of jpy DLL fails for user-specific Python installations on Windows
            if (DEBUG) System.out.printf("org.jpy.PyLib: System.load(\"%s\")%n", pythonLibPath);
            try {
                System.load(pythonLibPath);
            } catch (Exception e) {
                String message = "Failed to load Python shared library '" + pythonLibPath + "': " + e.getMessage();
                System.err.println(message);
            }
        }
    }

    private PyLib() {
    }

    /**
     * We call this method to ensure that this class gets loaded, and thus the static block gets run
     *
     * We don't technically need this method. Callers could instead rely on reflection, using
     * {@link Class#forName(String)}, but this method is better because it is much more explicit.
     */
    static void dummyMethodForInitialization() { }

    static {
        // see documentation in PyLibInitializer for explanation
        PyLibInitializer.pyLibInitialized = true;
        if (DEBUG) System.out.println("org.jpy.PyLib: entered static initializer");
        loadLib();
        if (DEBUG) System.out.println("org.jpy.PyLib: exited static initializer");
    }
}


