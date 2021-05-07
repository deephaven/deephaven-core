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

/**
 * A replacement for {@link System#load(String)} with support for POSIX {@code dlopen} flags.
 * <p>
 * <i>Important note: This class is useful on POSIX (Unix/Linux) systems only. On Windows OSes, all methods
 * are no-ops.</i>
 *
 * @author Norman Fomferra
 * @see <a href="http://man7.org/linux/man-pages/man3/dlopen.3.html">dlopen(3) - Linux manual page</a>
 * @since 0.7
 */
public class DL {
    /**
     * Resolve undefined symbols as code from the dynamic library is executed.
     */
    public static final int RTLD_LAZY = 0x0001;
    /**
     * Resolve all undefined symbols before {@link #dlopen} returns and fail if this cannot be done.
     */
    public static final int RTLD_NOW = 0x0002;
    /**
     * This is the converse of RTLD_GLOBAL, and the default if neither flag is specified.
     */
    public static final int RTLD_LOCAL = 0x0004;
    /**
     * External symbols defined in the library will be made available to subsequently loaded libraries.
     */
    public static final int RTLD_GLOBAL = 0x0008;

    /**
     * loads the dynamic library file named by the null-terminated string filename and returns
     * an opaque "handle" for the dynamic library. If filename is {@code null}, then the returned handle
     * is for the main program. If filename contains a slash ("/"), then it is interpreted as a
     * (relative or absolute) pathname.
     *
     * @param filename dynamic library filename or {@code null}
     * @param flag     combination of {@link #RTLD_GLOBAL} or {@link #RTLD_LOCAL} with {@link #RTLD_LAZY},
     *                 {@link #RTLD_NOW}.
     * @return opaque "handle" for the dynamic library.
     */
    public static native long dlopen(String filename, int flag);

    public static native int dlclose(long handle);

    public static native String dlerror();

    static {
        // see documentation in PyLibInitializer for explanation
        PyLibInitializer.dlInitialized = true;
        try {
            System.loadLibrary("jdl");
        } catch (Throwable t) {
            String jdlLibPath = System.getProperty(PyLibConfig.JDL_LIB_KEY);
            if (jdlLibPath != null) {
                System.load(jdlLibPath);
            } else {
                throw new RuntimeException("Failed to load 'jdl' shared library. You can use system property 'jpy.jdlLib' to specify it.", t);
            }
        }
    }
}
