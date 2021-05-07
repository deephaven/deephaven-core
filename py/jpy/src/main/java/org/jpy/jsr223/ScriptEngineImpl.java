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

package org.jpy.jsr223;

import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;
import org.jpy.PyInputMode;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.util.stream.Collectors;

/**
 * jpy's ScriptEngine implementation of JSR 223: <i>Scripting for the Java Platform</i>.
 *
 * @author Norman Fomferra
 * @since 0.8
 */
class ScriptEngineImpl extends AbstractScriptEngine implements Invocable {

    public static final String EXTRA_PATHS_KEY = ScriptEngineImpl.class.getName() + ".extraPaths";

    private final ScriptEngineFactoryImpl factory;

    ScriptEngineImpl(ScriptEngineFactoryImpl factory) {
        this.factory = factory;
        PyLib.startPython(System.getProperty(EXTRA_PATHS_KEY, "").split(File.pathSeparator));
    }

    /**
     * Returns a <code>ScriptEngineFactory</code> for the class to which this <code>ScriptEngine</code> belongs.
     *
     * @return The <code>ScriptEngineFactory</code>
     */
    @Override
    public ScriptEngineFactory getFactory() {
        return factory;
    }

    /**
     * Returns an uninitialized <code>Bindings</code>.
     *
     * @return A <code>Bindings</code> that can be used to replace the state of this <code>ScriptEngine</code>.
     **/
    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    /**
     * Same as <code>eval(String, ScriptContext)</code> where the source of the script
     * is read from a <code>Reader</code>.
     *
     * @param reader  The source of the script to be executed by the script engine.
     * @param context The <code>ScriptContext</code> passed to the script engine.
     * @return The value returned from the execution of the script.
     * @throws ScriptException      if an error occurs in script.
     * @throws NullPointerException if either argument is null.
     */
    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        return eval(new BufferedReader(reader).lines().collect(Collectors.joining("\n")), context);
    }

    /**
     * Causes the immediate execution of the script whose source is the String
     * passed as the first argument.  The script may be reparsed or recompiled before
     * execution.  State left in the engine from previous executions, including
     * variable values and compiled procedures may be visible during this execution.
     *
     * @param script  The script to be executed by the script engine.
     * @param context A <code>ScriptContext</code> exposing sets of attributes in
     *                different scopes.  The meanings of the scopes <code>ScriptContext.GLOBAL_SCOPE</code>,
     *                and <code>ScriptContext.ENGINE_SCOPE</code> are defined in the specification.
     *                <br><br>
     *                The <code>ENGINE_SCOPE</code> <code>Bindings</code> of the <code>ScriptContext</code> contains the
     *                bindings of scripting variables to application objects to be used during this
     *                script execution.
     * @return The value returned from the execution of the script.
     * @throws ScriptException      if an error occurs in script. ScriptEngines should create and throw
     *                              <code>ScriptException</code> wrappers for checked Exceptions thrown by underlying scripting
     *                              implementations.
     * @throws NullPointerException if either argument is null.
     */
    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return PyObject.executeCode(script,
                                    PyInputMode.SCRIPT,
                                    context.getBindings(ScriptContext.GLOBAL_SCOPE),
                                    context.getBindings(ScriptContext.ENGINE_SCOPE));
    }

    /**
     * Calls a method on a script object compiled during a previous script execution,
     * which is retained in the state of the <code>ScriptEngine</code>.
     *
     * @param thiz If the procedure is a member  of a class
     *             defined in the script and thiz is an instance of that class
     *             returned by a previous execution or invocation, the named method is
     *             called through that instance.
     * @param name The name of the procedure to be called.
     * @param args Arguments to pass to the procedure.  The rules for converting
     *             the arguments to scripting variables are implementation-specific.
     * @return The value returned by the procedure.  The rules for converting the scripting
     * variable returned by the script method to a Java Object are implementation-specific.
     * @throws ScriptException          if an error occurs during invocation of the method.
     * @throws NoSuchMethodException    if method with given name or matching argument types cannot be found.
     * @throws NullPointerException     if the method name is null.
     * @throws IllegalArgumentException if the specified thiz is null or the specified Object is
     *                                  does not represent a scripting object.
     */
    @Override
    public Object invokeMethod(Object thiz, String name, Object... args) throws ScriptException, NoSuchMethodException {
        if (!(thiz instanceof PyObject)) {
            throw new IllegalArgumentException("'thiz' must be an instance of " + PyObject.class.getName());
        }
        return ((PyObject) thiz).call(name, args);
    }

    /**
     * Used to call top-level procedures and functions defined in scripts.
     *
     * @param name of the procedure or function to call
     * @param args Arguments to pass to the procedure or function
     * @return The value returned by the procedure or function
     * @throws ScriptException       if an error occurs during invocation of the method.
     * @throws NoSuchMethodException if method with given name or matching argument types cannot be found.
     * @throws NullPointerException  if method name is null.
     */
    @Override
    public Object invokeFunction(String name, Object... args) throws ScriptException, NoSuchMethodException {
        return PyModule.getMain().call(name, args);
    }

    /**
     * Returns an implementation of an interface using functions compiled in
     * the interpreter. The methods of the interface
     * may be implemented using the <code>invokeFunction</code> method.
     *
     * @param clasz The <code>Class</code> object of the interface to return.
     * @return An instance of requested interface - null if the requested interface is unavailable,
     * i. e. if compiled functions in the <code>ScriptEngine</code> cannot be found matching
     * the ones in the requested interface.
     * @throws IllegalArgumentException if the specified <code>Class</code> object
     *                                  is null or is not an interface.
     */
    @Override
    public <T> T getInterface(Class<T> clasz) {
        if (clasz == null || !clasz.isInterface()) {
            throw new IllegalArgumentException("'clasz' must be an interface");
        }
        return PyModule.getMain().createProxy(clasz);
    }

    /**
     * Returns an implementation of an interface using member functions of
     * a scripting object compiled in the interpreter. The methods of the
     * interface may be implemented using the <code>invokeMethod</code> method.
     *
     * @param thiz  The scripting object whose member functions are used to implement the methods of the interface.
     * @param clasz The <code>Class</code> object of the interface to return.
     * @return An instance of requested interface - null if the requested interface is unavailable,
     * i. e. if compiled methods in the <code>ScriptEngine</code> cannot be found matching
     * the ones in the requested interface.
     * @throws IllegalArgumentException if the specified <code>Class</code> object
     *                                  is null or is not an interface, or if the specified Object is
     *                                  null or does not represent a scripting object.
     */
    @Override
    public <T> T getInterface(Object thiz, Class<T> clasz) {
        if (clasz == null || !clasz.isInterface()) {
            throw new IllegalArgumentException("'clasz' must be an interface");
        }
        if (!(thiz instanceof PyObject)) {
            throw new IllegalArgumentException("'thiz' must be an instance of " + PyObject.class.getName());
        }
        PyObject pyObject = (PyObject) thiz;
        return pyObject.createProxy(clasz);
    }
}
