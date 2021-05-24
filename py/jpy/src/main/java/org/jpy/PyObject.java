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

import static org.jpy.PyLib.assertPythonRuns;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a Python object (of Python/C API type {@code PyObject*}) in the Python interpreter.
 *
 * @author Norman Fomferra
 * @since 0.7
 */
public class PyObject implements AutoCloseable {

    // Having this static is less than optimal (would be much better to have it attached to the
    // running python instance) - but that's a lot harder to do.
    private static final PyObjectReferences REFERENCES = new PyObjectReferences();

    private static final AtomicReference<Thread> CLEANUP_THREAD = new AtomicReference<>();

    private static final boolean CLEANUP_ON_INIT = Boolean.parseBoolean(System.getProperty("PyObject.cleanup_on_init", "true"));

    private static final boolean CLEANUP_ON_THREAD = Boolean.parseBoolean(System.getProperty("PyObject.cleanup_on_thread", "true"));

    private static void startCleanupThread() {
        if (CLEANUP_THREAD.get() == null) {
            final Thread thread = REFERENCES.createCleanupThread("PyObject-cleanup");
            thread.setDaemon(true);
            if (!CLEANUP_THREAD.compareAndSet(null, thread)) {
                return;
            }
            thread.start();
        }
    }

    public static int cleanup() {
        return REFERENCES.asProxy().cleanupOnlyUseFromGIL();
    }

    private final PyObjectState state;

    PyObject(long pointer) {
        this(pointer, false);
    }

    @SuppressWarnings("WeakerAccess")
    PyObject(long pointer, boolean fromJNI) {
        state = new PyObjectState(pointer);
        if (fromJNI) {
            if (CLEANUP_ON_INIT && PyLib.hasGil()) {
                REFERENCES.cleanupOnlyUseFromGIL(); // only performs *one* cleanup
            }
            if (CLEANUP_ON_THREAD) {
                // ensures that we've only started after python has been started, and we know there is something to cleanup
                startCleanupThread();
            }
        }
        registerSelfInto(REFERENCES);
    }

    final void registerSelfInto(PyObjectReferences references) {
        references.register(this);
    }

    final PyObjectState getState() {
        return state;
    }

    /**
     * Decreases the reference count of {@code this} on first invocation.
     */
    @Override
    public final void close() {
        state.close();
    }

    /**
     * Executes Python source code.
     *
     * @param code The Python source code.
     * @param mode The execution mode.
     * @return The result of executing the code as a Python object.
     */
    public static PyObject executeCode(String code, PyInputMode mode) {
        return executeCode(code, mode, null, null);
    }

    /**
     * Executes Python source script.
     *
     * @param script The Python source script.
     * @param mode The execution mode.
     * @return The result of executing the script as a Python object.
     */
    public static PyObject executeScript(String script, PyInputMode mode) throws FileNotFoundException {
        return executeScript(script, mode, null, null);
    }

    /**
     * Executes Python source code in the context specified by the {@code globals} and {@code locals} maps.
     * <p>
     * If a Java value in the {@code globals} and {@code locals} maps cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If a Java value is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param code    The Python source code.
     * @param mode    The execution mode.
     * @param globals The global variables to be set, or {@code null}.
     * @param locals  The locals variables to be set, or {@code null}.
     * @return The result of executing the code as a Python object.
     */
    public static PyObject executeCode(String code, PyInputMode mode, Object globals, Object locals) {
        Objects.requireNonNull(code, "code must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        return new PyObject(PyLib.executeCode(code, mode.value(), globals, locals));
    }


    /**
     * Executes Python source script in the context specified by the {@code globals} and {@code locals} maps.
     * <p>
     * If a Java value in the {@code globals} and {@code locals} maps cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If a Java value is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param script    The Python source script.
     * @param mode    The execution mode.
     * @param globals The global variables to be set, or {@code null}.
     * @param locals  The locals variables to be set, or {@code null}.
     * @return The result of executing the script as a Python object.
     * @throws FileNotFoundException if the script file is not found
     */
    public static PyObject executeScript(String script, PyInputMode mode, Object globals, Object locals) throws FileNotFoundException {
        Objects.requireNonNull(script, "script must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        return new PyObject(PyLib.executeScript(script, mode.value(), globals, locals));
    }

    /**
     * @return A unique pointer to the wrapped Python object.
     */
    public final long getPointer() {
        return state.borrowPointer();
    }

    /**
     * @return This Python object as a Java {@code int} value.
     */
    public int getIntValue() {
        assertPythonRuns();
        return PyLib.getIntValue(getPointer());
    }

    /**
     * @return This Python object as a Java {@code long} value.
     */
    public long getLongValue() {
        assertPythonRuns();
        return PyLib.getLongValue(getPointer());
    }

    /**
     * @return This Python object as a Java {@code boolean} value.
     */
    public boolean getBooleanValue() {
        assertPythonRuns();
        return PyLib.getBooleanValue(getPointer());
    }

    /**
     * @return This Python object as a Java {@code double} value.
     */
    public double getDoubleValue() {
        assertPythonRuns();
        return PyLib.getDoubleValue(getPointer());
    }

    /**
     * @return This Python object as a Java {@code String} value.
     */
    public String getStringValue() {
        assertPythonRuns();
        return PyLib.getStringValue(getPointer());
    }

    /**
     * Gets this Python object as Java {@code Object} value.
     * <p>
     * If this Python object cannot be converted into a Java object, a Java wrapper of type {@link PyObject} will be returned.
     * If this Python object is a wrapped Java object, it will be unwrapped.
     *
     * @return This Python object as a Java {@code Object} value.
     */
    public Object getObjectValue() {
        assertPythonRuns();
        return PyLib.getObjectValue(getPointer());
    }

    /**
     * Gets the Python type object for this wrapped object.
     *
     * @return This Python object's type as a {@code PyObject} wrapped value.
     */
    public PyObject getType() {
        assertPythonRuns();
        return new PyObject(PyLib.getType(getPointer()));
    }

    public boolean isDict() {
        assertPythonRuns();
        return PyLib.pyDictCheck(getPointer());
    }

    public boolean isList() {
        assertPythonRuns();
        return PyLib.pyListCheck(getPointer());
    }

    public boolean isBoolean() {
        assertPythonRuns();
        return PyLib.pyBoolCheck(getPointer());
    }

    public boolean isLong() {
        assertPythonRuns();
        return PyLib.pyLongCheck(getPointer());
    }

    public boolean isInt() {
        assertPythonRuns();
        return PyLib.pyIntCheck(getPointer());
    }

    public boolean isNone() {
        assertPythonRuns();
        return PyLib.pyNoneCheck(getPointer());
    }

    public boolean isFloat() {
        assertPythonRuns();
        return PyLib.pyFloatCheck(getPointer());
    }

    public boolean isCallable() {
        assertPythonRuns();
        return PyLib.pyCallableCheck(getPointer());
    }

    public boolean isFunction() {
        assertPythonRuns();
        return PyLib.pyFunctionCheck(getPointer());
    }

    public boolean isModule() {
        assertPythonRuns();
        return PyLib.pyModuleCheck(getPointer());
    }

    public boolean isTuple() {
        assertPythonRuns();
        return PyLib.pyTupleCheck(getPointer());
    }

    public boolean isString() {
        assertPythonRuns();
        return PyLib.pyStringCheck(getPointer());
    }

    public boolean isConvertible() {
        assertPythonRuns();
        return PyLib.isConvertible(getPointer());
    }

    public List<PyObject> asList() {
        if (!isList()) {
            throw new ClassCastException("Can not convert non-list type to a list!");
        }
        return new PyListWrapper(this);
    }

    public PyDictWrapper asDict() {
        if (!isDict()) {
            throw new ClassCastException("Can not convert non-list type to a dictionary!");
        }
        return new PyDictWrapper(this);
    }

    /**
     * Gets this Python object as Java {@code Object[]} value of the given item type.
     * Appropriate type conversions from Python to Java will be performed as needed.
     * <p>
     * If a Python item value cannot be converted into a Java item object, a Java wrapper of type {@link PyObject} will be returned.
     * If a Python item value is a wrapped Java object, it will be unwrapped.
     * If this Python object value is a wrapped Java array object of given type, it will be unwrapped.
     *
     * @param itemType The expected item type class.
     * @param <T> The expected item type name.
     * @return This Python object as a Java {@code Object[]} value.
     */
    public <T> T[] getObjectArrayValue(Class<? extends T> itemType) {
        assertPythonRuns();
        Objects.requireNonNull(itemType, "itemType must not be null");
        return PyLib.getObjectArrayValue(getPointer(), itemType);
    }

    /**
     * Gets the Python value of a Python attribute.
     * <p>
     * If the Python value cannot be converted into a Java object, a Java wrapper of type {@link PyObject} will be returned.
     * If the Python value is a wrapped Java object, it will be unwrapped.
     *
     * @param name A name of the Python attribute.
     * @return A wrapper for the returned Python attribute value.
     */
    public PyObject getAttribute(String name) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        long pointer = PyLib.getAttributeObject(getPointer(), name);
        return pointer != 0 ? new PyObject(pointer) : null;
    }

    /**
     * Gets the value of a Python attribute as Java object of a given type.
     * Appropriate type conversions from Python to Java will be performed using the given value type.
     * <p>
     * If the Python value cannot be converted into a Java object, a Java wrapper of type {@link PyObject} will be returned.
     * If the Python value is a wrapped Java object, it will be unwrapped.
     *
     * @param name      A name of the Python attribute.
     * @param valueType The type of the value or {@code null}, if unknown.
     * @param <T>       The expected value type name.
     * @return The Python attribute value as Java object.
     */
    public <T> T getAttribute(String name, Class<? extends T> valueType) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        return PyLib.getAttributeValue(getPointer(), name, valueType);
    }

    /**
     * Sets the value of a Python attribute from the given Java object.
     * <p>
     * If the Java {@code value} cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If the Java {@code value} is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param name  A name of the Python attribute.
     * @param value The new attribute value as Java object. May be {@code null}.
     * @param <T>   The value type name.
     */
    public <T> void setAttribute(String name, T value) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        PyLib.setAttributeValue(getPointer(), name, value, value != null ? value.getClass() : null);
    }

    /**
     * Deletes the value of a Python attribute.
     *
     * @param name the name of the Python attribute.
     */
    public void delAttribute(String name) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        PyLib.delAttribute(getPointer(), name);
    }

    /**
     * Checks for the existence of a Python attribute..
     *
     * @param name the name of the Python attribute.
     * @return whether this attribute exists for this object
     */
    public boolean hasAttribute(String name) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        return PyLib.hasAttribute(getPointer(), name);
    }

    /**
     * Sets the value of a Python attribute from the given Java object and Java type (if given).
     * Appropriate type conversions from Java to Python will be performed using the given value type.
     * <p>
     * If the Java {@code value} cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If the Java {@code value} is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param name      A name of the Python attribute.
     * @param value     The new attribute value as Java object.
     * @param valueType The value type used for the conversion. May be {@code null}.
     * @param <T>       The value type name.
     */
    public <T> void setAttribute(String name, T value, Class<? extends T> valueType) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        PyLib.setAttributeValue(getPointer(), name, value, valueType);
    }

    /**
     * Call the callable Python method with the given name and arguments.
     * <p>
     * If a Java value in {@code args} cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If the Java value in {@code args} is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param name A name of a Python attribute that evaluates to a callable object.
     * @param args The arguments for the method call.
     * @return A wrapper for the returned Python object.
     */
    public PyObject callMethod(String name, Object... args) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        long pointer = PyLib.callAndReturnObject(getPointer(), true, name, args.length, args, null);
        return pointer != 0 ? new PyObject(pointer) : null;
    }

    /**
     * Call the callable Python object with the given name and arguments.
     * <p>
     * If a Java value in {@code args} cannot be directly converted into a Python object, a Java wrapper will be created instead.
     * If the Java value in {@code args} is a wrapped Python object of type {@link PyObject}, it will be unwrapped.
     *
     * @param name A name of a Python attribute that evaluates to a callable object,
     * @param args The arguments for the call.
     * @return A wrapper for the returned Python object.
     */
    public PyObject call(String name, Object... args) {
        assertPythonRuns();
        Objects.requireNonNull(name, "name must not be null");
        long pointer = PyLib.callAndReturnObject(getPointer(), false, name, args.length, args, null);
        return pointer != 0 ? new PyObject(pointer) : null;
    }

    public <T> T call(Class<T> returnType, String name, Class<?>[] paramTypes, Object[] args) {
        Objects.requireNonNull(returnType, "returnType must not be null");
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(paramTypes, "paramTypes must not be null");
        Objects.requireNonNull(args, "args must not be null");
        if (paramTypes.length != args.length) {
            throw new IllegalArgumentException("paramTypes and args must be of equal length");
        }
        for (int i = 0; i < args.length; ++i) {
            // Let's be defensive here right now - we can loosen this in the future if necessary.
            Objects.requireNonNull(paramTypes[i], "paramTypes items must be non null");
            Objects.requireNonNull(args[i], "args items must be non null");
            if (!Assignment.isAssignableFrom(paramTypes[i], args[i])) {
                throw new IllegalArgumentException(String.format(
                    "Argument %d of type '%s' is not assignable to type '%s'",
                    i,
                    args[i].getClass(),
                    paramTypes[i]));
            }
        }
        assertPythonRuns();
        return PyLib.callAndReturnValue(getPointer(), false, name, args.length, args, paramTypes, returnType);
    }

    public <T, A0> T call(Class<T> returnType, String name, Class<A0> clazz0, A0 arg0) {
        return call(returnType, name, new Class[] { clazz0 }, new Object[] { arg0 });
    }

    public <T, A0, A1> T call(Class<T> returnType, String name, Class<A0> clazz0, A0 arg0, Class<A1> clazz1, A1 arg1) {
        return call(returnType, name, new Class[] { clazz0, clazz1 }, new Object[] { arg0, arg1 });
    }

    public <T, A0, A1, A2> T call(Class<T> returnType, String name, Class<A0> clazz0, A0 arg0, Class<A1> clazz1, A1 arg1, Class<A2> clazz2, A2 arg2) {
        return call(returnType, name, new Class[] { clazz0, clazz1, clazz2 }, new Object[] { arg0, arg1, arg2 });
    }

    /**
     * Create a Java proxy instance of this Python object which contains compatible methods to the ones provided in the
     * interface given by the {@code type} parameter.
     *
     * @param type The interface class.
     * @param <T>  The interface name.
     * @return A (proxy) instance implementing the given interface.
     */
    public <T> T createProxy(Class<T> type) {
        assertPythonRuns();
        //noinspection unchecked
        Objects.requireNonNull(type, "type must not be null");
        return (T) createProxy(PyLib.CallableKind.METHOD, type);
    }

    /**
     * Create a Java proxy instance of this Python object (or module) which contains compatible methods
     * (or functions) to the ones provided in the interfaces given by all the {@code type} parameters.
     *
     * @param callableKind The kind of calls to be made.
     * @param types        The interface types.
     * @return A instance implementing the all the given interfaces which serves as a proxy for the given Python object (or module).
     */
    public Object createProxy(PyLib.CallableKind callableKind, Class<?>... types) {
        assertPythonRuns();
        ClassLoader classLoader = types[0].getClassLoader();
        InvocationHandler invocationHandler = new PyProxyHandler(this, callableKind);
        return Proxy.newProxyInstance(classLoader, types, invocationHandler);
    }

    /**
     * Unwraps the original Python object used to create {@code object}. The inverse of
     * {@link #createProxy(Class)}.
     *
     * @param object The object that may be a proxy.
     * @return The Python object, or null if the object is not a proxy.
     */
    public static PyObject unwrapProxy(Object object) {
        if (!Proxy.isProxyClass(object.getClass())) {
            return null;
        }
        InvocationHandler handler = Proxy.getInvocationHandler(object);
        if (!(handler instanceof PyProxyHandler)) {
            return null;
        }
        return ((PyProxyHandler)handler).getPyObject();
    }

    /**
     * Gets the python string representation of this object.
     *
     * @return A string representation of the object.
     * @see #getPointer()
     */
    @Override
    public final String toString() {
	    return str();
    }

    /**
     * Gets a the python repr of this object
     *
     * @return A string representation of the object.
     * @see #getPointer()
     */
    public final String repr() {
	    return PyLib.repr(getPointer());
    }

    /**
     * Runs the python str function on this object.
     * @return The String representation of this object.
     */
    public final String str() {
        return PyLib.str(getPointer());
    }

    /**
     * Runs the python hash function on this object.
     * @return The hash.
     */
    public final long hash() {
        return PyLib.hash(getPointer());
    }

    public final boolean eq(Object other) {
        return PyLib.eq(getPointer(), other);
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param o The other object.
     * @return {@code true} if the other object is an instance of {@link org.jpy.PyObject} and if their pointers are equal, {@code false} otherwise.
     * @see #getPointer()
     */
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PyObject)) {
            return false;
        }
        PyObject pyObject = (PyObject) o;
        return getPointer() == pyObject.getPointer();
    }

    /**
     * Computes a hash code from this object's pointer value.
     *
     * @return A hash code value for this object.
     * @see #getPointer()
     */
    @Override
    public final int hashCode() {
        final long pointer = getPointer();
        return (int) (pointer ^ (pointer >>> 32));
    }
}
