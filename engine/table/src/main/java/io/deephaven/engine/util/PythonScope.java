//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import org.jpy.PyDictWrapper;
import org.jpy.PyObject;

import java.util.Optional;

/**
 * A collection of methods around retrieving objects from the given Python scope.
 * <p>
 * The scope is likely coming from some sort of Python dictionary. The scope might be local, global, or other.
 *
 * @param <PyObj> the implementation's raw Python object type
 */
public interface PythonScope<PyObj> {
    /**
     * Retrieves a value from the given scope.
     * <p>
     * No conversion is done.
     *
     * @param name the name of the python variable
     * @return the value, or empty
     */
    Optional<PyObj> getValueRaw(String name);

    /**
     * The helper method to turn a raw key into a string key.
     * <p>
     * Note: this assumes that all the keys are strings, which is not always true. Indices can also be tuples. TODO:
     * revise interface as appropriate if this becomes an issue.
     *
     * @param key the raw key
     * @return the string key
     * @throws IllegalArgumentException if the key is not a string
     */
    String convertStringKey(PyObj key);

    /**
     * The helper method to turn a raw value into an implementation specific object.
     * <p>
     * This method should NOT convert PyObj of None type to null - we need to preserve the None object so it works with
     * other Optional return values.
     *
     * @param value the raw value
     * @return the converted object value
     */
    Object convertValue(PyObj value);

    /**
     * Finds out if a variable is in scope
     *
     * @param name the name of the python variable
     * @return true iff the scope contains the variable
     */
    default boolean containsKey(String name) {
        return getValueRaw(name).isPresent();
    }

    /**
     * Equivalent to {@link #getValueRaw(String)}.map({@link #convertValue(Object)})
     *
     * @param name the name of the python variable
     * @return the converted object value, or empty
     */
    default Optional<Object> getValue(String name) {
        return getValueRaw(name)
                .map(this::convertValue);
    }

    /**
     * Equivalent to {@link #getValue(String)}.map({@code clazz}.{@link Class#cast(Object)})
     *
     * @param name the name of the python variable
     * @param clazz the class to cast to
     * @param <T> the return type
     * @return the converted casted value, or empty
     */
    default <T> Optional<T> getValue(String name, Class<T> clazz) {
        return getValue(name)
                .map(clazz::cast);
    }

    /**
     * Equivalent to {@link #getValue(String)}.map(x -&gt; (T)x);
     *
     * @param name the name of the python variable
     * @param <T> the return type
     * @return the converted casted value, or empty
     */
    default <T> Optional<T> getValueUnchecked(String name) {
        // noinspection unchecked
        return getValue(name)
                .map(x -> (T) x);
    }

    /**
     * @return the Python's __main__ module namespace
     */
    PyDictWrapper mainGlobals();

    /**
     * @return the current scope or the main globals if no scope is set
     */
    PyDictWrapper currentScope();

    /**
     * Push the provided Python scope into the thread scope stack for subsequent operations on Tables
     *
     * @param pydict a Python dictionary representing the current scope under which the Python code in running, it is
     *        the combination of module globals and function locals
     */
    void pushScope(PyObject pydict);

    /**
     * Pop the last Python scope pushed by {@link #pushScope(PyObject pydict)} from the thread scope stack
     */
    void popScope();
}
