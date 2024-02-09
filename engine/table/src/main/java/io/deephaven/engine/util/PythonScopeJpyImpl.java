/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import org.jpy.PyDictWrapper;
import org.jpy.PyLib;
import org.jpy.PyObject;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

public class PythonScopeJpyImpl implements PythonScope<PyObject> {
    private final PyDictWrapper dict;

    private static final ThreadLocal<Deque<PyDictWrapper>> threadScopeStack = new ThreadLocal<>();

    public static PythonScopeJpyImpl ofMainGlobals() {
        return new PythonScopeJpyImpl(PyLib.getMainGlobals().asDict());
    }

    public PythonScopeJpyImpl(PyDictWrapper dict) {
        this.dict = dict;
    }

    private PyDictWrapper currentScope() {
        Deque<PyDictWrapper> scopeStack = threadScopeStack.get();
        if (scopeStack == null || scopeStack.isEmpty()) {
            return this.dict;
        } else {
            return scopeStack.peek();
        }
    }

    @Override
    public Optional<PyObject> getValueRaw(String name) {
        // note: we *may* be returning Optional.of(None)
        // None is a valid PyObject, and can be in scope
        return Optional.ofNullable(currentScope().get(name));
    }

    @Override
    public Stream<PyObject> getKeysRaw() {
        return currentScope().keySet().stream();
    }

    @Override
    public Stream<Entry<PyObject, PyObject>> getEntriesRaw() {
        return currentScope().entrySet().stream();
    }

    @Override
    public boolean containsKey(String name) {
        return currentScope().containsKey(name);
    }

    @Override
    public String convertStringKey(PyObject key) {
        if (!key.isString()) {
            throw new IllegalArgumentException(
                    "Found non-string key! Expecting only string keys. " + key);
        }
        return key.toString();
    }

    @Override
    public Object convertValue(PyObject value) {
        if (value.isNone()) {
            return value;
        }
        return convert(value);
    }

    /**
     * Converts a pyObject into an appropriate Java object for use outside of JPy.
     * <p>
     * If we're a List, Dictionary, or Callable, then we wrap them in a java object.
     * <p>
     * If it is a primitive (or a wrapped Java object); we convert it to the java object.
     * <p>
     * Otherwise we return the raw PyObject and the user can do with it what they will.
     *
     * @param pyObject the JPy wrapped PyObject.
     * @return a Java object representing the underlying JPy object.
     */
    public static Object convert(PyObject pyObject) {
        if (pyObject.isList()) {
            return pyObject.asList();
        } else if (pyObject.isDict()) {
            return pyObject.asDict();
        } else if (pyObject.isCallable()) {
            return new PyCallableWrapperJpyImpl(pyObject);
        } else if (pyObject.isConvertible()) {
            return pyObject.getObjectValue();
        }
        return pyObject;
    }

    public PyDictWrapper mainGlobals() {
        return dict;
    }

    @Override
    public void pushScope(PyObject pydict) {
        Deque<PyDictWrapper> scopeStack = threadScopeStack.get();
        if (scopeStack == null) {
            scopeStack = new ArrayDeque<>();
            threadScopeStack.set(scopeStack);
        }
        scopeStack.push(pydict.asDict());
    }

    @Override
    public void popScope() {
        Deque<PyDictWrapper> scopeStack = threadScopeStack.get();
        if (scopeStack == null) {
            throw new IllegalStateException("The thread scope stack is empty.");
        }
        PyDictWrapper pydict = scopeStack.pop();
        pydict.close();
    }
}
