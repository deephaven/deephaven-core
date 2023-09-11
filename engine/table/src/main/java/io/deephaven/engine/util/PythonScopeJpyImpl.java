/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import org.jpy.PyDictWrapper;
import org.jpy.PyLib;
import org.jpy.PyObject;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

public class PythonScopeJpyImpl implements PythonScope<PyObject> {
    private final PyDictWrapper dict;

    private static final ThreadLocal<Deque<PyDictWrapper>> threadScopeStack = new ThreadLocal<>();
    private static final ThreadLocal<Deque<Map<PyObject, Object>>> threadConvertedMapStack = new ThreadLocal<>();

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

    private static Deque<Map<PyObject, Object>> ensureConvertedMap() {
        Deque<Map<PyObject, Object>> convertedMapStack = threadConvertedMapStack.get();
        if (convertedMapStack == null) {
            convertedMapStack = new ArrayDeque<>();
            threadConvertedMapStack.set(convertedMapStack);
        }
        // the current thread doesn't have a default map for the default main scope yet
        if (convertedMapStack.isEmpty()) {
            HashMap<PyObject, Object> convertedMap = new HashMap<>();
            convertedMapStack.push(convertedMap);
        }
        return convertedMapStack;
    }

    private static Map<PyObject, Object> currentConvertedMap() {
        Deque<Map<PyObject, Object>> convertedMapStack = ensureConvertedMap();
        return convertedMapStack.peek();
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
        Map<PyObject, Object> convertedMap = currentConvertedMap();
        return convertedMap.computeIfAbsent(pyObject, PythonScopeJpyImpl::convertInternal);
    }

    private static Object convertInternal(PyObject pyObject) {
        if (pyObject.isList()) {
            return pyObject.asList();
        } else if (pyObject.isDict()) {
            return pyObject.asDict();
        } else if (pyObject.isCallable()) {
            return new PyCallableWrapperJpyImpl(pyObject);
        } else if (pyObject.isConvertible()) {
            return pyObject.getObjectValue();
        } else {
            return pyObject;
        }
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

        Deque<Map<PyObject, Object>> convertedMapStack = ensureConvertedMap();
        HashMap<PyObject, Object> convertedMap = new HashMap<>();
        convertedMapStack.push(convertedMap);
    }

    @Override
    public void popScope() {
        Deque<PyDictWrapper> scopeStack = threadScopeStack.get();
        if (scopeStack == null) {
            throw new IllegalStateException("The thread scope stack is empty.");
        }
        PyDictWrapper pydict = scopeStack.pop();
        pydict.close();

        Deque<Map<PyObject, Object>> convertedMapStack = threadConvertedMapStack.get();
        if (convertedMapStack == null) {
            throw new IllegalStateException("The thread converted-map stack is empty.");
        }
        convertedMapStack.pop();
    }
}
