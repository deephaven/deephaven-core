package io.deephaven.db.util;

import org.jpy.PyDictWrapper;
import org.jpy.PyObject;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

public class PythonScopeJpyImpl implements PythonScope<PyObject> {
    private final PyDictWrapper dict;

    public PythonScopeJpyImpl(PyDictWrapper dict) {
        this.dict = dict;
    }

    @Override
    public Optional<PyObject> getValueRaw(String name) {
        // note: we *may* be returning Optional.of(None)
        // None is a valid PyObject, and can be in scope
        return Optional.ofNullable(dict.get(name));
    }

    @Override
    public Stream<PyObject> getKeysRaw() {
        return dict.keySet().stream();
    }

    @Override
    public Stream<Entry<PyObject, PyObject>> getEntriesRaw() {
        return dict.entrySet().stream();
    }

    @Override
    public boolean containsKey(String name) {
        return dict.containsKey(name);
    }

    @Override
    public String convertStringKey(PyObject key) {
        if (!key.isString()) {
            throw new IllegalArgumentException(
                    "Found non-string key! Expecting only string keys. " + key.toString());
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
     * When given a pyObject that is a callable, we stick it inside the callable wrapper, which implements a call()
     * varargs method, so that we can call it using __call__ without all of the JPy nastiness.
     */
    public static class CallableWrapper {
        private PyObject pyObject;

        private CallableWrapper(PyObject pyObject) {
            this.pyObject = pyObject;
        }

        public Object call(Object... args) {
            return convert(pyObject.callMethod("__call__", args));
        }
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
            return new CallableWrapper(pyObject);
        } else if (pyObject.isConvertible()) {
            return pyObject.getObjectValue();
        } else {
            return pyObject;
        }
    }

    public PyDictWrapper globals() {
        return dict;
    }
}
