package io.deephaven.db.util;

import org.jpy.PyDictWrapper;
import org.jpy.PyObject;

import java.util.*;
import java.util.Map.Entry;
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

        public CallableWrapper(PyObject pyObject) {
            this.pyObject = pyObject;
        }

        public Object call(Object... args) {
            return convert(pyObject.callMethod("__call__", args));
        }

        public PyObject getPyObject() {
            return pyObject;
        }
    }

    public static final class NumbaCallableWrapper extends CallableWrapper {
        private List<Class> paramTypes;
        private Class returnType;

        public NumbaCallableWrapper(PyObject pyObject, Class returnType, List<Class> paramTypes) {
            super(pyObject);
            this.returnType = returnType;
            this.paramTypes = paramTypes;
        }

        public Class getReturnType() {
            return returnType;
        }

        public List<Class> getParamTypes() {
            return paramTypes;
        }
    }

    private static CallableWrapper wrapCallable(PyObject pyObject) {
        if (pyObject.hasAttribute("ufunc")) {
            List<PyObject> params = pyObject.getAttribute("types").asList();
            if (params.isEmpty()) {
                return new CallableWrapper(pyObject);
            }
            String numbaFuncTypes = params.get(0).getStringValue();
            return parseNumbaVectorized(pyObject, numbaFuncTypes);
        } else {
            return new CallableWrapper(pyObject);
        }
    }

    private static final Map<Character, Class> numpyType2JavaClass = new HashMap<Character, Class>();
    {
        numpyType2JavaClass.put('i', int.class);
        numpyType2JavaClass.put('l', long.class);
        numpyType2JavaClass.put('h', short.class);
        numpyType2JavaClass.put('f', float.class);
        numpyType2JavaClass.put('d', double.class);
        numpyType2JavaClass.put('b', byte.class);
        numpyType2JavaClass.put('?', boolean.class);
    }

    private static CallableWrapper parseNumbaVectorized(PyObject pyObject, String numbaFuncTypes) {
        char numpyTypeCode = numbaFuncTypes.charAt(numbaFuncTypes.length() - 1);
        Class returnType = numpyType2JavaClass.get(numpyTypeCode);
        if (returnType == null) {
            return new CallableWrapper(pyObject);
        }

        List<Class> paramTypes = new ArrayList<>();
        for (char numpyTypeChar : numbaFuncTypes.toCharArray()) {
            if (numpyTypeChar != '-') {
                paramTypes.add(numpyType2JavaClass.get(numpyTypeChar));
            } else {
                break;
            }
        }

        return new NumbaCallableWrapper(pyObject, returnType, paramTypes);
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
            return wrapCallable(pyObject);
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
