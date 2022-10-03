/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import org.jpy.PyDictWrapper;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

public class PythonScopeJpyImpl implements PythonScope<PyObject> {
    private final PyDictWrapper dict;

    private static final ThreadLocal<Deque<PyDictWrapper>> threadScopeStack = new ThreadLocal<>();
    private static final ThreadLocal<Deque<HashMap<PyObject, ?>>> threadConvertedMapStack = new ThreadLocal<>();

    private static final PyObject NUMBA_VECTORIZED_FUNC_TYPE = getNumbaVectorizedFuncType();

    private static final PyModule dh_table_module = PyModule.importModule("deephaven.table");

    // this assumes that the Python interpreter won't be re-initialized during a session, if this turns out to be a
    // false assumption, then we'll need to make this initialization code 'python restart' proof.
    private static PyObject getNumbaVectorizedFuncType() {
        try {
            return PyModule.importModule("numba.np.ufunc.dufunc").getAttribute("DUFunc");
        } catch (Exception e) {
            return null;
        }
    }

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
     * When given a pyObject that is a callable, we stick it inside the callable wrapper, which implements a call()
     * varargs method, so that we can call it using __call__ without all of the JPy nastiness.
     */
    public static class CallableWrapper {
        private static final Map<Character, Class<?>> numpyType2JavaClass = new HashMap<>();

        static {
            numpyType2JavaClass.put('i', int.class);
            numpyType2JavaClass.put('l', long.class);
            numpyType2JavaClass.put('h', short.class);
            numpyType2JavaClass.put('f', float.class);
            numpyType2JavaClass.put('d', double.class);
            numpyType2JavaClass.put('b', byte.class);
            numpyType2JavaClass.put('?', boolean.class);
            numpyType2JavaClass.put('O', Object.class);
        }

        private final PyObject pyObject;

        private String signature = null;
        private List<Class<?>> paramTypes;
        private Class<?> returnType;
        private boolean vectorizable = false;
        private boolean vectorized = false;

        // first value of the pair is null if it is a resolved constant arg, otherwise it is the used column name when
        // uninitialized and an index value of the chunked arrays for the column sources used as args when initialized
        private ArrayList<Pair<?, ?>> args = new ArrayList<>();
        private Class<?>[] argTypes;

        public CallableWrapper(PyObject pyObject) {
            this.pyObject = pyObject;
        }

        private void prepareSignature() {
            if (pyObject.getType().equals(NUMBA_VECTORIZED_FUNC_TYPE)) {
                List<PyObject> params = pyObject.getAttribute("types").asList();
                if (params.isEmpty()) {
                    throw new IllegalArgumentException(
                            "numba vectorized function must have an explicit signature: " + pyObject);
                }
                // numba allows a vectorized function to have multiple signatures, only the first one
                // will be accepted by DH
                signature = params.get(0).getStringValue();
                vectorized = true;
            } else if (pyObject.hasAttribute("dh_vectorized")) {
                signature = pyObject.getAttribute("signature").toString();
                vectorized = true;
            } else {
                signature = dh_table_module.call("_encode_signature", pyObject).toString();
                vectorized = false;
            }
        }

        public void parseSignature() {
            if (signature != null) {
                return;
            }

            prepareSignature();

            // the 'types' field of a vectorized function follows the pattern of '[ilhfdb?O]*->[ilhfdb?O]',
            // eg. [ll->d] defines two int64 (long) arguments and a double return type.
            if (signature == null || signature.length() == 0) {
                throw new IllegalStateException("Signature should always be available.");
            }

            char numpyTypeCode = signature.charAt(signature.length() - 1);
            Class<?> returnType = numpyType2JavaClass.get(numpyTypeCode);
            if (returnType == null) {
                throw new IllegalStateException(
                        "Vectorized functions should always have an integral, floating point, boolean, or Object return type.");
            }

            List<Class<?>> paramTypes = new ArrayList<>();
            for (char numpyTypeChar : signature.toCharArray()) {
                if (numpyTypeChar != '-') {
                    Class<?> paramType = numpyType2JavaClass.get(numpyTypeChar);
                    if (paramType == null) {
                        throw new IllegalStateException(
                                "Parameters of vectorized functions should always be of integral, floating point, boolean, or Object type.");
                    }
                    paramTypes.add(paramType);
                } else {
                    break;
                }
            }

            this.paramTypes = paramTypes;
            this.returnType = returnType;
        }

        public Object call(Object... args) {
            return convert(pyObject.callMethod("__call__", args));
        }

        public PyObject getPyObject() {
            return pyObject;
        }

        public Class<?> getReturnType() {
            return returnType;
        }

        public List<Class<?>> getParamTypes() {
            return paramTypes;
        }

        public boolean isVectorized() {
            return vectorized;
        }

        public boolean isVectorizable() {
            return vectorizable;
        }

        public void setVectorizable(boolean vectorizable) {
            this.vectorizable = vectorizable;
        }

        public void clearArgs() {
            this.args.clear();
        }

        public void addArg(Pair<?, ?> arg) {
            this.args.add(arg);
        }

        public ArrayList<Pair<?, ?>> getArgs() {
            return this.args;
        }

        public void setArgs(ArrayList<Pair<?, ?>> args) {
            this.args = args;
        }

        public Class<?>[] getArgTypes() {
            return argTypes;
        }

        public void setArgTypes(Class<?>[] argTypes) {
            this.argTypes = argTypes;
        }
    }

    public static CallableWrapper vectorizeCallable(PyObject pyObject) {
        PyObject vectorized = dh_table_module.call("dh_vectorize", pyObject);
        CallableWrapper vectorizedCallableWrapper = new CallableWrapper(vectorized);
        vectorizedCallableWrapper.parseSignature();
        return vectorizedCallableWrapper;
    }

    private static void ensureConvertedMap() {
        Deque<HashMap<PyObject, ?>> convertedMapStack = threadConvertedMapStack.get();
        if (convertedMapStack == null) {
            convertedMapStack = new ArrayDeque<>();
            threadConvertedMapStack.set(convertedMapStack);
        }
        if (convertedMapStack.isEmpty()) {
            HashMap<PyObject, ?> convertedMap = new HashMap<>();
            convertedMapStack.push(convertedMap);
        }
    }

    private static HashMap<PyObject, ?> currentConvertedMap() {
        ensureConvertedMap();
        return threadConvertedMapStack.get().peek();
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
        HashMap convertedMap = currentConvertedMap();

        return convertedMap.computeIfAbsent(pyObject, po -> convertInternal((PyObject) po));
    }

    private static Object convertInternal(PyObject pyObject) {
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

        ensureConvertedMap();
    }

    @Override
    public void popScope() {
        Deque<PyDictWrapper> scopeStack = threadScopeStack.get();
        if (scopeStack == null) {
            throw new IllegalStateException("The thread scope stack is empty.");
        }
        PyDictWrapper pydict = scopeStack.pop();
        pydict.close();

        Deque<HashMap<PyObject, ?>> convertedMapStack = threadConvertedMapStack.get();
        if (convertedMapStack == null) {
            throw new IllegalStateException("The thread converted-map stack is empty.");
        }
        convertedMapStack.pop();
    }
}
