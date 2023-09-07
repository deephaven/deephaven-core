package io.deephaven.engine.util;

import io.deephaven.engine.table.impl.select.python.ArgumentsChunked;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * When given a pyObject that is a callable, we stick it inside the callable wrapper, which implements a call() varargs
 * method, so that we can call it using __call__ without all of the JPy nastiness.
 */
public class PyCallableWrapperJpyImpl implements PyCallableWrapper {
    private static final Logger log = LoggerFactory.getLogger(PyCallableWrapperJpyImpl.class);

    private static final PyObject NUMBA_VECTORIZED_FUNC_TYPE = getNumbaVectorizedFuncType();

    private static final PyModule dh_table_module = PyModule.importModule("deephaven.table");

    private static final Map<Character, Class<?>> numpyType2JavaClass = new HashMap<>();

    static {
        numpyType2JavaClass.put('i', int.class);
        numpyType2JavaClass.put('l', long.class);
        numpyType2JavaClass.put('h', short.class);
        numpyType2JavaClass.put('f', float.class);
        numpyType2JavaClass.put('d', double.class);
        numpyType2JavaClass.put('b', byte.class);
        numpyType2JavaClass.put('?', boolean.class);
        numpyType2JavaClass.put('U', String.class);
        numpyType2JavaClass.put('O', Object.class);
    }

    private final PyObject pyCallable;

    private String signature = null;
    private List<Class<?>> paramTypes;
    private Class<?> returnType;
    private boolean vectorizable = false;
    private boolean vectorized = false;
    private Collection<ChunkArgument> chunkArguments;
    private boolean numbaVectorized;
    private PyObject unwrapped;

    public PyCallableWrapperJpyImpl(PyObject pyCallable) {
        this.pyCallable = pyCallable;
    }

    @Override
    public PyObject getAttribute(String name) {
        return this.pyCallable.getAttribute(name);
    }

    @Override
    public <T> T getAttribute(String name, Class<? extends T> valueType) {
        return this.pyCallable.getAttribute(name, valueType);
    }

    public ArgumentsChunked buildArgumentsChunked(List<String> columnNames) {
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof ColumnChunkArgument) {
                String columnName = ((ColumnChunkArgument) arg).getColumnName();
                int chunkSourceIndex = columnNames.indexOf(columnName);
                if (chunkSourceIndex < 0) {
                    throw new IllegalArgumentException("Column source not found: " + columnName);
                }
                ((ColumnChunkArgument) arg).setSourceChunkIndex(chunkSourceIndex);
            }
        }
        return new ArgumentsChunked(chunkArguments, returnType, numbaVectorized);
    }

    /**
     * This assumes that the Python interpreter won't be re-initialized during a session, if this turns out to be a
     * false assumption, then we'll need to make this initialization code 'python restart' proof.
     */
    private static PyObject getNumbaVectorizedFuncType() {
        try {
            return PyModule.importModule("numba.np.ufunc.dufunc").getAttribute("DUFunc");
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Numba isn't installed in the Python environment.");
            }
            return null;
        }
    }

    private void prepareSignature() {
        if (pyCallable.getType().equals(NUMBA_VECTORIZED_FUNC_TYPE)) {
            List<PyObject> params = pyCallable.getAttribute("types").asList();
            if (params.isEmpty()) {
                throw new IllegalArgumentException(
                        "numba vectorized function must have an explicit signature: " + pyCallable);
            }
            // numba allows a vectorized function to have multiple signatures
            if (params.size() > 1) {
                throw new UnsupportedOperationException(
                        pyCallable
                                + " has multiple signatures; this is not currently supported for numba vectorized functions");
            }
            signature = params.get(0).getStringValue();
            unwrapped = null;
            numbaVectorized = true;
            vectorized = true;
        } else if (pyCallable.hasAttribute("dh_vectorized")) {
            signature = pyCallable.getAttribute("signature").toString();
            unwrapped = pyCallable.getAttribute("callable");
            numbaVectorized = false;
            vectorized = true;
        } else {
            signature = dh_table_module.call("_encode_signature", pyCallable).toString();
            unwrapped = pyCallable;
            numbaVectorized = false;
            vectorized = false;
        }
    }

    @Override
    public void parseSignature() {
        if (signature != null) {
            return;
        }

        prepareSignature();

        // the 'types' field of a vectorized function follows the pattern of '[ilhfdb?O]*->[ilhfdb?O]',
        // eg. [ll->d] defines two int64 (long) arguments and a double return type.
        if (signature == null || signature.isEmpty()) {
            throw new IllegalStateException("Signature should always be available.");
        }

        char numpyTypeCode = signature.charAt(signature.length() - 1);
        Class<?> returnType = numpyType2JavaClass.get(numpyTypeCode);
        if (returnType == null) {
            throw new IllegalStateException(
                    "Vectorized functions should always have an integral, floating point, boolean, String, or Object return type: "
                            + numpyTypeCode);
        }

        List<Class<?>> paramTypes = new ArrayList<>();
        for (char numpyTypeChar : signature.toCharArray()) {
            if (numpyTypeChar != '-') {
                Class<?> paramType = numpyType2JavaClass.get(numpyTypeChar);
                if (paramType == null) {
                    throw new IllegalStateException(
                            "Parameters of vectorized functions should always be of integral, floating point, boolean, String, or Object type: "
                                    + numpyTypeChar + " of " + signature);
                }
                paramTypes.add(paramType);
            } else {
                break;
            }
        }

        this.paramTypes = paramTypes;
        if (returnType == Object.class) {
            this.returnType = PyObject.class;
        } else if (returnType == boolean.class) {
            this.returnType = Boolean.class;
        } else {
            this.returnType = returnType;
        }
    }

    public PyObject vectorizedCallable() {
        if (numbaVectorized) {
            return pyCallable;
        } else {
            return dh_table_module.call("dh_vectorize", unwrapped);
        }
    }

    @Override
    public Object call(Object... args) {
        return PythonScopeJpyImpl.convert(pyCallable.callMethod("__call__", args));
    }

    @Override
    public List<Class<?>> getParamTypes() {
        return paramTypes;
    }

    @Override
    public boolean isVectorized() {
        return vectorized;
    }

    @Override
    public boolean isVectorizable() {
        return vectorizable;
    }

    @Override
    public void setVectorizable(boolean vectorizable) {
        this.vectorizable = vectorizable;
    }

    @Override
    public void initializeChunkArguments() {
        this.chunkArguments = new ArrayList<>();
    }

    @Override
    public void addChunkArgument(ChunkArgument chunkArgument) {
        this.chunkArguments.add(chunkArgument);
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

}
