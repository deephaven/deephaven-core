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
public class PyCallableWrapper {
    private static final Logger log = LoggerFactory.getLogger(PyCallableWrapper.class);

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

    public PyCallableWrapper(PyObject pyCallable) {
        this.pyCallable = pyCallable;
    }

    public ArgumentsChunked buildArgumentsChunked(List<String> columnNames) {
        Object[] args;
        Class<?>[] argTypes;

        if (numbaVectorized) {
            args = new Object[chunkArguments.size()];
            argTypes = new Class[chunkArguments.size()];
        } else {
            // For DH vectorized func, we prepend 1 parameter to communicate the chunk size
            args = new Object[chunkArguments.size() + 1];
            argTypes = new Class[chunkArguments.size() + 1];
        }

        // For DH vectorized, we add a parameter at the beginning for chunk size
        int i = numbaVectorized ? 0 : 1;
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof ColumnChunkArgument) {
                String columnName = ((ColumnChunkArgument) arg).getColumnName();
                int chunkSourceIndex = columnNames.indexOf(columnName);
                if (chunkSourceIndex < 0) {
                    throw new IllegalArgumentException("Column source not found: " + columnName);
                }
                ((ColumnChunkArgument) arg).setChunkSourceIndex(chunkSourceIndex);
            } else {
                args[i] = ((ConstantChunkArgument) arg).getValue();
            }
            argTypes[i] = arg.getType();
            i++;
        }
        return new ArgumentsChunked(chunkArguments, args, argTypes, numbaVectorized);
    }

    public static abstract class ChunkArgument {
        private Class<?> type;

        public Class<?> getType() {
            return type;
        }

        public ChunkArgument(Class<?> type) {
            this.type = type;
        }
    }

    public static class ColumnChunkArgument extends ChunkArgument {
        private final String columnName;
        private int chunkSourceIndex;
        private boolean resolved = false;

        public ColumnChunkArgument(String columnName, Class<?> type) {
            super(type);
            this.columnName = columnName;
        }

        public void setChunkSourceIndex(int chunkSourceIndex) {
            this.resolved = true;
            this.chunkSourceIndex = chunkSourceIndex;
        }

        public int getChunkSourceIndex() {
            if (!resolved) {
                throw new IllegalStateException("The column chunk argument hasn't been resolved " + columnName);
            }
            return chunkSourceIndex;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    public static class ConstantChunkArgument extends ChunkArgument {
        private final Object value;

        public ConstantChunkArgument(Object value, Class<?> type) {
            super(type);
            this.value = value;
        }

        public Object getValue() {
            return value;
        }
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
                        "Multiple signatures on numba vectorized functions aren't supported yet. " + pyCallable);
            }
            signature = params.get(0).getStringValue();
            unwrapped = null;
            vectorized = true;
            numbaVectorized = true;
        } else if (pyCallable.hasAttribute("dh_vectorized")) {
            signature = pyCallable.getAttribute("signature").toString();
            unwrapped = pyCallable.getAttribute("callable");
            vectorized = true;
        } else {
            signature = dh_table_module.call("_encode_signature", pyCallable).toString();
            unwrapped = pyCallable;
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
        if (signature == null || signature.isEmpty()) {
            throw new IllegalStateException("Signature should always be available.");
        }

        char numpyTypeCode = signature.charAt(signature.length() - 1);
        Class<?> returnType = numpyType2JavaClass.get(numpyTypeCode);
        if (returnType == null) {
            throw new IllegalStateException(
                    "Vectorized functions should always have an integral, floating point, boolean, or Object return type: "
                            + numpyTypeCode);
        }

        List<Class<?>> paramTypes = new ArrayList<>();
        for (char numpyTypeChar : signature.toCharArray()) {
            if (numpyTypeChar != '-') {
                Class<?> paramType = numpyType2JavaClass.get(numpyTypeChar);
                if (paramType == null) {
                    throw new IllegalStateException(
                            "Parameters of vectorized functions should always be of integral, floating point, boolean, or Object type: "
                                    + numpyTypeChar + " of " + signature);
                }
                paramTypes.add(paramType);
            } else {
                break;
            }
        }

        this.paramTypes = paramTypes;
        this.returnType = returnType;
    }

    public PyObject vectorizedCallable() {
        if (numbaVectorized) {
            return pyCallable;
        } else {
            return dh_table_module.call("_DhVectorize", unwrapped);
        }
    }

    public Object call(Object... args) {
        return PythonScopeJpyImpl.convert(pyCallable.callMethod("__call__", args));
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

    public void initializeChunkArguments() {
        this.chunkArguments = new ArrayList<>();
    }

    public void addChunkArgument(ChunkArgument chunkArgument) {
        this.chunkArguments.add(chunkArgument);
    }

    public Class<?> getReturnType() {
        return returnType;
    }

}
