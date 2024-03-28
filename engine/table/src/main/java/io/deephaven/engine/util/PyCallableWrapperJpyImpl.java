//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.table.impl.select.python.ArgumentsChunked;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.time.Instant;
import java.util.*;

import static io.deephaven.engine.table.impl.lang.QueryLanguageParser.NULL_CLASS;

/**
 * When given a pyObject that is a callable, we stick it inside the callable wrapper, which implements a call() varargs
 * method, so that we can call it using __call__ without all of the JPy nastiness.
 */
public class PyCallableWrapperJpyImpl implements PyCallableWrapper {
    private static final Logger log = LoggerFactory.getLogger(PyCallableWrapperJpyImpl.class);

    private static final PyObject NUMBA_VECTORIZED_FUNC_TYPE = getNumbaVectorizedFuncType();
    private static final PyObject NUMBA_GUVECTORIZED_FUNC_TYPE = getNumbaGUVectorizedFuncType();

    private static final PyModule dh_udf_module = PyModule.importModule("deephaven._udf");

    private static final Map<Character, Class<?>> numpyType2JavaClass = new HashMap<>();
    private static final Map<Character, Class<?>> numpyType2JavaArrayClass = new HashMap<>();

    private static final Map<Class<?>, Character> javaClass2NumpyType = new HashMap<>();

    static {
        numpyType2JavaClass.put('b', byte.class);
        numpyType2JavaClass.put('h', short.class);
        numpyType2JavaClass.put('H', char.class);
        numpyType2JavaClass.put('i', int.class);
        numpyType2JavaClass.put('l', long.class);
        numpyType2JavaClass.put('f', float.class);
        numpyType2JavaClass.put('d', double.class);
        numpyType2JavaClass.put('?', Boolean.class);
        numpyType2JavaClass.put('U', String.class);
        numpyType2JavaClass.put('M', Instant.class);
        numpyType2JavaClass.put('O', Object.class);

        numpyType2JavaArrayClass.put('b', byte[].class);
        numpyType2JavaArrayClass.put('h', short[].class);
        numpyType2JavaArrayClass.put('H', char[].class);
        numpyType2JavaArrayClass.put('i', int[].class);
        numpyType2JavaArrayClass.put('l', long[].class);
        numpyType2JavaArrayClass.put('f', float[].class);
        numpyType2JavaArrayClass.put('d', double[].class);
        numpyType2JavaArrayClass.put('?', Boolean[].class);
        numpyType2JavaArrayClass.put('U', String[].class);
        numpyType2JavaArrayClass.put('M', Instant[].class);
        numpyType2JavaArrayClass.put('O', Object[].class);

        for (Map.Entry<Character, Class<?>> classClassEntry : numpyType2JavaClass.entrySet()) {
            javaClass2NumpyType.put(classClassEntry.getValue(), classClassEntry.getKey());
        }
        for (Map.Entry<Character, Class<?>> classClassEntry : numpyType2JavaArrayClass.entrySet()) {
            javaClass2NumpyType.put(classClassEntry.getValue(), classClassEntry.getKey());
        }
    }

    /**
     * Ensure that the class initializer runs.
     */
    public static void init() {}

    // TODO: support for vectorizing functions that return arrays
    // https://github.com/deephaven/deephaven-core/issues/4649
    private static final Set<Class<?>> vectorizableReturnTypes = Set.of(
            Boolean.class, Boolean[].class,
            byte.class, byte[].class,
            short.class, short[].class,
            char.class, char[].class,
            int.class, int[].class,
            long.class, long[].class,
            float.class, float[].class,
            double.class, double[].class,
            String.class, String[].class,
            Instant.class, Instant[].class,
            PyObject.class, PyObject[].class,
            Object.class, Object[].class);

    @Override
    public boolean isVectorizableReturnType() {
        parseSignature();
        return vectorizableReturnTypes.contains(signature.getReturnType());
    }

    private final PyObject pyCallable;
    private String signatureString = null;
    private Signature signature;
    private boolean vectorizable = false;
    private boolean vectorized = false;
    private Collection<ChunkArgument> chunkArguments;
    private boolean numbaVectorized;
    private PyObject pyUdfDecorator;
    private PyObject pyUdfWrapper;
    private String argTypesStr = null;

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
        return new ArgumentsChunked(chunkArguments, signature.getReturnType(), numbaVectorized);
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

    private static PyObject getNumbaGUVectorizedFuncType() {
        try {
            return PyModule.importModule("numba.np.ufunc.gufunc").getAttribute("GUFunc");
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Numba isn't installed in the Python environment.");
            }
            return null;
        }
    }

    private void prepareSignature() {
        boolean isNumbaVectorized = pyCallable.getType().equals(NUMBA_VECTORIZED_FUNC_TYPE);
        boolean isNumbaGUVectorized = pyCallable.equals(NUMBA_GUVECTORIZED_FUNC_TYPE);
        if (isNumbaGUVectorized || isNumbaVectorized) {
            List<PyObject> params = pyCallable.getAttribute("types").asList();
            if (params.isEmpty()) {
                throw new IllegalArgumentException(
                        "numba vectorized/guvectorized function must have an explicit signature: " + pyCallable);
            }
            // numba allows a vectorized function to have multiple signatures
            if (params.size() > 1) {
                throw new UnsupportedOperationException(
                        pyCallable
                                + " has multiple signatures; this is not currently supported for numba vectorized/guvectorized functions");
            }
            numbaVectorized = isNumbaVectorized;
            vectorized = isNumbaVectorized;
        } else {
            numbaVectorized = false;
            vectorized = false;
        }
        pyUdfDecorator = dh_udf_module.call("_udf_parser", pyCallable);
        signatureString = pyUdfDecorator.getAttribute("signature").toString();
    }


    @Override
    public void parseSignature() {
        if (signatureString != null) {
            return;
        }

        prepareSignature();

        // the 'types' field of a vectorized function follows the pattern of '[ilhfdb?O]*->[ilhfdb?O]',
        // eg. [ll->d] defines two int64 (long) arguments and a double return type.
        if (signatureString == null || signatureString.isEmpty()) {
            throw new IllegalStateException("Signature should always be available.");
        }

        String pyEncodedParamsStr = signatureString.split("->")[0];
        List<Parameter> parameters = new ArrayList<>();
        if (!pyEncodedParamsStr.isEmpty()) {
            String[] pyEncodedParams = pyEncodedParamsStr.split(",");
            for (String pyEncodedParam : pyEncodedParams) {
                String[] paramDetail = pyEncodedParam.split(":");
                String paramName = paramDetail[0];
                String paramTypeCodes = paramDetail[1];
                Set<Class<?>> possibleTypes = new HashSet<>();
                for (int ti = 0; ti < paramTypeCodes.length(); ti++) {
                    char typeCode = paramTypeCodes.charAt(ti);
                    if (typeCode == '[') {
                        // skip the array type code
                        ti++;
                        possibleTypes.add(numpyType2JavaArrayClass.get(paramTypeCodes.charAt(ti)));
                    } else if (typeCode == 'N') {
                        possibleTypes.add(NULL_CLASS);
                    } else {
                        possibleTypes.add(numpyType2JavaClass.get(typeCode));
                    }
                }
                parameters.add(new Parameter(paramName, possibleTypes));
            }
        }

        Class<?> returnType = pyUdfDecorator.getAttribute("return_type", null);
        if (returnType == null) {
            throw new IllegalStateException(
                    "Python functions should always have an integral, floating point, boolean, String, arrays, or Object return type");
        }
        signature = new Signature(parameters, returnType);

    }

    private Class<?> findSafelyCastable(Set<Class<?>> types, Class<?> type) {
        for (Class<?> t : types) {
            if (t.isAssignableFrom(type)) {
                return t;
            }
            if (t.isPrimitive() && type.isPrimitive() && isLosslessWideningPrimitiveConversion(type, t)) {
                return t;
            }
        }
        return null;
    }

    public static boolean isLosslessWideningPrimitiveConversion(Class<?> original, Class<?> target) {
        if (original == null || !original.isPrimitive() || target == null || !target.isPrimitive()
                || original.equals(void.class) || target.equals(void.class)) {
            throw new IllegalArgumentException("Arguments must be a primitive type (excluding void)!");
        }

        if (original.equals(target)) {
            return true;
        }

        if (original.equals(byte.class)) {
            return target == short.class || target == int.class || target == long.class;
        } else if (original.equals(short.class) || original.equals(char.class)) { // char is unsigned, so it's a
                                                                                  // lossless conversion to int
            return target == int.class || target == long.class;
        } else if (original.equals(int.class)) {
            return target == long.class;
        } else if (original.equals(float.class)) {
            return target == double.class;
        }

        return false;
    }

    public void verifyArguments(Class<?>[] argTypes) {
        String callableName = pyCallable.getAttribute("__name__").toString();
        List<Parameter> parameters = signature.getParameters();

        StringBuilder argTypesStr = new StringBuilder();
        for (int i = 0; i < argTypes.length; i++) {
            Class<?> argType = argTypes[i];

            // if there are more arguments than parameters, we'll need to consider the last parameter as a varargs
            // parameter. This is not ideal. We should look for a better way to handle this, i.e. a way to convey that
            // the function is variadic.
            if (parameters.size() == 0) {
                throw new IllegalArgumentException(callableName + ": " + "Expected no arguments, got " + argTypes.length);
            }
            Set<Class<?>> types =
                    parameters.get(Math.min(i, parameters.size() - 1)).getPossibleTypes();


            // to prevent the unpacking of an array column when calling a Python function, we prefix the column accessor
            // with a cast to generic Object type, until we can find a way to convey that info, we'll just skip the
            // check for Object type but instead if there is only one possible type, we'll use that type.
            if (argType == Object.class) {
                if (types.size() == 1) {
                    argType = types.iterator().next();
                    if (argType.isArray())
                        argTypesStr.append('[');
                    argTypesStr.append(javaClass2NumpyType.get(argType)).append(',');
                    continue;
                }
                argTypesStr.append("O,");
                continue;
            }

            if (!types.contains(argType) && !types.contains(Object.class)) {
                Class<?> t = findSafelyCastable(types, argType);
                if (t == null) {
                    throw new IllegalArgumentException(
                            callableName + ": " + "Expected argument (" + parameters.get(i).getName()
                                    + ") to be either one of "
                                    + parameters.get(i).getPossibleTypes() + " or their compatible ones, got "
                                    + (argType.equals(NULL_CLASS) ? "null" : argType));
                }
                argType = t;
            }
            if (argType.isArray()) {
                argTypesStr.append('[');
            }
            argTypesStr.append(javaClass2NumpyType.get(argType)).append(',');
        }

        if (argTypesStr.length() > 0) {
            argTypesStr.deleteCharAt(argTypesStr.length() - 1);
        }
        this.argTypesStr = argTypesStr.toString();
    }

    // In vectorized mode, we want to call the vectorized function directly.
    public PyObject vectorizedCallable() {
        if (numbaVectorized) {
            return pyCallable;
        } else {
            return pyUdfDecorator.call("__call__", this.argTypesStr, true);
        }
    }

    // In non-vectorized mode, we want to call the udf decorated function or the original function.
    @Override
    public Object call(Object... args) {
        if (argTypesStr != null && pyUdfWrapper == null) {
            pyUdfWrapper = pyUdfDecorator.call("__call__", this.argTypesStr, false);
        }
        PyObject pyCallable = this.pyUdfWrapper != null ? this.pyUdfWrapper : this.pyCallable;
        return PythonScopeJpyImpl.convert(pyCallable.callMethod("__call__", args));
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
    public Signature getSignature() {
        return signature;
    }

}
